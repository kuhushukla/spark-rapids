#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Capture Spark's exact FilePartition ranges and footer-row predictions."""

import argparse
import json
import os

from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, LongType, StructField, StructType

MIB = 1024 * 1024
CANDIDATES = (32, 64, 128, 256, 512, 1024, 2048, 4096, 8192)
EPISODES = (
    ("train_2009", "2009-01", "2009-12"),
    ("validation_2010", "2010-01", "2010-12"),
    ("test_2011", "2011-01", "2011-12"),
)


def align64(value):
    return ((value + 63) // 64) * 64


def predicted_fixed_bytes(rows):
    return 2 * align64(8 * rows) + 2 * align64((rows + 7) // 8)


def find_file_scan(plan):
    if plan.getClass().getSimpleName() in {"FileSourceScanExec", "GpuFileSourceScanExec"}:
        return plan
    children = plan.children()
    for index in range(children.size()):
        found = find_file_scan(children.apply(index))
        if found is not None:
            return found
    return None


def partition_files(partition):
    name = partition.getClass().getSimpleName()
    if name == "FilePartition":
        return partition.files()
    if name == "DataSourceRDDPartition":
        nested = partition.inputPartitions()
        if nested.size() != 1:
            raise RuntimeError("expected one nested input partition")
        file_partition = nested.apply(0)
        if file_partition.getClass().getSimpleName() != "FilePartition":
            raise RuntimeError("nested input is not FilePartition")
        return file_partition.files()
    raise RuntimeError("unsupported partition " + name)


def month_paths(data_dir, start_month, end_month):
    return [
        os.path.join(data_dir, "yellow_tripdata_" + month)
        for month in sorted(
            name.removeprefix("yellow_tripdata_") for name in os.listdir(data_dir)
            if name.startswith("yellow_tripdata_")
            and start_month <= name.removeprefix("yellow_tripdata_") <= end_month
        )
    ]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", required=True)
    parser.add_argument("--footer-census", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if os.path.exists(args.output):
        raise FileExistsError("refusing to overwrite " + args.output)
    with open(args.footer_census, encoding="utf-8") as stream:
        census = json.load(stream)
    by_file = {}
    for record in census["records"]:
        for file_info in record["files"]:
            if len(file_info["row_groups"]) != 1:
                raise ValueError("planner census requires one row group per file")
            by_file[file_info["file"]] = {
                "file_bytes": int(file_info["bytes"]),
                "rows": int(file_info["row_groups"][0]["row_count"]),
            }
    schema = StructType([
        StructField("passenger_count", LongType(), True),
        StructField("trip_distance", DoubleType(), True),
    ])
    spark = (
        SparkSession.builder.appName("uncompressed-size-planner-census")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.files.minPartitionNum", "1")
        .config("spark.sql.files.openCostInBytes", str(4 * MIB))
        .getOrCreate()
    )
    layouts = []
    try:
        for episode, start, end in EPISODES:
            paths = month_paths(args.data_dir, start, end)
            for candidate in CANDIDATES:
                spark.conf.set("spark.sql.files.maxPartitionBytes", str(candidate * MIB))
                source = spark.read.schema(schema).parquet(*paths)
                scan = find_file_scan(source._jdf.queryExecution().executedPlan())
                if scan is None:
                    raise RuntimeError("file scan not found")
                input_rdds = scan.inputRDDs()
                if input_rdds.size() != 1:
                    raise RuntimeError("expected one scan RDD")
                tasks = []
                for partition_id, partition in enumerate(input_rdds.apply(0).partitions()):
                    ranges = []
                    assigned_bytes = 0
                    rows = 0
                    for byte_range in partition_files(partition):
                        filename = byte_range.toPath().getName()
                        start_byte = int(byte_range.start())
                        length = int(byte_range.length())
                        if filename not in by_file:
                            raise KeyError(filename)
                        if start_byte != 0 or length != by_file[filename]["file_bytes"]:
                            raise ValueError("derived files unexpectedly split: " + filename)
                        assigned_bytes += length
                        rows += by_file[filename]["rows"]
                        ranges.append({
                            "file": filename,
                            "start": start_byte,
                            "length": length,
                            "footer_rows": by_file[filename]["rows"],
                        })
                    tasks.append({
                        "partition_id": partition_id,
                        "assigned_range_bytes": assigned_bytes,
                        "footer_rows": rows,
                        "predicted_fixed_gpu_bytes": predicted_fixed_bytes(rows),
                        "ranges": ranges,
                    })
                layouts.append({
                    "episode": episode,
                    "start_month": start,
                    "end_month": end,
                    "max_partition_mib": candidate,
                    "task_count": len(tasks),
                    "tasks": tasks,
                })
    finally:
        spark.stop()
    with open(args.output, "x", encoding="utf-8") as stream:
        json.dump({"schema_version": 1, "layouts": layouts}, stream, indent=2, sort_keys=True)
        stream.write("\n")


if __name__ == "__main__":
    main()
