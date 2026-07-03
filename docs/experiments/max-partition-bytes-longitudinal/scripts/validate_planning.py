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

"""Compare the simulator with Spark's exact materialized file/range/task layout."""

import argparse
import hashlib
import json
import os


def stable_hash(value):
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def file_sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def schema_for(query_name):
    from pyspark.sql.types import (
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
    )
    fields = [
        StructField("passenger_count", LongType(), True),
        StructField("trip_distance", DoubleType(), True),
    ]
    if query_name == "variable_width":
        fields.append(StructField("payment_type", StringType(), True))
    elif query_name == "missing_location":
        fields.append(StructField("PULocationID", LongType(), True))
    elif query_name != "common":
        raise ValueError("unsupported query " + query_name)
    return StructType(fields)


def find_file_scan(plan):
    if plan.getClass().getSimpleName() in {
        "FileSourceScanExec",
        "GpuFileSourceScanExec",
    }:
        return plan
    children = plan.children()
    for index in range(children.size()):
        found = find_file_scan(children.apply(index))
        if found is not None:
            return found
    return None


def exact_layout(source, file_metadata):
    plan = source._jdf.queryExecution().executedPlan()
    scan = find_file_scan(plan)
    if scan is None:
        raise RuntimeError("FileSourceScanExec not found under " + plan.nodeName())
    input_rdds = scan.inputRDDs()
    if input_rdds.size() != 1:
        raise RuntimeError("expected exactly one FileSourceScanExec input RDD")
    input_rdd = input_rdds.apply(0)

    by_file = {item["file"]: item for item in file_metadata}
    physical_layout = []
    useful_groups = []
    empty_tasks = 0
    empty_ranges = 0
    planned_ranges = 0
    for partition in input_rdd.partitions():
        task_ranges = []
        task_owned = []
        partition_class = partition.getClass().getSimpleName()
        if partition_class == "FilePartition":
            partition_files = partition.files()
        elif partition_class == "DataSourceRDDPartition":
            input_partitions = partition.inputPartitions()
            if input_partitions.size() != 1:
                raise RuntimeError(
                    "expected one InputPartition per DataSourceRDDPartition"
                )
            file_partition = input_partitions.apply(0)
            if file_partition.getClass().getSimpleName() != "FilePartition":
                raise RuntimeError(
                    "expected nested FilePartition, found "
                    + file_partition.getClass().getSimpleName()
                )
            partition_files = file_partition.files()
        else:
            raise RuntimeError("unsupported scan partition " + partition_class)
        for byte_range in partition_files:
            planned_ranges += 1
            filename = byte_range.toPath().getName()
            start = int(byte_range.start())
            length = int(byte_range.length())
            end = start + length
            range_owned = []
            for row_group in by_file[filename]["row_groups"]:
                midpoint = row_group["midpoint"]
                if midpoint is not None and start <= midpoint < end:
                    range_owned.append((filename, row_group["index"]))
            range_owned = sorted(set(range_owned))
            if not range_owned:
                empty_ranges += 1
            task_owned.extend(range_owned)
            task_ranges.append({
                "file": filename,
                "length": length,
                "row_groups": range_owned,
                "start": start,
            })
        task_owned = sorted(set(task_owned))
        if not task_owned:
            empty_tasks += 1
        else:
            useful_groups.append(task_owned)
        physical_layout.append(task_ranges)

    useful_signature = sorted(
        useful_groups,
        key=lambda groups: json.dumps(groups, separators=(",", ":")),
    )
    return {
        "empty_ranges": empty_ranges,
        "empty_tasks": empty_tasks,
        "physical_layout_sha256": stable_hash(physical_layout),
        "planned_ranges": planned_ranges,
        "planned_tasks": len(physical_layout),
        "useful_layout_sha256": stable_hash(useful_signature),
        "useful_tasks": len(useful_groups),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--census", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--require-gpu-scan", action="store_true")
    args = parser.parse_args()
    if os.path.exists(args.output):
        raise FileExistsError("refusing to overwrite " + args.output)
    with open(args.census, encoding="utf-8") as stream:
        census = json.load(stream)

    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder
        .appName("longitudinal-planning-compliance")
        .config("spark.sql.files.openCostInBytes", str(census["open_cost_bytes"]))
        .config("spark.sql.files.minPartitionNum", str(census["min_partitions"]))
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.caseSensitive", "false")
        .getOrCreate()
    )
    records = []
    try:
        if spark.conf.get("spark.sql.files.maxPartitionNum", None) is not None:
            raise RuntimeError("maxPartitionNum must remain unset")
        for episode_name, episode in sorted(census["episodes"].items()):
            paths = [
                os.path.join(census["data_dir"], name) for name in episode["files"]
            ]
            episode_files = [
                item for item in census["files"] if item["file"] in set(episode["files"])
            ]
            for prediction in episode["candidate_layouts"]:
                configured_mib = prediction["configured_mib"]
                spark.conf.set(
                    "spark.sql.files.maxPartitionBytes",
                    str(configured_mib * 1024 * 1024),
                )
                source = spark.read.schema(schema_for(episode["query"])).parquet(*paths)
                executed_plan = source._jdf.queryExecution().executedPlan()
                scan = find_file_scan(executed_plan)
                if scan is None:
                    raise RuntimeError(
                        "file scan not found under " + executed_plan.nodeName()
                    )
                scan_class = scan.getClass().getSimpleName()
                expected_scan_class = (
                    "GpuFileSourceScanExec"
                    if args.require_gpu_scan
                    else "FileSourceScanExec"
                )
                if scan_class != expected_scan_class:
                    raise RuntimeError(
                        "required {}, found {}".format(
                            expected_scan_class, scan_class
                        )
                    )
                actual = exact_layout(source, episode_files)
                expected = {
                    key: prediction[key]
                    for key in (
                        "empty_ranges",
                        "empty_tasks",
                        "physical_layout_sha256",
                        "planned_ranges",
                        "planned_tasks",
                        "useful_layout_sha256",
                        "useful_tasks",
                    )
                }
                comparisons = {
                    key: actual[key] == expected[key] for key in expected
                }
                plan = source._jdf.queryExecution().executedPlan().toString()
                record = {
                    "actual": actual,
                    "comparisons": comparisons,
                    "configured_mib": configured_mib,
                    "episode": episode_name,
                    "expected": expected,
                    "match": all(comparisons.values()),
                    "plan_sha256": hashlib.sha256(
                        plan.encode("utf-8")
                    ).hexdigest(),
                    "query": episode["query"],
                    "scan_class": scan_class,
                }
                records.append(record)
                if not record["match"]:
                    failed = sorted(
                        key for key, matched in comparisons.items() if not matched
                    )
                    raise RuntimeError(
                        "exact planning mismatch for {} {} MiB: {}".format(
                            episode_name, configured_mib, failed
                        )
                    )
    finally:
        spark.stop()

    result = {
        "all_match": all(item["match"] for item in records),
        "census_sha256": file_sha256(args.census),
        "record_count": len(records),
        "required_gpu_scan": args.require_gpu_scan,
        "scan_classes": sorted(set(item["scan_class"] for item in records)),
        "records": records,
        "validated_fields": [
            "planned_tasks",
            "planned_ranges",
            "useful_tasks",
            "empty_tasks",
            "empty_ranges",
            "physical_layout_sha256",
            "useful_layout_sha256",
        ],
    }
    os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
    with open(args.output, "x", encoding="utf-8") as stream:
        json.dump(result, stream, indent=2, sort_keys=True)
        stream.write("\n")


if __name__ == "__main__":
    main()
