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

"""Run GPU scan-size shmoo treatments from a frozen schedule."""

import argparse
import hashlib
import json
import os
import time

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType


MIB = 1024 * 1024


def schema_for(query):
    fields = [
        StructField("passenger_count", LongType(), True),
        StructField("trip_distance", DoubleType(), True),
    ]
    if query == "variable_width":
        fields.append(StructField("payment_type", StringType(), True))
    elif query == "schema_evolution":
        fields.append(StructField("PULocationID", LongType(), True))
    elif query not in ("common", "filtered"):
        raise ValueError("unsupported query " + query)
    return StructType(fields)


def build_query(spark, paths, query):
    source = spark.read.schema(schema_for(query)).parquet(*paths)
    if query == "filtered":
        source = source.filter(
            (F.col("trip_distance") >= F.lit(1.0))
            & (F.col("trip_distance") < F.lit(10.0))
            & (F.col("passenger_count") > F.lit(0))
        )
    metrics = [
        F.count(F.lit(1)).alias("rows"),
        F.sum(F.coalesce(F.col("passenger_count"), F.lit(0))).alias("passengers"),
        F.sum(F.coalesce(F.col("trip_distance"), F.lit(0.0))).alias("distance"),
    ]
    if query == "variable_width":
        metrics.extend([
            F.count(F.col("payment_type")).alias("payment_non_null"),
            F.sum(F.length(F.col("payment_type"))).alias("payment_chars"),
        ])
    elif query == "schema_evolution":
        metrics.append(F.count(F.col("PULocationID")).alias("location_non_null"))
    return source.agg(*metrics)


def canonical(rows):
    values = [list(row) for row in rows]
    payload = json.dumps(values, default=str, separators=(",", ":"), sort_keys=True)
    return values, hashlib.sha256(payload.encode("utf-8")).hexdigest()


def month_paths(data_dir, start_month, end_month):
    names = sorted(
        name for name in os.listdir(data_dir)
        if name.startswith("yellow_tripdata_") and os.path.isdir(os.path.join(data_dir, name))
    )
    selected = [
        os.path.join(data_dir, name) for name in names
        if start_month <= name.removeprefix("yellow_tripdata_") <= end_month
    ]
    if not selected:
        raise ValueError("no derived months selected")
    return selected


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", required=True)
    parser.add_argument("--schedule", required=True)
    parser.add_argument("--journal", required=True)
    parser.add_argument("--event-log-dir", required=True)
    args = parser.parse_args()

    if os.path.exists(args.journal):
        raise FileExistsError("refusing to overwrite " + args.journal)
    with open(args.schedule, encoding="utf-8") as stream:
        schedule = json.load(stream)
    os.makedirs(args.event_log_dir, exist_ok=False)
    spark = (
        SparkSession.builder.appName("uncompressed-size-shmoo")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "file://" + os.path.abspath(args.event_log_dir))
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.files.minPartitionNum", "1")
        .config("spark.sql.files.openCostInBytes", str(4 * MIB))
        .config("spark.rapids.sql.metrics.level", "DEBUG")
        .config("spark.rapids.sql.concurrentGpuTasks", "1")
        .config("spark.rapids.sql.concurrentGpuTasks.dynamic", "false")
        .config("spark.rapids.sql.batchSizeBytes", str(1024 * MIB))
        .config("spark.rapids.sql.reader.batchSizeBytes", str(2048 * MIB))
        .config("spark.rapids.sql.reader.batchSizeRows", str(2147483647))
        .getOrCreate()
    )
    journal = open(args.journal, "x", encoding="utf-8")
    try:
        for item in schedule["runs"]:
            spark.conf.set(
                "spark.sql.files.maxPartitionBytes",
                str(int(item["max_partition_mib"]) * MIB),
            )
            paths = month_paths(args.data_dir, item["start_month"], item["end_month"])
            query = build_query(spark, paths, item["query"])
            plan = query._jdf.queryExecution().executedPlan().toString()
            run_id = item["run_id"]
            spark.sparkContext.setJobGroup(run_id, run_id, interruptOnCancel=True)
            started = time.monotonic_ns()
            rows = query.collect()
            elapsed_ms = (time.monotonic_ns() - started) / 1_000_000.0
            values, result_sha = canonical(rows)
            record = dict(item)
            record.update({
                "elapsed_ms": elapsed_ms,
                "gpu_scan_in_plan": "Gpu" in plan and "Scan" in plan,
                "plan": plan,
                "planned_input_partitions": query.rdd.getNumPartitions(),
                "result": values,
                "result_sha256": result_sha,
            })
            journal.write(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n")
            journal.flush()
            os.fsync(journal.fileno())
            spark.sparkContext.setLocalProperty("spark.jobGroup.id", None)
            spark.sparkContext.setLocalProperty("spark.job.description", None)
    finally:
        journal.close()
        spark.stop()


if __name__ == "__main__":
    main()
