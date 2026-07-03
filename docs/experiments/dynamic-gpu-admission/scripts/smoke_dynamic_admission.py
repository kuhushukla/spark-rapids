#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES.
# Licensed under the Apache License, Version 2.0.

"""Smoke-check runtime adaptation of RAPIDS GPU task admission.

This is an instrumentation validation, not a performance benchmark.
"""
import argparse
import hashlib
import json
import os


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("dynamic", "static"))
    parser.add_argument("--event-log-dir", required=True)
    parser.add_argument("--rows", type=int, default=100_000_000)
    parser.add_argument("--partitions", type=int, default=64)
    args = parser.parse_args()

    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    os.makedirs(args.event_log_dir, exist_ok=True)
    builder = (
        SparkSession.builder
        .appName("dynamic-gpu-admission-smoke-" + args.mode)
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "file://" + os.path.abspath(args.event_log_dir))
        .config("spark.rapids.sql.enabled", "true")
        .config("spark.rapids.memory.gpu.allocFraction", "0.5")
        .config("spark.rapids.sql.concurrentGpuTasks", "2")
        .config(
            "spark.rapids.sql.concurrentGpuTasks.dynamic",
            "true" if args.mode == "dynamic" else "false",
        )
        .config("spark.sql.shuffle.partitions", str(args.partitions))
    )

    spark = builder.getOrCreate()
    try:
        df = (
            spark.range(0, args.rows, 1, args.partitions)
            .select(
                (F.col("id") % 997).alias("k"),
                (F.col("id") * 3).alias("v"),
            )
        )
        result = (
            df.groupBy("k")
            .agg(F.sum("v").alias("s"), F.count("v").alias("c"))
            .orderBy(F.desc("s"), F.asc("k"))
            .limit(10)
            .collect()
        )
        canonical = json.dumps(
            [row.asDict(recursive=True) for row in result],
            sort_keys=True,
            separators=(",", ":"),
        )
        print("RESULT_JSON", canonical)
        print("RESULT_SHA256", hashlib.sha256(canonical.encode("utf-8")).hexdigest())
        print("APP_ID", spark.sparkContext.applicationId)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
