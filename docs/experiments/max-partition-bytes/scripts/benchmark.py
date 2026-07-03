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

"""Run one CPU reference or a journaled GPU maxPartitionBytes sweep."""
import argparse
import datetime
import hashlib
import json
import os
import random
import sys
import time


class Journal:
    def __init__(self, path):
        self._stream = open(path, "x", encoding="utf-8")

    def append(self, value):
        self._stream.write(json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n")
        self._stream.flush()
        os.fsync(self._stream.fileno())

    def close(self):
        self._stream.close()


def canonical(rows):
    normalized = []
    for row in rows:
        values = []
        for value in row:
            if isinstance(value, (datetime.datetime, datetime.date)):
                values.append(value.isoformat())
            else:
                values.append(value)
        normalized.append(values)
    normalized.sort(key=lambda value: json.dumps(value, sort_keys=True, default=str))
    payload = json.dumps(normalized, separators=(",", ":"), default=str)
    return normalized, hashlib.sha256(payload.encode("utf-8")).hexdigest()


def build_query(spark, path):
    from pyspark.sql import functions as F

    source = (
        spark.read.parquet(path)
        .select("PULocationID", "payment_type", "passenger_count", "trip_distance")
        .where(
            (F.col("trip_distance") >= F.lit(0.0))
            & F.col("PULocationID").isNotNull()
        )
    )
    result = source.groupBy("PULocationID", "payment_type").agg(
        F.count(F.lit(1)).alias("trip_count"),
        F.sum(F.coalesce(F.col("passenger_count"), F.lit(0)).cast("long"))
        .alias("passenger_count_sum"),
    )
    return source, result


def is_gpu_scan(plan):
    return any(name in plan for name in (
        "GpuScan parquet", "GpuFileSourceScan", "GpuBatchScan", "GpuFileGpuScan"
    ))


def execute(
        spark,
        path,
        mib,
        run_id,
        phase,
        block,
        journal,
        expected_sha=None,
        require_gpu=False):
    scope = {
        "block": block,
        "configured_mib": mib,
        "phase": phase,
        "run_id": run_id,
    }
    journal.append({
        **scope,
        "event": "run_start",
        "timestamp_ns": time.time_ns(),
    })
    job_group_set = False
    try:
        spark.conf.set("spark.sql.files.maxPartitionBytes", str(mib * 1024 * 1024))
        source, result = build_query(spark, path)
        planned_partitions = source.rdd.getNumPartitions()
        spark.sparkContext.setJobGroup(run_id, run_id, interruptOnCancel=True)
        job_group_set = True
        started_ns = time.monotonic_ns()
        rows = result.collect()
        elapsed_ms = (time.monotonic_ns() - started_ns) / 1_000_000.0

        canonical_rows, result_sha = canonical(rows)
        plan = result._jdf.queryExecution().executedPlan().toString()
        record = {
            **scope,
            "elapsed_ms": elapsed_ms,
            "event": "run_result",
            "gpu_scan_in_plan": is_gpu_scan(plan),
            "plan": plan,
            "plan_sha256": hashlib.sha256(plan.encode("utf-8")).hexdigest(),
            "planned_file_partitions": planned_partitions,
            "result_rows": len(canonical_rows),
            "result_sha256": result_sha,
            "timestamp_ns": time.time_ns(),
        }
        journal.append(record)
        if expected_sha is not None and result_sha != expected_sha:
            raise RuntimeError("result mismatch in " + run_id)
        if require_gpu and not record["gpu_scan_in_plan"]:
            raise RuntimeError("GPU scan absent in " + run_id)
        journal.append({
            **scope,
            "event": "run_terminal",
            "status": "success",
            "timestamp_ns": time.time_ns(),
        })
        return record, canonical_rows
    except BaseException as error:
        journal.append({
            **scope,
            "error_message": str(error),
            "error_type": type(error).__name__,
            "event": "run_terminal",
            "status": "error",
            "timestamp_ns": time.time_ns(),
        })
        raise
    finally:
        if job_group_set:
            spark.sparkContext.setLocalProperty("spark.jobGroup.id", None)
            spark.sparkContext.setLocalProperty("spark.job.description", None)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", required=True)
    parser.add_argument("--event-log-dir", required=True)
    parser.add_argument("--journal", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--mode", choices=("cpu", "gpu"), required=True)
    parser.add_argument("--expected-result-sha")
    parser.add_argument("--values-mib", default="8,16,32,64,128")
    parser.add_argument("--blocks", type=int, default=7)
    parser.add_argument("--seed", type=int, default=20260703)
    args = parser.parse_args()

    values = [int(value) for value in args.values_mib.split(",")]
    if len(values) != len(set(values)) or not values:
        raise ValueError("values must be a non-empty unique list")
    for path in (args.output, args.journal, args.output + ".plans.json"):
        if os.path.exists(path):
            raise FileExistsError("refusing to overwrite " + path)
    os.makedirs(args.event_log_dir, exist_ok=True)
    os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
    os.makedirs(os.path.dirname(os.path.abspath(args.journal)), exist_ok=True)

    journal = Journal(args.journal)
    spark = None
    records = []
    plans = {}
    try:
        journal.append({
            "blocks": args.blocks,
            "data_absolute_path": os.path.abspath(args.data),
            "event": "session_start",
            "mode": args.mode,
            "seed": args.seed,
            "values_mib": values,
        })
        if args.mode == "gpu" and not args.expected_result_sha:
            raise ValueError("--expected-result-sha is required in GPU mode")

        from pyspark.sql import SparkSession

        spark = (
            SparkSession.builder
            .appName("max-partition-bytes-study-" + args.mode)
            .config("spark.eventLog.enabled", "true")
            .config("spark.eventLog.dir", "file://" + os.path.abspath(args.event_log_dir))
            .config("spark.rapids.sql.enabled", "true" if args.mode == "gpu" else "false")
            .config("spark.rapids.sql.concurrentGpuTasks", "2")
            .config("spark.rapids.sql.concurrentGpuTasks.dynamic", "false")
            .config("spark.rapids.sql.format.parquet.reader.type", "COALESCING")
            .config("spark.sql.files.openCostInBytes", "1")
            .config("spark.sql.files.minPartitionNum", "1")
            .config("spark.sql.adaptive.enabled", "false")
            .config("spark.sql.shuffle.partitions", "32")
            .getOrCreate()
        )

        if args.mode == "cpu":
            record, canonical_rows = execute(
                spark, args.data, 128, "cpu-reference", "reference", -1, journal
            )
            output = {
                "canonical_rows": canonical_rows,
                "cpu_reference": {key: value for key, value in record.items() if key != "plan"},
            }
            plans[record["plan_sha256"]] = record["plan"]
        else:
            schedule = []
            rng = random.Random(args.seed)
            for block in range(args.blocks):
                order = list(values)
                rng.shuffle(order)
                schedule.append(order)
            journal.append({
                "event": "allocation",
                "measured_schedule": schedule,
                "warmup_order": values,
            })

            for mib in values:
                record, _ = execute(
                    spark, args.data, mib, "warmup-{}m".format(mib),
                    "warmup", -1, journal,
                    expected_sha=args.expected_result_sha, require_gpu=True
                )
                records.append({key: value for key, value in record.items() if key != "plan"})
                plans[record["plan_sha256"]] = record["plan"]

            for block, order in enumerate(schedule):
                for mib in order:
                    record, _ = execute(
                        spark, args.data, mib, "block-{:02d}-{}m".format(block, mib),
                        "measured", block, journal,
                        expected_sha=args.expected_result_sha, require_gpu=True
                    )
                    records.append({key: value for key, value in record.items() if key != "plan"})
                    plans[record["plan_sha256"]] = record["plan"]
            output = {
                "cpu_reference_sha256": args.expected_result_sha,
                "records": records,
                "schedule": schedule,
                "seed": args.seed,
                "values_mib": values,
            }

        with open(args.output, "x", encoding="utf-8") as stream:
            json.dump(output, stream, indent=2, sort_keys=True)
            stream.write("\n")
        with open(args.output + ".plans.json", "x", encoding="utf-8") as stream:
            json.dump(plans, stream, indent=2, sort_keys=True)
            stream.write("\n")
        journal.append({"event": "terminal", "status": "success"})
    except BaseException as error:
        journal.append({
            "error_message": str(error),
            "error_type": type(error).__name__,
            "event": "terminal",
            "status": "error",
        })
        raise
    finally:
        if spark is not None:
            spark.stop()
        journal.close()


if __name__ == "__main__":
    main()
