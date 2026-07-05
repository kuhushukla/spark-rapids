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

"""Run the frozen cross-dataset scan-transfer workload."""

import argparse
import hashlib
import json
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as functions


PROJECTED_COLUMNS = (
    "PULocationID",
    "DOLocationID",
    "congestion_surcharge",
)


def canonical(rows):
    payload = [
        {key: str(value) if value is not None else None for key, value in row.asDict().items()}
        for row in rows
    ]
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return payload, hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--schedule", required=True)
    parser.add_argument("--journal", required=True)
    parser.add_argument("--mode", choices=("cpu", "gpu"), required=True)
    args = parser.parse_args()

    if os.path.exists(args.journal):
        raise FileExistsError("refusing to overwrite " + args.journal)
    with open(args.schedule, encoding="utf-8") as stream:
        schedule = json.load(stream)

    spark = SparkSession.builder.appName(
        "cross-dataset-scan-transfer-" + args.mode
    ).getOrCreate()
    journal = open(args.journal, "x", encoding="utf-8")
    try:
        for item in schedule["runs"]:
            path = item["path"]
            if not os.path.isfile(path):
                raise FileNotFoundError(path)
            frame = spark.read.parquet(path)
            projected = frame.select(*PROJECTED_COLUMNS)
            actual_types = {
                field.name: field.dataType.simpleString() for field in projected.schema.fields
            }
            expected_types = {
                "PULocationID": "int",
                "DOLocationID": "int",
                "congestion_surcharge": "double",
            }
            if actual_types != expected_types:
                raise ValueError(
                    "projected schema mismatch for {}: {}".format(path, actual_types)
                )
            query = projected.agg(
                functions.count("*").alias("row_count"),
                functions.sum("PULocationID").alias("sum_pu"),
                functions.sum("DOLocationID").alias("sum_do"),
                functions.sum("congestion_surcharge").alias("sum_congestion"),
            )
            run_id = "{}-{}".format(args.mode, item["run_id"])
            spark.sparkContext.setJobGroup(run_id, run_id, interruptOnCancel=True)
            plan = query._jdf.queryExecution().executedPlan().toString()
            started = time.monotonic_ns()
            rows = query.collect()
            elapsed_ms = (time.monotonic_ns() - started) / 1_000_000.0
            result, result_sha256 = canonical(rows)
            record = dict(item)
            record.update(
                {
                    "mode": args.mode,
                    "protocol_run_id": item["run_id"],
                    "run_id": run_id,
                    "job_group_id": run_id,
                    "input_file_bytes": os.path.getsize(path),
                    "projected_schema_json": projected.schema.json(),
                    "plan": plan,
                    "planned_input_partitions": query.rdd.getNumPartitions(),
                    "elapsed_ms": elapsed_ms,
                    "result": result,
                    "result_sha256": result_sha256,
                }
            )
            journal.write(
                json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n"
            )
            journal.flush()
            os.fsync(journal.fileno())
            spark.sparkContext.setLocalProperty("spark.jobGroup.id", None)
            spark.sparkContext.setLocalProperty("spark.job.description", None)
    finally:
        journal.close()
        spark.stop()


if __name__ == "__main__":
    main()
