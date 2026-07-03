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

"""Validate frozen schedule order and Spark application/task lifecycle."""

import argparse
import gzip
import hashlib
import json
import os
import re


def sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def open_log(path):
    return gzip.open(path, "rt", encoding="utf-8") if path.endswith(".gz") else open(
        path, encoding="utf-8"
    )


def accum_long(value):
    text = str(value)
    matched = re.search(r"\((\d+) bytes\)$", text)
    return int(matched.group(1)) if matched else int(text)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--schedule", required=True)
    parser.add_argument("--journal", required=True)
    parser.add_argument("--event-log", required=True)
    parser.add_argument("--require-gpu-scan", action="store_true")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if os.path.exists(args.output):
        raise FileExistsError("refusing to overwrite " + args.output)
    with open(args.schedule, encoding="utf-8") as stream:
        schedule = json.load(stream)
    with open(args.journal, encoding="utf-8") as stream:
        journal = [json.loads(line) for line in stream]
    expected = schedule["runs"]
    if [item["run_id"] for item in journal] != [item["run_id"] for item in expected]:
        raise ValueError("journal run order differs from frozen schedule")
    for planned, actual in zip(expected, journal):
        for key, value in planned.items():
            if actual.get(key) != value:
                raise ValueError("{} differs for {}".format(key, planned["run_id"]))
        if args.require_gpu_scan and not actual.get("gpu_scan_in_plan"):
            raise ValueError("GPU scan absent in " + actual["run_id"])

    task_ends = 0
    failed_tasks = []
    executor_removed = 0
    application_start = 0
    application_end = 0
    log_start = 0
    spark_version = None
    environment_update = 0
    jvm_information = {}
    sql_descriptions = []
    gpu_retry = 0
    gpu_split_retry = 0
    gpu_spill_bytes = 0
    spark_spill_bytes = 0
    environment = {}
    with open_log(args.event_log) as stream:
        for line in stream:
            event = json.loads(line)
            kind = event.get("Event")
            if kind == "SparkListenerLogStart":
                log_start += 1
                spark_version = event.get("Spark Version")
            elif kind == "SparkListenerApplicationStart":
                application_start += 1
            elif kind == "SparkListenerApplicationEnd":
                application_end += 1
            elif kind == "SparkListenerExecutorRemoved":
                executor_removed += 1
            elif kind == "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart":
                sql_descriptions.append(event["description"])
            elif kind == "SparkListenerEnvironmentUpdate":
                environment_update += 1
                # Spark 3.x writes these sections directly on the event. Retain the
                # nested fallback for older/normalized event-log representations.
                environment = event.get("Environment Details", event)
                jvm_information = environment.get("JVM Information", {})
            elif kind == "SparkListenerTaskEnd":
                task_ends += 1
                reason = event.get("Task End Reason", {}).get("Reason")
                if reason != "Success":
                    failed_tasks.append({
                        "stage_id": event.get("Stage ID"),
                        "task_id": event.get("Task Info", {}).get("Task ID"),
                        "reason": event.get("Task End Reason"),
                    })
                metrics = event.get("Task Metrics", {})
                spark_spill_bytes += int(metrics.get("Memory Bytes Spilled", 0))
                spark_spill_bytes += int(metrics.get("Disk Bytes Spilled", 0))
                for accum in event.get("Task Info", {}).get("Accumulables", []):
                    if "Update" not in accum:
                        continue
                    name = accum.get("Name")
                    if name == "gpuRetryCount":
                        gpu_retry += accum_long(accum["Update"])
                    elif name == "gpuSplitAndRetryCount":
                        gpu_split_retry += accum_long(accum["Update"])
                    elif name in ("gpuSpillToHostBytes", "gpuSpillToDiskBytes"):
                        gpu_spill_bytes += accum_long(accum["Update"])

    expected_descriptions = [item["run_id"] for item in expected]
    if sql_descriptions != expected_descriptions:
        raise ValueError("SQL execution descriptions/order differ from schedule")
    if application_start != 1 or application_end != 1:
        raise ValueError("application lifecycle is incomplete")
    if executor_removed or failed_tasks:
        raise ValueError("executor removal or failed task observed")

    raw_properties = environment.get("Spark Properties", {})
    spark_properties = (dict(raw_properties) if isinstance(raw_properties, dict)
                        else dict(raw_properties))
    expected_properties = {
        "spark.app.name": "uncompressed-size-shmoo",
        "spark.master": "local[8]",
        "spark.rapids.sql.batchSizeBytes": "1073741824",
        "spark.rapids.sql.reader.batchSizeBytes": "2147483648",
        "spark.rapids.sql.reader.batchSizeRows": "2147483647",
        "spark.rapids.sql.concurrentGpuTasks": "1",
        "spark.rapids.sql.concurrentGpuTasks.dynamic": "false",
        "spark.sql.files.minPartitionNum": "1",
        "spark.sql.files.openCostInBytes": "4194304",
    }
    mismatched_properties = {
        key: {"expected": value, "actual": spark_properties.get(key)}
        for key, value in expected_properties.items()
        if spark_properties.get(key) != value
    }
    expected_plugin = "com.nvidia.spark.SQLPlugin" if args.require_gpu_scan else None
    if spark_properties.get("spark.plugins") != expected_plugin:
        mismatched_properties["spark.plugins"] = {
            "expected": expected_plugin,
            "actual": spark_properties.get("spark.plugins"),
        }
    if mismatched_properties:
        raise ValueError("critical Spark properties differ: " +
                         json.dumps(mismatched_properties, sort_keys=True))
    if log_start != 1 or spark_version != "3.5.5":
        raise ValueError("unexpected or missing Spark log-start/version event")
    if environment_update != 1 or not jvm_information.get("Java Version"):
        raise ValueError("missing Spark environment or JVM information")

    result = {
        "schema_version": 1,
        "schedule_sha256": sha256(args.schedule),
        "journal_sha256": sha256(args.journal),
        "event_log_sha256": sha256(args.event_log),
        "scheduled_runs": len(expected),
        "journal_runs": len(journal),
        "sql_executions": len(sql_descriptions),
        "task_ends": task_ends,
        "failed_tasks": len(failed_tasks),
        "executor_removed": executor_removed,
        "application_start": application_start,
        "application_end": application_end,
        "log_start": log_start,
        "spark_version": spark_version,
        "environment_update": environment_update,
        "jvm_information": jvm_information,
        "gpu_retry_count_all_tasks": gpu_retry,
        "gpu_split_retry_count_all_tasks": gpu_split_retry,
        "gpu_spill_bytes_all_tasks": gpu_spill_bytes,
        "spark_spill_bytes_all_tasks": spark_spill_bytes,
        "spark_properties": {
            key: spark_properties.get(key) for key in (
                "spark.app.name",
                "spark.master",
                "spark.plugins",
                "spark.rapids.sql.batchSizeBytes",
                "spark.rapids.sql.reader.batchSizeBytes",
                "spark.rapids.sql.reader.batchSizeRows",
                "spark.rapids.sql.concurrentGpuTasks",
                "spark.rapids.sql.concurrentGpuTasks.dynamic",
                "spark.sql.files.minPartitionNum",
                "spark.sql.files.openCostInBytes",
            )
        },
    }
    with open(args.output, "x", encoding="utf-8") as stream:
        json.dump(result, stream, indent=2, sort_keys=True)
        stream.write("\n")


if __name__ == "__main__":
    main()
