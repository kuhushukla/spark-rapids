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

"""Extract per-task GPU scan boundary metrics from a Spark event log."""

import argparse
import gzip
import json
import os
import re
import statistics


METRIC_NAMES = {
    "sum of output GPU batch bytes": "output_batch_bytes",
    "maximum output GPU batch bytes per task": "max_output_batch_bytes",
    "maximum output GPU batch rows per task": "max_output_batch_rows",
    "output rows": "output_rows",
    "output columnar batches": "output_batches",
    "GPU decode time": "gpu_decode_ns",
    "scan time": "scan_ns",
    "size of read buffer": "read_buffer_bytes",
}


def open_log(path):
    return gzip.open(path, "rt", encoding="utf-8") if path.endswith(".gz") else open(
        path, encoding="utf-8"
    )


def scan_metrics(plan):
    scans = []
    filters = []
    def visit(node):
        name = node.get("nodeName", "")
        if name.startswith("GpuScan"):
            scans.append({
                int(metric["accumulatorId"]): METRIC_NAMES[metric["name"]]
                for metric in node.get("metrics", [])
                if metric.get("name") in METRIC_NAMES
            })
        elif name.startswith("GpuFilter"):
            filters.append({
                int(metric["accumulatorId"]): "filter_" + METRIC_NAMES[metric["name"]]
                for metric in node.get("metrics", [])
                if metric.get("name") in METRIC_NAMES
            })
        for child in node.get("children", []):
            visit(child)
    visit(plan)
    if len(scans) != 1:
        raise ValueError("expected exactly one GPU scan node, found {}".format(len(scans)))
    if len(filters) > 1:
        raise ValueError("expected at most one GPU filter node")
    missing = set(METRIC_NAMES.values()) - set(scans[0].values())
    if missing:
        raise ValueError("GPU scan lacks required metrics: " + ",".join(sorted(missing)))
    result = dict(scans[0])
    if filters:
        result.update(filters[0])
    return result


def quantiles(values):
    ordered = sorted(values)
    if not ordered:
        return {"count": 0}
    def at(fraction):
        return ordered[round(fraction * (len(ordered) - 1))]
    return {
        "count": len(ordered),
        "min": ordered[0],
        "p50": statistics.median(ordered),
        "p90": at(0.90),
        "p95": at(0.95),
        "max": ordered[-1],
        "sum": sum(ordered),
    }


def accumulator_value(value):
    text = str(value)
    matched = re.search(r"\((\d+) bytes\)$", text)
    if matched:
        return int(matched.group(1))
    clock = re.fullmatch(r"(\d+):(\d+):(\d+(?:\.\d+)?)", text)
    if clock:
        hours, minutes, seconds = clock.groups()
        return round(
            (int(hours) * 3600 + int(minutes) * 60 + float(seconds)) * 1_000_000_000
        )
    try:
        return int(text)
    except ValueError:
        return float(text)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--event-log", required=True)
    parser.add_argument("--journal", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if os.path.exists(args.output):
        raise FileExistsError("refusing to overwrite " + args.output)

    executions = {}
    stage_to_run = {}
    stage_planned_tasks = {}
    task_events = {}
    with open_log(args.event_log) as stream:
        for line in stream:
            event = json.loads(line)
            kind = event.get("Event")
            if kind == "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart":
                run_id = event["description"]
                executions[run_id] = {
                    "execution_id": int(event["executionId"]),
                    "metric_ids": scan_metrics(event["sparkPlanInfo"]),
                }
            elif kind == "SparkListenerJobStart":
                run_id = event.get("Properties", {}).get("spark.jobGroup.id")
                if run_id:
                    for stage_id in event.get("Stage IDs", []):
                        stage_to_run[int(stage_id)] = run_id
            elif kind == "SparkListenerStageSubmitted":
                stage_info = event["Stage Info"]
                stage_planned_tasks[int(stage_info["Stage ID"])] = int(
                    stage_info["Number of Tasks"]
                )
            elif kind == "SparkListenerTaskEnd":
                stage_id = int(event["Stage ID"])
                run_id = stage_to_run.get(stage_id)
                if not run_id or run_id not in executions:
                    continue
                metric_ids = executions[run_id]["metric_ids"]
                values = {}
                task_gpu_names = {
                    "gpuTime": "gpu_semaphore_holding_ns",
                    "gpuSemaphoreWait": "gpu_semaphore_wait_ns",
                    "gpuRetryCount": "gpu_retry_count",
                    "gpuSplitAndRetryCount": "gpu_split_retry_count",
                    "gpuRetryBlockTime": "gpu_retry_block_ns",
                    "gpuRetryComputationTime": "gpu_retry_computation_ns",
                    "gpuSpillToHostBytes": "gpu_spill_host_bytes",
                    "gpuSpillToDiskBytes": "gpu_spill_disk_bytes",
                    "gpuSpillToHostTime": "gpu_spill_host_ns",
                    "gpuSpillToDiskTime": "gpu_spill_disk_ns",
                    "gpuReadSpillFromHostTime": "gpu_read_spill_host_ns",
                    "gpuReadSpillFromDiskTime": "gpu_read_spill_disk_ns",
                    "gpuMaxDeviceMemoryBytes": "gpu_max_device_memory_bytes",
                    "gpuMaxTaskFootprint": "gpu_max_task_footprint",
                    "gpuOnGpuTasksWaitingGPUAvgCount": "gpu_waiting_tasks_avg",
                    "gpuOnGpuTasksWaitingGPUMaxCount": "gpu_waiting_tasks_max",
                    "gpuMaxConcurrentGpuTasks": "gpu_max_concurrent_tasks",
                    "multithreadReaderMaxParallelism": "multithread_reader_max_parallelism",
                }
                for accum in event["Task Info"].get("Accumulables", []):
                    metric_name = metric_ids.get(int(accum["ID"]))
                    if metric_name is None:
                        metric_name = task_gpu_names.get(accum.get("Name"))
                    if metric_name is not None and "Update" in accum:
                        values[metric_name] = accumulator_value(accum["Update"])
                has_scan_metric = "output_batch_bytes" in values
                info = event["Task Info"]
                standard = event.get("Task Metrics", {})
                input_metrics = standard.get("Input Metrics", {})
                values.update({
                    "duration_ms": int(info["Finish Time"]) - int(info["Launch Time"]),
                    "launch_time_ms": int(info["Launch Time"]),
                    "finish_time_ms": int(info["Finish Time"]),
                    "input_bytes": int(input_metrics.get("Bytes Read", 0)),
                    "input_records": int(input_metrics.get("Records Read", 0)),
                    "spark_disk_spill_bytes": int(standard.get("Disk Bytes Spilled", 0)),
                    "spark_memory_spill_bytes": int(standard.get("Memory Bytes Spilled", 0)),
                    "spark_peak_execution_memory": int(standard.get("Peak Execution Memory", 0)),
                    "partition_id": int(info["Partition ID"]),
                    "stage_id": stage_id,
                    "task_id": int(info["Task ID"]),
                    "_has_scan_metric": has_scan_metric,
                })
                task_events.setdefault(run_id, []).append(values)

    tasks = {}
    output_tasks = {}
    empty_tasks = {}
    for run_id, rows in task_events.items():
        scan_stage_ids = {
            row["stage_id"] for row in rows if row["_has_scan_metric"]
        }
        if len(scan_stage_ids) != 1:
            raise ValueError(
                "expected one metric-bearing scan stage for {}, found {}".format(
                    run_id, sorted(scan_stage_ids)
                )
            )
        scan_stage_id = next(iter(scan_stage_ids))
        scan_rows = []
        for row in rows:
            if row["stage_id"] != scan_stage_id:
                continue
            row.pop("_has_scan_metric")
            for name in executions[run_id]["metric_ids"].values():
                row.setdefault(name, 0)
            scan_rows.append(row)
        tasks[run_id] = scan_rows
        output_tasks[run_id] = [
            row for row in scan_rows if row.get("output_batch_bytes", 0) > 0
        ]
        empty_tasks[run_id] = [
            row for row in scan_rows if row.get("output_batch_bytes", 0) == 0
        ]

    with open(args.journal, encoding="utf-8") as stream:
        journal = {
            item["run_id"]: item for item in (json.loads(line) for line in stream)
        }
    output = {
        "schema_version": 3,
        "accumulator_duration_resolution_ns": 1_000_000,
        "runs": [],
    }
    for run_id, record in journal.items():
        task_rows = tasks.get(run_id, [])
        if not task_rows:
            raise ValueError("no GPU scan-stage tasks for " + run_id)
        output_task_rows = output_tasks.get(run_id, [])
        empty_task_rows = empty_tasks.get(run_id, [])
        if not output_task_rows:
            raise ValueError("no output-producing GPU scan tasks for " + run_id)
        metric_keys = sorted({
            key for row in task_rows for key in row
            if key not in {
                "partition_id", "stage_id", "task_id", "launch_time_ms", "finish_time_ms"
            }
        })
        output["runs"].append({
            "run_id": run_id,
            "phase": record["phase"],
            "block": record.get("block"),
            "repeat": record.get("repeat"),
            "episode": record.get("episode"),
            "start_month": record.get("start_month"),
            "end_month": record.get("end_month"),
            "query": record["query"],
            "max_partition_mib": record["max_partition_mib"],
            "rapids_batch_mib": record.get("rapids_batch_mib"),
            "reader_batch_mib": record.get("reader_batch_mib"),
            "elapsed_ms": record["elapsed_ms"],
            "result_sha256": record["result_sha256"],
            # benchmark.py recorded the final result RDD partition count, not scan planning.
            "journal_result_rdd_partitions": record.get("planned_input_partitions"),
            "planned_scan_stage_tasks": stage_planned_tasks[task_rows[0]["stage_id"]],
            "scan_task_count": len(task_rows),
            "output_producing_scan_task_count": len(output_task_rows),
            "empty_scan_task_count": len(task_rows) - len(output_task_rows),
            "scan_task_span_ms": (
                max(row["finish_time_ms"] for row in task_rows)
                - min(row["launch_time_ms"] for row in task_rows)
            ),
            "task_metrics": {
                key: quantiles([row.get(key, 0) for row in task_rows])
                for key in metric_keys
            },
            "output_task_metrics": {
                key: quantiles([row.get(key, 0) for row in output_task_rows])
                for key in metric_keys
            },
            "empty_task_metrics": {
                key: quantiles([row.get(key, 0) for row in empty_task_rows])
                for key in metric_keys
            },
            "tasks": sorted(task_rows, key=lambda row: (row["stage_id"], row["partition_id"])),
        })
        for key in (
            "study", "layout", "concurrent_gpu_tasks", "dynamic_concurrency", "data_dir"
        ):
            if key in record:
                output["runs"][-1][key] = record[key]
    os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
    with open(args.output, "x", encoding="utf-8") as stream:
        json.dump(output, stream, indent=2, sort_keys=True)
        stream.write("\n")


if __name__ == "__main__":
    main()
