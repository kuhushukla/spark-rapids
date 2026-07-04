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

"""Extract treatment and GPU safety metrics from the holdout event logs."""

import argparse
import gzip
import json
import os
import re


GPU_ACC = {
    "gpuRetryCount": "retry",
    "gpuSplitAndRetryCount": "split_retry",
    "gpuSpillToHostBytes": "spill_host_bytes",
    "gpuSpillToDiskBytes": "spill_disk_bytes",
    "gpuMaxTaskFootprint": "max_task_footprint",
    "gpuMaxConcurrentGpuTasks": "max_concurrent_gpu_tasks",
    "gpuOnGpuTasksWaitingGPUMaxCount": "waiting_tasks_max",
}
SUM_KEYS = {"retry", "split_retry", "spill_host_bytes", "spill_disk_bytes"}


def accumulator_value(value):
    text = str(value)
    match = re.search(r"\((\d+) bytes\)$", text)
    if match:
        return int(match.group(1))
    try:
        return int(text)
    except ValueError:
        try:
            return float(text)
        except ValueError:
            return None


def open_eventlog(path):
    if path.endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8")
    return open(path, encoding="utf-8")


def parse_app(path):
    stage_to_cell = {}
    cells = {}
    with open_eventlog(path) as stream:
        for line in stream:
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            kind = event.get("Event")
            if kind == "SparkListenerJobStart":
                cell = (event.get("Properties") or {}).get("spark.jobGroup.id")
                if cell:
                    for stage_id in event.get("Stage IDs", []):
                        stage_to_cell[int(stage_id)] = cell
            elif kind == "SparkListenerTaskEnd":
                stage_id = int(event["Stage ID"])
                cell = stage_to_cell.get(stage_id)
                if cell is None:
                    continue
                current = cells.setdefault(cell, {"stages": {}, "gpu": {}})
                stage = current["stages"].setdefault(
                    stage_id, {"tasks": 0, "input_bytes": 0})
                stage["tasks"] += 1
                stage["input_bytes"] += int(
                    (event.get("Task Metrics") or {}).get(
                        "Input Metrics", {}).get("Bytes Read", 0))
                for accumulator in (event.get("Task Info") or {}).get(
                        "Accumulables", []):
                    key = GPU_ACC.get(accumulator.get("Name"))
                    if key is None or "Update" not in accumulator:
                        continue
                    value = accumulator_value(accumulator["Update"])
                    if value is None:
                        continue
                    gpu = current["gpu"]
                    if key in SUM_KEYS:
                        gpu[key] = gpu.get(key, 0) + value
                    else:
                        gpu[key] = max(gpu.get(key, 0), value)

    result = {}
    for cell, current in cells.items():
        scan_stages = {
            stage_id: stage for stage_id, stage in current["stages"].items()
            if stage["input_bytes"] > 0
        }
        result[cell] = {
            "scan_stage_task_counts": sorted(
                stage["tasks"] for stage in scan_stages.values()),
            "scan_stage_count": len(scan_stages),
            "total_scan_tasks": sum(
                stage["tasks"] for stage in scan_stages.values()),
            "total_tasks_all_stages": sum(
                stage["tasks"] for stage in current["stages"].values()),
            "gpu": current["gpu"],
        }
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--eventlog-dir", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if os.path.exists(args.output):
        raise FileExistsError("refusing to overwrite " + args.output)
    result = {}
    for name in sorted(os.listdir(args.eventlog_dir)):
        if name.endswith(".inprogress"):
            continue
        path = os.path.join(args.eventlog_dir, name)
        if os.path.isfile(path):
            app_id = name[:-3] if name.endswith(".gz") else name
            result[app_id] = parse_app(path)
    with open(args.output, "x", encoding="utf-8") as stream:
        json.dump(result, stream, indent=1, sort_keys=True)
        stream.write("\n")


if __name__ == "__main__":
    main()
