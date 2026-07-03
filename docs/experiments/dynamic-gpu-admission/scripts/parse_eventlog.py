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

"""Extract auditable GPU-admission evidence from plain Spark JSON event logs.

Rolling or compressed event-log directories must be materialized to plain JSON
files before use.
"""
import argparse
import json
from collections import defaultdict


PLAN_NODES = (
    "GpuRange",
    "GpuProject",
    "GpuHashAggregate",
    "GpuColumnarExchange",
)


def parse(path):
    summary = {
        "event_log": path,
        "application_id": None,
        "application_name": None,
        "successful_task_attempts_by_stage": {},
        "failed_task_attempts_by_stage": {},
        "max_concurrent_gpu_tasks_by_stage": {},
        "admission_updates_by_stage": {},
        "gpu_plan_nodes": [],
    }
    successful = defaultdict(int)
    failed = defaultdict(int)
    per_stage = defaultdict(int)
    updates = defaultdict(list)
    plan_nodes = set()
    with open(path, encoding="utf-8") as event_log:
        for line_number, line in enumerate(event_log, 1):
            try:
                event = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"{path}:{line_number}: invalid JSON") from exc
            event_type = event.get("Event")
            if event_type == "SparkListenerApplicationStart":
                summary["application_id"] = event.get("App ID")
                summary["application_name"] = event.get("App Name")
            elif event_type == "SparkListenerTaskEnd":
                stage_id = event.get("Stage ID")
                task_info = event.get("Task Info", {})
                reason = event.get("Task End Reason", {}).get("Reason")
                is_success = reason == "Success" and not task_info.get("Failed", False)
                if is_success:
                    successful[stage_id] += 1
                else:
                    failed[stage_id] += 1
                for accumulator in task_info.get("Accumulables", []):
                    if accumulator.get("Name") == "gpuMaxConcurrentGpuTasks":
                        update = accumulator.get("Update")
                        if update is not None and is_success:
                            value = int(update)
                            per_stage[stage_id] = max(per_stage[stage_id], value)
                            updates[stage_id].append({
                                "task_id": task_info.get("Task ID"),
                                "attempt": task_info.get("Attempt"),
                                "value": value,
                            })
            plan_text = json.dumps(event.get("physicalPlanDescription", ""))
            for node in PLAN_NODES:
                if node in plan_text:
                    plan_nodes.add(node)

    def keyed(values):
        return {str(stage): value for stage, value in sorted(values.items())}

    summary["successful_task_attempts_by_stage"] = keyed(successful)
    summary["failed_task_attempts_by_stage"] = keyed(failed)
    summary["max_concurrent_gpu_tasks_by_stage"] = keyed(per_stage)
    summary["admission_updates_by_stage"] = keyed(updates)
    summary["gpu_plan_nodes"] = sorted(plan_nodes)
    return summary


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("event_logs", nargs="+")
    parser.add_argument("--output")
    args = parser.parse_args()
    result = [parse(path) for path in args.event_logs]
    rendered = json.dumps(result, indent=2, sort_keys=True)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as output:
            output.write(rendered + "\n")
    else:
        print(rendered)


if __name__ == "__main__":
    main()
