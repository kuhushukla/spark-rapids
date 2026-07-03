#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES.
# Licensed under the Apache License, Version 2.0.

"""Extract GPU admission by stage from plain Spark JSON event-log files.

The maximum includes successful task attempts only. Failed-attempt counts are
reported separately. Rolling or compressed event-log directories must first be
materialized to plain JSON files.
"""
import argparse
import json
from collections import defaultdict


def extract(path):
    maximum = defaultdict(int)
    successful = defaultdict(int)
    failed = defaultdict(int)
    app = {}
    with open(path, encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, 1):
            try:
                event = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"{path}:{line_number}: invalid JSON") from exc
            if event.get("Event") == "SparkListenerApplicationStart":
                app = {"id": event.get("App ID"), "name": event.get("App Name")}
            elif event.get("Event") == "SparkListenerTaskEnd":
                stage = event.get("Stage ID")
                task_info = event.get("Task Info", {})
                reason = event.get("Task End Reason", {}).get("Reason")
                is_success = reason == "Success" and not task_info.get("Failed", False)
                if is_success:
                    successful[stage] += 1
                else:
                    failed[stage] += 1
                for accumulator in task_info.get("Accumulables", []):
                    if accumulator.get("Name") == "gpuMaxConcurrentGpuTasks":
                        value = accumulator.get("Update")
                        if value is not None and is_success:
                            maximum[stage] = max(maximum[stage], int(value))

    def keyed(values):
        return {str(stage): value for stage, value in sorted(values.items())}

    return {
        "event_log": path,
        "application": app,
        "successful_task_attempts_by_stage": keyed(successful),
        "failed_task_attempts_by_stage": keyed(failed),
        "max_concurrent_gpu_tasks_by_stage": keyed(maximum),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("event_logs", nargs="+")
    args = parser.parse_args()
    print(json.dumps([extract(path) for path in args.event_logs], indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
