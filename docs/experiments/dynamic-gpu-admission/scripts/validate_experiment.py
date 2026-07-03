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

"""Validate one matched dynamic/static instrumentation run and emit its verdict."""
import argparse
import hashlib
import json


REQUIRED_PLAN_NODES = {
    "GpuRange",
    "GpuProject",
    "GpuHashAggregate",
    "GpuColumnarExchange",
}


def stdout_fields(path):
    fields = {}
    with open(path, encoding="utf-8") as stream:
        for line in stream:
            for key in ("RESULT_JSON", "RESULT_SHA256", "APP_ID"):
                prefix = key + " "
                if line.startswith(prefix):
                    fields[key] = line[len(prefix):].strip()
    missing = {"RESULT_JSON", "RESULT_SHA256", "APP_ID"} - fields.keys()
    if missing:
        raise ValueError(f"{path}: missing fields {sorted(missing)}")
    actual_hash = hashlib.sha256(fields["RESULT_JSON"].encode("utf-8")).hexdigest()
    if actual_hash != fields["RESULT_SHA256"]:
        raise ValueError(f"{path}: result hash does not match RESULT_JSON")
    return fields


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--eventlog-summary", required=True)
    parser.add_argument("--dynamic-stdout", required=True)
    parser.add_argument("--static-stdout", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    with open(args.eventlog_summary, encoding="utf-8") as stream:
        eventlogs = json.load(stream)
    by_mode = {}
    for eventlog in eventlogs:
        name = eventlog["application_name"]
        if name.endswith("-dynamic"):
            by_mode["dynamic"] = eventlog
        elif name.endswith("-static"):
            by_mode["static"] = eventlog
    if set(by_mode) != {"dynamic", "static"} or len(eventlogs) != 2:
        raise ValueError("expected exactly one dynamic and one static application")

    outputs = {
        "dynamic": stdout_fields(args.dynamic_stdout),
        "static": stdout_fields(args.static_stdout),
    }
    if outputs["dynamic"]["RESULT_SHA256"] != outputs["static"]["RESULT_SHA256"]:
        raise ValueError("full canonical result hashes differ")

    for mode in ("dynamic", "static"):
        eventlog = by_mode[mode]
        if eventlog["application_id"] != outputs[mode]["APP_ID"]:
            raise ValueError(f"{mode}: stdout and event-log application IDs differ")
        missing_nodes = REQUIRED_PLAN_NODES - set(eventlog["gpu_plan_nodes"])
        if missing_nodes:
            raise ValueError(f"{mode}: missing GPU plan nodes {sorted(missing_nodes)}")
        if eventlog["failed_task_attempts_by_stage"]:
            raise ValueError(f"{mode}: failed task attempts observed")

    dynamic_max = by_mode["dynamic"]["max_concurrent_gpu_tasks_by_stage"].get("0")
    static_max = by_mode["static"]["max_concurrent_gpu_tasks_by_stage"].get("0")
    if not (dynamic_max is not None and dynamic_max > 2):
        raise ValueError(f"dynamic stage-0 maximum did not exceed initial target: {dynamic_max}")
    if static_max != 2:
        raise ValueError(f"static stage-0 observed maximum was not 2: {static_max}")

    verdict = {
        "conclusion": "SUPPORTED WITHIN VALIDITY REGION",
        "claim": (
            "With the same initial concurrentGpuTasks=2 input, enabling dynamic "
            "estimation increased the observed stage-0 admission maximum above two."
        ),
        "dynamic": {
            "application_id": by_mode["dynamic"]["application_id"],
            "stage_0_max_concurrent_gpu_tasks": dynamic_max,
        },
        "static": {
            "application_id": by_mode["static"]["application_id"],
            "stage_0_max_concurrent_gpu_tasks": static_max,
        },
        "canonical_result_sha256_both_modes": outputs["dynamic"]["RESULT_SHA256"],
        "gpu_plan_nodes_required": sorted(REQUIRED_PLAN_NODES),
        "performance_claim": None,
        "performance_note": (
            "A single unrandomized run per mode cannot establish a latency or "
            "throughput benefit."
        ),
    }
    with open(args.output, "w", encoding="utf-8") as output:
        json.dump(verdict, output, indent=2, sort_keys=True)
        output.write("\n")


if __name__ == "__main__":
    main()
