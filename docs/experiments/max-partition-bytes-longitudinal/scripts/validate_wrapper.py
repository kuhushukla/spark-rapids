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

"""Validate the finalized wrapper journal and authority budgets."""

import argparse
import hashlib
import json
import os


EXPECTED_EVENTS = [
    "wrapper_start",
    "preregistration_verified",
    "stage0_replay",
    "source_identity",
    "cpu_complete",
    "gpu_complete",
    "validation_complete",
    "replay_complete",
    "budget_check",
    "wrapper_terminal",
]


def sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--journal", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if os.path.exists(args.output):
        raise FileExistsError("refusing to overwrite " + args.output)
    with open(args.journal, encoding="utf-8") as stream:
        records = [json.loads(line) for line in stream if line.strip()]
    events = [item.get("event") for item in records]
    if events != EXPECTED_EVENTS:
        raise RuntimeError("wrapper event sequence mismatch: " + str(events))
    if any(
        item.get("status") != "success"
        for item in records[1:]
        if item.get("event") != "wrapper_start"
    ):
        raise RuntimeError("wrapper contains non-success status")
    if int(records[0].get("timeout_seconds", -1)) != 1800:
        raise RuntimeError("whole-wrapper timeout is not 1800 seconds")
    gpu = next(item for item in records if item["event"] == "gpu_complete")
    budget = next(item for item in records if item["event"] == "budget_check")
    if int(gpu["elapsed_seconds"]) > 1230:
        raise RuntimeError("GPU phase exceeds 1230 seconds")
    if int(budget["gpu_elapsed_seconds"]) != int(gpu["elapsed_seconds"]):
        raise RuntimeError("GPU duration records disagree")
    if int(budget["wrapper_elapsed_seconds"]) > 1800:
        raise RuntimeError("wrapper exceeds 1800 seconds")
    result = {
        "gpu_elapsed_seconds": int(gpu["elapsed_seconds"]),
        "gpu_hours_upper_observed": int(gpu["elapsed_seconds"]) / 3600.0,
        "journal_sha256": sha256(args.journal),
        "status": "SUPPORTED",
        "wrapper_elapsed_seconds": int(budget["wrapper_elapsed_seconds"]),
    }
    with open(args.output, "x", encoding="utf-8") as stream:
        json.dump(result, stream, indent=2, sort_keys=True)
        stream.write("\n")


if __name__ == "__main__":
    main()
