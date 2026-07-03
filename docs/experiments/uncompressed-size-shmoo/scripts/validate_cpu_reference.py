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

"""Compare frozen CPU reference hashes with all primary GPU treatments."""

import argparse
import json
import os
from collections import defaultdict


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cpu-journal", required=True)
    parser.add_argument("--gpu-metrics", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if os.path.exists(args.output):
        raise FileExistsError("refusing to overwrite " + args.output)
    with open(args.cpu_journal, encoding="utf-8") as stream:
        cpu = [json.loads(line) for line in stream]
    with open(args.gpu_metrics, encoding="utf-8") as stream:
        gpu = json.load(stream)["runs"]
    cpu_by_key = {(item["episode"], item["query"]): item["result_sha256"] for item in cpu}
    gpu_by_key = defaultdict(set)
    for item in gpu:
        if item["phase"] == "measure":
            gpu_by_key[(item["episode"], item["query"])].add(item["result_sha256"])
    if len(cpu_by_key) != 12 or len(gpu_by_key) != 12:
        raise ValueError("expected twelve CPU/GPU episode-query groups")
    comparisons = []
    for key in sorted(cpu_by_key):
        hashes = gpu_by_key[key]
        if len(hashes) != 1:
            raise ValueError("GPU hash instability for " + str(key))
        gpu_hash = next(iter(hashes))
        matches = cpu_by_key[key] == gpu_hash
        comparisons.append({
            "episode": key[0],
            "query": key[1],
            "cpu_sha256": cpu_by_key[key],
            "gpu_sha256": gpu_hash,
            "matches": matches,
        })
    if not all(item["matches"] for item in comparisons):
        raise ValueError("CPU/GPU result mismatch")
    with open(args.output, "x", encoding="utf-8") as stream:
        json.dump({
            "schema_version": 1,
            "all_cpu_gpu_results_match": True,
            "comparison_count": len(comparisons),
            "comparisons": comparisons,
        }, stream, indent=2, sort_keys=True)
        stream.write("\n")


if __name__ == "__main__":
    main()
