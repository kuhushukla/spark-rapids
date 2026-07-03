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

"""Freeze cumulative-table-growth treatments."""

import argparse
import json
import os
import random

CANDIDATES = (2048, 4096, 8192)
WINDOWS = (
    ("growth_1m", "common", "2009-01"),
    ("growth_3m", "common", "2009-03"),
    ("growth_6m", "common", "2009-06"),
    ("growth_12m", "common", "2009-12"),
    ("growth_24m", "common", "2010-12"),
    ("growth_36m", "common", "2011-12"),
    ("mixed_evolution_24m", "schema_evolution", "2010-12"),
    ("mixed_evolution_36m", "schema_evolution", "2011-12"),
)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    parser.add_argument("--seed", type=int, default=20260706)
    args = parser.parse_args()
    if os.path.exists(args.output):
        raise FileExistsError("refusing to overwrite " + args.output)
    base = [
        {
            "episode": episode,
            "start_month": "2009-01",
            "end_month": end,
            "query": query,
            "max_partition_mib": candidate,
        }
        for episode, query, end in WINDOWS for candidate in CANDIDATES
    ]
    runs = [
        {
            "run_id": "growth-warmup-" + episode,
            "phase": "warmup",
            "block": -1,
            "repeat": 0,
            "episode": episode,
            "start_month": "2009-01",
            "end_month": end,
            "query": query,
            "max_partition_mib": 4096,
        }
        for episode, query, end in WINDOWS
    ]
    rng = random.Random(args.seed)
    orders = []
    for block in range(3):
        treatments = list(base)
        rng.shuffle(treatments)
        orders.append([
            "{}:{}m".format(x["episode"], x["max_partition_mib"]) for x in treatments
        ])
        for item in treatments:
            runs.append({
                **item,
                "phase": "measure",
                "block": block,
                "repeat": block,
                "run_id": "growth-b{}-{}-{}m".format(
                    block, item["episode"], item["max_partition_mib"]),
            })
    result = {
        "schema_version": 1,
        "purpose": "test cumulative 1-to-36-month growth and mixed missing/present schema epochs",
        "seed": args.seed,
        "candidate_mib": list(CANDIDATES),
        "windows": [
            {"episode": episode, "query": query, "start_month": "2009-01", "end_month": end}
            for episode, query, end in WINDOWS
        ],
        "measured_blocks": 3,
        "block_orders": orders,
        "runs": runs,
    }
    os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
    with open(args.output, "x", encoding="utf-8") as stream:
        json.dump(result, stream, indent=2, sort_keys=True)
        stream.write("\n")


if __name__ == "__main__":
    main()
