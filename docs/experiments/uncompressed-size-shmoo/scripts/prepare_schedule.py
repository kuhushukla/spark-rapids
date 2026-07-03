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

"""Create the deterministic blocked shmoo schedule."""

import argparse
import json
import os
import random


CANDIDATES = (32, 64, 128, 256, 512, 1024, 2048)
EPISODES = (
    ("train_2009", "2009-01", "2009-12"),
    ("validation_2010", "2010-01", "2010-12"),
    ("test_2011", "2011-01", "2011-12"),
)
QUERIES = ("common", "filtered", "variable_width", "schema_evolution")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    parser.add_argument("--seed", type=int, default=20260703)
    parser.add_argument("--blocks", type=int, default=3)
    args = parser.parse_args()
    if os.path.exists(args.output):
        raise FileExistsError("refusing to overwrite " + args.output)

    base = [
        {
            "episode": episode,
            "start_month": start,
            "end_month": end,
            "query": query,
            "max_partition_mib": candidate,
        }
        for episode, start, end in EPISODES
        for query in QUERIES
        for candidate in CANDIDATES
    ]
    runs = []
    for episode, start, end in EPISODES:
        for query in QUERIES:
            runs.append({
                "run_id": "warmup-{}-{}-256".format(episode, query),
                "phase": "warmup",
                "block": -1,
                "repeat": 0,
                "episode": episode,
                "start_month": start,
                "end_month": end,
                "query": query,
                "max_partition_mib": 256,
            })
    rng = random.Random(args.seed)
    block_orders = []
    for block in range(args.blocks):
        treatments = list(base)
        rng.shuffle(treatments)
        block_orders.append([
            "{}:{}:{}m".format(item["episode"], item["query"], item["max_partition_mib"])
            for item in treatments
        ])
        for item in treatments:
            runs.append({
                **item,
                "run_id": "measure-b{}-{}-{}-{}m".format(
                    block, item["episode"], item["query"], item["max_partition_mib"]
                ),
                "phase": "measure",
                "block": block,
                "repeat": block,
            })
    output = {
        "schema_version": 1,
        "seed": args.seed,
        "measured_blocks": args.blocks,
        "candidate_mib": list(CANDIDATES),
        "episodes": [
            {"name": name, "start_month": start, "end_month": end}
            for name, start, end in EPISODES
        ],
        "queries": list(QUERIES),
        "blocking": "each block contains every episode/query/candidate once in seeded order",
        "block_orders": block_orders,
        "runs": runs,
    }
    os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
    with open(args.output, "x", encoding="utf-8") as stream:
        json.dump(output, stream, indent=2, sort_keys=True)
        stream.write("\n")


if __name__ == "__main__":
    main()
