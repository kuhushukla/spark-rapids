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

"""Freeze the sequential high-size extension after the primary knee was unresolved."""

import argparse
import json
import os
import random

CANDIDATES = (2048, 4096, 8192)
EPISODES = (
    ("train_2009", "2009-01", "2009-12"),
    ("validation_2010", "2010-01", "2010-12"),
    ("test_2011", "2011-01", "2011-12"),
)
QUERIES = ("common", "filtered", "variable_width", "schema_evolution")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    parser.add_argument("--seed", type=int, default=20260704)
    args = parser.parse_args()
    if os.path.exists(args.output):
        raise FileExistsError("refusing to overwrite " + args.output)
    base = [
        {"episode": episode, "start_month": start, "end_month": end,
         "query": query, "max_partition_mib": candidate}
        for episode, start, end in EPISODES
        for query in QUERIES for candidate in CANDIDATES
    ]
    runs = []
    for episode, start, end in EPISODES:
        for query in QUERIES:
            runs.append({
                "run_id": "extension-warmup-{}-{}-4096".format(episode, query),
                "phase": "warmup", "block": -1, "repeat": 0, "episode": episode,
                "start_month": start, "end_month": end, "query": query,
                "max_partition_mib": 4096,
            })
    rng = random.Random(args.seed)
    orders = []
    for block in range(3):
        treatments = list(base)
        rng.shuffle(treatments)
        orders.append([
            "{}:{}:{}m".format(x["episode"], x["query"], x["max_partition_mib"])
            for x in treatments
        ])
        for item in treatments:
            runs.append({
                **item, "phase": "measure", "block": block, "repeat": block,
                "run_id": "extension-b{}-{}-{}-{}m".format(
                    block, item["episode"], item["query"], item["max_partition_mib"])
            })
    result = {
        "schema_version": 1,
        "purpose": "sequential extension because every primary curve was still improving at 2048 MiB",
        "seed": args.seed,
        "candidate_mib": list(CANDIDATES),
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
