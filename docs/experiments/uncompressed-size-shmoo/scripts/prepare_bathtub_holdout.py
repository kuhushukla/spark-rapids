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

"""Create the frozen 2010/2011 held-out bounded-regret schedule."""

import argparse
import json
import os
import random


def item(run_id, phase, block, episode, start, end, query, candidate):
    return {
        "run_id": run_id,
        "phase": phase,
        "study": "bounded_regret_holdout",
        "block": block,
        "repeat": 0,
        "episode": episode,
        "start_month": start,
        "end_month": end,
        "query": query,
        "max_partition_mib": candidate,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if os.path.exists(args.output):
        raise FileExistsError("refusing to overwrite " + args.output)
    episodes = (
        ("validation_2010", "2010-01", "2010-12"),
        ("test_2011", "2011-01", "2011-12"),
    )
    queries = ("common", "variable_width")
    runs = []
    for episode, start, end in episodes:
        for query in queries:
            runs.append(item(
                f"holdout-warmup-{episode}-{query}", "warmup", -1,
                episode, start, end, query, 512))
    rng = random.Random(20260705)
    for block in range(5):
        block_runs = [
            item(
                f"holdout-b{block}-{episode}-{query}-{candidate}",
                "measure", block, episode, start, end, query, candidate)
            for episode, start, end in episodes
            for query in queries
            for candidate in (128, 512, 2048, 4096)
        ]
        rng.shuffle(block_runs)
        runs.extend(block_runs)
    with open(args.output, "x", encoding="utf-8") as stream:
        json.dump({"schema_version": 1, "seed": 20260705, "runs": runs},
                  stream, indent=2, sort_keys=True)
        stream.write("\n")


if __name__ == "__main__":
    main()
