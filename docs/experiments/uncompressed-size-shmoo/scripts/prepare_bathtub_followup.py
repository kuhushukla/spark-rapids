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

"""Create frozen randomized schedules for the bathtub follow-up studies."""

import argparse
import json
import os
import random

SEED = 20260704
QUERIES = ("common", "variable_width")


def write(path, runs):
    if os.path.exists(path):
        raise FileExistsError("refusing to overwrite " + path)
    with open(path, "x", encoding="utf-8") as stream:
        json.dump({"schema_version": 1, "seed": SEED, "runs": runs},
                  stream, indent=2, sort_keys=True)
        stream.write("\n")


def base(run_id, phase, query, partition_mib, study, block, repeat=0):
    return {
        "run_id": run_id,
        "phase": phase,
        "study": study,
        "block": block,
        "repeat": repeat,
        "episode": "train_2009",
        "start_month": "2009-01",
        "end_month": "2009-12",
        "query": query,
        "max_partition_mib": partition_mib,
    }


def mechanism_schedule():
    runs = [
        base(f"mechanism-warmup-{query}", "warmup", query, 2048,
             "partition_mechanisms", -1)
        for query in QUERIES
    ]
    rng = random.Random(SEED)
    candidates = (128, 512, 2048, 4096, 8192, 16384, 32768)
    for block in range(5):
        block_runs = []
        for query in QUERIES:
            for candidate in candidates:
                block_runs.append(base(
                    f"mechanism-b{block}-{query}-{candidate}",
                    "measure", query, candidate, "partition_mechanisms", block))
            block_runs.append(base(
                f"mechanism-b{block}-{query}-128-extra",
                "measure", query, 128, "default_variance", block, repeat=1))
        rng.shuffle(block_runs)
        runs.extend(block_runs)
    return runs


def batch_schedule():
    runs = []
    for query in QUERIES:
        item = base(f"batch-warmup-{query}", "warmup", query, 4096,
                    "batch_fill", -1)
        item.update({"rapids_batch_mib": 1024, "reader_batch_mib": 4096})
        runs.append(item)
    rng = random.Random(SEED + 1)
    for block in range(5):
        block_runs = []
        for query in QUERIES:
            for target in (256, 512, 1024, 2048, 4096):
                item = base(
                    f"batch-b{block}-{query}-{target}",
                    "measure", query, 4096, "batch_fill", block)
                item.update({
                    "rapids_batch_mib": target,
                    "reader_batch_mib": 4096,
                })
                block_runs.append(item)
        rng.shuffle(block_runs)
        runs.extend(block_runs)
    return runs


def layout_schedule(layout):
    runs = [
        dict(base(f"layout-{layout}-warmup", "warmup", "common", 2048,
                  "physical_layout", -1), layout=layout)
    ]
    rng = random.Random(SEED + (2 if layout == "sharded" else 3))
    for block in range(5):
        block_runs = [
            dict(base(f"layout-{layout}-b{block}-{candidate}", "measure",
                      "common", candidate, "physical_layout", block),
                 layout=layout)
            for candidate in (128, 2048, 8192)
        ]
        rng.shuffle(block_runs)
        runs.extend(block_runs)
    return runs


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mechanism-output", required=True)
    parser.add_argument("--batch-output", required=True)
    parser.add_argument("--sharded-layout-output", required=True)
    parser.add_argument("--source-layout-output", required=True)
    args = parser.parse_args()
    write(args.mechanism_output, mechanism_schedule())
    write(args.batch_output, batch_schedule())
    write(args.sharded_layout_output, layout_schedule("sharded"))
    write(args.source_layout_output, layout_schedule("source"))


if __name__ == "__main__":
    main()
