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

"""Freeze the MPB x RAPIDS-target x reader-limit mediation schedule."""

import argparse
import json
import os
import random

MPB = (2048, 4096, 8192)
RAPIDS_BATCH = (512, 1024, 2048)
READER_BATCH = (1024, 2048, 4096)
QUERIES = ("common", "filtered")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    parser.add_argument("--seed", type=int, default=20260705)
    args = parser.parse_args()
    if os.path.exists(args.output):
        raise FileExistsError("refusing to overwrite " + args.output)
    base = [
        {
            "episode": "train_2009",
            "start_month": "2009-01",
            "end_month": "2009-12",
            "query": query,
            "max_partition_mib": mpb,
            "rapids_batch_mib": rapids_batch,
            "reader_batch_mib": reader_batch,
        }
        for query in QUERIES
        for mpb in MPB
        for rapids_batch in RAPIDS_BATCH
        for reader_batch in READER_BATCH
    ]
    runs = [
        {
            "run_id": "mediation-warmup-" + query,
            "phase": "warmup",
            "block": -1,
            "repeat": 0,
            "episode": "train_2009",
            "start_month": "2009-01",
            "end_month": "2009-12",
            "query": query,
            "max_partition_mib": 4096,
            "rapids_batch_mib": 1024,
            "reader_batch_mib": 2048,
        }
        for query in QUERIES
    ]
    rng = random.Random(args.seed)
    orders = []
    for block in range(3):
        treatments = list(base)
        rng.shuffle(treatments)
        orders.append([
            "{}:{}m:r{}m:read{}m".format(
                x["query"], x["max_partition_mib"], x["rapids_batch_mib"],
                x["reader_batch_mib"])
            for x in treatments
        ])
        for item in treatments:
            runs.append({
                **item,
                "phase": "measure",
                "block": block,
                "repeat": block,
                "run_id": "mediation-b{}-{}-{}m-r{}m-read{}m".format(
                    block, item["query"], item["max_partition_mib"],
                    item["rapids_batch_mib"], item["reader_batch_mib"]),
            })
    result = {
        "schema_version": 1,
        "purpose": "test whether the observed one-GiB batch cap mediates the MPB turnover",
        "seed": args.seed,
        "max_partition_mib": list(MPB),
        "rapids_batch_mib": list(RAPIDS_BATCH),
        "reader_batch_mib": list(READER_BATCH),
        "queries": list(QUERIES),
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
