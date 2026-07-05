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

"""Validate first/middle/last rolling-window results against CPU Spark."""

import argparse
import json
import os

from pyspark.sql import SparkSession

from benchmark import build_query, canonical


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--schedule", required=True)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--gpu-results", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--limit-windows", type=int)
    args = parser.parse_args()
    if os.path.exists(args.output):
        raise FileExistsError("refusing to overwrite " + args.output)

    with open(args.schedule, encoding="utf-8") as stream:
        schedule = json.load(stream)
    dataset = next(
        item for item in schedule["datasets"]
        if item["logical_table"] == args.dataset
    )
    gpu = {}
    with open(args.gpu_results, encoding="utf-8") as stream:
        for line in stream:
            record = json.loads(line)
            if record["phase"] == "measured":
                gpu.setdefault(record["window_id"], record["result_sha256"])

    spark = SparkSession.builder.appName(
        "rolling-split-autotuning-cpu-" + args.dataset
    ).getOrCreate()
    windows = dataset["windows"]
    if args.limit_windows is not None:
        windows = windows[:args.limit_windows]
    reference_indices = sorted({0, len(windows) // 2, len(windows) - 1})
    records = []
    try:
        for index in reference_indices:
            window = windows[index]
            rows = build_query(spark, dataset, window).collect()
            payload, result_sha256 = canonical(rows)
            expected = gpu.get(window["window_id"])
            records.append({
                "window_id": window["window_id"],
                "window_index": index,
                "result": payload,
                "result_sha256": result_sha256,
                "gpu_result_sha256": expected,
                "match": result_sha256 == expected,
            })
    finally:
        spark.stop()

    output = {
        "schema_version": "rolling-split-autotuning/cpu-validation-v1",
        "dataset": args.dataset,
        "all_match": all(record["match"] for record in records),
        "records": records,
    }
    with open(args.output, "x", encoding="utf-8") as stream:
        json.dump(output, stream, indent=2, sort_keys=True)
        stream.write("\n")
    if not output["all_match"]:
        raise SystemExit("CPU/GPU result mismatch")


if __name__ == "__main__":
    main()
