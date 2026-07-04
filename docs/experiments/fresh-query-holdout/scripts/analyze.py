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

"""Reproduce descriptive medians, regrets, safety, and treatment checks."""

import argparse
import json
import os
import statistics


QUERIES = [
    "q1_multikey_agg",
    "q2_selfjoin",
    "q3_window_topn",
    "q4_wide_selective_filter",
]
SIZES = [128, 512, 2048, 8192]


def load_records(root):
    records = []
    for block in (1, 2, 3):
        path = os.path.join(root, "raw", f"results_block{block}.jsonl")
        with open(path, encoding="utf-8") as stream:
            for line in stream:
                record = json.loads(line)
                if record.get("mode") == "measure" and record.get("measured"):
                    records.append(record)
    return records


def analyze(root):
    records = load_records(root)
    with open(os.path.join(root, "analysis", "eventlog-metrics.json"),
              encoding="utf-8") as stream:
        eventlogs = json.load(stream)
    cell_metrics = {}
    for cells in eventlogs.values():
        cell_metrics.update(cells)

    table = {}
    for record in records:
        key = (record["query"], record["size_mib"])
        table.setdefault(key, {})[record["block"]] = record
    if len(records) != 48 or any(len(table.get((q, s), {})) != 3
                                 for q in QUERIES for s in SIZES):
        raise ValueError("expected exactly three blocks for all 16 query-size cells")

    summary = {}
    hashes = {}
    safety_flags = []
    treatment = {}
    cell_details = {}
    for query in QUERIES:
        hashes[query] = sorted({
            record["result_sha256"] for record in records if record["query"] == query
        })
        medians = {}
        for size in SIZES:
            row = table[(query, size)]
            times = [row[block]["elapsed_s"] for block in (1, 2, 3)]
            medians[size] = statistics.median(times)
            metrics = [cell_metrics.get(row[block]["cell_id"], {}) for block in (1, 2, 3)]
            for block, metric in zip((1, 2, 3), metrics):
                gpu = metric.get("gpu", {})
                if any(gpu.get(key, 0) for key in (
                        "retry", "split_retry", "spill_host_bytes", "spill_disk_bytes")):
                    safety_flags.append({
                        "query": query, "size_mib": size, "block": block, "gpu": gpu
                    })
            cell_details[f"{query}|{size}"] = {
                "times_s": times,
                "median_s": medians[size],
                "scan_stage_task_counts_by_block": [
                    metric.get("scan_stage_task_counts") for metric in metrics
                ],
                "max_gpu_concurrent_tasks": max(
                    metric.get("gpu", {}).get("max_concurrent_gpu_tasks", 0)
                    for metric in metrics
                ),
                "max_task_footprint_bytes": max(
                    metric.get("gpu", {}).get("max_task_footprint", 0)
                    for metric in metrics
                ),
            }
        best = min(medians, key=medians.get)
        regrets = {size: medians[size] / medians[best] - 1.0 for size in SIZES}
        summary[query] = {"medians": medians, "best": best, "regret": regrets}
        treatment[query] = {
            size: cell_details[f"{query}|{size}"]["scan_stage_task_counts_by_block"][0]
            for size in SIZES
        }

    worst_regret = {
        size: max(summary[query]["regret"][size] for query in QUERIES)
        for size in SIZES
    }
    minimax = min(worst_regret, key=worst_regret.get)
    return {
        "schema_version": 2,
        "measured_runs": len(records),
        "summary": summary,
        "hashes": hashes,
        "hash_ok": all(len(values) == 1 for values in hashes.values()),
        "verdict_512_within_10pct": all(
            summary[query]["regret"][512] <= 0.10 for query in QUERIES
        ),
        "verdict_2048_within_10pct": all(
            summary[query]["regret"][2048] <= 0.10 for query in QUERIES
        ),
        "worst_regret_by_size": worst_regret,
        "minimax_size_mib": minimax,
        "safety_flags": safety_flags,
        "treatment": treatment,
        "cells": cell_details,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=os.path.dirname(os.path.abspath(__file__)))
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if os.path.exists(args.output):
        raise FileExistsError("refusing to overwrite " + args.output)
    result = analyze(os.path.abspath(args.root))
    with open(args.output, "x", encoding="utf-8") as stream:
        json.dump(result, stream, indent=2, sort_keys=True)
        stream.write("\n")


if __name__ == "__main__":
    main()
