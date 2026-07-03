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

"""Validate the preregistered high-size sequential extension."""

import argparse
import json
import os
import statistics
from collections import defaultdict

CANDIDATES = (2048, 4096, 8192)
EPISODES = ("train_2009", "validation_2010", "test_2011")
QUERIES = ("common", "filtered", "variable_width", "schema_evolution")


def pct(values, fraction):
    ordered = sorted(values)
    return ordered[round(fraction * (len(ordered) - 1))]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--metrics", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--markdown", required=True)
    args = parser.parse_args()
    if os.path.exists(args.output) or os.path.exists(args.markdown):
        raise FileExistsError("refusing to overwrite output")
    with open(args.metrics, encoding="utf-8") as stream:
        runs = json.load(stream)["runs"]
    measured = [run for run in runs if run["phase"] == "measure"]
    if len(runs) != 120 or len(measured) != 108:
        raise ValueError("expected 120 total and 108 measured runs")
    groups = defaultdict(list)
    hashes = defaultdict(set)
    for run in measured:
        key = (run["episode"], run["query"])
        hashes[key].add(run["result_sha256"])
        groups[(run["episode"], run["query"], int(run["max_partition_mib"]))].append(run)
    if any(len(value) != 1 for value in hashes.values()):
        raise ValueError("result hashes differ across treatments")
    if len(groups) != 36 or any(len(value) != 3 for value in groups.values()):
        raise ValueError("incomplete blocked cells")

    cells = []
    for episode in EPISODES:
        for query in QUERIES:
            counts = []
            for candidate in CANDIDATES:
                selected = groups[(episode, query, candidate)]
                tasks = [task for run in selected for task in run["tasks"]]
                counts.append(round(statistics.median(run["scan_task_count"] for run in selected)))
                cells.append({
                    "episode": episode,
                    "query": query,
                    "max_partition_mib": candidate,
                    "elapsed_ms_values": [run["elapsed_ms"] for run in selected],
                    "elapsed_ms_median": statistics.median(
                        run["elapsed_ms"] for run in selected
                    ),
                    "scan_tasks_median": counts[-1],
                    "decoded_task_bytes_p50": statistics.median(
                        task["output_batch_bytes"] for task in tasks
                    ),
                    "decoded_task_bytes_p95": pct(
                        [task["output_batch_bytes"] for task in tasks], 0.95
                    ),
                    "decoded_task_rows_p50": statistics.median(
                        task["output_rows"] for task in tasks
                    ),
                    "surviving_task_bytes_p50": (
                        statistics.median(task["filter_output_batch_bytes"] for task in tasks)
                        if query == "filtered" else None
                    ),
                    "surviving_task_rows_p50": (
                        statistics.median(task["filter_output_rows"] for task in tasks)
                        if query == "filtered" else None
                    ),
                    "output_batches_p50": statistics.median(
                        task["output_batches"] for task in tasks
                    ),
                    "output_batches_max": max(task["output_batches"] for task in tasks),
                    "max_emitted_batch_bytes": max(
                        task["max_output_batch_bytes"] for task in tasks
                    ),
                    "gpu_task_footprint_p95": pct(
                        [task.get("gpu_max_task_footprint", 0) for task in tasks], 0.95
                    ),
                    "retry_count": sum(task.get("gpu_retry_count", 0) for task in tasks),
                    "split_retry_count": sum(
                        task.get("gpu_split_retry_count", 0) for task in tasks
                    ),
                    "spill_bytes": sum(
                        task.get("gpu_spill_host_bytes", 0)
                        + task.get("gpu_spill_disk_bytes", 0)
                        + task.get("spark_disk_spill_bytes", 0)
                        + task.get("spark_memory_spill_bytes", 0)
                        for task in tasks
                    ),
                })
            if len(set(counts)) != len(counts):
                raise ValueError("extension candidates are not distinct")
    knees = []
    for episode in EPISODES:
        for query in QUERIES:
            chosen = [c for c in cells if c["episode"] == episode and c["query"] == query]
            best = min(c["elapsed_ms_median"] for c in chosen)
            within = [
                c["max_partition_mib"] for c in chosen
                if c["elapsed_ms_median"] <= 1.05 * best
            ]
            knees.append({
                "episode": episode,
                "query": query,
                "best_observed_mib": min(
                    c["max_partition_mib"] for c in chosen
                    if c["elapsed_ms_median"] == best
                ),
                "within_5_percent_mib": within,
                "smallest_within_5_percent_mib": min(within),
            })
    transfer_regret = {}
    for query in QUERIES:
        train_cells = [
            cell for cell in cells
            if cell["episode"] == "train_2009" and cell["query"] == query
        ]
        selected = min(train_cells, key=lambda cell: cell["elapsed_ms_median"])[
            "max_partition_mib"
        ]
        transfer_regret[query] = {"train_selected_mib": selected}
        for episode in ("validation_2010", "test_2011"):
            heldout = [
                cell for cell in cells
                if cell["episode"] == episode and cell["query"] == query
            ]
            oracle = min(cell["elapsed_ms_median"] for cell in heldout)
            selected_time = next(
                cell["elapsed_ms_median"] for cell in heldout
                if cell["max_partition_mib"] == selected
            )
            transfer_regret[query][episode] = {
                "selected_elapsed_ms": selected_time,
                "oracle_elapsed_ms": oracle,
                "regret_percent": 100.0 * (selected_time / oracle - 1.0),
            }

    validation = {
        "runs": len(runs),
        "measured_runs": len(measured),
        "complete_cells": len(groups),
        "result_hashes_stable": True,
        "candidate_task_counts_distinct": True,
        "retry_count": sum(c["retry_count"] for c in cells),
        "split_retry_count": sum(c["split_retry_count"] for c in cells),
        "spill_bytes": sum(c["spill_bytes"] for c in cells),
    }
    result = {
        "schema_version": 1,
        "validation": validation,
        "knees": knees,
        "transfer_regret": transfer_regret,
        "cells": cells,
    }
    with open(args.output, "x", encoding="utf-8") as stream:
        json.dump(result, stream, indent=2, sort_keys=True)
        stream.write("\n")

    lines = [
        "# High-size sequential extension",
        "",
        "Status: **MEASURED LOCALLY; SEQUENTIAL EXPLORATION**",
        "",
        "All 120 runs completed, including 108 measured runs in 36 complete cells.",
        "Result hashes were stable; retry, split-retry, and spill totals were {}, {}, and {} bytes.".format(
            validation["retry_count"], validation["split_retry_count"], validation["spill_bytes"]
        ),
        "",
        "| Episode | Query | Best MiB | Within 5% | Smallest |",
        "|---|---|---:|---|---:|",
    ]
    for knee in knees:
        lines.append("| {episode} | {query} | {best_observed_mib} | {within} | {smallest_within_5_percent_mib} |".format(
            within=", ".join(str(value) for value in knee["within_5_percent_mib"]), **knee
        ))
    lines.extend([
        "",
        "## 2009-selected candidate regret",
        "",
        "| Query | Selected MiB | 2010 regret | 2011 regret |",
        "|---|---:|---:|---:|",
    ])
    for query in QUERIES:
        item = transfer_regret[query]
        lines.append("| {} | {} | {:.2f}% | {:.2f}% |".format(
            query,
            item["train_selected_mib"],
            item["validation_2010"]["regret_percent"],
            item["test_2011"]["regret_percent"],
        ))
    lines.extend([
        "",
        "Per-cell decoded bytes, rows, batch counts, footprint, and timings are in",
        "analysis/validated-extension.json. This extension was triggered after inspecting",
        "the primary range, so it brackets hypotheses but is not a jointly preregistered",
        "confirmatory comparison.",
        "",
    ])
    with open(args.markdown, "x", encoding="utf-8") as stream:
        stream.write("\n".join(lines))


if __name__ == "__main__":
    main()
