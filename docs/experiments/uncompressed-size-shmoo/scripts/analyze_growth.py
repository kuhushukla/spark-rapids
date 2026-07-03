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

"""Validate cumulative growth and mixed-schema results."""

import argparse
import json
import os
import statistics
from collections import defaultdict


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
    if len(runs) != 80 or len(measured) != 72:
        raise ValueError("expected 80 total and 72 measured")
    groups = defaultdict(list)
    hashes = defaultdict(set)
    for run in measured:
        hashes[run["episode"]].add(run["result_sha256"])
        groups[(run["episode"], int(run["max_partition_mib"]))].append(run)
    if any(len(values) != 1 for values in hashes.values()):
        raise ValueError("result mismatch")
    if len(groups) != 24 or any(len(values) != 3 for values in groups.values()):
        raise ValueError("incomplete growth cells")

    cells = []
    for key, selected in sorted(groups.items()):
        episode, candidate = key
        tasks = [task for run in selected for task in run["tasks"]]
        cells.append({
            "episode": episode,
            "query": selected[0]["query"],
            "end_month": selected[0]["end_month"],
            "max_partition_mib": candidate,
            "elapsed_ms_values": [run["elapsed_ms"] for run in selected],
            "elapsed_ms_median": statistics.median(run["elapsed_ms"] for run in selected),
            "scan_tasks_median": round(statistics.median(
                run["scan_task_count"] for run in selected
            )),
            "decoded_task_bytes_p50": statistics.median(
                task["output_batch_bytes"] for task in tasks
            ),
            "decoded_task_bytes_p95": pct(
                [task["output_batch_bytes"] for task in tasks], 0.95
            ),
            "decoded_task_rows_p50": statistics.median(
                task["output_rows"] for task in tasks
            ),
            "output_batches_p50": statistics.median(
                task["output_batches"] for task in tasks
            ),
            "max_emitted_batch_bytes": max(
                task["max_output_batch_bytes"] for task in tasks
            ),
            "gpu_task_footprint_p95": pct(
                [task.get("gpu_max_task_footprint", 0) for task in tasks], 0.95
            ),
            "retry_count": sum(task.get("gpu_retry_count", 0) for task in tasks),
            "spill_bytes": sum(
                task.get("gpu_spill_host_bytes", 0)
                + task.get("gpu_spill_disk_bytes", 0)
                + task.get("spark_disk_spill_bytes", 0)
                + task.get("spark_memory_spill_bytes", 0)
                for task in tasks
            ),
        })
    windows = []
    for episode in sorted(hashes):
        chosen = [cell for cell in cells if cell["episode"] == episode]
        best = min(cell["elapsed_ms_median"] for cell in chosen)
        within = [
            cell["max_partition_mib"] for cell in chosen
            if cell["elapsed_ms_median"] <= 1.05 * best
        ]
        windows.append({
            "episode": episode,
            "query": chosen[0]["query"],
            "end_month": chosen[0]["end_month"],
            "best_observed_mib": min(
                cell["max_partition_mib"] for cell in chosen
                if cell["elapsed_ms_median"] == best
            ),
            "within_5_percent_mib": within,
            "smallest_within_5_percent_mib": min(within),
        })
    validation = {
        "runs": len(runs),
        "measured_runs": len(measured),
        "complete_cells": len(groups),
        "result_hashes_stable": True,
        "retry_count": sum(cell["retry_count"] for cell in cells),
        "spill_bytes": sum(cell["spill_bytes"] for cell in cells),
    }
    result = {
        "schema_version": 1,
        "validation": validation,
        "windows": windows,
        "cells": cells,
    }
    with open(args.output, "x", encoding="utf-8") as stream:
        json.dump(result, stream, indent=2, sort_keys=True)
        stream.write("\n")

    lines = [
        "# Cumulative growth and mixed-schema results",
        "",
        "Status: **MEASURED LOCALLY; EXPLORATORY GROWTH STUDY**",
        "",
        "All 80 runs completed; 72 measured runs form 24 complete cells.",
        "Result hashes were stable; observed retry and spill totals were {} and {} bytes.".format(
            validation["retry_count"], validation["spill_bytes"]
        ),
        "",
        "| Window | Query | End | Best MiB | Within 5% | Smallest |",
        "|---|---|---|---:|---|---:|",
    ]
    for item in windows:
        lines.append("| {episode} | {query} | {end_month} | {best_observed_mib} | {within} | {smallest_within_5_percent_mib} |".format(
            within=", ".join(str(value) for value in item["within_5_percent_mib"]),
            **item
        ))
    lines.extend([
        "",
        "The complete cell table includes task count, decoded byte/row p50 and p95,",
        "batch count, footprint, and all three elapsed observations. Small windows can",
        "collapse multiple configured candidates onto the same physical layout; those",
        "configured labels are not independent treatments.",
        "",
    ])
    with open(args.markdown, "x", encoding="utf-8") as stream:
        stream.write("\n".join(lines))


if __name__ == "__main__":
    main()
