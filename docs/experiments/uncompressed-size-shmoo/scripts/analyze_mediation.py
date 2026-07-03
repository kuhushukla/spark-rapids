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

"""Validate and summarize the batch-control mediation factorial."""

import argparse
import json
import os
import statistics
from collections import defaultdict


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
    if len(runs) != 164 or len(measured) != 162:
        raise ValueError("expected 164 total and 162 measured")
    groups = defaultdict(list)
    hashes = defaultdict(set)
    for run in measured:
        hashes[run["query"]].add(run["result_sha256"])
        key = (
            run["query"], int(run["max_partition_mib"]),
            int(run["rapids_batch_mib"]), int(run["reader_batch_mib"])
        )
        groups[key].append(run)
    if any(len(values) != 1 for values in hashes.values()):
        raise ValueError("result mismatch")
    if len(groups) != 54 or any(len(values) != 3 for values in groups.values()):
        raise ValueError("incomplete factorial")

    cells = []
    for key, selected in sorted(groups.items()):
        query, mpb, rapids_batch, reader_batch = key
        tasks = [task for run in selected for task in run["tasks"]]
        cells.append({
            "query": query,
            "max_partition_mib": mpb,
            "rapids_batch_mib": rapids_batch,
            "reader_batch_mib": reader_batch,
            "elapsed_ms_values": [run["elapsed_ms"] for run in selected],
            "elapsed_ms_median": statistics.median(run["elapsed_ms"] for run in selected),
            "decoded_task_bytes_p50": statistics.median(
                task["output_batch_bytes"] for task in tasks
            ),
            "max_emitted_batch_bytes": max(
                task["max_output_batch_bytes"] for task in tasks
            ),
            "output_batches_p50": statistics.median(
                task["output_batches"] for task in tasks
            ),
            "output_batches_max": max(task["output_batches"] for task in tasks),
            "gpu_task_footprint_p95": sorted(
                task.get("gpu_max_task_footprint", 0) for task in tasks
            )[round(0.95 * (len(tasks) - 1))],
            "retry_count": sum(task.get("gpu_retry_count", 0) for task in tasks),
            "spill_bytes": sum(
                task.get("gpu_spill_host_bytes", 0)
                + task.get("gpu_spill_disk_bytes", 0)
                + task.get("spark_disk_spill_bytes", 0)
                + task.get("spark_memory_spill_bytes", 0)
                for task in tasks
            ),
        })

    target_response = []
    for query in ("common", "filtered"):
        for mpb in (2048, 4096, 8192):
            for reader in (1024, 2048, 4096):
                chosen = [
                    cell for cell in cells
                    if cell["query"] == query
                    and cell["max_partition_mib"] == mpb
                    and cell["reader_batch_mib"] == reader
                ]
                target_response.append({
                    "query": query,
                    "max_partition_mib": mpb,
                    "reader_batch_mib": reader,
                    "max_batch_by_rapids_target": {
                        str(cell["rapids_batch_mib"]): cell["max_emitted_batch_bytes"]
                        for cell in chosen
                    },
                    "elapsed_by_rapids_target": {
                        str(cell["rapids_batch_mib"]): cell["elapsed_ms_median"]
                        for cell in chosen
                    },
                })

    reader_response = []
    for query in ("common", "filtered"):
        for mpb in (2048, 4096, 8192):
            for rapids_batch in (512, 1024, 2048):
                chosen = [
                    cell for cell in cells
                    if cell["query"] == query
                    and cell["max_partition_mib"] == mpb
                    and cell["rapids_batch_mib"] == rapids_batch
                ]
                elapsed = [cell["elapsed_ms_median"] for cell in chosen]
                reader_response.append({
                    "query": query,
                    "max_partition_mib": mpb,
                    "rapids_batch_mib": rapids_batch,
                    "elapsed_by_reader_limit": {
                        str(cell["reader_batch_mib"]): cell["elapsed_ms_median"]
                        for cell in chosen
                    },
                    "relative_elapsed_range": (max(elapsed) - min(elapsed)) / min(elapsed),
                })

    best = {}
    for query in ("common", "filtered"):
        chosen = [cell for cell in cells if cell["query"] == query]
        fastest = min(cell["elapsed_ms_median"] for cell in chosen)
        best[query] = [
            cell for cell in chosen if cell["elapsed_ms_median"] <= 1.05 * fastest
        ]

    result = {
        "schema_version": 1,
        "validation": {
            "runs": len(runs),
            "measured_runs": len(measured),
            "complete_cells": len(groups),
            "result_hashes_stable": True,
            "retry_count": sum(cell["retry_count"] for cell in cells),
            "spill_bytes": sum(cell["spill_bytes"] for cell in cells),
        },
        "cells": cells,
        "target_response": target_response,
        "reader_response": reader_response,
        "within_5_percent_of_best": best,
    }
    with open(args.output, "x", encoding="utf-8") as stream:
        json.dump(result, stream, indent=2, sort_keys=True)
        stream.write("\n")

    lines = [
        "# Batch-control mediation factorial",
        "",
        "Status: **MEASURED LOCALLY; EXPLORATORY FACTORIAL**",
        "",
        "All 164 runs completed. The 162 measured runs form 54 complete cells.",
        "Result hashes were stable; observed retry and spill totals were {} and {} bytes.".format(
            result["validation"]["retry_count"], result["validation"]["spill_bytes"]
        ),
        "",
        "| Query | MPB MiB | RAPIDS target MiB | reader MiB | median ms | max emitted batch bytes | batches/task p50 |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for cell in cells:
        lines.append("| {query} | {max_partition_mib} | {rapids_batch_mib} | {reader_batch_mib} | {elapsed_ms_median:.1f} | {max_emitted_batch_bytes} | {output_batches_p50} |".format(**cell))
    lines.extend([
        "",
        "The complete factor-response maps and within-5% cells are in",
        "analysis/validated-mediation.json. With three repeats, these are mechanism",
        "and interaction observations, not confidence-supported production choices.",
        "",
    ])
    with open(args.markdown, "x", encoding="utf-8") as stream:
        stream.write("\n".join(lines))


if __name__ == "__main__":
    main()
