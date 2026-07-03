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

"""Validate and summarize the frozen uncompressed-size shmoo."""

import argparse
import json
import math
import os
import statistics
from collections import defaultdict

CANDIDATES = (32, 64, 128, 256, 512, 1024, 2048)
QUERIES = ("common", "filtered", "variable_width", "schema_evolution")
EPISODES = ("train_2009", "validation_2010", "test_2011")


def percentile(values, fraction):
    ordered = sorted(values)
    return ordered[round(fraction * (len(ordered) - 1))]


def align64(value):
    return ((value + 63) // 64) * 64


def fixed_width_prediction(rows):
    return 2 * align64(8 * rows) + 2 * align64((rows + 7) // 8)


def mape(pairs):
    return 100.0 * statistics.mean(
        abs(actual - predicted) / actual for actual, predicted in pairs
    )


def solve(matrix, target):
    n = len(target)
    augmented = [list(matrix[row]) + [target[row]] for row in range(n)]
    for column in range(n):
        pivot = max(range(column, n), key=lambda row: abs(augmented[row][column]))
        augmented[column], augmented[pivot] = augmented[pivot], augmented[column]
        if abs(augmented[column][column]) < 1e-12:
            augmented[column][column] += 1e-9
        scale = augmented[column][column]
        augmented[column] = [value / scale for value in augmented[column]]
        for row in range(n):
            if row != column:
                factor = augmented[row][column]
                augmented[row] = [
                    augmented[row][index] - factor * augmented[column][index]
                    for index in range(n + 1)
                ]
    return [augmented[row][-1] for row in range(n)]


def fit_ols(rows, features):
    design = [[1.0] + [math.log(row[name]) for name in features] for row in rows]
    target = [math.log(row["gpu_decode_ns"]) for row in rows]
    width = len(design[0])
    gram = [[sum(x[i] * x[j] for x in design) for j in range(width)] for i in range(width)]
    rhs = [sum(x[i] * y for x, y in zip(design, target)) for i in range(width)]
    return solve(gram, rhs)


def score_ols(rows, features, coefficients):
    squared = []
    absolute_percentage = []
    for row in rows:
        prediction_log = coefficients[0] + sum(
            coefficient * math.log(row[name])
            for coefficient, name in zip(coefficients[1:], features)
        )
        prediction = math.exp(prediction_log)
        actual = row["gpu_decode_ns"]
        squared.append((math.log(actual) - prediction_log) ** 2)
        absolute_percentage.append(abs(actual - prediction) / actual)
    return {
        "count": len(rows),
        "log_rmse": math.sqrt(statistics.mean(squared)),
        "mape_percent": 100.0 * statistics.mean(absolute_percentage),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--metrics", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--markdown", required=True)
    args = parser.parse_args()
    if os.path.exists(args.output) or os.path.exists(args.markdown):
        raise FileExistsError("refusing to overwrite analysis output")

    with open(args.metrics, encoding="utf-8") as stream:
        runs = json.load(stream)["runs"]
    if len(runs) != 264:
        raise ValueError("expected 264 runs, found {}".format(len(runs)))
    measured = [run for run in runs if run["phase"] == "measure"]
    if len(measured) != 252:
        raise ValueError("expected 252 measured runs")

    hashes = defaultdict(set)
    groups = defaultdict(list)
    for run in measured:
        hashes[(run["episode"], run["query"])].add(run["result_sha256"])
        groups[(run["episode"], run["query"], int(run["max_partition_mib"]))].append(run)
    bad_hashes = {str(key): sorted(value) for key, value in hashes.items() if len(value) != 1}
    if bad_hashes:
        raise ValueError("cross-treatment result mismatch: " + json.dumps(bad_hashes))
    if len(groups) != 84 or any(len(value) != 3 for value in groups.values()):
        raise ValueError("incomplete episode/query/candidate blocking")

    shmoo = []
    for episode in EPISODES:
        for query in QUERIES:
            task_counts = []
            for candidate in CANDIDATES:
                selected = groups[(episode, query, candidate)]
                tasks = [task for run in selected for task in run["tasks"]]
                elapsed = [run["elapsed_ms"] for run in selected]
                task_count = round(statistics.median(run["scan_task_count"] for run in selected))
                task_counts.append(task_count)
                spill_sum = sum(
                    task.get("gpu_spill_host_bytes", 0)
                    + task.get("gpu_spill_disk_bytes", 0)
                    + task.get("spark_disk_spill_bytes", 0)
                    + task.get("spark_memory_spill_bytes", 0)
                    for task in tasks
                )
                shmoo.append({
                    "episode": episode,
                    "query": query,
                    "max_partition_mib": candidate,
                    "elapsed_ms": {
                        "values": elapsed,
                        "median": statistics.median(elapsed),
                        "min": min(elapsed),
                        "max": max(elapsed),
                    },
                    "scan_tasks_median": task_count,
                    "decoded_task_bytes": {
                        "p50": statistics.median(task["output_batch_bytes"] for task in tasks),
                        "p95": percentile([task["output_batch_bytes"] for task in tasks], 0.95),
                        "max": max(task["output_batch_bytes"] for task in tasks),
                    },
                    "decoded_task_rows": {
                        "p50": statistics.median(task["output_rows"] for task in tasks),
                        "p95": percentile([task["output_rows"] for task in tasks], 0.95),
                        "max": max(task["output_rows"] for task in tasks),
                    },
                    "max_emitted_batch_bytes": max(
                        task["max_output_batch_bytes"] for task in tasks
                    ),
                    "gpu_task_footprint_p95": percentile(
                        [task.get("gpu_max_task_footprint", 0) for task in tasks], 0.95
                    ),
                    "retry_count": sum(task.get("gpu_retry_count", 0) for task in tasks),
                    "split_retry_count": sum(
                        task.get("gpu_split_retry_count", 0) for task in tasks
                    ),
                    "spill_bytes": spill_sum,
                })
            if len(set(task_counts)) != len(task_counts):
                raise ValueError(
                    "candidate task counts not distinct for {} {}".format(episode, query)
                )

    fixed_pairs = []
    fixed_total = 0
    for run in measured:
        if run["query"] in ("common", "filtered"):
            for task in run["tasks"]:
                fixed_total += 1
                if task["output_batches"] == 1:
                    fixed_pairs.append((
                        task["output_batch_bytes"],
                        fixed_width_prediction(task["output_rows"]),
                    ))

    train_tasks = [
        task for run in measured if run["episode"] == "train_2009"
        for task in run["tasks"] if task["gpu_decode_ns"] > 0
    ]
    model_scores = {}
    for model, features in (
        ("bytes_only", ("output_batch_bytes",)),
        ("rows_only", ("output_rows",)),
        ("bytes_and_rows", ("output_batch_bytes", "output_rows")),
    ):
        coefficients = fit_ols(train_tasks, features)
        model_scores[model] = {
            "features": list(features),
            "coefficients": coefficients,
            "train_2009": score_ols(train_tasks, features, coefficients),
        }
        for episode in ("validation_2010", "test_2011"):
            evaluation = [
                task for run in measured if run["episode"] == episode
                for task in run["tasks"] if task["gpu_decode_ns"] > 0
            ]
            model_scores[model][episode] = score_ols(evaluation, features, coefficients)

    calibration = {}
    for query in QUERIES:
        train = [
            task for run in measured
            if run["episode"] == "train_2009" and run["query"] == query
            for task in run["tasks"]
        ]
        bpr = statistics.median(
            task["output_batch_bytes"] / task["output_rows"] for task in train
        )
        q = statistics.median(
            task["input_bytes"] / task["output_batch_bytes"] for task in train
        )
        calibration[query] = {"train_bytes_per_row": bpr, "train_compressed_over_gpu": q}
        for episode in ("validation_2010", "test_2011"):
            evaluation = [
                task for run in measured
                if run["episode"] == episode and run["query"] == query
                for task in run["tasks"]
            ]
            calibration[query][episode] = {
                "rows_only_mape_percent": mape([
                    (task["output_batch_bytes"], task["output_rows"] * bpr)
                    for task in evaluation
                ]),
                "feedback_ratio_mape_percent": mape([
                    (task["output_batch_bytes"], task["input_bytes"] / q)
                    for task in evaluation
                ]),
            }

    knees = []
    for episode in EPISODES:
        for query in QUERIES:
            cells = [
                item for item in shmoo
                if item["episode"] == episode and item["query"] == query
            ]
            best = min(item["elapsed_ms"]["median"] for item in cells)
            within = [
                item["max_partition_mib"] for item in cells
                if item["elapsed_ms"]["median"] <= 1.05 * best
            ]
            knees.append({
                "episode": episode,
                "query": query,
                "best_observed_mib": min(
                    item["max_partition_mib"] for item in cells
                    if item["elapsed_ms"]["median"] == best
                ),
                "within_5_percent_mib": within,
                "exploratory_smallest_within_5_percent_mib": min(within),
            })

    output = {
        "schema_version": 1,
        "validation": {
            "runs": len(runs),
            "measured_runs": len(measured),
            "complete_cells": len(groups),
            "result_hash_groups": len(hashes),
            "all_result_hashes_stable": True,
            "all_candidate_task_counts_distinct": True,
            "total_retry_count": sum(item["retry_count"] for item in shmoo),
            "total_split_retry_count": sum(item["split_retry_count"] for item in shmoo),
            "total_spill_bytes": sum(item["spill_bytes"] for item in shmoo),
        },
        "first_principles_fixed_width": {
            "formula": "2*align64(8*rows)+2*align64(ceil(rows/8))",
            "one_batch_task_coverage": len(fixed_pairs),
            "eligible_tasks": fixed_total,
            "mape_percent": mape(fixed_pairs),
            "max_absolute_error_bytes": max(
                abs(actual - predicted) for actual, predicted in fixed_pairs
            ),
        },
        "chronological_calibration": calibration,
        "decode_time_models": model_scores,
        "knees": knees,
        "shmoo": shmoo,
    }
    with open(args.output, "x", encoding="utf-8") as stream:
        json.dump(output, stream, indent=2, sort_keys=True)
        stream.write("\n")

    first = output["first_principles_fixed_width"]
    valid = output["validation"]
    lines = [
        "# Full uncompressed-size shmoo results",
        "",
        "Status: **MEASURED LOCALLY; EXPLORATORY, NOT A PRODUCTION DEFAULT**",
        "",
        "All 264 runs completed; 252 measured runs form 84 complete cells with three",
        "randomized blocks. Results were stable within every episode/query. Observed",
        "retry, split-retry, and spill totals are {}, {}, and {} bytes.".format(
            valid["total_retry_count"], valid["total_split_retry_count"],
            valid["total_spill_bytes"]
        ),
        "",
        "## First-principles byte prediction",
        "",
        "For the two-nullable-fixed-width scan, {} predicted one-batch task".format(
            first["formula"]
        ),
        "footprint with {:.6f}% MAPE and {} bytes maximum absolute error over {}/{} eligible tasks.".format(
            first["mape_percent"], first["max_absolute_error_bytes"],
            first["one_batch_task_coverage"], first["eligible_tasks"]
        ),
        "",
        "## Exploratory knees",
        "",
        "| Episode | Query | Best observed MiB | Within 5% | Smallest within 5% |",
        "|---|---|---:|---|---:|",
    ]
    for knee in knees:
        lines.append(
            "| {episode} | {query} | {best_observed_mib} | {within} | "
            "{exploratory_smallest_within_5_percent_mib} |".format(
                within=", ".join(str(value) for value in knee["within_5_percent_mib"]),
                **knee
            )
        )
    lines.extend([
        "",
        "These are hypotheses for confirmation, not selected defaults. Bytes and rows are",
        "strongly coupled within each fixed schema, so model comparison must use the",
        "cross-query/schema holdouts rather than claim causal separation from one curve.",
        "",
        "## Chronological transfer",
        "",
        "| Query | 2010 row MAPE | 2010 ratio MAPE | 2011 row MAPE | 2011 ratio MAPE |",
        "|---|---:|---:|---:|---:|",
    ])
    for query in QUERIES:
        item = calibration[query]
        lines.append("| {} | {:.2f}% | {:.2f}% | {:.2f}% | {:.2f}% |".format(
            query,
            item["validation_2010"]["rows_only_mape_percent"],
            item["validation_2010"]["feedback_ratio_mape_percent"],
            item["test_2011"]["rows_only_mape_percent"],
            item["test_2011"]["feedback_ratio_mape_percent"],
        ))
    lines.extend([
        "",
        "Complete per-cell and held-out model results are in analysis/validated-analysis.json.",
        "",
    ])
    with open(args.markdown, "x", encoding="utf-8") as stream:
        stream.write("\n".join(lines))


if __name__ == "__main__":
    main()
