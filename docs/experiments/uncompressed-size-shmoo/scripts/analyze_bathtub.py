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

"""Reframe the existing partition-size sweep as a bounded plateau analysis."""

import argparse
import json
import math
import os
import statistics

DEFAULT_MIB = 128
BATCH_TARGET_BYTES = 1024 ** 3
PLATEAU_TOLERANCE = 0.05


def load(path):
    with open(path, encoding="utf-8") as stream:
        return json.load(stream)


def median(values):
    return statistics.median(values) if values else None


def task_metric(run, name, field, default=0):
    return run.get("task_metrics", {}).get(name, {}).get(field, default)


def run_features(run):
    span_ms = run["scan_task_span_ms"]
    holding_ms = task_metric(run, "gpu_semaphore_holding_ns", "sum") / 1_000_000
    wait_ms = task_metric(run, "gpu_semaphore_wait_ns", "sum") / 1_000_000
    decode_ns = task_metric(run, "gpu_decode_ns", "sum")
    output_bytes = task_metric(run, "output_batch_bytes", "sum")
    return {
        "elapsed_ms": run["elapsed_ms"],
        "scan_task_span_ms": span_ms,
        "scan_span_share": span_ms / run["elapsed_ms"],
        "gpu_holding_ms": holding_ms,
        "gpu_busy_fraction_of_scan_span": holding_ms / span_ms if span_ms else None,
        "gpu_semaphore_wait_ms": wait_ms,
        "decoded_bytes_per_decode_second": (
            output_bytes / (decode_ns / 1_000_000_000) if decode_ns else None
        ),
        "scan_task_count": run["scan_task_count"],
        "output_batches_per_task": (
            task_metric(run, "output_batches", "sum") / run["scan_task_count"]
        ),
        "decoded_task_bytes_p50": task_metric(run, "output_batch_bytes", "p50"),
        "decoded_task_rows_p50": task_metric(run, "output_rows", "p50"),
        "max_emitted_batch_bytes": task_metric(run, "max_output_batch_bytes", "max"),
        "gpu_max_concurrent_tasks": task_metric(
            run, "gpu_max_concurrent_tasks", "max"
        ),
        "gpu_max_task_footprint": task_metric(
            run, "gpu_max_task_footprint", "max"
        ),
        "gpu_max_device_memory_bytes": task_metric(
            run, "gpu_max_device_memory_bytes", "max"
        ),
        "multithread_reader_max_parallelism": task_metric(
            run, "multithread_reader_max_parallelism", "max"
        ),
        "retry_count": task_metric(run, "gpu_retry_count", "sum"),
        "split_retry_count": task_metric(run, "gpu_split_retry_count", "sum"),
        "spill_bytes": (
            task_metric(run, "gpu_spill_host_bytes", "sum")
            + task_metric(run, "gpu_spill_disk_bytes", "sum")
        ),
    }


def group_runs(document):
    groups = {}
    for run in document["runs"]:
        if run["phase"] != "measure":
            continue
        key = (run["episode"], run["query"], run["max_partition_mib"])
        groups.setdefault(key, []).append(run_features(run))
    return groups


def summarize_cell(rows):
    numeric_keys = rows[0].keys()
    summary = {"repetitions": len(rows)}
    for key in numeric_keys:
        values = [row[key] for row in rows if row[key] is not None]
        summary[key + "_median"] = median(values)
        summary[key + "_values"] = values
    elapsed = summary["elapsed_ms_values"]
    summary["elapsed_ms_stdev"] = statistics.stdev(elapsed) if len(elapsed) > 1 else 0
    summary["elapsed_cv"] = (
        summary["elapsed_ms_stdev"] / summary["elapsed_ms_median"]
        if summary["elapsed_ms_median"] else None
    )
    return summary


def linear_fit(points):
    xs = [point[0] for point in points]
    ys = [point[1] for point in points]
    x_mean, y_mean = statistics.mean(xs), statistics.mean(ys)
    denominator = sum((x - x_mean) ** 2 for x in xs)
    slope = sum((x - x_mean) * (y - y_mean) for x, y in points) / denominator
    intercept = y_mean - slope * x_mean
    residual = sum((y - (intercept + slope * x)) ** 2 for x, y in points)
    total = sum((y - y_mean) ** 2 for y in ys)
    return {
        "intercept_ms": intercept,
        "effective_incremental_ms_per_scan_task": slope,
        "r_squared": 1 - residual / total if total else 1.0,
        "candidate_count": len(points),
    }


def contiguous_plateau(candidates, medians, best_candidate, threshold):
    best_index = candidates.index(best_candidate)
    low = best_index
    high = best_index
    while low > 0 and medians[candidates[low - 1]] <= threshold:
        low -= 1
    while high + 1 < len(candidates) and medians[candidates[high + 1]] <= threshold:
        high += 1
    return candidates[low:high + 1]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--primary-metrics", required=True)
    parser.add_argument("--extension-metrics", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--markdown", required=True)
    args = parser.parse_args()
    for path in (args.output, args.markdown):
        if os.path.exists(path):
            raise FileExistsError("refusing to overwrite " + path)

    primary = group_runs(load(args.primary_metrics))
    extension = group_runs(load(args.extension_metrics))
    cells = []
    overlap = []
    default_regret = []
    small_ramp = []
    concurrency_values = set()
    large_side_slowdowns = 0

    episode_queries = sorted({(key[0], key[1]) for key in primary})
    for episode, query in episode_queries:
        primary_cells = {
            candidate: summarize_cell(rows)
            for (ep, q, candidate), rows in primary.items()
            if ep == episode and q == query
        }
        extension_cells = {
            candidate: summarize_cell(rows)
            for (ep, q, candidate), rows in extension.items()
            if ep == episode and q == query
        }
        primary_2048 = primary_cells[2048]["elapsed_ms_median"]
        extension_2048 = extension_cells[2048]["elapsed_ms_median"]
        bridge = primary_2048 / extension_2048
        overlap.append({
            "episode": episode,
            "query": query,
            "primary_2048_ms": primary_2048,
            "extension_2048_ms": extension_2048,
            "extension_over_primary_ratio": extension_2048 / primary_2048,
            "extension_bridge_multiplier": bridge,
        })

        combined = dict(primary_cells)
        for candidate, summary in extension_cells.items():
            if candidate <= 2048:
                continue
            adjusted = dict(summary)
            adjusted["elapsed_ms_bridge_adjusted_median"] = (
                summary["elapsed_ms_median"] * bridge
            )
            combined[candidate] = adjusted
        for candidate, summary in primary_cells.items():
            summary["elapsed_ms_bridge_adjusted_median"] = summary["elapsed_ms_median"]

        candidates = sorted(combined)
        medians = {
            candidate: combined[candidate]["elapsed_ms_bridge_adjusted_median"]
            for candidate in candidates
        }
        best_candidate = min(candidates, key=medians.get)
        best_time = medians[best_candidate]
        threshold = best_time * (1 + PLATEAU_TOLERANCE)
        plateau_set = [candidate for candidate in candidates if medians[candidate] <= threshold]
        contiguous = contiguous_plateau(candidates, medians, best_candidate, threshold)
        default_time = medians[DEFAULT_MIB]
        default_row = combined[DEFAULT_MIB]
        cv = default_row["elapsed_cv"]
        normal_mde_3 = 2.8 * math.sqrt(2 / 3) * cv * 100
        normal_mde_10 = 2.8 * math.sqrt(2 / 10) * cv * 100

        record = {
            "episode": episode,
            "query": query,
            "candidates_mib": candidates,
            "bridge_adjusted_elapsed_ms": medians,
            "best_observed_mib": best_candidate,
            "plateau_tolerance_percent": PLATEAU_TOLERANCE * 100,
            "within_tolerance_mib": plateau_set,
            "contiguous_plateau_around_best_mib": contiguous,
            "default_mib": DEFAULT_MIB,
            "default_regret_percent": (default_time / best_time - 1) * 100,
            "default_inside_descriptive_plateau": DEFAULT_MIB in plateau_set,
            "default_elapsed_cv_percent": cv * 100,
            "normal_approx_two_sample_mde_percent_n3": normal_mde_3,
            "normal_approx_two_sample_mde_percent_n10": normal_mde_10,
            "smallest_candidate_reaching_90_percent_batch_target_mib": next(
                (
                    candidate for candidate in candidates
                    if combined[candidate]["max_emitted_batch_bytes_median"]
                    >= 0.9 * BATCH_TARGET_BYTES
                ),
                None,
            ),
            "observed_max_task_footprint_bytes": max(
                combined[candidate]["gpu_max_task_footprint_median"]
                for candidate in candidates
            ),
            "observed_retry_or_spill": any(
                combined[candidate]["retry_count_median"]
                or combined[candidate]["split_retry_count_median"]
                or combined[candidate]["spill_bytes_median"]
                for candidate in candidates
            ),
            "cells": {str(candidate): combined[candidate] for candidate in candidates},
        }
        cells.append(record)
        default_regret.append(record["default_regret_percent"])
        if medians[8192] > medians[4096]:
            large_side_slowdowns += 1
        small_points = [
            (primary_cells[candidate]["scan_task_count_median"],
             primary_cells[candidate]["elapsed_ms_median"])
            for candidate in sorted(primary_cells) if candidate <= 512
        ]
        fit = linear_fit(small_points)
        fit.update({"episode": episode, "query": query})
        small_ramp.append(fit)
        for candidate in candidates:
            values = combined[candidate]["gpu_max_concurrent_tasks_values"]
            concurrency_values.update(value for value in values if value)

    result = {
        "schema_version": 1,
        "contract": {
            "objective": "whole-query elapsed time; scan-span share reported separately",
            "default_mib": DEFAULT_MIB,
            "descriptive_plateau_tolerance_percent": PLATEAU_TOLERANCE * 100,
            "safety": "no retry, split-retry, or spill regression",
            "scope": "single local RTX A6000, static GPU concurrency one",
        },
        "identifiability": {
            "observed_positive_gpu_max_concurrent_tasks": sorted(concurrency_values),
            "admission_concurrency_model_fittable": len(concurrency_values) > 1,
            "wave_quantization_at_multiple_gpu_slots_fittable": len(concurrency_values) > 1,
            "upper_safety_wall_observed": any(
                cell["observed_retry_or_spill"] for cell in cells
            ),
        },
        "overlap_bridge": overlap,
        "cells": cells,
        "small_partition_effective_ramp_fits": small_ramp,
        "summary": {
            "annual_query_cells": len(cells),
            "default_inside_descriptive_plateau_cells": sum(
                cell["default_inside_descriptive_plateau"] for cell in cells
            ),
            "default_regret_percent": {
                "min": min(default_regret),
                "median": median(default_regret),
                "max": max(default_regret),
            },
            "large_side_8192_slower_than_4096_cells": large_side_slowdowns,
            "large_side_cells": len(cells),
            "minimum_default_repetitions": min(
                len(cell["cells"][str(DEFAULT_MIB)]["elapsed_ms_values"])
                for cell in cells
            ),
            "multi_candidate_contiguous_plateau_cells": sum(
                len(cell["contiguous_plateau_around_best_mib"]) > 1
                for cell in cells
            ),
            "four_x_contiguous_plateau_cells": sum(
                max(cell["contiguous_plateau_around_best_mib"])
                / min(cell["contiguous_plateau_around_best_mib"]) >= 4
                for cell in cells
            ),
            "default_scan_span_share_median": median([
                cell["cells"][str(DEFAULT_MIB)]["scan_span_share_median"]
                for cell in cells
            ]),
            "default_gpu_busy_fraction_of_scan_span_median": median([
                cell["cells"][str(DEFAULT_MIB)][
                    "gpu_busy_fraction_of_scan_span_median"
                ]
                for cell in cells
            ]),
            "effective_small_side_ms_per_task": {
                "min": min(
                    fit["effective_incremental_ms_per_scan_task"]
                    for fit in small_ramp
                ),
                "median": median([
                    fit["effective_incremental_ms_per_scan_task"]
                    for fit in small_ramp
                ]),
                "max": max(
                    fit["effective_incremental_ms_per_scan_task"]
                    for fit in small_ramp
                ),
                "minimum_r_squared": min(
                    fit["r_squared"] for fit in small_ramp
                ),
            },
            "overlap_extension_drift_percent": {
                "min": min(
                    (item["extension_over_primary_ratio"] - 1) * 100
                    for item in overlap
                ),
                "max": max(
                    (item["extension_over_primary_ratio"] - 1) * 100
                    for item in overlap
                ),
            },
        },
        "limitations": [
            "The 5% plateau is descriptive; three repetitions do not provide confirmatory bounds.",
            "Extension times are bridge-adjusted through a separately run 2048-MiB overlap cell.",
            "Static concurrency one prevents fitting c(P) or multi-slot wave quantization.",
            "No retry, spill, split-retry, or OOM occurred, so the upper safety wall is censored.",
            "Semaphore duration accumulators are event-log display values rounded to milliseconds.",
            "The effective small-partition slope includes all changes correlated with task count.",
        ],
    }

    with open(args.output, "x", encoding="utf-8") as stream:
        json.dump(result, stream, indent=2, sort_keys=True)
        stream.write("\n")

    summary = result["summary"]
    lines = [
        "# Bathtub/plateau reanalysis of the committed shmoo",
        "",
        "## Verdict",
        "",
        "The existing runs support a strong small-partition ramp and a high-size turnover, "
        "but they do **not** establish the hypothesized broad 4–16× plateau or identify an "
        "admission/memory cliff. All measured GPU tasks reported maximum concurrency one, and "
        "no retry/spill event occurred. A two-wall controller therefore remains a useful "
        "hypothesis requiring a concurrency-enabled sweep.",
        "",
        "## Default-regret screen",
        "",
        f"- Annual query cells: {summary['annual_query_cells']}",
        f"- Spark 3.5.5 default ({DEFAULT_MIB} MiB) inside the descriptive 5% set: "
        f"{summary['default_inside_descriptive_plateau_cells']}/"
        f"{summary['annual_query_cells']}",
        "- Bridge-adjusted default regret: "
        f"min {summary['default_regret_percent']['min']:.2f}%, "
        f"median {summary['default_regret_percent']['median']:.2f}%, "
        f"max {summary['default_regret_percent']['max']:.2f}%.",
        f"- 8192 MiB slower than 4096 MiB in "
        f"{summary['large_side_8192_slower_than_4096_cells']}/"
        f"{summary['large_side_cells']} cells.",
        f"- More than one adjacent candidate inside the 5% region: "
        f"{summary['multi_candidate_contiguous_plateau_cells']}/"
        f"{summary['annual_query_cells']} cells; a contiguous 4× region: "
        f"{summary['four_x_contiguous_plateau_cells']}/"
        f"{summary['annual_query_cells']}.",
        f"- At the default, median scan-span/query share was "
        f"{summary['default_scan_span_share_median'] * 100:.1f}% and median GPU "
        f"semaphore-holding/scan-span share was "
        f"{summary['default_gpu_busy_fraction_of_scan_span_median'] * 100:.1f}%.",
        "",
        "These are exploratory medians, not confidence-bounded production regret. The original "
        "and extension attempts were separate applications; high-size times are normalized "
        "through their shared 2048-MiB cell.",
        "",
        "## Per-cell bounds",
        "",
        "| Episode | Query | Best observed MiB | Contiguous 5% region MiB | Default regret | "
        "Default CV | Approx. n=10 detectable effect | 90% batch-fill candidate |",
        "|---|---|---:|---|---:|---:|---:|---:|",
    ]
    for cell in cells:
        plateau = cell["contiguous_plateau_around_best_mib"]
        lines.append(
            f"| {cell['episode']} | {cell['query']} | {cell['best_observed_mib']} | "
            f"{min(plateau)}–{max(plateau)} | {cell['default_regret_percent']:.2f}% | "
            f"{cell['default_elapsed_cv_percent']:.2f}% | "
            f"{cell['normal_approx_two_sample_mde_percent_n10']:.2f}% | "
            f"{cell['smallest_candidate_reaching_90_percent_batch_target_mib']} |"
        )
    lines.extend([
        "",
        "The detectable-effect column is only a normal-approximation planning number based on "
        "three observed default runs. Ten dedicated default repetitions are still required "
        "before preregistering a confirmatory effect threshold.",
        "",
        "## Mechanism conclusions",
        "",
        "1. **Small side:** elapsed time is fit against actual scan-task count for candidates "
        "through 512 MiB. The slope is an effective incremental task cost, not pure setup time.",
        "2. **Batch filling:** the first candidate whose observed maximum batch reaches 90% of "
        "the 1-GiB target is reported independently from partition-size performance.",
        "3. **Admission:** unidentifiable here because static concurrency was pinned to one.",
        "4. **Upper wall:** censored; the sweep observed no spill/retry/OOM. A slowdown at 8192 "
        "MiB is performance evidence, not a measured memory cliff.",
        "5. **Critical-path gate:** scan task span and GPU semaphore-holding share are now "
        "recoverable from the event log. They are proxies; neither is GPU SM utilization.",
        "",
        "Across the 12 cells, the small-side fit estimated "
        f"{summary['effective_small_side_ms_per_task']['min']:.1f}–"
        f"{summary['effective_small_side_ms_per_task']['max']:.1f} effective additional "
        "milliseconds per scan task (median "
        f"{summary['effective_small_side_ms_per_task']['median']:.1f} ms, minimum "
        f"R² {summary['effective_small_side_ms_per_task']['minimum_r_squared']:.5f}). "
        "This unusually linear result is strong evidence for a task-count mechanism in this "
        "static-concurrency lane, but the slope still combines setup, scheduling, and any "
        "other effect correlated with task count.",
        "",
        "## Required next experiment",
        "",
        "Run a separately preregistered, concurrency-enabled mechanism sweep. Include ten "
        "default repetitions, log-spaced partition sizes, at least narrow fixed-width and wide "
        "string projections, and a fixed-large-partition batch-size sweep. Stop escalation at "
        "the first retry/spill/split event. Validate a bounded plateau policy on held-out runs "
        "instead of selecting and validating a point optimum on the same sweep.",
        "",
    ])
    with open(args.markdown, "x", encoding="utf-8") as stream:
        stream.write("\n".join(lines))


if __name__ == "__main__":
    main()
