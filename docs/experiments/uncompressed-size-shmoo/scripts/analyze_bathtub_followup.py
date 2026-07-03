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

"""Analyze the preregistered dynamic-concurrency bathtub follow-up."""

import argparse
import itertools
import json
import math
import os
import random
import statistics

PLATEAU_TOLERANCE = 0.05
T_CRITICAL_95_DF4 = 2.776


def load(path):
    with open(path, encoding="utf-8") as stream:
        return json.load(stream)


OUTPUT_ONLY_METRICS = {
    "output_batch_bytes",
    "max_output_batch_bytes",
    "max_output_batch_rows",
    "output_rows",
    "output_batches",
    "gpu_decode_ns",
    "scan_ns",
    "read_buffer_bytes",
}


def metric(run, name, field, default=0):
    if name in OUTPUT_ONLY_METRICS:
        metrics = run.get("output_task_metrics") or run.get("task_metrics", {})
    else:
        metrics = run.get("task_metrics", {})
    return metrics.get(name, {}).get(field, default)


def median(values):
    return statistics.median(values) if values else None


def percentile(values, fraction):
    ordered = sorted(values)
    return ordered[round(fraction * (len(ordered) - 1))]


def bootstrap_median_ci(rows, seed):
    """Cluster-bootstrap randomized blocks, preserving repeated defaults within a block."""
    by_block = {}
    for row in rows:
        by_block.setdefault(row["block"], []).append(row["elapsed_ms"])
    blocks = sorted(by_block)
    rng = random.Random(seed)
    estimates = []
    for _ in range(10000):
        sampled = [rng.choice(blocks) for _ in blocks]
        values = [
            value for block in sampled for value in by_block[block]
        ]
        estimates.append(median(values))
    return [percentile(estimates, 0.025), percentile(estimates, 0.975)]


def run_features(run):
    concurrency = max(1, int(metric(run, "gpu_max_concurrent_tasks", "max", 1)))
    tasks = run["scan_task_count"]
    output_tasks = run.get("output_producing_scan_task_count", tasks)
    holding_ms = metric(run, "gpu_semaphore_holding_ns", "sum") / 1_000_000
    span_ms = run["scan_task_span_ms"]
    remainder = output_tasks % concurrency
    empty_metrics = run.get("empty_task_metrics", {})
    empty_holding_ms = (
        empty_metrics.get("gpu_semaphore_holding_ns", {}).get("sum", 0) / 1_000_000
    )
    return {
        "run_id": run["run_id"],
        "block": run.get("block"),
        "repeat": run.get("repeat"),
        "query": run["query"],
        "episode": run.get("episode"),
        "study": run.get("study"),
        "layout": run.get("layout"),
        "max_partition_mib": run["max_partition_mib"],
        "rapids_batch_mib": run.get("rapids_batch_mib", 1024),
        "reader_batch_mib": run.get("reader_batch_mib", 2048),
        "elapsed_ms": run["elapsed_ms"],
        "planned_scan_stage_tasks": run.get("planned_scan_stage_tasks"),
        "scan_task_count": tasks,
        "output_producing_scan_task_count": run.get(
            "output_producing_scan_task_count", tasks
        ),
        "empty_scan_task_count": run.get("empty_scan_task_count", 0),
        "scan_task_span_ms": span_ms,
        "scan_span_share": span_ms / run["elapsed_ms"],
        "gpu_max_concurrent_tasks": concurrency,
        # This is a data-bearing GPU-wave proxy. Empty tasks can still acquire the
        # semaphore briefly, so their holding time is tracked separately.
        "max_concurrency_wave_proxy": math.ceil(output_tasks / concurrency),
        "last_wave_occupancy_proxy": remainder / concurrency if remainder else 1.0,
        "gpu_capacity_busy_proxy": (
            holding_ms / (span_ms * concurrency) if span_ms else None
        ),
        "gpu_semaphore_wait_ms": (
            metric(run, "gpu_semaphore_wait_ns", "sum") / 1_000_000
        ),
        "empty_task_gpu_holding_ms": empty_holding_ms,
        "output_batches_per_task": (
            metric(run, "output_batches", "sum") / output_tasks
        ),
        "decoded_task_bytes_p50": metric(run, "output_batch_bytes", "p50"),
        "decoded_task_rows_p50": metric(run, "output_rows", "p50"),
        "max_emitted_batch_bytes": metric(run, "max_output_batch_bytes", "max"),
        "gpu_max_task_footprint": metric(run, "gpu_max_task_footprint", "max"),
        "gpu_max_device_memory_bytes": metric(
            run, "gpu_max_device_memory_bytes", "max"
        ),
        "multithread_reader_max_parallelism": metric(
            run, "multithread_reader_max_parallelism", "max"
        ),
        "retry_count": metric(run, "gpu_retry_count", "sum"),
        "split_retry_count": metric(run, "gpu_split_retry_count", "sum"),
        "spill_bytes": (
            metric(run, "gpu_spill_host_bytes", "sum")
            + metric(run, "gpu_spill_disk_bytes", "sum")
        ),
        "result_sha256": run["result_sha256"],
    }


def measured(document):
    return [
        run_features(run) for run in document["runs"] if run["phase"] == "measure"
    ]


def summarize(rows, seed):
    keys = [
        "elapsed_ms", "planned_scan_stage_tasks", "scan_task_count",
        "output_producing_scan_task_count", "empty_scan_task_count",
        "scan_task_span_ms", "scan_span_share",
        "gpu_max_concurrent_tasks", "max_concurrency_wave_proxy",
        "last_wave_occupancy_proxy", "gpu_capacity_busy_proxy",
        "gpu_semaphore_wait_ms", "empty_task_gpu_holding_ms",
        "output_batches_per_task",
        "decoded_task_bytes_p50", "decoded_task_rows_p50",
        "max_emitted_batch_bytes", "gpu_max_task_footprint",
        "gpu_max_device_memory_bytes", "multithread_reader_max_parallelism",
        "retry_count", "split_retry_count", "spill_bytes",
    ]
    result = {"repetitions": len(rows)}
    for key in keys:
        values = [row[key] for row in rows if row[key] is not None]
        result[key + "_values"] = values
        result[key + "_median"] = median(values)
    elapsed = result["elapsed_ms_values"]
    result["elapsed_ms_bootstrap_median_ci95"] = bootstrap_median_ci(rows, seed)
    result["elapsed_ms_mean"] = statistics.mean(elapsed)
    result["elapsed_ms_stdev"] = statistics.stdev(elapsed) if len(elapsed) > 1 else 0
    result["elapsed_cv_percent"] = (
        result["elapsed_ms_stdev"] / result["elapsed_ms_mean"] * 100
    )
    result["result_hashes"] = sorted({row["result_sha256"] for row in rows})
    return result


def group(rows, keys):
    grouped = {}
    for row in rows:
        key = tuple(row[name] for name in keys)
        grouped.setdefault(key, []).append(row)
    return grouped


def exact_sign_flip_p(log_differences):
    observed = abs(statistics.mean(log_differences))
    permutations = [
        abs(statistics.mean([
            sign * value for sign, value in zip(signs, log_differences)
        ]))
        for signs in itertools.product((-1, 1), repeat=len(log_differences))
    ]
    return sum(value >= observed - 1e-15 for value in permutations) / len(permutations)


def paired_effect(treatment, reference):
    by_block_treatment = {}
    by_block_reference = {}
    for row in treatment:
        by_block_treatment.setdefault(row["block"], []).append(row["elapsed_ms"])
    for row in reference:
        by_block_reference.setdefault(row["block"], []).append(row["elapsed_ms"])
    blocks = sorted(set(by_block_treatment) & set(by_block_reference))
    log_ratios = [
        math.log(median(by_block_treatment[block]) / median(by_block_reference[block]))
        for block in blocks
    ]
    mean_log = statistics.mean(log_ratios)
    stdev = statistics.stdev(log_ratios)
    half_width = T_CRITICAL_95_DF4 * stdev / math.sqrt(len(log_ratios))
    return {
        "blocks": len(blocks),
        "geometric_mean_change_percent": (math.exp(mean_log) - 1) * 100,
        "paired_log_ratio_ci95_percent": [
            (math.exp(mean_log - half_width) - 1) * 100,
            (math.exp(mean_log + half_width) - 1) * 100,
        ],
        "exact_sign_flip_p": exact_sign_flip_p(log_ratios),
        "block_log_ratios": log_ratios,
    }


def holm_adjust(records):
    ordered = sorted(records, key=lambda item: item["exact_sign_flip_p"])
    running = 0
    total = len(ordered)
    for index, item in enumerate(ordered):
        running = max(
            running,
            min(1.0, (total - index) * item["exact_sign_flip_p"]),
        )
        item["holm_adjusted_p"] = running


def cells(rows, key_names):
    result = {}
    for key, values in group(rows, key_names).items():
        label = "|".join(str(value) for value in key)
        result[label] = summarize(values, label)
        result[label]["key"] = dict(zip(key_names, key))
    return result


def comparison_family(rows, family_key, treatment_key, reference_value):
    grouped = group(rows, [family_key, treatment_key])
    records = []
    for family in sorted({row[family_key] for row in rows}):
        reference = grouped[(family, reference_value)]
        family_records = []
        for treatment in sorted({
                row[treatment_key] for row in rows
                if row[family_key] == family
                and row[treatment_key] != reference_value}):
            effect = paired_effect(grouped[(family, treatment)], reference)
            effect.update({
                family_key: family,
                treatment_key: treatment,
                "reference": reference_value,
            })
            family_records.append(effect)
        holm_adjust(family_records)
        records.extend(family_records)
    return records


def safety_summary(rows):
    return {
        "retry_count": sum(row["retry_count"] for row in rows),
        "split_retry_count": sum(row["split_retry_count"] for row in rows),
        "spill_bytes": sum(row["spill_bytes"] for row in rows),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mechanism-metrics", required=True)
    parser.add_argument("--batch-metrics", required=True)
    parser.add_argument("--sharded-layout-metrics", required=True)
    parser.add_argument("--source-layout-metrics", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--markdown", required=True)
    args = parser.parse_args()
    for path in (args.output, args.markdown):
        if os.path.exists(path):
            raise FileExistsError("refusing to overwrite " + path)

    mechanism = measured(load(args.mechanism_metrics))
    batch = measured(load(args.batch_metrics))
    sharded = measured(load(args.sharded_layout_metrics))
    source = measured(load(args.source_layout_metrics))
    layout = sharded + source
    all_rows = mechanism + batch + layout

    mechanism_cells = cells(mechanism, ["query", "max_partition_mib"])
    batch_cells = cells(batch, ["query", "rapids_batch_mib"])
    layout_cells = cells(layout, ["layout", "max_partition_mib"])
    mechanism_effects = comparison_family(
        mechanism, "query", "max_partition_mib", 128
    )
    batch_effects = comparison_family(batch, "query", "rapids_batch_mib", 1024)
    layout_effects = comparison_family(
        layout, "layout", "max_partition_mib", 128
    )

    mechanism_groups = group(mechanism, ["query", "max_partition_mib"])
    plateaus = {}
    default_variance = {}
    concurrency_ranges = {}
    for query in sorted({row["query"] for row in mechanism}):
        summaries = {
            key[1]: summarize(values, f"plateau-{query}-{key[1]}")
            for key, values in mechanism_groups.items() if key[0] == query
        }
        best = min(
            summaries, key=lambda candidate: summaries[candidate]["elapsed_ms_median"]
        )
        best_time = summaries[best]["elapsed_ms_median"]
        within = sorted(
            candidate for candidate, summary in summaries.items()
            if summary["elapsed_ms_median"] <= best_time * (1 + PLATEAU_TOLERANCE)
        )
        plateaus[query] = {
            "best_observed_mib": best,
            "within_5_percent_mib": within,
        }
        default = summaries[128]
        default_variance[query] = {
            "values_ms": default["elapsed_ms_values"],
            "mean_ms": default["elapsed_ms_mean"],
            "stdev_ms": default["elapsed_ms_stdev"],
            "cv_percent": default["elapsed_cv_percent"],
            "bootstrap_median_ci95_ms": default[
                "elapsed_ms_bootstrap_median_ci95"
            ],
            "normal_approx_independent_n5_detectable_effect_percent": (
                2.8 * math.sqrt(2 / 5) * default["elapsed_cv_percent"]
            ),
        }
        concurrency_ranges[query] = {}
        for candidate in sorted(summaries):
            values = summaries[candidate]["gpu_max_concurrent_tasks_values"]
            concurrency_ranges[query][str(candidate)] = {
                "min": min(values),
                "median": median(values),
                "max": max(values),
            }

    candidates = sorted({
        row["max_partition_mib"] for row in mechanism
    })
    query_best = {
        query: min(
            mechanism_cells[f"{query}|{candidate}"]["elapsed_ms_median"]
            for candidate in candidates
        )
        for query in sorted({row["query"] for row in mechanism})
    }
    bounded_regret = []
    for candidate in candidates:
        per_query = {
            query: (
                mechanism_cells[f"{query}|{candidate}"]["elapsed_ms_median"]
                / query_best[query] - 1
            ) * 100
            for query in query_best
        }
        bounded_regret.append({
            "max_partition_mib": candidate,
            "regret_percent_by_query": per_query,
            "worst_query_regret_percent": max(per_query.values()),
            "minimum_median_wave_proxy": min(
                mechanism_cells[f"{query}|{candidate}"][
                    "max_concurrency_wave_proxy_median"
                ]
                for query in query_best
            ),
            "maximum_median_task_footprint_bytes": max(
                mechanism_cells[f"{query}|{candidate}"][
                    "gpu_max_task_footprint_median"
                ]
                for query in query_best
            ),
        })
    maximin_candidate = min(
        bounded_regret, key=lambda item: item["worst_query_regret_percent"]
    )

    stable_hashes = all(
        len(summary["result_hashes"]) == 1
        for summary in list(mechanism_cells.values())
        + list(batch_cells.values()) + list(layout_cells.values())
    )
    result = {
        "schema_version": 2,
        "validation": {
            "runs": len(all_rows),
            "mechanism_runs": len(mechanism),
            "batch_runs": len(batch),
            "layout_runs": len(layout),
            "stable_result_hashes_per_cell": stable_hashes,
            "source_sharded_result_hash_match": (
                len({row["result_sha256"] for row in layout}) == 1
            ),
            "safety": safety_summary(all_rows),
        },
        "default_variance": default_variance,
        "mechanism_cells": mechanism_cells,
        "mechanism_paired_effects_vs_128": mechanism_effects,
        "batch_cells": batch_cells,
        "batch_paired_effects_vs_1024": batch_effects,
        "layout_cells": layout_cells,
        "layout_paired_effects_vs_128": layout_effects,
        "descriptive_plateaus": plateaus,
        "dynamic_concurrency_ranges": concurrency_ranges,
        "cross_query_bounded_regret": bounded_regret,
        "exploratory_minimax_candidate": maximin_candidate,
        "physical_layout_scan_task_counts": {
            layout_name: {
                str(candidate): {
                    name: sorted({
                        row[name] for row in layout_rows
                        if row["max_partition_mib"] == candidate
                    })
                    for name in (
                        "planned_scan_stage_tasks",
                        "scan_task_count",
                        "output_producing_scan_task_count",
                        "empty_scan_task_count",
                    )
                }
                for candidate in (128, 2048, 8192)
            }
            for layout_name, layout_rows in (("sharded", sharded), ("source", source))
        },
        "interpretation_limits": [
            "Five paired blocks give a minimum two-sided exact sign-flip p-value of 0.0625.",
            "Bootstrap intervals are not simultaneous confidence bands.",
            "gpuMaxConcurrentGpuTasks is a task maximum, not a fixed stage-wide c(P).",
            "Wave counts based on maximum concurrency are diagnostic proxies.",
            "No retry/spill event means the memory wall remains right-censored.",
            "The source and sharded layouts differ in more than file count.",
            "No codec contrast, GPU utilization, or SM occupancy was measured.",
        ],
    }
    with open(args.output, "x", encoding="utf-8") as stream:
        json.dump(result, stream, indent=2, sort_keys=True)
        stream.write("\n")

    safety = result["validation"]["safety"]
    lines = [
        "# Dynamic-concurrency bathtub follow-up",
        "",
        "## Validation",
        "",
        f"- Measured runs: {len(all_rows)} ({len(mechanism)} partition, "
        f"{len(batch)} batch, {len(layout)} layout).",
        f"- Stable result hashes within every cell: {stable_hashes}.",
        f"- Source and sharded layout result hash match: "
        f"{len({row['result_sha256'] for row in layout}) == 1}.",
        f"- Retry / split-retry / spill bytes: {safety['retry_count']} / "
        f"{safety['split_retry_count']} / {safety['spill_bytes']}.",
        "",
        "## Default variance",
        "",
        "| Query | Repetitions | Mean ms | CV | Block-bootstrap median 95% interval | "
        "Approx. n=5 detectable effect |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for query, summary in default_variance.items():
        ci = summary["bootstrap_median_ci95_ms"]
        lines.append(
            f"| {query} | {len(summary['values_ms'])} | {summary['mean_ms']:.1f} | "
            f"{summary['cv_percent']:.2f}% | {ci[0]:.1f}–{ci[1]:.1f} ms | "
            f"{summary['normal_approx_independent_n5_detectable_effect_percent']:.2f}% |"
        )
    lines.extend([
        "",
        "Five paired blocks imply a minimum two-sided exact sign-flip p-value of 0.0625, "
        "even before Holm correction. Effect sizes and mechanism response are primary; the "
        "detectable-effect value is only a planning approximation.",
        "",
        "## Partition mechanism sweep",
        "",
        "| Query | Partition MiB | Median ms [block bootstrap 95%] | "
        "Stage/output/empty tasks | Max c | Output-task wave proxy | "
        "Last-wave proxy | Decoded output-task MiB | Max batch MiB | "
        "Max footprint MiB | Wait ms |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ])
    for query in sorted({row["query"] for row in mechanism}):
        candidates = sorted({
            row["max_partition_mib"] for row in mechanism if row["query"] == query
        })
        for candidate in candidates:
            summary = mechanism_cells[f"{query}|{candidate}"]
            ci = summary["elapsed_ms_bootstrap_median_ci95"]
            lines.append(
                f"| {query} | {candidate} | {summary['elapsed_ms_median']:.1f} "
                f"[{ci[0]:.1f}, {ci[1]:.1f}] | "
                f"{summary['scan_task_count_median']:.0f}/"
                f"{summary['output_producing_scan_task_count_median']:.0f}/"
                f"{summary['empty_scan_task_count_median']:.0f} | "
                f"{summary['gpu_max_concurrent_tasks_median']:.1f} | "
                f"{summary['max_concurrency_wave_proxy_median']:.1f} | "
                f"{summary['last_wave_occupancy_proxy_median']:.2f} | "
                f"{summary['decoded_task_bytes_p50_median'] / 1024**2:.1f} | "
                f"{summary['max_emitted_batch_bytes_median'] / 1024**2:.1f} | "
                f"{summary['gpu_max_task_footprint_median'] / 1024**2:.1f} | "
                f"{summary['gpu_semaphore_wait_ms_median']:.1f} |"
            )
    lines.extend(["", "Descriptive 5% regions:"])
    for query, plateau in plateaus.items():
        lines.append(
            f"- {query}: best {plateau['best_observed_mib']} MiB; "
            f"within 5% {plateau['within_5_percent_mib']}."
        )
    lines.extend([
        "",
        "Paired block effects versus 128 MiB (negative is faster):",
        "",
        "| Query | Partition MiB | Geometric mean change | Paired log-ratio 95% CI | "
        "Exact sign p | Holm p |",
        "|---|---:|---:|---:|---:|---:|",
    ])
    for effect in mechanism_effects:
        ci = effect["paired_log_ratio_ci95_percent"]
        lines.append(
            f"| {effect['query']} | {effect['max_partition_mib']} | "
            f"{effect['geometric_mean_change_percent']:.1f}% | "
            f"[{ci[0]:.1f}%, {ci[1]:.1f}%] | "
            f"{effect['exact_sign_flip_p']:.4f} | "
            f"{effect['holm_adjusted_p']:.4f} |"
        )
    lines.extend([
        "",
        "## Cross-query bounded regret",
        "",
        "| Partition MiB | Common regret | Variable-width regret | Worst regret | "
        "Minimum wave proxy | Maximum median footprint MiB |",
        "|---:|---:|---:|---:|---:|---:|",
    ])
    for item in bounded_regret:
        lines.append(
            f"| {item['max_partition_mib']} | "
            f"{item['regret_percent_by_query']['common']:.1f}% | "
            f"{item['regret_percent_by_query']['variable_width']:.1f}% | "
            f"{item['worst_query_regret_percent']:.1f}% | "
            f"{item['minimum_median_wave_proxy']:.1f} | "
            f"{item['maximum_median_task_footprint_bytes'] / 1024**2:.1f} |"
        )
    lines.extend([
        "",
        f"The exploratory point-median minimax calculation gives "
        f"{maximin_candidate['max_partition_mib']} MiB "
        f"{maximin_candidate['worst_query_regret_percent']:.1f}% worst observed regret. "
        "Its 0.3-percentage-point advantage over 2,048 MiB is far below observed "
        "variation; 512 MiB is a conservative footprint/wave tie-break, not a uniquely "
        "identified optimum. This is a post-sweep diagnostic, not a validated policy.",
        "",
        "## Batch-size sweep at fixed 4096-MiB partition treatment",
        "",
        "| Query | Batch target MiB | Median ms [block bootstrap 95%] | Max batch MiB | "
        "Batches/task | Max c | Max footprint MiB |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ])
    for query in sorted({row["query"] for row in batch}):
        targets = sorted({
            row["rapids_batch_mib"] for row in batch if row["query"] == query
        })
        for target in targets:
            summary = batch_cells[f"{query}|{target}"]
            ci = summary["elapsed_ms_bootstrap_median_ci95"]
            lines.append(
                f"| {query} | {target} | {summary['elapsed_ms_median']:.1f} "
                f"[{ci[0]:.1f}, {ci[1]:.1f}] | "
                f"{summary['max_emitted_batch_bytes_median'] / 1024**2:.1f} | "
                f"{summary['output_batches_per_task_median']:.1f} | "
                f"{summary['gpu_max_concurrent_tasks_median']:.1f} | "
                f"{summary['gpu_max_task_footprint_median'] / 1024**2:.1f} |"
            )
    lines.extend([
        "",
        "Paired block effects versus the 1024-MiB batch target (negative is faster):",
        "",
        "| Query | Batch target MiB | Geometric mean change | Paired log-ratio 95% CI | "
        "Exact sign p | Holm p |",
        "|---|---:|---:|---:|---:|---:|",
    ])
    for effect in batch_effects:
        ci = effect["paired_log_ratio_ci95_percent"]
        lines.append(
            f"| {effect['query']} | {effect['rapids_batch_mib']} | "
            f"{effect['geometric_mean_change_percent']:.1f}% | "
            f"[{ci[0]:.1f}%, {ci[1]:.1f}%] | "
            f"{effect['exact_sign_flip_p']:.4f} | "
            f"{effect['holm_adjusted_p']:.4f} |"
        )
    lines.extend([
        "",
        "## Physical-layout contrast",
        "",
        "| Layout | Partition MiB | Median ms [block bootstrap 95%] | "
        "Planned/stage/output/empty tasks | Empty-task GPU hold ms | Max c | "
        "Max batch MiB |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ])
    for layout_name in ("sharded", "source"):
        for candidate in (128, 2048, 8192):
            summary = layout_cells[f"{layout_name}|{candidate}"]
            ci = summary["elapsed_ms_bootstrap_median_ci95"]
            lines.append(
                f"| {layout_name} | {candidate} | {summary['elapsed_ms_median']:.1f} "
                f"[{ci[0]:.1f}, {ci[1]:.1f}] | "
                f"{summary['planned_scan_stage_tasks_median']:.0f}/"
                f"{summary['scan_task_count_median']:.0f}/"
                f"{summary['output_producing_scan_task_count_median']:.0f}/"
                f"{summary['empty_scan_task_count_median']:.0f} | "
                f"{summary['empty_task_gpu_holding_ms_median']:.1f} | "
                f"{summary['gpu_max_concurrent_tasks_median']:.1f} | "
                f"{summary['max_emitted_batch_bytes_median'] / 1024**2:.1f} |"
            )
    lines.extend([
        "",
        "## Interpretation",
        "",
        "- Partition and batch sizing are coupled but distinct actuators. Once a task "
        "contains enough data, the batch target directly moves the emitted GPU boundary; "
        "partition sizing controls available task volume, task count, and batches/task.",
        "- Dynamic admission is observable, but its task maximum is not a constant stage-wide "
        "concurrency and must not be substituted blindly into a wave equation.",
        "- Physical file and row-group layout determines whether maxPartitionBytes can move "
        "actual task granularity.",
        "- No retry/spill cliff was reached. The upper safety wall remains censored.",
        "- These runs identify mechanisms and a candidate region; they do not validate a "
        "production bounded-box policy on untouched independent workloads.",
        "",
    ])
    with open(args.markdown, "x", encoding="utf-8") as stream:
        stream.write("\n".join(lines))


if __name__ == "__main__":
    main()
