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

"""Score the frozen 512-MiB policy in a prospective 2010/2011 transfer rerun."""

import argparse
import json
import os

from analyze_bathtub_followup import (
    group,
    holm_adjust,
    load,
    measured,
    paired_effect,
    summarize,
)

SELECTED_MIB = 512
ACCEPTANCE_REGRET_PERCENT = 10
EXPECTED_EPISODES = {"validation_2010", "test_2011"}
EXPECTED_QUERIES = {"common", "variable_width"}
EXPECTED_CANDIDATES = {128, 512, 2048, 4096}
EXPECTED_BLOCKS = {0, 1, 2, 3, 4}
EXPECTED_MEASURED_RUNS = 80


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--metrics", required=True)
    parser.add_argument("--lifecycle", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--markdown", required=True)
    args = parser.parse_args()
    for path in (args.output, args.markdown):
        if os.path.exists(path):
            raise FileExistsError("refusing to overwrite " + path)

    rows = measured(load(args.metrics))
    lifecycle = load(args.lifecycle)
    grouped = group(rows, ["episode", "query", "max_partition_mib"])
    episodes = sorted({row["episode"] for row in rows})
    queries = sorted({row["query"] for row in rows})
    candidates = sorted({row["max_partition_mib"] for row in rows})
    if len(rows) != EXPECTED_MEASURED_RUNS:
        raise ValueError(
            f"expected {EXPECTED_MEASURED_RUNS} measured runs, found {len(rows)}"
        )
    if set(episodes) != EXPECTED_EPISODES:
        raise ValueError(f"unexpected episodes: {episodes}")
    if set(queries) != EXPECTED_QUERIES:
        raise ValueError(f"unexpected queries: {queries}")
    if set(candidates) != EXPECTED_CANDIDATES:
        raise ValueError(f"unexpected candidates: {candidates}")
    for episode in EXPECTED_EPISODES:
        for query in EXPECTED_QUERIES:
            for candidate in EXPECTED_CANDIDATES:
                cell_rows = grouped.get((episode, query, candidate), [])
                blocks = {row["block"] for row in cell_rows}
                if len(cell_rows) != len(EXPECTED_BLOCKS) or blocks != EXPECTED_BLOCKS:
                    raise ValueError(
                        "incomplete frozen cell {}|{}|{}: {} rows, blocks {}".format(
                            episode, query, candidate, len(cell_rows), sorted(blocks)
                        )
                    )
    cells = {}
    transfer = []
    paired = []

    for episode in episodes:
        for query in queries:
            summaries = {}
            for candidate in candidates:
                key = (episode, query, candidate)
                label = "|".join(str(value) for value in key)
                summaries[candidate] = summarize(grouped[key], label)
                cells[label] = summaries[candidate]
            best = min(
                candidates,
                key=lambda candidate: summaries[candidate]["elapsed_ms_median"],
            )
            best_time = summaries[best]["elapsed_ms_median"]
            selected_time = summaries[SELECTED_MIB]["elapsed_ms_median"]
            regret = (selected_time / best_time - 1) * 100
            transfer.append({
                "episode": episode,
                "query": query,
                "selected_mib": SELECTED_MIB,
                "best_observed_mib": best,
                "selected_median_ms": selected_time,
                "best_median_ms": best_time,
                "regret_percent": regret,
                "acceptance_threshold_percent": ACCEPTANCE_REGRET_PERCENT,
                "regret_criterion_passes": regret <= ACCEPTANCE_REGRET_PERCENT,
            })
            reference = grouped[(episode, query, SELECTED_MIB)]
            family = []
            for candidate in candidates:
                if candidate == SELECTED_MIB:
                    continue
                effect = paired_effect(grouped[(episode, query, candidate)], reference)
                effect.update({
                    "episode": episode,
                    "query": query,
                    "max_partition_mib": candidate,
                    "reference_mib": SELECTED_MIB,
                })
                family.append(effect)
            holm_adjust(family)
            paired.extend(family)

    stable_hashes = all(
        len({row["result_sha256"] for row in rows
             if row["episode"] == episode and row["query"] == query}) == 1
        for episode in episodes for query in queries
    )
    safety = {
        "retry_count": sum(row["retry_count"] for row in rows),
        "split_retry_count": sum(row["split_retry_count"] for row in rows),
        "spill_bytes": sum(row["spill_bytes"] for row in rows),
    }
    point_median_regret_pass = all(
        item["regret_criterion_passes"] for item in transfer
    )
    metric_safety_pass = all(value == 0 for value in safety.values())
    expected_runs = lifecycle["scheduled_runs"]
    lifecycle_pass = (
        lifecycle["application_start"] == 1
        and lifecycle["application_end"] == 1
        and lifecycle["failed_tasks"] == 0
        and lifecycle["executor_removed"] == 0
        and lifecycle["gpu_retry_count_all_tasks"] == 0
        and lifecycle["gpu_split_retry_count_all_tasks"] == 0
        and lifecycle["gpu_spill_bytes_all_tasks"] == 0
        and lifecycle["spark_spill_bytes_all_tasks"] == 0
        and lifecycle["journal_runs"] == expected_runs
        and lifecycle["sql_executions"] == expected_runs
    )
    overall_acceptance = (
        point_median_regret_pass
        and stable_hashes
        and metric_safety_pass
        and lifecycle_pass
    )
    result = {
        "schema_version": 2,
        "frozen_selected_mib": SELECTED_MIB,
        "acceptance_regret_percent": ACCEPTANCE_REGRET_PERCENT,
        "validation": {
            "measured_runs": len(rows),
            "stable_result_hashes_by_episode_query": stable_hashes,
            "safety": safety,
            "metric_safety_pass": metric_safety_pass,
            "lifecycle_pass": lifecycle_pass,
            "lifecycle": lifecycle,
        },
        "cells": cells,
        "transfer": transfer,
        "paired_effects_vs_selected": paired,
        "point_median_regret_criterion_pass": point_median_regret_pass,
        "overall_acceptance_pass": overall_acceptance,
        "maximum_regret_percent": max(item["regret_percent"] for item in transfer),
        "limitations": [
            "The selection used only 2009, but 2010/2011 had been studied in earlier static-concurrency experiments.",
            "Five paired blocks cannot produce two-sided exact sign-flip p below 0.0625.",
            "The candidate oracle is restricted to 128, 512, 2048, and 4096 MiB.",
            "Passing does not validate codecs, clusters, or unrelated query families.",
        ],
    }
    with open(args.output, "x", encoding="utf-8") as stream:
        json.dump(result, stream, indent=2, sort_keys=True)
        stream.write("\n")

    lines = [
        "# Prospective bounded-regret transfer rerun",
        "",
        f"The 512-MiB policy was frozen from the new 2009 dynamic-concurrency sweep "
        f"before this run. The descriptive point-median criterion required no more than "
        f"{ACCEPTANCE_REGRET_PERCENT}% regret in every 2010/2011 episode/query cell "
        "against the restricted {128, 512, 2048, 4096}-MiB comparator set. The epochs "
        "were held out from that 2009 selection calculation, but had been studied in "
        "earlier static-concurrency experiments; this is not an untouched independent holdout.",
        "",
        f"- Measured runs: {len(rows)}",
        f"- Stable result hashes by episode/query: {stable_hashes}",
        f"- Retry / split-retry / spill bytes: {safety['retry_count']} / "
        f"{safety['split_retry_count']} / {safety['spill_bytes']}",
        f"- Point-median regret criterion passes: {point_median_regret_pass}",
        f"- Lifecycle validation passes: {lifecycle_pass}",
        f"- Overall preregistered acceptance passes: {overall_acceptance}",
        f"- Maximum comparator-set point-median regret: "
        f"{result['maximum_regret_percent']:.2f}%",
        "",
        "| Episode | Query | Best comparator MiB | Best median ms | 512 median ms | "
        "512 regret | Pass |",
        "|---|---|---:|---:|---:|---:|---:|",
    ]
    for item in transfer:
        lines.append(
            f"| {item['episode']} | {item['query']} | "
            f"{item['best_observed_mib']} | {item['best_median_ms']:.1f} | "
            f"{item['selected_median_ms']:.1f} | {item['regret_percent']:.2f}% | "
            f"{item['regret_criterion_passes']} |"
        )
    lines.extend([
        "",
        "Paired block effects relative to the frozen 512-MiB policy; positive means the "
        "comparator was slower:",
        "",
        "| Episode | Query | Comparator MiB | Geometric mean change | "
        "Paired log-ratio 95% CI | Exact sign p | Holm p |",
        "|---|---|---:|---:|---:|---:|---:|",
    ])
    for effect in paired:
        ci = effect["paired_log_ratio_ci95_percent"]
        lines.append(
            f"| {effect['episode']} | {effect['query']} | "
            f"{effect['max_partition_mib']} | "
            f"{effect['geometric_mean_change_percent']:.1f}% | "
            f"[{ci[0]:.1f}%, {ci[1]:.1f}%] | "
            f"{effect['exact_sign_flip_p']:.4f} | "
            f"{effect['holm_adjusted_p']:.4f} |"
        )
    lines.extend([
        "",
        "The preregistered descriptive criterion passes across these two schema/time "
        "epochs. Five blocks do not confidence-bound true regret below 10%, and the "
        "comparator oracle is restricted. This does not promote 512 MiB to a universal "
        "default; the physical-layout experiment shows that table layout can move the "
        "useful region.",
        "",
    ])
    with open(args.markdown, "x", encoding="utf-8") as stream:
        stream.write("\n".join(lines))


if __name__ == "__main__":
    main()
