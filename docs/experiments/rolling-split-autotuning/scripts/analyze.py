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

"""Analyze rolling autotuning accuracy, paired performance, and safety."""

import argparse
import json
import math
import os
import random
import statistics


BOOTSTRAP_SEED = 881923
BOOTSTRAP_REPEATS = 5000
BLOCK_LENGTH = 12
SAFETY_FIELDS = (
    "gpu_retry_count",
    "gpu_split_retry_count",
    "gpu_spill_host_bytes",
    "gpu_spill_disk_bytes",
    "spark_disk_spill_bytes",
    "spark_memory_spill_bytes",
)


def load_json(path):
    with open(path, encoding="utf-8") as stream:
        return json.load(stream)


def load_jsonl(path):
    with open(path, encoding="utf-8") as stream:
        return [json.loads(line) for line in stream]


def percentile(values, fraction):
    ordered = sorted(values)
    if not ordered:
        return None
    return ordered[round(fraction * (len(ordered) - 1))]


def summary(values):
    return {
        "count": len(values),
        "median": statistics.median(values) if values else None,
        "p90": percentile(values, 0.90),
        "min": min(values) if values else None,
        "max": max(values) if values else None,
    }


def ape(predicted, actual):
    return abs(predicted - actual) / actual


def moving_block_median_ci(values, rng):
    if not values:
        return None
    length = min(BLOCK_LENGTH, len(values))
    starts = list(range(len(values) - length + 1))
    samples = []
    for _ in range(BOOTSTRAP_REPEATS):
        generated = []
        while len(generated) < len(values):
            start = rng.choice(starts)
            generated.extend(values[start:start + length])
        samples.append(statistics.median(generated[:len(values)]))
    return {
        "method": "12-window-moving-block-percentile",
        "level": 0.95,
        "lower": percentile(samples, 0.025),
        "upper": percentile(samples, 0.975),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset-package", action="append", required=True,
                        help="dataset=results.jsonl,scan-summary.json,cpu.json")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if os.path.exists(args.output):
        raise FileExistsError("refusing to overwrite " + args.output)

    rng = random.Random(BOOTSTRAP_SEED)
    datasets = {}
    for package in args.dataset_package:
        name, paths = package.split("=", 1)
        results_path, summary_path, cpu_path = paths.split(",")
        result_rows = [
            row for row in load_jsonl(results_path)
            if row["phase"] == "measured"
        ]
        event_rows = {
            row["run_id"]: row for row in load_json(summary_path)["runs"]
            if row["phase"] == "measured"
        }
        cpu = load_json(cpu_path)
        if not cpu["all_match"]:
            raise ValueError("CPU validation failed for " + name)

        by_window = {}
        safety = {field: 0 for field in SAFETY_FIELDS}
        reconciliation = []
        for row in result_rows:
            by_window.setdefault(row["window_id"], {})[row["treatment"]] = row
            event = event_rows.get(row["run_id"])
            if event is None:
                raise ValueError("missing event summary for " + row["run_id"])
            direct = row["direct_scan_metrics"]
            event_bytes = event["task_metrics"]["output_batch_bytes"]["sum"]
            event_rows_count = event["task_metrics"]["output_rows"]["sum"]
            reconciliation.append({
                "run_id": row["run_id"],
                "decoded_bytes_match": direct["decoded_bytes"] == event_bytes,
                "decoded_rows_match": direct["decoded_rows"] == event_rows_count,
                "planned_tasks_match": (
                    direct["planned_scan_tasks"] == event["planned_scan_stage_tasks"]),
            })
            for task in event["tasks"]:
                for field in SAFETY_FIELDS:
                    safety[field] += task.get(field, 0)

        byte_errors = []
        row_errors = []
        elapsed_ratio_128 = []
        elapsed_ratio_1024 = []
        scan_ratio_128 = []
        scan_ratio_1024 = []
        regrets = []
        trajectory = []
        for window_id, treatments in sorted(by_window.items()):
            if set(treatments) != {"enabled", "fixed-128", "fixed-1024"}:
                raise ValueError("incomplete treatment block " + window_id)
            hashes = {row["result_sha256"] for row in treatments.values()}
            if len(hashes) != 1:
                raise ValueError("cross-treatment mismatch " + window_id)
            enabled = treatments["enabled"]
            fixed128 = treatments["fixed-128"]
            fixed1024 = treatments["fixed-1024"]
            enabled_event = event_rows[enabled["run_id"]]
            fixed128_event = event_rows[fixed128["run_id"]]
            fixed1024_event = event_rows[fixed1024["run_id"]]
            decision = enabled["enabled_decision"]
            actual_bytes = enabled["direct_scan_metrics"]["decoded_bytes"]
            actual_rows = enabled["direct_scan_metrics"]["decoded_rows"]
            byte_error = None
            row_error = None
            if decision["predicted_decoded_bytes"] is not None:
                byte_error = ape(decision["predicted_decoded_bytes"], actual_bytes)
                row_error = ape(decision["predicted_decoded_rows"], actual_rows)
                byte_errors.append(byte_error)
                row_errors.append(row_error)
            ratio128 = enabled["elapsed_ms"] / fixed128["elapsed_ms"]
            ratio1024 = enabled["elapsed_ms"] / fixed1024["elapsed_ms"]
            scan128 = (
                enabled_event["scan_task_span_ms"]
                / fixed128_event["scan_task_span_ms"])
            scan1024 = (
                enabled_event["scan_task_span_ms"]
                / fixed1024_event["scan_task_span_ms"])
            regret = max(
                0.0,
                enabled["elapsed_ms"]
                / min(fixed128["elapsed_ms"], fixed1024["elapsed_ms"]) - 1.0)
            elapsed_ratio_128.append(ratio128)
            elapsed_ratio_1024.append(ratio1024)
            scan_ratio_128.append(scan128)
            scan_ratio_1024.append(scan1024)
            regrets.append(regret)
            trajectory.append({
                "window_id": window_id,
                "selected_mib": enabled["max_partition_mib"],
                "history_count": decision["history_count"],
                "predicted_decoded_bytes": decision["predicted_decoded_bytes"],
                "actual_decoded_bytes": actual_bytes,
                "decoded_byte_ape": byte_error,
                "predicted_decoded_rows": decision["predicted_decoded_rows"],
                "actual_decoded_rows": actual_rows,
                "decoded_row_ape": row_error,
                "enabled_elapsed_ms": enabled["elapsed_ms"],
                "fixed_128_elapsed_ms": fixed128["elapsed_ms"],
                "fixed_1024_elapsed_ms": fixed1024["elapsed_ms"],
                "enabled_regret": regret,
                "enabled_scan_tasks": enabled_event["scan_task_count"],
                "fixed_128_scan_tasks": fixed128_event["scan_task_count"],
                "fixed_1024_scan_tasks": fixed1024_event["scan_task_count"],
            })

        byte_result = summary(byte_errors)
        row_result = summary(row_errors)
        regret_result = summary(regrets)
        gates = {
            "decoded_bytes_median_ape_le_20pct": byte_result["median"] <= 0.20,
            "decoded_bytes_p90_ape_le_35pct": byte_result["p90"] <= 0.35,
            "decoded_rows_median_ape_le_20pct": row_result["median"] <= 0.20,
            "decoded_rows_p90_ape_le_35pct": row_result["p90"] <= 0.35,
            "performance_median_regret_le_10pct": regret_result["median"] <= 0.10,
            "performance_p90_regret_le_25pct": regret_result["p90"] <= 0.25,
            "no_retry_or_spill": all(value == 0 for value in safety.values()),
            "direct_event_metrics_reconcile": all(
                all(value for key, value in item.items() if key != "run_id")
                for item in reconciliation),
            "cpu_references_match": cpu["all_match"],
        }
        datasets[name] = {
            "window_count": len(by_window),
            "prediction_window_count": len(byte_errors),
            "decoded_byte_error": byte_result,
            "decoded_row_error": row_result,
            "enabled_regret": regret_result,
            "paired_elapsed_ratio_vs_128": summary(elapsed_ratio_128),
            "paired_elapsed_ratio_vs_1024": summary(elapsed_ratio_1024),
            "paired_scan_span_ratio_vs_128": summary(scan_ratio_128),
            "paired_scan_span_ratio_vs_1024": summary(scan_ratio_1024),
            "median_ratio_ci": {
                "elapsed_vs_128": moving_block_median_ci(elapsed_ratio_128, rng),
                "elapsed_vs_1024": moving_block_median_ci(elapsed_ratio_1024, rng),
                "regret": moving_block_median_ci(regrets, rng),
            },
            "safety_totals": safety,
            "cpu_validation": cpu,
            "metric_reconciliation": reconciliation,
            "gates": gates,
            "trajectory": trajectory,
        }

    output = {
        "schema_version": "rolling-split-autotuning/result-v1",
        "claim": "estimation-only",
        "bootstrap_seed": BOOTSTRAP_SEED,
        "datasets": datasets,
        "all_safety_and_correctness_gates_pass": all(
            result["gates"]["no_retry_or_spill"]
            and result["gates"]["direct_event_metrics_reconcile"]
            and result["gates"]["cpu_references_match"]
            for result in datasets.values()),
    }
    os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
    with open(args.output, "x", encoding="utf-8") as stream:
        json.dump(output, stream, indent=2, sort_keys=True)
        stream.write("\n")


if __name__ == "__main__":
    main()
