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

"""Exploratory prequential estimator, drift, order, and actuator diagnostics."""

import argparse
import json
import math
import os
import random
import statistics


DATASETS = (
    "yellow",
    "green",
    "for-hire",
    "high-volume-for-hire",
    "fre-crt-stacr-dnhq",
)
SAFETY_FIELDS = (
    "gpu_retry_count",
    "gpu_split_retry_count",
    "gpu_spill_host_bytes",
    "gpu_spill_disk_bytes",
    "spark_disk_spill_bytes",
    "spark_memory_spill_bytes",
)
METHODS = (
    "last",
    "mean-3",
    "mean-6",
    "mean-12",
    "median-3",
    "median-6",
    "median-12",
    "ewma-1.5",
    "ewma-3",
    "ewma-6",
    "robust-ewma-3",
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


def describe(values):
    return {
        "count": len(values),
        "median": statistics.median(values) if values else None,
        "p90": percentile(values, 0.90),
        "max": max(values) if values else None,
    }


def alpha_for_half_life(half_life):
    return 1.0 - math.pow(0.5, 1.0 / half_life)


def forecasts(values, method):
    result = [None]
    if method == "last":
        return [None] + values[:-1]
    if method.startswith("mean-") or method.startswith("median-"):
        kind, width_text = method.split("-")
        width = int(width_text)
        for index in range(1, len(values)):
            recent = values[max(0, index - width):index]
            result.append(
                statistics.mean(recent)
                if kind == "mean" else statistics.median(recent))
        return result
    robust = method.startswith("robust-")
    half_life = float(method.rsplit("-", 1)[1])
    alpha = alpha_for_half_life(half_life)
    center = values[0]
    prior_errors = []
    for index in range(1, len(values)):
        result.append(center)
        error = values[index] - center
        update = error
        if robust and len(prior_errors) >= 5:
            scale = percentile([abs(item) for item in prior_errors], 0.90)
            limit = max(0.02, 3.0 * scale)
            update = max(-limit, min(limit, error))
        center += alpha * update
        prior_errors.append(error)
    return result


def score_methods(ratios):
    logs = [math.log(value) for value in ratios]
    output = {}
    forecast_by_method = {}
    for method in METHODS:
        predicted_logs = forecasts(logs, method)
        forecast_by_method[method] = predicted_logs
        log_errors = [
            predicted - actual
            for predicted, actual in zip(predicted_logs, logs)
            if predicted is not None
        ]
        apes = [
            abs(math.exp(error) - 1.0)
            for error in log_errors
        ]
        output[method] = {
            "absolute_log_error": describe([abs(value) for value in log_errors]),
            "absolute_percentage_error": describe(apes),
            "signed_log_error_median": statistics.median(log_errors),
        }
    return output, forecast_by_method


def autocorrelation(values, lag):
    if len(values) <= lag:
        return None
    mean = statistics.mean(values)
    denominator = sum((value - mean) ** 2 for value in values)
    if denominator == 0:
        return 0.0
    numerator = sum(
        (values[index] - mean) * (values[index - lag] - mean)
        for index in range(lag, len(values))
    )
    return numerator / denominator


def moving_block_ci(values, block_length, seed, repeats=2000):
    if not values:
        return None
    rng = random.Random(seed + block_length)
    width = min(block_length, len(values))
    starts = list(range(len(values) - width + 1))
    medians = []
    for _ in range(repeats):
        sample = []
        while len(sample) < len(values):
            start = rng.choice(starts)
            sample.extend(values[start:start + width])
        medians.append(statistics.median(sample[:len(values)]))
    return {
        "block_length": block_length,
        "lower": percentile(medians, 0.025),
        "upper": percentile(medians, 0.975),
    }


def drift_diagnostics(actual_logs, predicted_logs):
    errors = []
    triggers = []
    abstain = []
    cooldown = 0
    for index, (actual, predicted) in enumerate(zip(actual_logs, predicted_logs)):
        if cooldown > 0:
            cooldown -= 1
        if predicted is None:
            errors.append(None)
            abstain.append(True)
            continue
        prior = [abs(value) for value in errors if value is not None]
        if len(prior) < 10:
            threshold = None
            abstain.append(True)
        else:
            statistical_threshold = percentile(prior, 0.90)
            threshold = max(math.log(1.10), statistical_threshold)
            multiplicative_width = math.exp(2.0 * statistical_threshold)
            abstain.append(multiplicative_width > 2.0)
        error = actual - predicted
        errors.append(error)
        if index >= 2 and threshold is not None and cooldown == 0:
            recent = errors[-3:]
            positive = sum(value is not None and value > threshold for value in recent)
            negative = sum(value is not None and value < -threshold for value in recent)
            if positive >= 2 or negative >= 2:
                triggers.append({
                    "index": index,
                    "direction": "positive" if positive >= 2 else "negative",
                    "historical_p90_absolute_log_error": statistical_threshold,
                    "effective_log_error_threshold": threshold,
                    "signed_log_error": error,
                })
                cooldown = 6
    return {
        "trigger_count": len(triggers),
        "triggers": triggers,
        "abstain_count": sum(abstain),
        "abstain_fraction": sum(abstain) / len(abstain),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-root", required=True)
    parser.add_argument("--schedule", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if os.path.exists(args.output):
        raise FileExistsError("refusing to overwrite " + args.output)

    schedule = load_json(args.schedule)
    schedule_by_dataset = {
        item["logical_table"]: item for item in schedule["datasets"]
    }
    output = {
        "schema_version": "rolling-split-autotuning/exploratory-v1",
        "status": "POST_HOC_EXPLORATORY",
        "constraints": [
            "Every forecast for t uses observations before t only.",
            "Control outcomes never enter estimator forecasts.",
            "Estimator comparison does not replace the frozen verdict.",
        ],
        "datasets": {},
    }
    estimator_scores = {}
    forecast_cache = {}
    for dataset_name in DATASETS:
        root = os.path.join(args.run_root, dataset_name)
        results = [
            row for row in load_jsonl(os.path.join(root, "results.jsonl"))
            if row["phase"] == "measured"
        ]
        events = {
            row["run_id"]: row
            for row in load_json(os.path.join(root, "scan-summary.json"))["runs"]
            if row["phase"] == "measured"
        }
        by_window = {}
        order = {}
        for row in results:
            by_window.setdefault(row["window_id"], {})[row["treatment"]] = row
            order.setdefault(row["window_id"], []).append(row["treatment"])
        windows = sorted(
            by_window.values(), key=lambda rows: rows["enabled"]["block"])
        enabled = [rows["enabled"] for rows in windows]
        byte_ratios = [
            row["direct_scan_metrics"]["decoded_bytes"] / row["listed_file_bytes"]
            for row in enabled
        ]
        row_ratios = [
            row["direct_scan_metrics"]["decoded_rows"] / row["listed_file_bytes"]
            for row in enabled
        ]
        byte_scores, byte_forecasts = score_methods(byte_ratios)
        row_scores, row_forecasts = score_methods(row_ratios)
        estimator_scores[dataset_name] = byte_scores
        forecast_cache[dataset_name] = (byte_ratios, byte_forecasts)

        invariance = {
            "decoded_row_relative_spread": [],
            "decoded_byte_relative_spread": [],
            "output_batch_relative_spread": [],
        }
        elapsed_log_128 = []
        elapsed_log_1024 = []
        replicate_logs = []
        no_op_windows = 0
        position = {0: [], 1: [], 2: []}
        predecessor = {"none": [], "enabled": [], "fixed-128": [], "fixed-1024": []}
        safety_presence = {field: [0, 0] for field in SAFETY_FIELDS}
        for rows in windows:
            metrics = [row["direct_scan_metrics"] for row in rows.values()]
            for field, key in (
                    ("decoded_row_relative_spread", "decoded_rows"),
                    ("decoded_byte_relative_spread", "decoded_bytes"),
                    ("output_batch_relative_spread", "output_batches")):
                values = [item[key] for item in metrics]
                invariance[field].append(
                    (max(values) - min(values)) / statistics.mean(values)
                    if statistics.mean(values) else 0.0)
            auto = rows["enabled"]
            fixed128 = rows["fixed-128"]
            fixed1024 = rows["fixed-1024"]
            log128 = math.log(auto["elapsed_ms"] / fixed128["elapsed_ms"])
            log1024 = math.log(auto["elapsed_ms"] / fixed1024["elapsed_ms"])
            elapsed_log_128.append(log128)
            elapsed_log_1024.append(log1024)
            configured = auto["max_partition_mib"]
            if configured == 128:
                replicate_logs.append(log128)
            if configured == 1024:
                replicate_logs.append(log1024)
            tasks128 = fixed128["direct_scan_metrics"]["planned_scan_tasks"]
            tasks1024 = fixed1024["direct_scan_metrics"]["planned_scan_tasks"]
            batches128 = fixed128["direct_scan_metrics"]["output_batches"]
            batches1024 = fixed1024["direct_scan_metrics"]["output_batches"]
            if tasks128 == tasks1024 and batches128 == batches1024:
                no_op_windows += 1
            sequence = order[auto["window_id"]]
            auto_position = sequence.index("enabled")
            position[auto_position].append(log128)
            previous = sequence[auto_position - 1] if auto_position > 0 else "none"
            predecessor[previous].append(log128)
            for row in rows.values():
                for task in events[row["run_id"]]["tasks"]:
                    for field in SAFETY_FIELDS:
                        safety_presence[field][1] += 1
                        safety_presence[field][0] += int(field in task)

        actual_logs = [math.log(value) for value in byte_ratios]
        frozen_predicted_logs = [
            None if row["enabled_decision"]["decoded_bytes_per_listed_byte"] is None
            else math.log(row["enabled_decision"]["decoded_bytes_per_listed_byte"])
            for row in enabled
        ]
        drift = drift_diagnostics(actual_logs, frozen_predicted_logs)
        for trigger in drift["triggers"]:
            trigger["window_id"] = enabled[trigger["index"]]["window_id"]

        schedule_windows = {
            item["window_id"]: item
            for item in schedule_by_dataset[dataset_name]["windows"]
        }
        path_count_error = {}
        if dataset_name == "fre-crt-stacr-dnhq":
            buckets = {}
            for row, actual, predicted in zip(
                    enabled, actual_logs, frozen_predicted_logs):
                if predicted is None:
                    continue
                count = len(schedule_windows[row["window_id"]]["paths"])
                buckets.setdefault(str(count), []).append(
                    abs(math.exp(predicted - actual) - 1.0))
            path_count_error = {
                count: describe(values) for count, values in buckets.items()
            }

        output["datasets"][dataset_name] = {
            "byte_estimator_scores": byte_scores,
            "row_estimator_scores": row_scores,
            "frozen_estimator_drift": drift,
            "shape_autocorrelation": {
                str(lag): autocorrelation(actual_logs, lag)
                for lag in (1, 6, 12, 24, 36)
            },
            "cross_treatment_invariance": {
                key: describe(values) for key, values in invariance.items()
            },
            "actuator": {
                "fixed_128_vs_1024_same_tasks_and_batches": no_op_windows,
                "window_count": len(windows),
            },
            "identical_configuration_timing_noise": {
                "pair_count": len(replicate_logs),
                "absolute_log_ratio": describe(
                    [abs(value) for value in replicate_logs]),
                "ratio": describe([math.exp(value) for value in replicate_logs]),
            },
            "paired_log_ratio": {
                "enabled_vs_128": describe(elapsed_log_128),
                "enabled_vs_1024": describe(elapsed_log_1024),
                "acf_vs_128": {
                    str(lag): autocorrelation(elapsed_log_128, lag)
                    for lag in (1, 6, 12, 24, 36)
                },
                "block_ci_sensitivity_vs_128": [
                    moving_block_ci(elapsed_log_128, width, 92117)
                    for width in (6, 12, 24, 36)
                ],
                "block_ci_sensitivity_vs_1024": [
                    moving_block_ci(elapsed_log_1024, width, 19217)
                    for width in (6, 12, 24, 36)
                ],
            },
            "order_effect_diagnostic": {
                "enabled_position_log_ratio_vs_128": {
                    str(key): describe(values) for key, values in position.items()
                },
                "enabled_predecessor_log_ratio_vs_128": {
                    key: describe(values) for key, values in predecessor.items()
                },
            },
            "safety_metric_presence": {
                field: {
                    "present": counts[0],
                    "tasks": counts[1],
                    "fraction": counts[0] / counts[1] if counts[1] else None,
                }
                for field, counts in safety_presence.items()
            },
            "freddie_selected_path_count_error": path_count_error,
        }

    yellow_scores = estimator_scores["yellow"]
    selected = min(
        METHODS,
        key=lambda method: (
            yellow_scores[method]["absolute_log_error"]["median"],
            yellow_scores[method]["absolute_log_error"]["p90"],
            method,
        ),
    )
    output["yellow_selected_estimator"] = {
        "method": selected,
        "selection_rule": "minimum Yellow median absolute log error, then p90",
        "status": "exploratory training selection",
    }
    output["selected_estimator_transfer"] = {
        dataset: estimator_scores[dataset][selected]
        for dataset in DATASETS
    }
    with open(args.output, "x", encoding="utf-8") as stream:
        json.dump(output, stream, indent=2, sort_keys=True)
        stream.write("\n")


if __name__ == "__main__":
    main()
