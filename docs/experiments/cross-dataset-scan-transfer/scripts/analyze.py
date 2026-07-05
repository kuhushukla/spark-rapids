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

"""Evaluate frozen yellow-to-HVFHV component transfer."""

import argparse
import json
import math
import os
import statistics


def load_journal(path):
    with open(path, encoding="utf-8") as stream:
        return {
            row["protocol_run_id"]: row
            for row in (json.loads(line) for line in stream)
        }


def percentile(values, fraction):
    ordered = sorted(values)
    return ordered[round(fraction * (len(ordered) - 1))]


def upper(values):
    return max(values) if len(values) < 5 else percentile(values, 0.90)


def ape(predicted, actual):
    return abs(predicted - actual) / actual if actual else math.inf


def summarize(errors):
    return {
        "count": len(errors),
        "median": statistics.median(errors),
        "p90": percentile(errors, 0.90),
        "max": max(errors),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cpu-journal", required=True)
    parser.add_argument("--gpu-journal", required=True)
    parser.add_argument("--scan-summary", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if os.path.exists(args.output):
        raise FileExistsError("refusing to overwrite " + args.output)

    cpu = load_journal(args.cpu_journal)
    gpu = load_journal(args.gpu_journal)
    if set(cpu) != set(gpu):
        raise ValueError("CPU/GPU run sets differ")
    correctness = {
        run_id: cpu[run_id]["result_sha256"] == gpu[run_id]["result_sha256"]
        for run_id in cpu
    }
    if not all(correctness.values()):
        raise ValueError("CPU/GPU result mismatch")

    with open(args.scan_summary, encoding="utf-8") as stream:
        summary = json.load(stream)
    runs = {}
    for run in summary["runs"]:
        journal = gpu[run["run_id"].removeprefix("gpu-")]
        runs[journal["protocol_run_id"]] = (journal, run)

    safety_fields = (
        "gpu_retry_count",
        "gpu_split_retry_count",
        "gpu_spill_host_bytes",
        "gpu_spill_disk_bytes",
        "spark_disk_spill_bytes",
        "spark_memory_spill_bytes",
    )
    safety_totals = {field: 0 for field in safety_fields}
    training = []
    holdout = []
    for protocol_id, (journal, run) in runs.items():
        for task in run["tasks"]:
            for field in safety_fields:
                safety_totals[field] += task.get(field, 0)
        metrics = run["task_metrics"]
        input_bytes = metrics["input_bytes"]["sum"]
        output_bytes = metrics["output_batch_bytes"]["sum"]
        output_rows = metrics["output_rows"]["sum"]
        if input_bytes <= 0 or output_bytes <= 0 or output_rows <= 0:
            raise ValueError("non-positive scan measurement for " + protocol_id)
        record = {
            "run_id": protocol_id,
            "input_bytes": input_bytes,
            "output_bytes": output_bytes,
            "output_rows": output_rows,
            "byte_ratio": output_bytes / input_bytes,
            "row_ratio": output_rows / input_bytes,
            "tasks": run["tasks"],
        }
        (training if journal["role"] == "train" else holdout).append(record)

    byte_ratios = [row["byte_ratio"] for row in training]
    row_ratios = [row["row_ratio"] for row in training]
    byte_median = statistics.median(byte_ratios)
    row_median = statistics.median(row_ratios)
    byte_upper = upper(byte_ratios)
    row_upper = upper(row_ratios)

    footprint_ratios = []
    for run in training:
        for task in run["tasks"]:
            batch_bytes = task.get("max_output_batch_bytes", 0)
            footprint = task.get("gpu_max_task_footprint", 0)
            if batch_bytes > 0 and footprint > 0:
                footprint_ratios.append(footprint / batch_bytes)
    if not footprint_ratios:
        raise ValueError("training produced no footprint/batch observations")
    footprint_median = statistics.median(footprint_ratios)
    footprint_upper = upper(footprint_ratios)

    byte_errors = []
    row_errors = []
    footprint_errors = []
    footprint_upper_hits = 0
    footprint_count = 0
    predictions = []
    for run in holdout:
        predicted_bytes = round(run["input_bytes"] * byte_median)
        predicted_rows = round(run["input_bytes"] * row_median)
        predicted_upper_bytes = round(run["input_bytes"] * byte_upper)
        predicted_upper_rows = round(run["input_bytes"] * row_upper)
        byte_error = ape(predicted_bytes, run["output_bytes"])
        row_error = ape(predicted_rows, run["output_rows"])
        byte_errors.append(byte_error)
        row_errors.append(row_error)
        task_predictions = []
        for task in run["tasks"]:
            batch_bytes = task.get("max_output_batch_bytes", 0)
            actual_footprint = task.get("gpu_max_task_footprint", 0)
            if batch_bytes <= 0 or actual_footprint <= 0:
                continue
            predicted_footprint = round(batch_bytes * footprint_median)
            predicted_upper_footprint = round(batch_bytes * footprint_upper)
            error = ape(predicted_footprint, actual_footprint)
            footprint_errors.append(error)
            footprint_count += 1
            footprint_upper_hits += int(actual_footprint <= predicted_upper_footprint)
            task_predictions.append(
                {
                    "partition_id": task["partition_id"],
                    "max_output_batch_bytes": batch_bytes,
                    "actual_footprint_bytes": actual_footprint,
                    "predicted_footprint_bytes": predicted_footprint,
                    "empirical_upper_footprint_bytes": predicted_upper_footprint,
                    "absolute_percentage_error": error,
                }
            )
        predictions.append(
            {
                "run_id": run["run_id"],
                "input_bytes": run["input_bytes"],
                "actual_output_bytes": run["output_bytes"],
                "predicted_output_bytes": predicted_bytes,
                "empirical_upper_output_bytes": predicted_upper_bytes,
                "byte_absolute_percentage_error": byte_error,
                "actual_output_rows": run["output_rows"],
                "predicted_output_rows": predicted_rows,
                "empirical_upper_output_rows": predicted_upper_rows,
                "row_absolute_percentage_error": row_error,
                "task_footprint_predictions": task_predictions,
            }
        )

    byte_summary = summarize(byte_errors)
    row_summary = summarize(row_errors)
    footprint_summary = summarize(footprint_errors)
    footprint_coverage = footprint_upper_hits / footprint_count
    gates = {
        "decoded_bytes_median_ape_le_20pct": byte_summary["median"] <= 0.20,
        "decoded_bytes_p90_ape_le_35pct": byte_summary["p90"] <= 0.35,
        "decoded_rows_median_ape_le_20pct": row_summary["median"] <= 0.20,
        "decoded_rows_p90_ape_le_35pct": row_summary["p90"] <= 0.35,
        "footprint_median_ape_le_30pct": footprint_summary["median"] <= 0.30,
        "footprint_empirical_upper_coverage_ge_90pct": footprint_coverage >= 0.90,
        "no_retry_or_spill": all(value == 0 for value in safety_totals.values()),
    }
    output = {
        "schema_version": "cross-dataset-scan-transfer/result-v1",
        "correctness": correctness,
        "training": {
            "run_count": len(training),
            "footprint_task_count": len(footprint_ratios),
            "byte_ratio_median": byte_median,
            "byte_ratio_empirical_upper": byte_upper,
            "row_ratio_median": row_median,
            "row_ratio_empirical_upper": row_upper,
            "footprint_per_max_batch_median": footprint_median,
            "footprint_per_max_batch_empirical_upper": footprint_upper,
        },
        "holdout": {
            "run_count": len(holdout),
            "footprint_task_count": footprint_count,
            "decoded_byte_error": byte_summary,
            "decoded_row_error": row_summary,
            "footprint_error": footprint_summary,
            "footprint_empirical_upper_coverage": footprint_coverage,
            "predictions": predictions,
        },
        "safety_totals": safety_totals,
        "gates": gates,
        "verdict": "SUPPORTED" if all(gates.values()) else "NOT_SUPPORTED",
    }
    os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
    with open(args.output, "x", encoding="utf-8") as stream:
        json.dump(output, stream, indent=2, sort_keys=True)
        stream.write("\n")


if __name__ == "__main__":
    main()
