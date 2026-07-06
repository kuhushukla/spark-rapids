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

"""Export committed rolling experiment evidence as spreadsheet-friendly CSV."""

import argparse
import csv
import json
import os


RUN_FIELDS = [
    "dataset", "block", "window_id", "start_month", "end_month", "phase",
    "treatment", "max_partition_mib", "listed_file_bytes", "elapsed_ms",
    "decoded_bytes", "decoded_rows", "max_batch_bytes", "max_batch_rows",
    "output_batches", "planned_scan_tasks", "gpu_scan_in_plan",
    "result_sha256", "plan_sha256", "enabled_reason", "history_count",
    "predicted_decoded_bytes", "predicted_decoded_rows",
    "estimated_decoded_bytes_per_listed_byte",
    "estimated_decoded_rows_per_listed_byte",
]

WINDOW_FIELDS = [
    "dataset", "block", "window_id", "start_month", "end_month",
    "selected_path_count", "listed_file_bytes", "selected_mib",
    "history_count", "predicted_decoded_bytes", "actual_decoded_bytes",
    "decoded_byte_ape", "predicted_decoded_rows", "actual_decoded_rows",
    "decoded_row_ape", "enabled_elapsed_ms", "fixed_128_elapsed_ms",
    "fixed_1024_elapsed_ms", "enabled_regret", "enabled_scan_tasks",
    "fixed_128_scan_tasks", "fixed_1024_scan_tasks",
]


def load_json(path):
    with open(path, encoding="utf-8") as stream:
        return json.load(stream)


def write_csv(path, fields, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(
            stream, fieldnames=fields, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def run_rows(experiment_dir, datasets):
    rows = []
    for dataset in datasets:
        path = os.path.join(experiment_dir, "raw", dataset, "results.jsonl")
        with open(path, encoding="utf-8") as stream:
            for line in stream:
                record = json.loads(line)
                metrics = record["direct_scan_metrics"]
                decision = record.get("enabled_decision") or {}
                rows.append({
                    **record,
                    **metrics,
                    "enabled_reason": decision.get("reason"),
                    "history_count": decision.get("history_count"),
                    "predicted_decoded_bytes": decision.get("predicted_decoded_bytes"),
                    "predicted_decoded_rows": decision.get("predicted_decoded_rows"),
                    "estimated_decoded_bytes_per_listed_byte":
                        decision.get("decoded_bytes_per_listed_byte"),
                    "estimated_decoded_rows_per_listed_byte":
                        decision.get("decoded_rows_per_listed_byte"),
                })
    return rows


def window_rows(experiment_dir, schedule):
    result = load_json(os.path.join(experiment_dir, "analysis", "result.json"))
    schedule_by_dataset = {
        dataset["logical_table"]: {
            window["window_id"]: window for window in dataset["windows"]
        }
        for dataset in schedule["datasets"]
    }
    rows = []
    for dataset in schedule["datasets"]:
        name = dataset["logical_table"]
        windows = schedule_by_dataset[name]
        trajectory = result["datasets"][name]["trajectory"]
        for block, record in enumerate(trajectory):
            window = windows[record["window_id"]]
            rows.append({
                "dataset": name,
                "block": block,
                "window_id": record["window_id"],
                "start_month": window["start_month"],
                "end_month": window["end_month"],
                "selected_path_count": len(window["paths"]),
                "listed_file_bytes": window["listed_file_bytes"],
                **record,
            })
    return rows


def main():
    default_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--experiment-dir", default=default_dir)
    parser.add_argument("--output-dir")
    args = parser.parse_args()

    experiment_dir = os.path.abspath(args.experiment_dir)
    output_dir = args.output_dir or os.path.join(
        experiment_dir, "raw", "spreadsheet")
    schedule = load_json(os.path.join(experiment_dir, "schedule.json"))
    datasets = [item["logical_table"] for item in schedule["datasets"]]

    write_csv(
        os.path.join(output_dir, "run-results.csv"),
        RUN_FIELDS,
        run_rows(experiment_dir, datasets),
    )
    write_csv(
        os.path.join(output_dir, "window-summary.csv"),
        WINDOW_FIELDS,
        window_rows(experiment_dir, schedule),
    )
    print(output_dir)


if __name__ == "__main__":
    main()
