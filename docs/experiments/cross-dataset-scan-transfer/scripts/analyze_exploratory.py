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

"""Post-hoc decomposition; never changes the preregistered verdict."""

import argparse
import json
import os
import statistics


def percentile(values, fraction):
    ordered = sorted(values)
    return ordered[round(fraction * (len(ordered) - 1))]


def summarize(values):
    return {
        "count": len(values),
        "median": statistics.median(values),
        "p90": percentile(values, 0.90),
        "min": min(values),
        "max": max(values),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--gpu-journal", required=True)
    parser.add_argument("--scan-summary", required=True)
    parser.add_argument("--frozen-result", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if os.path.exists(args.output):
        raise FileExistsError("refusing to overwrite " + args.output)

    with open(args.gpu_journal, encoding="utf-8") as stream:
        journal = {
            row["run_id"]: row for row in (json.loads(line) for line in stream)
        }
    with open(args.scan_summary, encoding="utf-8") as stream:
        scan = json.load(stream)
    groups = {"train": [], "holdout": []}
    for run in scan["runs"]:
        metadata = journal[run["run_id"]]
        metrics = run["task_metrics"]
        input_bytes = metrics["input_bytes"]["sum"]
        output_bytes = metrics["output_batch_bytes"]["sum"]
        output_rows = metrics["output_rows"]["sum"]
        groups[metadata["role"]].append(
            {
                "run_id": metadata["protocol_run_id"],
                "rows_per_input_byte": output_rows / input_bytes,
                "output_bytes_per_row": output_bytes / output_rows,
            }
        )

    train_width = statistics.median(
        row["output_bytes_per_row"] for row in groups["train"]
    )
    width_errors = [
        abs(train_width - row["output_bytes_per_row"]) / row["output_bytes_per_row"]
        for row in groups["holdout"]
    ]
    with open(args.frozen_result, encoding="utf-8") as stream:
        frozen = json.load(stream)
    footprint_pairs = [
        task
        for run in frozen["holdout"]["predictions"]
        for task in run["task_footprint_predictions"]
    ]
    required_margins = [
        task["actual_footprint_bytes"] / task["empirical_upper_footprint_bytes"] - 1.0
        for task in footprint_pairs
    ]
    margin_coverages = {}
    for margin in (0.0, 0.000001, 0.001, 0.01):
        hits = sum(
            task["actual_footprint_bytes"]
            <= task["empirical_upper_footprint_bytes"] * (1.0 + margin)
            for task in footprint_pairs
        )
        margin_coverages[str(margin)] = hits / len(footprint_pairs)

    output = {
        "schema_version": "cross-dataset-scan-transfer/exploratory-v1",
        "label": "POST_HOC_EXPLORATORY_DO_NOT_CHANGE_FROZEN_VERDICT",
        "frozen_verdict": frozen["verdict"],
        "rows_per_input_byte": {
            "training": summarize(
                [row["rows_per_input_byte"] for row in groups["train"]]
            ),
            "holdout": summarize(
                [row["rows_per_input_byte"] for row in groups["holdout"]]
            ),
        },
        "output_bytes_per_row": {
            "training": summarize(
                [row["output_bytes_per_row"] for row in groups["train"]]
            ),
            "holdout": summarize(
                [row["output_bytes_per_row"] for row in groups["holdout"]]
            ),
            "training_median_holdout_error": summarize(width_errors),
        },
        "footprint_empirical_upper": {
            "required_relative_margin": summarize(required_margins),
            "coverage_by_relative_margin": margin_coverages,
        },
    }
    os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
    with open(args.output, "x", encoding="utf-8") as stream:
        json.dump(output, stream, indent=2, sort_keys=True)
        stream.write("\n")


if __name__ == "__main__":
    main()
