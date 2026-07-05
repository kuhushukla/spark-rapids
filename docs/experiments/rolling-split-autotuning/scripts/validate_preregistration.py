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

"""Mechanically validate the frozen rolling experiment inputs."""

import argparse
import hashlib
import json
import os


TREATMENTS = {"enabled", "fixed-128", "fixed-1024"}
EXPECTED_DATASETS = {
    "yellow",
    "green",
    "for-hire",
    "high-volume-for-hire",
    "fre-crt-stacr-dnhq",
}


def sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--experiment-dir", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if os.path.exists(args.output):
        raise FileExistsError("refusing to overwrite " + args.output)
    root = os.path.abspath(args.experiment_dir)
    with open(os.path.join(root, "schedule.json"), encoding="utf-8") as stream:
        schedule = json.load(stream)

    failures = []
    names = {item["logical_table"] for item in schedule["datasets"]}
    if names != EXPECTED_DATASETS:
        failures.append("dataset set mismatch")
    window_count = 0
    for dataset in schedule["datasets"]:
        windows = dataset["windows"]
        window_count += len(windows)
        for index, window in enumerate(windows):
            if set(window["treatment_order"]) != TREATMENTS:
                failures.append("treatment permutation " + window["window_id"])
            if len(window["treatment_order"]) != 3:
                failures.append("duplicate treatment " + window["window_id"])
            if window["listed_file_bytes"] <= 0:
                failures.append("non-positive listed bytes " + window["window_id"])
            if dataset["query_kind"] != "loan" and len(window["paths"]) != 12:
                failures.append("taxi window does not contain 12 files " + window["window_id"])
            for path in window["paths"]:
                if not os.path.exists(path):
                    failures.append("missing path " + path)
                if not os.path.abspath(path).startswith("/data/"):
                    failures.append("data path outside /data " + path)
        for item in dataset["inventory"]:
            path = item["path"]
            if not os.path.isfile(path):
                failures.append("missing inventory file " + path)
            else:
                stat = os.stat(path)
                if stat.st_size != item["size"] or stat.st_mtime_ns != item["mtime_ns"]:
                    failures.append("inventory identity changed " + path)
    if window_count != schedule["window_count"]:
        failures.append("window count mismatch")
    if schedule["run_count"] != 3 * window_count:
        failures.append("run count mismatch")

    forbidden = {
        "benchmark.py": (
            "ParquetFileReader",
            "pyarrow.parquet",
            "readFooter",
            "mergeSchema",
        ),
    }
    for filename, tokens in forbidden.items():
        path = os.path.join(root, "scripts", filename)
        with open(path, encoding="utf-8") as stream:
            text = stream.read()
        for token in tokens:
            if token in text:
                failures.append("forbidden tuning input token {} in {}".format(
                    token, filename))

    output = {
        "schema_version": "rolling-split-autotuning/prereg-validation-v1",
        "valid": not failures,
        "failures": failures,
        "window_count": window_count,
        "run_count": schedule["run_count"],
        "schedule_sha256": sha256(os.path.join(root, "schedule.json")),
        "script_sha256": {
            filename: sha256(os.path.join(root, "scripts", filename))
            for filename in (
                "prepare_schedule.py",
                "benchmark.py",
                "validate_cpu.py",
                "analyze.py",
                "run_experiment.sh",
            )
        },
    }
    os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
    with open(args.output, "x", encoding="utf-8") as stream:
        json.dump(output, stream, indent=2, sort_keys=True)
        stream.write("\n")
    if failures:
        raise SystemExit("preregistration validation failed")


if __name__ == "__main__":
    main()
