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

"""Freeze contiguous rolling windows and randomized treatment order from file listings."""

import argparse
import datetime
import hashlib
import json
import os
import random
import re


SEED = 730241
TREATMENTS = ("enabled", "fixed-128", "fixed-1024")
TAXI_TABLES = {
    "yellow": {
        "directory": "yellow",
        "query_kind": "taxi-double",
        "fields": [
            ["trip_distance", "double"],
            ["fare_amount", "double"],
            ["total_amount", "double"],
        ],
    },
    "green": {
        "directory": "green",
        "query_kind": "taxi-double",
        "fields": [
            ["trip_distance", "double"],
            ["fare_amount", "double"],
            ["total_amount", "double"],
        ],
    },
    "for-hire": {
        "directory": "for-hire",
        "query_kind": "fhv-string",
        "fields": [
            ["dispatching_base_num", "string"],
            ["Affiliated_base_number", "string"],
            ["pickup_datetime", "timestamp"],
        ],
    },
    "high-volume-for-hire": {
        "directory": "high-volume-for-hire",
        "query_kind": "taxi-double",
        "fields": [
            ["trip_miles", "double"],
            ["base_passenger_fare", "double"],
            ["driver_pay", "double"],
        ],
    },
}


def month_id(year, month):
    return year * 12 + month - 1


def month_text(value):
    return "{:04d}-{:02d}".format(value // 12, value % 12 + 1)


def file_identity(path):
    stat = os.stat(path)
    return {
        "path": os.path.abspath(path),
        "size": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
    }


def stable_hash(value):
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def taxi_dataset(root, name, spec, rng):
    directory = os.path.join(root, spec["directory"])
    monthly = {}
    for current, _, files in os.walk(directory):
        for filename in files:
            if not filename.endswith(".parquet"):
                continue
            match = re.search(r"(20\d\d|19\d\d)-(\d\d)", filename)
            if not match:
                continue
            key = month_id(int(match.group(1)), int(match.group(2)))
            path = os.path.join(current, filename)
            if key in monthly:
                raise ValueError("duplicate month {} for {}".format(month_text(key), name))
            monthly[key] = file_identity(path)
    ordered = sorted(monthly)
    if len(ordered) < 12:
        raise ValueError("fewer than 12 months for " + name)
    for previous, current in zip(ordered, ordered[1:]):
        if current != previous + 1:
            raise ValueError(
                "non-contiguous {} coverage: {} to {}".format(
                    name, month_text(previous), month_text(current)))
    windows = []
    for index in range(len(ordered) - 11):
        months = ordered[index:index + 12]
        paths = [monthly[value] for value in months]
        order = list(TREATMENTS)
        rng.shuffle(order)
        windows.append({
            "window_id": "{}-{}".format(name, month_text(months[0])),
            "start_month": month_text(months[0]),
            "end_month": month_text(months[-1]),
            "paths": [item["path"] for item in paths],
            "listed_file_bytes": sum(item["size"] for item in paths),
            "treatment_order": order,
        })
    inventory = [monthly[key] for key in ordered]
    return {
        "logical_table": name,
        "format": "parquet",
        "query_kind": spec["query_kind"],
        "fields": spec["fields"],
        "root": os.path.abspath(directory),
        "coverage": [month_text(ordered[0]), month_text(ordered[-1])],
        "inventory": inventory,
        "inventory_sha256": stable_hash(inventory),
        "windows": windows,
    }


def loan_dataset(root, rng):
    table_root = os.path.join(root, "parquet", "stacr_dnhq")
    first = month_id(2013, 7)
    last = month_id(2026, 1)
    windows = []
    inventory = []
    for current, _, files in os.walk(table_root):
        for filename in sorted(files):
            if filename.endswith(".parquet"):
                inventory.append(file_identity(os.path.join(current, filename)))
    inventory.sort(key=lambda item: item["path"])
    for start in range(first, last - 10):
        end = start + 11
        years = sorted({value // 12 for value in range(start, end + 1)})
        paths = [os.path.join(table_root, "year={}".format(year)) for year in years]
        files = [
            item for item in inventory
            if any(item["path"].startswith(path + os.sep) for path in paths)
        ]
        if not files:
            raise ValueError("no loan files for " + month_text(start))
        order = list(TREATMENTS)
        rng.shuffle(order)
        windows.append({
            "window_id": "fre-crt-stacr-dnhq-{}".format(month_text(start)),
            "start_month": month_text(start),
            "end_month": month_text(end),
            "predicate_start": int(month_text(start).replace("-", "")),
            "predicate_end": int(month_text(end).replace("-", "")),
            "paths": paths,
            "listed_file_bytes": sum(item["size"] for item in files),
            "treatment_order": order,
        })
    return {
        "logical_table": "fre-crt-stacr-dnhq",
        "format": "parquet",
        "query_kind": "loan",
        "fields": [
            ["period", "int"],
            ["current_actual_upb", "double"],
            ["credit_score", "long"],
            ["payment_history", "string"],
        ],
        "root": os.path.abspath(table_root),
        "coverage": [month_text(first), month_text(last)],
        "inventory": inventory,
        "inventory_sha256": stable_hash(inventory),
        "windows": windows,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--taxi-root", required=True)
    parser.add_argument("--loan-root", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if os.path.exists(args.output):
        raise FileExistsError("refusing to overwrite " + args.output)

    rng = random.Random(SEED)
    datasets = [
        taxi_dataset(args.taxi_root, name, spec, rng)
        for name, spec in TAXI_TABLES.items()
    ]
    datasets.append(loan_dataset(args.loan_root, rng))
    for dataset in datasets:
        count = len(dataset["windows"])
        dataset["cpu_reference_window_indices"] = sorted({0, count // 2, count - 1})
        dataset["warmup_treatments"] = list(TREATMENTS)

    output = {
        "schema_version": "rolling-split-autotuning/schedule-v1",
        "created_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "random_seed": SEED,
        "treatments": list(TREATMENTS),
        "datasets": datasets,
        "window_count": sum(len(item["windows"]) for item in datasets),
        "run_count": sum(3 * len(item["windows"]) for item in datasets),
    }
    os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
    with open(args.output, "x", encoding="utf-8") as stream:
        json.dump(output, stream, indent=2, sort_keys=True)
        stream.write("\n")


if __name__ == "__main__":
    main()
