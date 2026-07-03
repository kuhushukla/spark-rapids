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

"""Mechanically validate the pre-treatment metadata census and planning compliance."""

import argparse
import hashlib
import json
import os


EXPECTED_EPISODES = {
    "fixed_2009_first_3": "common",
    "fixed_2009_all_12": "common",
    "variable_2010_first_3": "variable_width",
    "variable_2010_all_12": "variable_width",
    "evolution_before_2011": "missing_location",
    "evolution_through_2011": "missing_location",
}
EXPECTED_CANDIDATES = [64, 128, 256, 512, 1024, 2048]


def load(path):
    with open(path, encoding="utf-8") as stream:
        return json.load(stream)


def sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate_planning_evidence(
        planning, census_path, expected_fields, expected_scan_class, require_gpu):
    if set(planning.get("validated_fields", [])) != expected_fields:
        raise RuntimeError("planning compliance does not validate the exact layout")
    if not planning["all_match"] or planning["record_count"] != 36:
        raise RuntimeError("planning compliance is incomplete")
    if planning["census_sha256"] != sha256(census_path):
        raise RuntimeError("planning compliance references a different census")
    if planning.get("required_gpu_scan") is not require_gpu:
        raise RuntimeError("planning compliance GPU requirement mismatch")
    if planning.get("scan_classes") != [expected_scan_class]:
        raise RuntimeError("planning compliance scan class mismatch")
    if not all(
            item["match"] and item.get("scan_class") == expected_scan_class
            for item in planning["records"]):
        raise RuntimeError("at least one Spark planning comparison failed")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--census", required=True)
    parser.add_argument("--planning-compliance", required=True)
    parser.add_argument("--gpu-planning-compliance", required=True)
    parser.add_argument("--source-hashes", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if os.path.exists(args.output):
        raise FileExistsError("refusing to overwrite " + args.output)

    census = load(args.census)
    planning = load(args.planning_compliance)
    gpu_planning = load(args.gpu_planning_compliance)
    sources = load(args.source_hashes)

    if census["file_count"] != 36 or sources["file_count"] != 36:
        raise RuntimeError("expected 36 census and source files")
    if census["total_file_bytes"] != sources["total_bytes"]:
        raise RuntimeError("census/source total bytes differ")
    source_by_name = {item["file"]: item for item in sources["files"]}
    for item in census["files"]:
        source = source_by_name.get(item["file"])
        if source is None or source["bytes"] != item["file_bytes"]:
            raise RuntimeError("source identity mismatch for " + item["file"])
        if len(item["row_groups"]) != 1:
            raise RuntimeError("expected one row group in " + item["file"])

    if census["candidates_mib"] != EXPECTED_CANDIDATES:
        raise RuntimeError("unexpected candidate family")
    if set(census["episodes"]) != set(EXPECTED_EPISODES):
        raise RuntimeError("unexpected episode registry")
    distinct_counts = {}
    for name, expected_query in EXPECTED_EPISODES.items():
        episode = census["episodes"][name]
        if episode["query"] != expected_query:
            raise RuntimeError("query mismatch for " + name)
        physical = {
            item["physical_layout_sha256"] for item in episode["candidate_layouts"]
        }
        distinct_counts[name] = len(physical)
        if len(physical) < 2:
            raise RuntimeError("episode lacks distinct treatment layouts: " + name)

    if census["collector"]["metadata_elapsed_ms"] > 120000:
        raise RuntimeError("metadata collection exceeded 120 seconds")
    expected_validated_fields = {
        "planned_tasks",
        "planned_ranges",
        "useful_tasks",
        "empty_tasks",
        "empty_ranges",
        "physical_layout_sha256",
        "useful_layout_sha256",
    }
    validate_planning_evidence(
        planning,
        args.census,
        expected_validated_fields,
        "FileSourceScanExec",
        False,
    )
    validate_planning_evidence(
        gpu_planning,
        args.census,
        expected_validated_fields,
        "GpuFileSourceScanExec",
        True,
    )

    result = {
        "census_sha256": sha256(args.census),
        "distinct_physical_layout_count": distinct_counts,
        "file_count": census["file_count"],
        "metadata_budget_ms": 120000,
        "metadata_elapsed_ms": census["collector"]["metadata_elapsed_ms"],
        "cpu_planning_comparisons": planning["record_count"],
        "cpu_planning_exact_match_count": sum(
            1 for item in planning["records"] if item["match"]
        ),
        "cpu_planning_sha256": sha256(args.planning_compliance),
        "cpu_scan_class": "FileSourceScanExec",
        "gpu_planning_comparisons": gpu_planning["record_count"],
        "gpu_planning_exact_match_count": sum(
            1 for item in gpu_planning["records"] if item["match"]
        ),
        "gpu_planning_sha256": sha256(args.gpu_planning_compliance),
        "gpu_scan_class": "GpuFileSourceScanExec",
        "planning_validated_fields": sorted(expected_validated_fields),
        "schema_fingerprint_count": len(census["schema_groups"]),
        "sensor_feasibility": "SUPPORTED",
        "source_hashes_sha256": sha256(args.source_hashes),
        "treatment_distinctness": "SUPPORTED",
        "total_file_bytes": census["total_file_bytes"],
        "total_rows": census["total_rows"],
    }
    os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
    with open(args.output, "x", encoding="utf-8") as stream:
        json.dump(result, stream, indent=2, sort_keys=True)
        stream.write("\n")


if __name__ == "__main__":
    main()
