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

"""Freeze the Stage-1 episode registry and deterministic treatment allocation."""

import argparse
import hashlib
import json
import os
import random


SCHEMA_VERSION = 1
DEFAULT_SEED = 20260703
STAGE1_EPISODES = (
    ("fixed_2009_all_12", (128, 256, 512)),
    ("variable_2010_all_12", (128, 256, 512, 1024)),
    ("evolution_through_2011", (128, 256, 512, 1024)),
)


def file_sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        while True:
            chunk = stream.read(1024 * 1024)
            if not chunk:
                return digest.hexdigest()
            digest.update(chunk)


def write_json_exclusive(path, value):
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "x", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2, sort_keys=True)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())


def selected_layouts(episode_name, episode, candidates):
    by_mib = {
        int(layout["configured_mib"]): layout
        for layout in episode["candidate_layouts"]
    }
    if len(by_mib) != len(episode["candidate_layouts"]):
        raise ValueError("duplicate candidates in census episode " + episode_name)
    missing = sorted(set(candidates) - set(by_mib))
    if missing:
        raise ValueError(
            "census episode {} lacks candidates {}".format(episode_name, missing)
        )
    return [
        {
            "absent_columns": list(by_mib[mib]["absent_columns"]),
            "configured_mib": mib,
            "dominated_by_mib": [
                int(value) for value in by_mib[mib]["dominated_by_mib"]
                if int(value) in candidates
            ],
            "effective_bytes": int(by_mib[mib]["effective_bytes"]),
            "missing_column_materialized_bytes_status":
                by_mib[mib]["missing_column_materialized_bytes_status"],
            "missing_column_row_values":
                int(by_mib[mib]["missing_column_row_values"]),
            "physical_layout_sha256": by_mib[mib]["physical_layout_sha256"],
            "planned_ranges": int(by_mib[mib]["planned_ranges"]),
            "planned_tasks": int(by_mib[mib]["planned_tasks"]),
            "predicted_empty_ranges": int(by_mib[mib]["empty_ranges"]),
            "predicted_empty_tasks": int(by_mib[mib]["empty_tasks"]),
            "predicted_useful_tasks": int(by_mib[mib]["useful_tasks"]),
            "present_columns": list(by_mib[mib]["present_columns"]),
            "useful_layout_sha256": by_mib[mib]["useful_layout_sha256"],
        }
        for mib in candidates
    ]


def treatment(episode, configured_mib):
    return {
        "configured_mib": configured_mib,
        "episode": episode,
        "treatment_id": "{}:{}m".format(episode, configured_mib),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--census", required=True)
    parser.add_argument("--registry-output", required=True)
    parser.add_argument("--schedule-output", required=True)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    args = parser.parse_args()

    for path in (args.registry_output, args.schedule_output):
        if os.path.exists(path):
            raise FileExistsError("refusing to overwrite " + path)

    with open(args.census, encoding="utf-8") as stream:
        census = json.load(stream)
    census_sha256 = file_sha256(args.census)

    registry_episodes = []
    for episode_name, candidates in STAGE1_EPISODES:
        if episode_name not in census["episodes"]:
            raise ValueError("missing census episode " + episode_name)
        episode = census["episodes"][episode_name]
        registry_episodes.append({
            "candidate_layouts": selected_layouts(
                episode_name, episode, candidates
            ),
            "candidates_mib": list(candidates),
            "episode": episode_name,
            "file_count": int(episode["file_count"]),
            "files": list(episode["files"]),
            "query": episode["query"],
            "total_file_bytes": int(episode["total_file_bytes"]),
            "total_rows": int(episode["total_rows"]),
        })

    treatment_count = sum(
        len(episode["candidates_mib"]) for episode in registry_episodes
    )
    if treatment_count != 11:
        raise AssertionError("expected exactly 11 Stage-1 treatments")

    registry = {
        "census_sha256": census_sha256,
        "data_dir": census["data_dir"],
        "episode_count": len(registry_episodes),
        "episodes": registry_episodes,
        "open_cost_bytes": int(census["open_cost_bytes"]),
        "min_partitions": int(census["min_partitions"]),
        "schema_version": SCHEMA_VERSION,
        "treatment_count": treatment_count,
    }
    write_json_exclusive(args.registry_output, registry)
    registry_sha256 = file_sha256(args.registry_output)

    treatments = [
        treatment(episode["episode"], mib)
        for episode in registry_episodes
        for mib in episode["candidates_mib"]
    ]
    measured_blocks = []
    rng = random.Random(args.seed)
    for block in range(2):
        order = list(treatments)
        rng.shuffle(order)
        measured_blocks.append({
            "block": block,
            "treatments": order,
        })

    schedule = {
        "blocks": 2,
        "census_sha256": census_sha256,
        "cpu_reference_order": [
            episode["episode"] for episode in registry_episodes
        ],
        "measured_blocks": measured_blocks,
        "registry_sha256": registry_sha256,
        "schema_version": SCHEMA_VERSION,
        "seed": args.seed,
        "treatment_count": treatment_count,
        "warmup_order": treatments,
    }
    write_json_exclusive(args.schedule_output, schedule)


if __name__ == "__main__":
    main()
