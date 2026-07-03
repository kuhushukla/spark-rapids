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

"""Census Parquet metadata and simulate Spark 3.5.5 file-partition candidates."""

import argparse
import hashlib
import json
import math
import os
import re
import statistics
import time


MIB = 1024 * 1024
QUERY_COLUMNS = {
    "common": {"passenger_count", "trip_distance"},
    "variable_width": {
        "passenger_count", "trip_distance", "payment_type"
    },
    "missing_location": {
        "passenger_count", "trip_distance", "pulocationid"
    },
}
COLUMN_ALIASES = {
    "passenger_count": "passenger_count",
    "trip_distance": "trip_distance",
    "total_amt": "total_amount",
    "total_amount": "total_amount",
    "pulocationid": "pulocationid",
    "payment_type": "payment_type",
    "congestion_surcharge": "congestion_surcharge",
    "airport_fee": "airport_fee",
}


def canonical_column(name):
    return COLUMN_ALIASES.get(name.lower(), name.lower())


def stable_hash(value):
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def quantiles(values):
    if not values:
        return {"count": 0}
    ordered = sorted(values)
    def at(fraction):
        return ordered[round(fraction * (len(ordered) - 1))]
    return {
        "count": len(ordered),
        "min": ordered[0],
        "p25": at(0.25),
        "median": statistics.median(ordered),
        "p75": at(0.75),
        "max": ordered[-1],
    }


def read_file_metadata(spark, filename):
    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    path = jvm.org.apache.hadoop.fs.Path(os.path.abspath(filename))
    input_file = jvm.org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(path, conf)
    reader = jvm.org.apache.parquet.hadoop.ParquetFileReader.open(input_file)
    try:
        footer = reader.getFooter()
        schema = str(footer.getFileMetaData().getSchema())
        row_groups = []
        blocks = footer.getBlocks()
        for row_group_index in range(blocks.size()):
            block = blocks.get(row_group_index)
            columns = {}
            starts = []
            chunks = block.getColumns()
            for column_index in range(chunks.size()):
                chunk = chunks.get(column_index)
                physical_name = chunk.getPath().toDotString()
                logical_name = canonical_column(physical_name)
                starts.append(int(chunk.getStartingPos()))
                stats = chunk.getStatistics()
                null_count = None
                if stats is not None and not stats.isEmpty() and stats.isNumNullsSet():
                    null_count = int(stats.getNumNulls())
                columns[logical_name] = {
                    "compressed_bytes": int(chunk.getTotalSize()),
                    "uncompressed_bytes": int(chunk.getTotalUncompressedSize()),
                    "codec": str(chunk.getCodec()),
                    "null_count": null_count,
                    "physical_name": physical_name,
                    "starting_pos": int(chunk.getStartingPos()),
                }
            compressed_bytes = sum(
                c["compressed_bytes"] for c in columns.values()
            )
            starting_pos = (
                int(block.getStartingPos()) if starts else None
            )
            row_groups.append({
                "columns": columns,
                "compressed_bytes": compressed_bytes,
                "index": row_group_index,
                "midpoint": (
                    starting_pos + compressed_bytes // 2
                    if starting_pos is not None else None
                ),
                "row_count": int(block.getRowCount()),
                "starting_pos": starting_pos,
                "uncompressed_bytes": int(block.getTotalByteSize()),
            })
        return {
            "absolute_path": os.path.abspath(filename),
            "file": os.path.basename(filename),
            "file_bytes": os.path.getsize(filename),
            "modification_time_ns": os.stat(filename).st_mtime_ns,
            "row_groups": row_groups,
            "schema": schema,
            "schema_sha256": hashlib.sha256(schema.encode("utf-8")).hexdigest(),
        }
    finally:
        reader.close()


def split_ranges(files, max_split_bytes):
    ranges = []
    for file_info in files:
        start = 0
        length = file_info["file_bytes"]
        while start < length:
            split_length = min(max_split_bytes, length - start)
            ranges.append({
                "file": file_info["file"],
                "length": split_length,
                "start": start,
            })
            start += split_length
    return sorted(ranges, key=lambda item: item["length"], reverse=True)


def pack_ranges(ranges, max_split_bytes, open_cost_bytes):
    partitions = []
    current = []
    current_size = 0
    for byte_range in ranges:
        if current and current_size + byte_range["length"] > max_split_bytes:
            partitions.append(current)
            current = []
            current_size = 0
        current.append(byte_range)
        current_size += byte_range["length"] + open_cost_bytes
    if current:
        partitions.append(current)
    return partitions


def task_layout(files, configured_bytes, open_cost_bytes, min_partitions):
    total_bytes = sum(item["file_bytes"] + open_cost_bytes for item in files)
    bytes_per_core = total_bytes // min_partitions
    effective_bytes = min(
        configured_bytes,
        max(open_cost_bytes, bytes_per_core),
    )
    ranges = split_ranges(files, effective_bytes)
    partitions = pack_ranges(ranges, effective_bytes, open_cost_bytes)
    by_file = {item["file"]: item for item in files}
    tasks = []
    for task_index, partition in enumerate(partitions):
        owned = []
        task_ranges = []
        for byte_range in partition:
            range_end = byte_range["start"] + byte_range["length"]
            range_owned = []
            for row_group in by_file[byte_range["file"]]["row_groups"]:
                row_group_midpoint = row_group["midpoint"]
                if (
                    row_group_midpoint is not None
                    and byte_range["start"] <= row_group_midpoint < range_end
                ):
                    range_owned.append((byte_range["file"], row_group["index"]))
            owned.extend(range_owned)
            task_ranges.append({
                **byte_range,
                "row_groups": sorted(set(range_owned)),
            })
        owned = sorted(set(owned))
        tasks.append({
            "index": task_index,
            "ranges": task_ranges,
            "row_groups": owned,
        })
    return effective_bytes, tasks


def projected_row_group(file_info, row_group, query_columns):
    present = sorted(set(row_group["columns"]) & query_columns)
    absent = sorted(query_columns - set(row_group["columns"]))
    return {
        "absent_columns": absent,
        "compressed_bytes": sum(
            row_group["columns"][name]["compressed_bytes"] for name in present
        ),
        "present_columns": present,
        "row_count": row_group["row_count"],
        "uncompressed_bytes": sum(
            row_group["columns"][name]["uncompressed_bytes"] for name in present
        ),
    }


def simulate(files, candidate_mib, query_columns, open_cost_bytes, min_partitions):
    effective_bytes, tasks = task_layout(
        files, candidate_mib * MIB, open_cost_bytes, min_partitions
    )
    by_group = {
        (item["file"], row_group["index"]): projected_row_group(
            item, row_group, query_columns
        )
        for item in files
        for row_group in item["row_groups"]
    }
    useful = []
    empty = 0
    for task in tasks:
        if not task["row_groups"]:
            empty += 1
            continue
        groups = [by_group[key] for key in task["row_groups"]]
        useful.append({
            "compressed_bytes": sum(item["compressed_bytes"] for item in groups),
            "row_count": sum(item["row_count"] for item in groups),
            "row_groups": task["row_groups"],
            "uncompressed_bytes": sum(item["uncompressed_bytes"] for item in groups),
        })
    useful_signature = sorted(
        [item["row_groups"] for item in useful],
        key=lambda groups: json.dumps(groups, separators=(",", ":")),
    )
    range_count = sum(len(task["ranges"]) for task in tasks)
    empty_ranges = sum(
        1 for task in tasks for byte_range in task["ranges"]
        if not byte_range["row_groups"]
    )
    physical_layout = [
        [
            {
                "file": byte_range["file"],
                "length": byte_range["length"],
                "row_groups": byte_range["row_groups"],
                "start": byte_range["start"],
            }
            for byte_range in task["ranges"]
        ]
        for task in tasks
    ]
    all_groups = list(by_group.values())
    absent_columns = sorted({
        name for group in all_groups for name in group["absent_columns"]
    })
    present_columns = sorted({
        name for group in all_groups for name in group["present_columns"]
    })
    missing_column_row_values = sum(
        group["row_count"] * len(group["absent_columns"])
        for group in all_groups
    )
    return {
        "absent_columns": absent_columns,
        "configured_mib": candidate_mib,
        "effective_bytes": effective_bytes,
        "empty_ranges": empty_ranges,
        "empty_tasks": empty,
        "missing_column_materialized_bytes": None,
        "missing_column_materialized_bytes_status": "UNMODELED",
        "missing_column_row_values": missing_column_row_values,
        "physical_layout_sha256": stable_hash(physical_layout),
        "planned_ranges": range_count,
        "planned_tasks": len(tasks),
        "present_columns": present_columns,
        "projected_compressed_bytes_per_useful_task": quantiles(
            [item["compressed_bytes"] for item in useful]
        ),
        "projected_uncompressed_bytes_per_useful_task": quantiles(
            [item["uncompressed_bytes"] for item in useful]
        ),
        "rows_per_useful_task": quantiles([item["row_count"] for item in useful]),
        "useful_layout_sha256": stable_hash(useful_signature),
        "useful_tasks": len(useful),
    }


def mark_dominance(candidates):
    for candidate in candidates:
        candidate["dominated_by_mib"] = []
        candidate["equivalent_to_mib"] = []
        for other in candidates:
            if candidate is other:
                continue
            same_layout = (
                candidate["useful_layout_sha256"] == other["useful_layout_sha256"]
            )
            same_physical_work = (
                candidate["physical_layout_sha256"]
                == other["physical_layout_sha256"]
            )
            if same_physical_work:
                candidate["equivalent_to_mib"].append(other["configured_mib"])
            no_better_tasks = (
                candidate["planned_tasks"] >= other["planned_tasks"]
                and candidate["empty_tasks"] >= other["empty_tasks"]
            )
            no_better_ranges = (
                candidate["planned_ranges"] >= other["planned_ranges"]
                and candidate["empty_ranges"] >= other["empty_ranges"]
            )
            strictly_worse = (
                candidate["planned_tasks"] > other["planned_tasks"]
                or candidate["empty_tasks"] > other["empty_tasks"]
                or candidate["planned_ranges"] > other["planned_ranges"]
                or candidate["empty_ranges"] > other["empty_ranges"]
            )
            if same_layout and no_better_tasks and no_better_ranges and strictly_worse:
                candidate["dominated_by_mib"].append(other["configured_mib"])
        candidate["dominated_by_mib"].sort()
        candidate["equivalent_to_mib"].sort()


def episode_specs(file_count):
    required = [
        ("fixed_2009_first_3", 0, 3, "common"),
        ("fixed_2009_all_12", 0, 12, "common"),
        ("variable_2010_first_3", 12, 15, "variable_width"),
        ("variable_2010_all_12", 12, 24, "variable_width"),
        ("evolution_before_2011", 0, 24, "missing_location"),
        ("evolution_through_2011", 0, 36, "missing_location"),
    ]
    if file_count < 36:
        raise ValueError("the registered six episodes require all 36 monthly files")
    return required


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--candidates-mib", default="64,128,256,512,1024,2048")
    parser.add_argument("--open-cost-bytes", type=int, default=4 * MIB)
    parser.add_argument("--min-partitions", type=int, default=8)
    args = parser.parse_args()
    if os.path.exists(args.output):
        raise FileExistsError("refusing to overwrite " + args.output)
    candidates_mib = [int(value) for value in args.candidates_mib.split(",")]
    if not candidates_mib or len(candidates_mib) != len(set(candidates_mib)):
        raise ValueError("candidate list must be non-empty and unique")

    filenames = sorted(
        os.path.join(args.data_dir, name)
        for name in os.listdir(args.data_dir)
        if re.fullmatch(r"yellow_tripdata_\d{4}-\d{2}\.parquet", name)
    )
    if not filenames:
        raise ValueError("no expected taxi parquet files found")

    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("longitudinal-metadata-census").getOrCreate()
    started_ns = time.monotonic_ns()
    try:
        files = [read_file_metadata(spark, filename) for filename in filenames]
    finally:
        spark.stop()
    metadata_elapsed_ms = (time.monotonic_ns() - started_ns) / 1_000_000.0

    episodes = {}
    for episode_name, start, end, query_name in episode_specs(len(files)):
        episode_files = files[start:end]
        query_columns = QUERY_COLUMNS[query_name]
        simulated = [
            simulate(
                episode_files,
                candidate,
                query_columns,
                args.open_cost_bytes,
                args.min_partitions,
            )
            for candidate in candidates_mib
        ]
        mark_dominance(simulated)
        episodes[episode_name] = {
            "candidate_layouts": simulated,
            "file_count": len(episode_files),
            "files": [item["file"] for item in episode_files],
            "query": query_name,
            "total_file_bytes": sum(item["file_bytes"] for item in episode_files),
            "total_rows": sum(
                row_group["row_count"]
                for item in episode_files
                for row_group in item["row_groups"]
            ),
        }

    schema_groups = {}
    for item in files:
        schema_groups.setdefault(item["schema_sha256"], []).append(item["file"])

    result = {
        "candidates_mib": candidates_mib,
        "collector": {
            "decision_time_available": True,
            "metadata_elapsed_ms": metadata_elapsed_ms,
            "scope": "all local immutable Parquet footers",
        },
        "data_dir": os.path.abspath(args.data_dir),
        "episodes": episodes,
        "file_count": len(files),
        "files": files,
        "min_partitions": args.min_partitions,
        "open_cost_bytes": args.open_cost_bytes,
        "query_columns": {
            name: sorted(columns) for name, columns in QUERY_COLUMNS.items()
        },
        "schema_groups": schema_groups,
        "total_file_bytes": sum(item["file_bytes"] for item in files),
        "total_rows": sum(
            row_group["row_count"]
            for item in files
            for row_group in item["row_groups"]
        ),
    }
    os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
    with open(args.output, "x", encoding="utf-8") as stream:
        json.dump(result, stream, indent=2, sort_keys=True)
        stream.write("\n")


if __name__ == "__main__":
    main()
