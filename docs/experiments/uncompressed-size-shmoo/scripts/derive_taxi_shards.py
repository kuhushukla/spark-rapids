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

"""Derive small, provenance-preserving Parquet shards from each taxi source file."""

import argparse
import hashlib
import json
import math
import os
import time


MIB = 1024 * 1024


def sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        for chunk in iter(lambda: stream.read(MIB), b""):
            digest.update(chunk)
    return digest.hexdigest()


def derived_files(path):
    values = []
    for name in sorted(os.listdir(path)):
        full_path = os.path.join(path, name)
        if os.path.isfile(full_path) and name.endswith(".parquet"):
            values.append({
                "bytes": os.path.getsize(full_path),
                "file": name,
                "sha256": sha256(full_path),
            })
    return values


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--census", required=True)
    parser.add_argument("--data-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--only-source", action="append")
    parser.add_argument("--target-compressed-mib", type=int, default=16)
    parser.add_argument("--row-group-mib", type=int, default=32)
    args = parser.parse_args()

    if os.path.exists(args.manifest):
        raise FileExistsError("refusing to overwrite " + args.manifest)
    with open(args.census, encoding="utf-8") as stream:
        census = json.load(stream)
    data_dir = os.path.realpath(args.data_dir)
    if os.path.realpath(census["data_dir"]) != data_dir:
        raise RuntimeError("census data_dir differs from requested source")
    output_dir = os.path.realpath(args.output_dir)
    os.makedirs(output_dir, exist_ok=True)

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName("derive-taxi-small-parquet-shards")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )
    records = []
    started = time.time()
    try:
        sources = sorted(census["files"], key=lambda item: item["file"])
        if args.only_source:
            requested = set(args.only_source)
            sources = [item for item in sources if item["file"] in requested]
            found = {item["file"] for item in sources}
            if found != requested:
                raise RuntimeError(
                    "unknown requested source files: " + str(sorted(requested - found))
                )
        for source in sources:
            source_name = source["file"]
            source_path = os.path.join(data_dir, source_name)
            source_stem = os.path.splitext(source_name)[0]
            destination = os.path.join(output_dir, source_stem)
            success = os.path.join(destination, "_SUCCESS")
            shard_count = max(
                1,
                int(math.ceil(
                    source["file_bytes"] /
                    float(args.target_compressed_mib * MIB)
                )),
            )
            if os.path.exists(destination):
                if not os.path.isfile(success):
                    raise RuntimeError(
                        "partial destination requires manual audit: " + destination
                    )
            else:
                (
                    spark.read.parquet(source_path)
                    .repartition(shard_count)
                    .write
                    .mode("errorifexists")
                    .option("compression", "snappy")
                    .option("parquet.block.size", str(args.row_group_mib * MIB))
                    .parquet(destination)
                )
            files = derived_files(destination)
            if len(files) != shard_count:
                raise RuntimeError(
                    "{} produced {} files, expected {}".format(
                        source_name, len(files), shard_count
                    )
                )
            records.append({
                "derived_bytes": sum(item["bytes"] for item in files),
                "derived_file_count": len(files),
                "derived_files": files,
                "derived_relative_dir": source_stem,
                "expected_rows": sum(
                    row_group["row_count"] for row_group in source["row_groups"]
                ),
                "source_bytes": source["file_bytes"],
                "source_file": source_name,
                "source_sha256": sha256(source_path),
            })
    finally:
        spark.stop()

    result = {
        "elapsed_seconds": time.time() - started,
        "model_role": (
            "real-row distribution-preserving physical representation for split shmoo"
        ),
        "row_group_target_bytes": args.row_group_mib * MIB,
        "source_census_sha256": sha256(args.census),
        "source_data_dir": data_dir,
        "source_file_count": len(records),
        "target_compressed_bytes_per_file": args.target_compressed_mib * MIB,
        "total_derived_bytes": sum(item["derived_bytes"] for item in records),
        "total_derived_files": sum(item["derived_file_count"] for item in records),
        "total_expected_rows": sum(item["expected_rows"] for item in records),
        "records": records,
    }
    os.makedirs(os.path.dirname(os.path.abspath(args.manifest)), exist_ok=True)
    with open(args.manifest, "x", encoding="utf-8") as stream:
        json.dump(result, stream, indent=2, sort_keys=True)
        stream.write("\n")


if __name__ == "__main__":
    main()
