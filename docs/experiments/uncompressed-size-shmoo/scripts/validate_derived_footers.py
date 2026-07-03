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

"""Validate derived Parquet shards from footer metadata without decoding rows."""

import argparse
import hashlib
import json
import os
import statistics


MIB = 1024 * 1024


def quantiles(values):
    ordered = sorted(values)
    if not ordered:
        return {"count": 0}
    def at(fraction):
        return ordered[round(fraction * (len(ordered) - 1))]
    return {
        "count": len(ordered),
        "max": ordered[-1],
        "median": statistics.median(ordered),
        "min": ordered[0],
        "p25": at(0.25),
        "p75": at(0.75),
        "p95": at(0.95),
    }


def footer_metadata(spark, filename):
    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    path = jvm.org.apache.hadoop.fs.Path(os.path.abspath(filename))
    input_file = jvm.org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(path, conf)
    reader = jvm.org.apache.parquet.hadoop.ParquetFileReader.open(input_file)
    try:
        footer = reader.getFooter()
        schema = str(footer.getFileMetaData().getSchema())
        blocks = footer.getBlocks()
        row_groups = []
        for index in range(blocks.size()):
            block = blocks.get(index)
            row_groups.append({
                "compressed_bytes": sum(
                    int(block.getColumns().get(column).getTotalSize())
                    for column in range(block.getColumns().size())
                ),
                "index": index,
                "row_count": int(block.getRowCount()),
                "uncompressed_parquet_bytes": int(block.getTotalByteSize()),
            })
        return {
            "bytes": os.path.getsize(filename),
            "file": os.path.basename(filename),
            "row_groups": row_groups,
            "schema_sha256": hashlib.sha256(
                schema.encode("utf-8")
            ).hexdigest(),
        }
    finally:
        reader.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--derived-dir", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--max-file-mib", type=int, default=32)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if os.path.exists(args.output):
        raise FileExistsError("refusing to overwrite " + args.output)

    with open(args.manifest, encoding="utf-8") as stream:
        manifest = json.load(stream)

    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("validate-derived-taxi-footers").getOrCreate()
    records = []
    try:
        for source in manifest["records"]:
            directory = os.path.join(
                os.path.realpath(args.derived_dir),
                source["derived_relative_dir"],
            )
            files = [
                os.path.join(directory, name)
                for name in sorted(os.listdir(directory))
                if name.endswith(".parquet")
            ]
            metadata = [footer_metadata(spark, filename) for filename in files]
            rows = sum(
                row_group["row_count"]
                for item in metadata
                for row_group in item["row_groups"]
            )
            if rows != source["expected_rows"]:
                raise RuntimeError(
                    "{} derived rows {} != expected {}".format(
                        source["source_file"], rows, source["expected_rows"]
                    )
                )
            records.append({
                "derived_file_count": len(metadata),
                "derived_rows": rows,
                "derived_relative_dir": source["derived_relative_dir"],
                "files": metadata,
                "source_file": source["source_file"],
            })
    finally:
        spark.stop()

    file_sizes = [
        item["bytes"] for record in records for item in record["files"]
    ]
    row_groups = [
        row_group
        for record in records
        for item in record["files"]
        for row_group in item["row_groups"]
    ]
    max_file_bytes = args.max_file_mib * MIB
    if max(file_sizes) > max_file_bytes:
        raise RuntimeError(
            "derived file exceeds {} bytes: {}".format(
                max_file_bytes, max(file_sizes)
            )
        )
    expected_total = manifest["total_expected_rows"]
    actual_total = sum(record["derived_rows"] for record in records)
    if actual_total != expected_total:
        raise RuntimeError(
            "derived total rows {} != expected {}".format(
                actual_total, expected_total
            )
        )
    result = {
        "all_source_row_counts_match": True,
        "derived_file_bytes": quantiles(file_sizes),
        "derived_file_count": len(file_sizes),
        "derived_row_group_compressed_bytes": quantiles([
            item["compressed_bytes"] for item in row_groups
        ]),
        "derived_row_group_count": len(row_groups),
        "derived_row_group_rows": quantiles([
            item["row_count"] for item in row_groups
        ]),
        "max_file_bytes_contract": max_file_bytes,
        "records": records,
        "source_file_count": len(records),
        "total_rows": actual_total,
    }
    with open(args.output, "x", encoding="utf-8") as stream:
        json.dump(result, stream, indent=2, sort_keys=True)
        stream.write("\n")


if __name__ == "__main__":
    main()
