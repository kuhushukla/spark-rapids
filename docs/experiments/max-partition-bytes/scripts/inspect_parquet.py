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

"""Record Parquet schema and row-group metadata without reading treatment timings."""
import argparse
import hashlib
import json
import os


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if os.path.exists(args.output):
        raise FileExistsError("refusing to overwrite " + args.output)

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("max-partition-bytes-metadata").getOrCreate()
    try:
        jvm = spark._jvm
        conf = spark._jsc.hadoopConfiguration()
        path = jvm.org.apache.hadoop.fs.Path(os.path.abspath(args.data))
        input_file = jvm.org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(path, conf)
        reader = jvm.org.apache.parquet.hadoop.ParquetFileReader.open(input_file)
        try:
            footer = reader.getFooter()
            groups = []
            blocks = footer.getBlocks()
            for index in range(blocks.size()):
                block = blocks.get(index)
                columns = block.getColumns()
                starts = []
                compressed_columns = 0
                for column_index in range(columns.size()):
                    column = columns.get(column_index)
                    starts.append(int(column.getStartingPos()))
                    compressed_columns += int(column.getTotalSize())
                groups.append({
                    "compressed_column_bytes": compressed_columns,
                    "index": index,
                    "row_count": int(block.getRowCount()),
                    "starting_pos": min(starts) if starts else None,
                    "total_byte_size_uncompressed": int(block.getTotalByteSize()),
                })
            schema = str(footer.getFileMetaData().getSchema())
        finally:
            reader.close()

        sha = hashlib.sha256()
        with open(args.data, "rb") as stream:
            while True:
                chunk = stream.read(8 * 1024 * 1024)
                if not chunk:
                    break
                sha.update(chunk)
        result = {
            "absolute_path": os.path.abspath(args.data),
            "file_size_bytes": os.path.getsize(args.data),
            "row_group_count": len(groups),
            "row_groups": groups,
            "schema": schema,
            "sha256": sha.hexdigest(),
        }
        with open(args.output, "x", encoding="utf-8") as stream:
            json.dump(result, stream, indent=2, sort_keys=True)
            stream.write("\n")
        print(json.dumps(result, sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
