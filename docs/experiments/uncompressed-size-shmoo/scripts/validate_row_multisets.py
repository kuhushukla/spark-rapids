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

"""Compare source and rewritten monthly row multisets with commutative fingerprints."""

import argparse
import json
import os

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType, FloatType


def summarize(spark, path):
    frame = spark.read.parquet(path)
    columns = [F.col(name) for name in frame.columns]
    hashed = frame.select(F.xxhash64(*columns).alias("__row_hash"), *columns)
    metrics = [
        F.count(F.lit(1)).alias("row_count"),
        F.expr("bit_xor(__row_hash)").alias("row_hash_xor"),
        F.sum(F.col("__row_hash")).alias("row_hash_sum"),
    ]
    for field in frame.schema.fields:
        column = F.col(field.name)
        metrics.append(
            F.sum(F.when(column.isNull(), F.lit(1)).otherwise(F.lit(0))).alias(
                "null_" + field.name
            )
        )
        if isinstance(field.dataType, (DoubleType, FloatType)):
            metrics.append(
                F.sum(F.when(F.isnan(column), F.lit(1)).otherwise(F.lit(0))).alias(
                    "nan_" + field.name
                )
            )
    row = hashed.agg(*metrics).collect()[0].asDict(recursive=True)
    return {
        "schema_json": frame.schema.json(),
        "summary": {
            key: int(value) if value is not None else None for key, value in row.items()
        },
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-dir", required=True)
    parser.add_argument("--derived-dir", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if os.path.exists(args.output):
        raise FileExistsError("refusing to overwrite " + args.output)
    spark = SparkSession.builder.appName("taxi-row-multiset-validation").getOrCreate()
    records = []
    try:
        for year in (2009, 2010, 2011):
            for month_number in range(1, 13):
                month = "{:04d}-{:02d}".format(year, month_number)
                source = os.path.join(
                    args.source_dir, "yellow_tripdata_" + month + ".parquet"
                )
                derived = os.path.join(args.derived_dir, "yellow_tripdata_" + month)
                source_summary = summarize(spark, source)
                derived_summary = summarize(spark, derived)
                records.append({
                    "month": month,
                    "schema_matches": (
                        source_summary["schema_json"] == derived_summary["schema_json"]
                    ),
                    "row_multiset_fingerprints_match": (
                        source_summary["summary"] == derived_summary["summary"]
                    ),
                    "source": source_summary,
                    "derived": derived_summary,
                })
    finally:
        spark.stop()
    if not all(
        item["schema_matches"] and item["row_multiset_fingerprints_match"]
        for item in records
    ):
        raise ValueError("source/derived multiset validation failed")
    with open(args.output, "x", encoding="utf-8") as stream:
        json.dump({
            "schema_version": 1,
            "method": "count + bit_xor(xxhash64(all columns)) + sum(xxhash64(all columns)) + per-column null/NaN counts",
            "all_months_match": True,
            "records": records,
        }, stream, indent=2, sort_keys=True)
        stream.write("\n")


if __name__ == "__main__":
    main()
