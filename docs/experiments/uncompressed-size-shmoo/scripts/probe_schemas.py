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

"""Record Spark logical schemas for every derived month."""

import argparse
import json
import os
from pyspark.sql import SparkSession


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if os.path.exists(args.output):
        raise FileExistsError("refusing to overwrite " + args.output)
    spark = SparkSession.builder.appName("taxi-derived-schema-probe").getOrCreate()
    try:
        months = {}
        for name in sorted(os.listdir(args.data_dir)):
            path = os.path.join(args.data_dir, name)
            if name.startswith("yellow_tripdata_") and os.path.isdir(path):
                months[name.removeprefix("yellow_tripdata_")] = (
                    spark.read.parquet(path).schema.jsonValue()
                )
    finally:
        spark.stop()
    with open(args.output, "x", encoding="utf-8") as stream:
        json.dump({"schema_version": 1, "months": months}, stream, indent=2, sort_keys=True)
        stream.write("\n")


if __name__ == "__main__":
    main()
