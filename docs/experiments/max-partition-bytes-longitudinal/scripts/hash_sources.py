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

"""Hash immutable source files for experiment provenance, not model features."""

import argparse
import hashlib
import json
import os
import re


def sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if os.path.exists(args.output):
        raise FileExistsError("refusing to overwrite " + args.output)
    files = []
    for name in sorted(os.listdir(args.data_dir)):
        if re.fullmatch(r"yellow_tripdata_\d{4}-\d{2}\.parquet", name):
            path = os.path.join(args.data_dir, name)
            files.append({
                "bytes": os.path.getsize(path),
                "file": name,
                "sha256": sha256(path),
            })
    if len(files) != 36:
        raise RuntimeError("expected exactly 36 source files, found {}".format(len(files)))
    result = {
        "data_dir": os.path.abspath(args.data_dir),
        "file_count": len(files),
        "files": files,
        "total_bytes": sum(item["bytes"] for item in files),
    }
    os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
    with open(args.output, "x", encoding="utf-8") as stream:
        json.dump(result, stream, indent=2, sort_keys=True)
        stream.write("\n")


if __name__ == "__main__":
    main()
