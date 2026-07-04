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

"""Build the deterministic SHA-256 manifest for the profiled experiment."""

import hashlib
import os


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT = os.path.join(ROOT, "provenance", "manifest.txt")
EXCLUDED_SUFFIXES = ("/analysis/profile.json", "/analysis/profile.sqlite")


def main():
    entries = []
    for directory, _, files in os.walk(ROOT):
        for name in files:
            path = os.path.join(directory, name)
            relative = os.path.relpath(path, ROOT)
            normalized = relative.replace(os.sep, "/")
            if (path == OUTPUT or normalized.endswith(EXCLUDED_SUFFIXES) or
                    "/__pycache__/" in f"/{normalized}"):
                continue
            digest = hashlib.sha256()
            with open(path, "rb") as stream:
                for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                    digest.update(chunk)
            entries.append((normalized, digest.hexdigest()))
    with open(OUTPUT, "w", encoding="utf-8") as stream:
        for relative, digest in sorted(entries):
            stream.write(f"{digest}  {relative}\n")


if __name__ == "__main__":
    main()
