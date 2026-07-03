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

"""Print the CPU reference hash for the runner without shell-embedded code."""
import argparse
import json


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("cpu_output")
    args = parser.parse_args()
    with open(args.cpu_output, encoding="utf-8") as stream:
        value = json.load(stream)
    print(value["cpu_reference"]["result_sha256"])


if __name__ == "__main__":
    main()
