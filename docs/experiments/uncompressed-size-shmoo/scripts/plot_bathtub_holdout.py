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

"""Render held-out partition curves with the frozen candidate marked."""

import argparse
import json
from pathlib import Path

from plot_results import panel, write_svg


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--analysis", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    with args.analysis.open(encoding="utf-8") as stream:
        data = json.load(stream)
    candidates = [128, 512, 2048, 4096]
    panels = [
        ("validation_2010", "common"),
        ("validation_2010", "variable_width"),
        ("test_2011", "common"),
        ("test_2011", "variable_width"),
    ]

    def draw(elements):
        for index, (episode, query) in enumerate(panels):
            points = []
            for x, candidate in enumerate(candidates):
                summary = data["cells"][f"{episode}|{query}|{candidate}"]
                label = f"{candidate}" + (" selected" if candidate == 512 else "")
                points.append((x, summary["elapsed_ms_median"], label))
            panel(
                elements, (index % 2) * 600, 40 + (index // 2) * 400,
                600, 400, f"{episode}: {query.replace('_', ' ')}",
                "ordered partition candidates (point label: MiB)",
                "elapsed (ms)", [("held out", points)],
                annotate=lambda value: value[2],
                x_ticks=list(range(len(candidates))),
            )

    write_svg(
        args.output, 1200, 850,
        "Frozen 512-MiB bounded-regret policy on held-out epochs", draw
    )


if __name__ == "__main__":
    main()
