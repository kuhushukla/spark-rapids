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

"""Render the dynamic bathtub follow-up as a dependency-free SVG."""

import argparse
import json
from pathlib import Path

from plot_results import panel, write_svg


def load(path):
    with path.open(encoding="utf-8") as stream:
        return json.load(stream)


def point(summary, index, label):
    return (index, summary["elapsed_ms_median"], label)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--analysis", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    data = load(args.analysis)
    partition_candidates = [128, 512, 2048, 4096, 8192, 16384, 32768]
    batch_targets = [256, 512, 1024, 2048, 4096]
    layout_candidates = [128, 2048, 8192]

    def draw(elements):
        for panel_index, query in enumerate(("common", "variable_width")):
            points = []
            for index, candidate in enumerate(partition_candidates):
                summary = data["mechanism_cells"][f"{query}|{candidate}"]
                concurrency = summary["gpu_max_concurrent_tasks_median"]
                points.append(point(summary, index, f"{candidate}/c{concurrency:g}"))
            panel(
                elements, panel_index * 600, 40, 600, 410,
                query.replace("_", " ") + " partition sweep",
                "ordered log2 partition treatments (label: MiB / observed c)",
                "elapsed (ms)", [("dynamic", points)],
                annotate=lambda value: value[2],
                x_ticks=list(range(len(partition_candidates))),
            )

        batch_series = []
        for query in ("common", "variable_width"):
            points = [
                point(data["batch_cells"][f"{query}|{target}"], index, str(target))
                for index, target in enumerate(batch_targets)
            ]
            batch_series.append((query.replace("_", " "), points))
        panel(
            elements, 0, 450, 600, 410, "batch target at fixed 4096-MiB partition",
            "ordered batch targets (point label: MiB)", "elapsed (ms)",
            batch_series, annotate=lambda value: value[2],
            x_ticks=list(range(len(batch_targets))),
        )

        layout_series = []
        for layout in ("sharded", "source"):
            points = [
                point(data["layout_cells"][f"{layout}|{candidate}"], index, str(candidate))
                for index, candidate in enumerate(layout_candidates)
            ]
            layout_series.append((layout, points))
        panel(
            elements, 600, 450, 600, 410, "physical-layout response",
            "ordered partition treatments (point label: MiB)", "elapsed (ms)",
            layout_series, annotate=lambda value: value[2],
            x_ticks=list(range(len(layout_candidates))),
        )

    write_svg(
        args.output, 1200, 900,
        "Dynamic partition bathtub, batch actuator, and layout contrast", draw
    )


if __name__ == "__main__":
    main()
