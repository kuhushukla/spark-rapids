#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION.
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

"""Render dependency-free SVG plots from the committed experiment summaries."""

import argparse
import json
from pathlib import Path
from xml.sax.saxutils import escape

COLORS = ["#1f77b4", "#d62728", "#2ca02c", "#9467bd"]


def load(path):
    with path.open(encoding="utf-8") as stream:
        return json.load(stream)


def text(x, y, value, size=12, anchor="start", rotate=None):
    transform = f' transform="rotate({rotate} {x} {y})"' if rotate else ""
    return (f'<text x="{x:.1f}" y="{y:.1f}" font-size="{size}" '
            f'text-anchor="{anchor}"{transform}>{escape(str(value))}</text>')


def panel(elements, x0, y0, width, height, title, x_label, y_label, series,
          annotate=None, x_ticks=None):
    left, top, right, bottom = x0 + 75, y0 + 35, x0 + width - 25, y0 + height - 60
    all_points = [point for _, points in series for point in points]
    xmin, xmax = min(p[0] for p in all_points), max(p[0] for p in all_points)
    ymin, ymax = min(p[1] for p in all_points), max(p[1] for p in all_points)
    xpad = max((xmax - xmin) * 0.08, 0.01)
    ypad = max((ymax - ymin) * 0.12, 1.0)
    xmin, xmax, ymin, ymax = xmin - xpad, xmax + xpad, ymin - ypad, ymax + ypad

    def sx(value):
        return left + (value - xmin) * (right - left) / (xmax - xmin)

    def sy(value):
        return bottom - (value - ymin) * (bottom - top) / (ymax - ymin)

    elements.append(f'<rect x="{left}" y="{top}" width="{right-left}" '
                    f'height="{bottom-top}" fill="white" stroke="#777"/>')
    for fraction in [0, 0.25, 0.5, 0.75, 1]:
        gy = bottom - fraction * (bottom - top)
        elements.append(f'<line x1="{left}" y1="{gy}" x2="{right}" y2="{gy}" '
                        'stroke="#ddd"/>')
        elements.append(text(left - 8, gy + 4, f"{ymin + fraction*(ymax-ymin):.4g}",
                             10, "end"))
    tick_values = x_ticks if x_ticks is not None else [
        xmin + fraction * (xmax - xmin) for fraction in [0, 0.25, 0.5, 0.75, 1]
    ]
    for value in tick_values:
        gx = sx(value)
        elements.append(f'<line x1="{gx}" y1="{top}" x2="{gx}" y2="{bottom}" '
                        'stroke="#ddd"/>')
        label = str(value) if x_ticks is not None else f"{value:.2g}"
        elements.append(text(gx, bottom + 18, label, 10, "middle"))
    elements.append(text(x0 + width / 2, y0 + 20, title, 15, "middle"))
    elements.append(text(x0 + width / 2, y0 + height - 18, x_label, 11, "middle"))
    elements.append(text(x0 + 16, y0 + height / 2, y_label, 11, "middle", -90))
    for index, (label, points) in enumerate(series):
        color = COLORS[index % len(COLORS)]
        coordinates = " ".join(f"{sx(p[0]):.1f},{sy(p[1]):.1f}" for p in points)
        elements.append(f'<polyline points="{coordinates}" fill="none" '
                        f'stroke="{color}" stroke-width="2"/>')
        for point in points:
            px, py = sx(point[0]), sy(point[1])
            elements.append(f'<circle cx="{px:.1f}" cy="{py:.1f}" r="3.5" '
                            f'fill="{color}"/>')
            if annotate:
                elements.append(text(px + 5, py - 5, annotate(point), 8))
        ly = top + 15 + index * 17
        elements.append(f'<line x1="{right-115}" y1="{ly-4}" x2="{right-95}" '
                        f'y2="{ly-4}" stroke="{color}" stroke-width="2"/>')
        elements.append(text(right - 90, ly, label, 10))


def write_svg(path, width, height, title, draw):
    elements = [
        "<!--",
        " Copyright (c) 2026, NVIDIA CORPORATION.",
        "",
        " Licensed under the Apache License, Version 2.0 (the \"License\");",
        " you may not use this file except in compliance with the License.",
        " You may obtain a copy of the License at",
        "",
        "     http://www.apache.org/licenses/LICENSE-2.0",
        "",
        " Unless required by applicable law or agreed to in writing, software",
        " distributed under the License is distributed on an \"AS IS\" BASIS,",
        " WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.",
        " See the License for the specific language governing permissions and",
        " limitations under the License.",
        "-->",
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="white"/>',
        text(width / 2, 25, title, 18, "middle"),
    ]
    draw(elements)
    elements.append("</svg>")
    path.write_text("\n".join(elements) + "\n", encoding="utf-8")


def plot_shmoo(data, output):
    queries = ["common", "filtered", "variable_width", "schema_evolution"]
    episodes = [("train_2009", "2009"), ("validation_2010", "2010"), ("test_2011", "2011")]

    def draw(elements):
        for index, query in enumerate(queries):
            series = []
            for episode, label in episodes:
                cells = sorted(
                    (c for c in data["cells"]
                     if c["query"] == query and c["episode"] == episode),
                    key=lambda c: c["decoded_task_bytes_p50"])
                series.append((label, [
                    (c["decoded_task_bytes_p50"] / (1024 ** 3),
                     c["elapsed_ms_median"], c["max_partition_mib"]) for c in cells]))
            panel(elements, (index % 2) * 600, 40 + (index // 2) * 400,
                  600, 400, query.replace("_", " "),
                  "median decoded bytes/task (GiB)", "elapsed (ms)", series,
                  lambda p: str(p[2]))
    write_svg(output, 1200, 850,
              "Annual GPU shmoo; point labels are maxPartitionBytes MiB", draw)


def plot_mediation(data, output):
    def draw(elements):
        for index, query in enumerate(["common", "filtered"]):
            series = []
            for target in [512, 1024, 2048]:
                cells = sorted(
                    (c for c in data["cells"]
                     if c["query"] == query and c["max_partition_mib"] == 4096
                     and c["rapids_batch_mib"] == target),
                    key=lambda c: c["reader_batch_mib"])
                series.append((f"target={target} MiB", [
                    (c["reader_batch_mib"], c["elapsed_ms_median"]) for c in cells]))
            panel(elements, index * 600, 40, 600, 410, query,
                  "Parquet reader soft limit (MiB)", "elapsed (ms)", series,
                  x_ticks=[1024, 2048, 4096])
    write_svg(output, 1200, 500,
              "Batch-ceiling mediation at maxPartitionBytes=4096 MiB", draw)


def plot_growth(data, output):
    months = {
        "growth_1m": 1, "growth_3m": 3, "growth_6m": 6,
        "growth_12m": 12, "growth_24m": 24, "growth_36m": 36,
    }
    cells = [c for c in data["cells"] if c["query"] == "common"]

    def draw(elements):
        series = []
        for candidate in [2048, 4096, 8192]:
            selected = sorted(
                (c for c in cells
                 if c["episode"] in months and c["max_partition_mib"] == candidate),
                key=lambda c: months[c["episode"]])
            series.append((f"{candidate} MiB", [
                (months[c["episode"]], c["elapsed_ms_median"]) for c in selected]))
        panel(elements, 0, 40, 900, 470, "common projection",
              "cumulative table age (months)", "elapsed (ms)", series,
              x_ticks=[1, 3, 6, 12, 24, 36])
    write_svg(output, 900, 550, "Performance as the table grows", draw)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--experiment-root", type=Path, required=True)
    args = parser.parse_args()
    root = args.experiment_root
    analysis = root / "analysis"
    analysis.mkdir(exist_ok=True)
    plot_shmoo(load(root / "attempts/high-size-extension-001/analysis/"
                     "validated-extension.json"), analysis / "annual-shmoo.svg")
    plot_mediation(load(root / "attempts/batch-mediation-001/analysis/"
                        "validated-mediation.json"), analysis / "batch-mediation.svg")
    plot_growth(load(root / "attempts/growth-001/analysis/validated-growth.json"),
                analysis / "table-growth.svg")


if __name__ == "__main__":
    main()
