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

"""Summarize target scan-stage overlap from Nsight Systems SQLite exports."""

import argparse
import gzip
import json
import os
import sqlite3
import statistics


def union_ns(intervals):
    clipped = sorted((start, end) for start, end in intervals if end > start)
    if not clipped:
        return 0
    total = 0
    current_start, current_end = clipped[0]
    for start, end in clipped[1:]:
        if start > current_end:
            total += current_end - current_start
            current_start, current_end = start, end
        else:
            current_end = max(current_end, end)
    return total + current_end - current_start


def maximum_overlap(intervals):
    events = []
    for start, end in intervals:
        if end > start:
            events.append((start, 1))
            events.append((end, -1))
    active = 0
    maximum = 0
    for _, delta in sorted(events, key=lambda item: (item[0], item[1])):
        active += delta
        maximum = max(maximum, active)
    return maximum


def percentile(values, fraction):
    ordered = sorted(values)
    if not ordered:
        return None
    return ordered[round(fraction * (len(ordered) - 1))]


def overlap(interval, ranges):
    start, end = interval
    return any(start < range_end and end > range_start for range_start, range_end in ranges)


def text_expression():
    return "coalesce(n.text, s.value)"


def spark_stage_wall_ns(eventlog_path, stage_id):
    with gzip.open(eventlog_path, "rt", encoding="utf-8") as stream:
        for line in stream:
            event = json.loads(line)
            if event.get("Event") != "SparkListenerStageCompleted":
                continue
            info = event["Stage Info"]
            if info["Stage ID"] == stage_id:
                return (info["Completion Time"] - info["Submission Time"]) * 1_000_000
    raise ValueError(f"stage {stage_id} completion not found in {eventlog_path}")


def summarize(attempt):
    sqlite_path = os.path.join(attempt, "analysis", "profile.sqlite")
    journal_path = os.path.join(attempt, "raw", "journal.jsonl")
    eventlog_path = os.path.join(attempt, "raw", "eventlog.json.gz")
    spark_wall_ns = spark_stage_wall_ns(eventlog_path, 4)
    connection = sqlite3.connect(sqlite_path)
    connection.row_factory = sqlite3.Row
    text_expr = text_expression()
    task_rows = list(connection.execute(
        f"""select n.start, n.end, {text_expr} as name
            from NVTX_EVENTS n left join StringIds s on n.textId = s.id
            where {text_expr} like 'Stage 4 Task %' and n.end is not null
            order by n.start"""
    ))
    if not task_rows:
        raise ValueError("no completed Stage 4 task NVTX ranges in " + sqlite_path)
    stage_start = min(row["start"] for row in task_rows)
    stage_end = max(row["end"] for row in task_rows)
    stage_span = stage_end - stage_start

    decode_rows = list(connection.execute(
        f"""select n.start, n.end
            from NVTX_EVENTS n left join StringIds s on n.textId = s.id
            where {text_expr} = 'Parquet decode' and n.end is not null
              and n.start < ? and n.end > ?
            order by n.start""",
        (stage_end, stage_start),
    ))
    decode_ranges = [
        (max(row["start"], stage_start), min(row["end"], stage_end))
        for row in decode_rows
    ]

    kernel_rows = list(connection.execute(
        """select k.start, k.end, coalesce(sn.value, dn.value) as name, k.streamId
           from CUPTI_ACTIVITY_KIND_KERNEL k
           left join StringIds sn on k.shortName = sn.id
           left join StringIds dn on k.demangledName = dn.id
           where k.start < ? and k.end > ?
           order by k.start""",
        (stage_end, stage_start),
    ))
    kernel_intervals = [
        (max(row["start"], stage_start), min(row["end"], stage_end))
        for row in kernel_rows
    ]
    overlapping_decode_window_kernel_intervals = [
        interval for interval in kernel_intervals if overlap(interval, decode_ranges)
    ]

    memcpy_rows = list(connection.execute(
        """select start, end, bytes, copyKind
           from CUPTI_ACTIVITY_KIND_MEMCPY
           where start < ? and end > ? order by start""",
        (stage_end, stage_start),
    ))
    memcpy_intervals = [
        (max(row["start"], stage_start), min(row["end"], stage_end))
        for row in memcpy_rows
    ]

    kernel_by_name = {}
    for row in kernel_rows:
        name = row["name"] or "<unknown>"
        entry = kernel_by_name.setdefault(name, {"calls": 0, "duration_ns": 0})
        entry["calls"] += 1
        entry["duration_ns"] += row["end"] - row["start"]
    top_kernels = sorted(
        (dict(name=name, **values) for name, values in kernel_by_name.items()),
        key=lambda item: item["duration_ns"],
        reverse=True,
    )[:10]

    metric_names = {
        row["metricId"]: row["metricName"]
        for row in connection.execute(
            "select metricId, metricName from TARGET_INFO_GPU_METRICS"
        )
    }
    requested_metrics = {
        "SMs Active [Throughput %]",
        "SM Issue [Throughput %]",
        "DRAM Read Bandwidth [Throughput %]",
        "DRAM Write Bandwidth [Throughput %]",
        "PCIe RX Throughput [Throughput %]",
        "PCIe TX Throughput [Throughput %]",
    }
    gpu_metrics = {}
    for metric_id, name in metric_names.items():
        if name not in requested_metrics:
            continue
        values = [
            row[0] for row in connection.execute(
                """select value from GPU_METRICS
                   where metricId = ? and timestamp >= ? and timestamp <= ?
                   order by timestamp""",
                (metric_id, stage_start, stage_end),
            )
        ]
        gpu_metrics[name] = {
            "samples": len(values),
            "mean": statistics.mean(values) if values else None,
            "p50": statistics.median(values) if values else None,
            "p90": percentile(values, 0.90),
            "max": max(values) if values else None,
        }

    with open(journal_path, encoding="utf-8") as stream:
        records = [json.loads(line) for line in stream]
    target = next(record for record in records if record["phase"] == "profile")

    kernel_sum = sum(end - start for start, end in kernel_intervals)
    kernel_union = union_ns(kernel_intervals)
    overlapping_decode_window_kernel_sum = sum(end - start for start, end in overlapping_decode_window_kernel_intervals)
    overlapping_decode_window_kernel_union = union_ns(overlapping_decode_window_kernel_intervals)
    decode_range_sum = sum(end - start for start, end in decode_ranges)
    decode_range_union = union_ns(decode_ranges)
    memcpy_sum = sum(end - start for start, end in memcpy_intervals)
    memcpy_union = union_ns(memcpy_intervals)

    return {
        "attempt": os.path.basename(attempt),
        "max_partition_mib": target["max_partition_mib"],
        "profiled_query_elapsed_ms": target["elapsed_ms"],
        "stage": {
            "id": 4,
            "task_count": len(task_rows),
            "gpu_ownership_envelope_ns": stage_span,
            "spark_wall_ns": spark_wall_ns,
        },
        "cuda": {
            "kernel_calls": len(kernel_rows),
            "kernel_service_ns": kernel_sum,
            "kernel_busy_union_ns": kernel_union,
            "kernel_overlap_factor": kernel_sum / kernel_union if kernel_union else None,
            "maximum_simultaneous_kernels": maximum_overlap(kernel_intervals),
            "kernel_busy_fraction_of_spark_stage": kernel_union / spark_wall_ns,
            "kernel_busy_fraction_of_gpu_ownership_envelope": kernel_union / stage_span,
            "decode_nvtx_calls": len(decode_ranges),
            "decode_nvtx_service_ns": decode_range_sum,
            "decode_nvtx_union_ns": decode_range_union,
            "overlapping_decode_window_kernel_service_ns": overlapping_decode_window_kernel_sum,
            "overlapping_decode_window_kernel_union_ns": overlapping_decode_window_kernel_union,
            "overlapping_decode_window_kernel_fraction_of_nvtx_union": (
                overlapping_decode_window_kernel_union / decode_range_union if decode_range_union else None
            ),
            "memcpy_calls": len(memcpy_rows),
            "memcpy_bytes": sum(row["bytes"] for row in memcpy_rows),
            "memcpy_service_ns": memcpy_sum,
            "memcpy_union_ns": memcpy_union,
            "top_kernels": top_kernels,
        },
        "gpu_metrics": gpu_metrics,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--attempt", action="append", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--markdown", required=True)
    args = parser.parse_args()
    for path in (args.output, args.markdown):
        if os.path.exists(path):
            raise FileExistsError("refusing to overwrite " + path)

    results = sorted(
        [summarize(os.path.abspath(path)) for path in args.attempt],
        key=lambda item: item["max_partition_mib"],
    )
    document = {"schema_version": 2, "profiles": results}
    with open(args.output, "x", encoding="utf-8") as stream:
        json.dump(document, stream, indent=2, sort_keys=True)
        stream.write("\n")

    lines = [
        "# Nsight Systems scan-stage profile",
        "",
        "Profiled wall times are perturbed diagnostics. Kernel service is the sum of",
        "kernel durations; busy time is the union of kernel intervals and therefore",
        "does not double-count overlap.",
        "",
        "| Partition MiB | Tasks | Spark stage ms | GPU ownership envelope ms | "
        "Kernel calls | Kernel service ms | Kernel busy ms | Busy/Spark stage | "
        "Max simultaneous kernels | Decode NVTX ms | Kernels overlapping decode windows ms | "
        "Memcpy MiB |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for item in results:
        stage = item["stage"]
        cuda = item["cuda"]
        lines.append(
            f"| {item['max_partition_mib']} | {stage['task_count']} | "
            f"{stage['spark_wall_ns'] / 1e6:.1f} | "
            f"{stage['gpu_ownership_envelope_ns'] / 1e6:.1f} | "
            f"{cuda['kernel_calls']} | {cuda['kernel_service_ns'] / 1e6:.1f} | "
            f"{cuda['kernel_busy_union_ns'] / 1e6:.1f} | "
            f"{cuda['kernel_busy_fraction_of_spark_stage'] * 100:.1f}% | "
            f"{cuda['maximum_simultaneous_kernels']} | "
            f"{cuda['decode_nvtx_union_ns'] / 1e6:.1f} | "
            f"{cuda['overlapping_decode_window_kernel_union_ns'] / 1e6:.1f} | "
            f"{cuda['memcpy_bytes'] / 1024**2:.1f} |"
        )
    lines.extend(["", "GPU metric samples within each GPU-ownership envelope:", ""])
    for item in results:
        lines.append(f"## {item['max_partition_mib']} MiB")
        lines.append("")
        for name, values in sorted(item["gpu_metrics"].items()):
            lines.append(
                f"- {name}: mean {values['mean']:.1f}, p50 {values['p50']:.1f}, "
                f"p90 {values['p90']}, max {values['max']} "
                f"({values['samples']} samples)."
            )
        lines.append("")
    with open(args.markdown, "x", encoding="utf-8") as stream:
        stream.write("\n".join(lines))


if __name__ == "__main__":
    main()
