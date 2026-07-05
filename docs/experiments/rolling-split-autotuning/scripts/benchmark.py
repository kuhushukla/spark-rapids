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

"""Execute one table's frozen rolling-window GPU schedule."""

import argparse
import decimal
import hashlib
import json
import math
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as functions
from pyspark.sql import types


MIB = 1024 * 1024
GIB = 1024 * MIB
HISTORY_LIMIT = 12
HALF_LIFE = 3.0
TARGET_DECODED_BYTES = GIB
MIN_SPLIT_MIB = 64
MAX_SPLIT_MIB = 1024
ROUNDING_MIB = 16

DISPLAY_METRICS = {
    "sum of output GPU batch bytes": "decoded_bytes",
    "output rows": "decoded_rows",
    "output columnar batches": "output_batches",
    "maximum output GPU batch bytes per task": "max_batch_bytes",
    "maximum output GPU batch rows per task": "max_batch_rows",
}


class DurableJsonl:
    def __init__(self, path):
        if os.path.exists(path):
            raise FileExistsError("refusing to overwrite " + path)
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self.stream = open(path, "x", encoding="utf-8")

    def append(self, value):
        self.stream.write(json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n")
        self.stream.flush()
        os.fsync(self.stream.fileno())

    def close(self):
        self.stream.close()


def spark_type(name):
    mapping = {
        "double": types.DoubleType(),
        "int": types.IntegerType(),
        "long": types.LongType(),
        "string": types.StringType(),
        "timestamp": types.TimestampType(),
    }
    return mapping[name]


def schema_for(dataset):
    return types.StructType([
        types.StructField(name, spark_type(kind), True)
        for name, kind in dataset["fields"]
    ])


def canonical(rows):
    payload = []
    for row in rows:
        current = {}
        for key, value in row.asDict().items():
            if isinstance(value, decimal.Decimal):
                current[key] = format(value, "f")
            else:
                current[key] = None if value is None else str(value)
        payload.append(current)
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return payload, hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def build_query(spark, dataset, window):
    frame = spark.read.schema(schema_for(dataset)).parquet(*window["paths"])
    kind = dataset["query_kind"]
    if kind == "loan":
        frame = frame.where(
            (functions.col("period") >= functions.lit(window["predicate_start"]))
            & (functions.col("period") <= functions.lit(window["predicate_end"]))
        )
        return frame.agg(
            functions.count("*").alias("row_count"),
            functions.count("current_actual_upb").alias("upb_count"),
            functions.sum(
                functions.col("current_actual_upb").cast("decimal(38,6)")
            ).alias("upb_sum"),
            functions.sum("credit_score").alias("credit_score_sum"),
            functions.sum(functions.length("payment_history")).alias("history_chars"),
        )
    if kind == "taxi-double":
        fields = [name for name, _ in dataset["fields"]]
        expressions = [functions.count("*").alias("row_count")]
        for name in fields:
            expressions.append(functions.count(name).alias(name + "_count"))
            expressions.append(
                functions.sum(functions.col(name).cast("decimal(38,6)")).alias(
                    name + "_sum")
            )
        return frame.agg(*expressions)
    if kind == "fhv-string":
        return frame.agg(
            functions.count("*").alias("row_count"),
            functions.count("dispatching_base_num").alias("dispatch_count"),
            functions.sum(functions.length("dispatching_base_num")).alias(
                "dispatch_chars"),
            functions.count("Affiliated_base_number").alias("affiliate_count"),
            functions.sum(functions.length("Affiliated_base_number")).alias(
                "affiliate_chars"),
            functions.min("pickup_datetime").alias("pickup_min"),
            functions.max("pickup_datetime").alias("pickup_max"),
        )
    raise ValueError("unsupported query kind " + kind)


def plan_children(node):
    children = node.children()
    return [children.apply(index) for index in range(children.size())]


def find_gpu_scans(plan):
    scans = []
    stack = [plan]
    while stack:
        node = stack.pop()
        if node.nodeName().startswith("GpuScan"):
            scans.append(node)
        stack.extend(plan_children(node))
    return scans


def metric_display_name(metric):
    optional = metric.name()
    return optional.get() if optional.isDefined() else ""


def direct_scan_metrics(plan):
    scans = find_gpu_scans(plan)
    if len(scans) != 1:
        raise ValueError("expected one GpuScan, found {}".format(len(scans)))
    scan = scans[0]
    values = {}
    iterator = scan.metrics().iterator()
    while iterator.hasNext():
        entry = iterator.next()
        metric = entry._2()
        display = metric_display_name(metric)
        normalized = DISPLAY_METRICS.get(display)
        if normalized:
            values[normalized] = int(metric.value())
    missing = sorted(set(DISPLAY_METRICS.values()) - set(values))
    if missing:
        raise ValueError("direct scan metrics missing: " + ",".join(missing))
    input_rdds = scan.inputRDDs()
    if input_rdds.size() != 1:
        raise ValueError("expected one scan input RDD")
    values["planned_scan_tasks"] = int(input_rdds.apply(0).getNumPartitions())
    return values


def weighted_median(pairs):
    ordered = sorted(pairs, key=lambda item: item[0])
    total = sum(weight for _, weight in ordered)
    cumulative = 0.0
    for value, weight in ordered:
        cumulative += weight
        if cumulative >= total / 2.0:
            return value
    return ordered[-1][0]


def history_estimate(history, field):
    recent = history[-HISTORY_LIMIT:]
    pairs = []
    for age, record in enumerate(reversed(recent)):
        weight = math.pow(0.5, age / HALF_LIFE)
        pairs.append((record[field], weight))
    return weighted_median(pairs)


def round_split_mib(value):
    rounded = int(round(value / ROUNDING_MIB) * ROUNDING_MIB)
    return max(MIN_SPLIT_MIB, min(MAX_SPLIT_MIB, rounded))


def choose_enabled(history, listed_bytes):
    if not history:
        return {
            "selected_mib": 128,
            "reason": "cold-start-fallback",
            "history_count": 0,
            "predicted_decoded_bytes": None,
            "predicted_decoded_rows": None,
            "decoded_bytes_per_listed_byte": None,
            "decoded_rows_per_listed_byte": None,
        }
    byte_ratio = history_estimate(history, "decoded_bytes_per_listed_byte")
    row_ratio = history_estimate(history, "decoded_rows_per_listed_byte")
    if byte_ratio <= 0 or row_ratio <= 0:
        raise ValueError("history ratios must be positive")
    raw_mib = TARGET_DECODED_BYTES / byte_ratio / MIB
    return {
        "selected_mib": round_split_mib(raw_mib),
        "reason": "prior-enabled-data-shape",
        "history_count": len(history),
        "predicted_decoded_bytes": round(listed_bytes * byte_ratio),
        "predicted_decoded_rows": round(listed_bytes * row_ratio),
        "decoded_bytes_per_listed_byte": byte_ratio,
        "decoded_rows_per_listed_byte": row_ratio,
    }


def treatment_split(treatment, decision):
    if treatment == "enabled":
        return decision["selected_mib"]
    if treatment == "fixed-128":
        return 128
    if treatment == "fixed-1024":
        return 1024
    raise ValueError("unknown treatment " + treatment)


def execute(
        spark, dataset, window, treatment, split_mib, phase, index,
        decision, journal, results):
    run_id = "{}-{:04d}-{}-{}".format(
        dataset["logical_table"], index, phase, treatment)
    scope = {
        "run_id": run_id,
        "dataset": dataset["logical_table"],
        "window_id": window["window_id"],
        "start_month": window["start_month"],
        "end_month": window["end_month"],
        "phase": phase,
        "treatment": treatment,
        "max_partition_mib": split_mib,
    }
    journal.append({**scope, "event": "run_start", "timestamp_ns": time.time_ns()})
    group_set = False
    try:
        spark.conf.set("spark.sql.files.maxPartitionBytes", str(split_mib * MIB))
        query = build_query(spark, dataset, window)
        spark.sparkContext.setJobGroup(run_id, run_id, interruptOnCancel=True)
        group_set = True
        started_ns = time.monotonic_ns()
        rows = query.collect()
        elapsed_ms = (time.monotonic_ns() - started_ns) / 1_000_000.0
        plan = query._jdf.queryExecution().executedPlan()
        plan_text = plan.toString()
        metrics = direct_scan_metrics(plan)
        payload, result_sha256 = canonical(rows)
        result = {
            **scope,
            "query": dataset["query_kind"],
            "block": index,
            "listed_file_bytes": window["listed_file_bytes"],
            "elapsed_ms": elapsed_ms,
            "result": payload,
            "result_sha256": result_sha256,
            "plan_sha256": hashlib.sha256(plan_text.encode("utf-8")).hexdigest(),
            "gpu_scan_in_plan": "GpuScan" in plan_text,
            "direct_scan_metrics": metrics,
            "enabled_decision": decision if treatment == "enabled" else None,
            "rapids_batch_mib": TARGET_DECODED_BYTES // MIB,
            "reader_batch_mib": 2048,
        }
        if not result["gpu_scan_in_plan"]:
            raise RuntimeError("GPU scan absent from " + run_id)
        results.append(result)
        journal.append({**scope, "event": "run_result", **result})
        journal.append({
            **scope,
            "event": "run_terminal",
            "status": "success",
            "timestamp_ns": time.time_ns(),
        })
        return result
    except BaseException as error:
        journal.append({
            **scope,
            "event": "run_terminal",
            "status": "error",
            "error_type": type(error).__name__,
            "error_message": str(error),
            "timestamp_ns": time.time_ns(),
        })
        raise
    finally:
        if group_set:
            spark.sparkContext.setLocalProperty("spark.jobGroup.id", None)
            spark.sparkContext.setLocalProperty("spark.job.description", None)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--schedule", required=True)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--journal", required=True)
    parser.add_argument("--results", required=True)
    parser.add_argument("--history-output", required=True)
    parser.add_argument("--limit-windows", type=int)
    args = parser.parse_args()

    for path in (args.journal, args.results, args.history_output):
        if os.path.exists(path):
            raise FileExistsError("refusing to overwrite " + path)
    with open(args.schedule, encoding="utf-8") as stream:
        schedule = json.load(stream)
    matching = [
        item for item in schedule["datasets"]
        if item["logical_table"] == args.dataset
    ]
    if len(matching) != 1:
        raise ValueError("dataset not found: " + args.dataset)
    dataset = matching[0]
    windows = dataset["windows"]
    if args.limit_windows is not None:
        windows = windows[:args.limit_windows]
    if not windows:
        raise ValueError("no windows selected")

    journal = DurableJsonl(args.journal)
    results = DurableJsonl(args.results)
    spark = SparkSession.builder.appName(
        "rolling-split-autotuning-" + args.dataset
    ).getOrCreate()
    history = []
    try:
        first_window = windows[0]
        cold_decision = choose_enabled([], first_window["listed_file_bytes"])
        for treatment in dataset["warmup_treatments"]:
            execute(
                spark, dataset, first_window, treatment,
                treatment_split(treatment, cold_decision), "warmup", 0,
                cold_decision, journal, results)

        for index, window in enumerate(windows):
            decision = choose_enabled(history, window["listed_file_bytes"])
            block_results = {}
            for treatment in window["treatment_order"]:
                result = execute(
                    spark, dataset, window, treatment,
                    treatment_split(treatment, decision), "measured", index,
                    decision, journal, results)
                block_results[treatment] = result
            hashes = {item["result_sha256"] for item in block_results.values()}
            if len(hashes) != 1:
                raise RuntimeError("cross-treatment result mismatch for " + window["window_id"])
            enabled = block_results["enabled"]
            metrics = enabled["direct_scan_metrics"]
            decoded_bytes = metrics["decoded_bytes"]
            decoded_rows = metrics["decoded_rows"]
            if decoded_bytes <= 0 or decoded_rows <= 0:
                raise RuntimeError("non-positive enabled scan output for " + window["window_id"])
            history.append({
                "window_id": window["window_id"],
                "listed_file_bytes": window["listed_file_bytes"],
                "decoded_bytes": decoded_bytes,
                "decoded_rows": decoded_rows,
                "decoded_bytes_per_listed_byte": (
                    decoded_bytes / window["listed_file_bytes"]),
                "decoded_rows_per_listed_byte": (
                    decoded_rows / window["listed_file_bytes"]),
                "selected_mib": decision["selected_mib"],
                "elapsed_ms": enabled["elapsed_ms"],
            })
    finally:
        spark.stop()
        journal.close()
        results.close()

    with open(args.history_output, "x", encoding="utf-8") as stream:
        json.dump({
            "schema_version": "rolling-split-autotuning/history-v1",
            "dataset": args.dataset,
            "observations": history,
        }, stream, indent=2, sort_keys=True)
        stream.write("\n")


if __name__ == "__main__":
    main()
