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

"""Run the frozen longitudinal CPU references or GPU Stage-1 allocation."""

import argparse
import datetime
import hashlib
import json
import os
import random
import time

from validate_planning import exact_layout


MIB = 1024 * 1024
SCHEMA_VERSION = 1
EXPECTED_EPISODES = (
    ("fixed_2009_all_12", "common", (128, 256, 512)),
    ("variable_2010_all_12", "variable_width", (128, 256, 512, 1024)),
    ("evolution_through_2011", "missing_location", (128, 256, 512, 1024)),
)


class Journal:
    def __init__(self, path):
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self._stream = open(path, "x", encoding="utf-8")

    def append(self, value):
        self._stream.write(
            json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n"
        )
        self._stream.flush()
        os.fsync(self._stream.fileno())

    def close(self):
        self._stream.close()


def file_sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        while True:
            chunk = stream.read(1024 * 1024)
            if not chunk:
                return digest.hexdigest()
            digest.update(chunk)


def write_json_exclusive(path, value):
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "x", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2, sort_keys=True)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())


def canonical(rows):
    normalized = []
    for row in rows:
        values = []
        for value in row:
            if isinstance(value, (datetime.datetime, datetime.date)):
                values.append(value.isoformat())
            else:
                values.append(value)
        normalized.append(values)
    normalized.sort(
        key=lambda value: json.dumps(
            value, sort_keys=True, separators=(",", ":"), default=str
        )
    )
    payload = json.dumps(
        normalized, sort_keys=True, separators=(",", ":"), default=str
    )
    return normalized, hashlib.sha256(payload.encode("utf-8")).hexdigest()


def schema_for(query_name):
    from pyspark.sql.types import (
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    fields = [
        StructField("passenger_count", LongType(), True),
        StructField("trip_distance", DoubleType(), True),
    ]
    if query_name == "variable_width":
        fields.append(StructField("payment_type", StringType(), True))
    elif query_name == "missing_location":
        fields.append(StructField("PULocationID", LongType(), True))
    elif query_name != "common":
        raise ValueError("unsupported query " + query_name)
    return StructType(fields)


def build_query(spark, paths, query_name):
    from pyspark.sql import functions as F

    # This is intentionally a new DataFrame after every configuration change.
    source = spark.read.schema(schema_for(query_name)).parquet(*paths)
    metrics = [
        F.count(F.lit(1)).alias("row_count"),
        F.sum(F.coalesce(F.col("passenger_count"), F.lit(0)).cast("long"))
        .alias("passenger_count_sum"),
        F.count(F.col("trip_distance")).alias("trip_distance_non_null_count"),
        F.count(F.when(F.isnan(F.col("trip_distance")), F.lit(1)))
        .alias("trip_distance_nan_count"),
        F.count(F.when(F.col("trip_distance") >= F.lit(0.0), F.lit(1)))
        .alias("trip_distance_nonnegative_count"),
        F.count(F.when(F.col("trip_distance") < F.lit(0.0), F.lit(1)))
        .alias("trip_distance_negative_count"),
    ]
    if query_name == "variable_width":
        result = source.groupBy("payment_type").agg(*metrics)
    elif query_name == "missing_location":
        result = source.groupBy("PULocationID").agg(*metrics)
    else:
        result = source.agg(*metrics)
    return source, result


def is_gpu_scan(plan):
    return any(
        name in plan
        for name in (
            "GpuScan parquet",
            "GpuFileSourceScan",
            "GpuBatchScan",
            "GpuFileGpuScan",
        )
    )


def load_and_validate_inputs(census_path, registry_path, schedule_path):
    with open(census_path, encoding="utf-8") as stream:
        census = json.load(stream)
    with open(registry_path, encoding="utf-8") as stream:
        registry = json.load(stream)
    with open(schedule_path, encoding="utf-8") as stream:
        schedule = json.load(stream)

    census_sha256 = file_sha256(census_path)
    registry_sha256 = file_sha256(registry_path)
    schedule_sha256 = file_sha256(schedule_path)
    if registry.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unsupported registry schema version")
    if schedule.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unsupported schedule schema version")
    if registry.get("census_sha256") != census_sha256:
        raise ValueError("registry census hash mismatch")
    if schedule.get("census_sha256") != census_sha256:
        raise ValueError("schedule census hash mismatch")
    if schedule.get("registry_sha256") != registry_sha256:
        raise ValueError("schedule registry hash mismatch")
    if registry.get("data_dir") != census.get("data_dir"):
        raise ValueError("registry data directory does not match census")
    if int(registry.get("open_cost_bytes", -1)) != int(census["open_cost_bytes"]):
        raise ValueError("registry open cost does not match census")
    if int(registry.get("min_partitions", -1)) != int(census["min_partitions"]):
        raise ValueError("registry minimum partitions does not match census")

    registry_episodes = registry.get("episodes", [])
    if len(registry_episodes) != len(EXPECTED_EPISODES):
        raise ValueError("registry must contain exactly three episodes")
    by_episode = {}
    for actual, (name, query_name, candidates) in zip(
            registry_episodes, EXPECTED_EPISODES):
        if actual.get("episode") != name or actual.get("query") != query_name:
            raise ValueError("registry episode order or query mismatch for " + name)
        if tuple(actual.get("candidates_mib", [])) != candidates:
            raise ValueError("registry candidates mismatch for " + name)
        census_episode = census["episodes"].get(name)
        if census_episode is None:
            raise ValueError("census lacks episode " + name)
        for field in ("files", "file_count", "query", "total_file_bytes", "total_rows"):
            if actual.get(field) != census_episode.get(field):
                raise ValueError(
                    "registry {} mismatch for {}".format(field, name)
                )
        census_layouts = {
            int(item["configured_mib"]): item
            for item in census_episode["candidate_layouts"]
        }
        actual_layouts = actual.get("candidate_layouts", [])
        if [int(item["configured_mib"]) for item in actual_layouts] != list(candidates):
            raise ValueError("registry candidate layouts mismatch for " + name)
        for selected in actual_layouts:
            mib = int(selected["configured_mib"])
            source = census_layouts[mib]
            expected = {
                "absent_columns": list(source["absent_columns"]),
                "configured_mib": mib,
                "dominated_by_mib": [
                    int(value) for value in source["dominated_by_mib"]
                    if int(value) in candidates
                ],
                "effective_bytes": int(source["effective_bytes"]),
                "missing_column_materialized_bytes_status":
                    source["missing_column_materialized_bytes_status"],
                "missing_column_row_values":
                    int(source["missing_column_row_values"]),
                "physical_layout_sha256": source["physical_layout_sha256"],
                "planned_ranges": int(source["planned_ranges"]),
                "planned_tasks": int(source["planned_tasks"]),
                "predicted_empty_ranges": int(source["empty_ranges"]),
                "predicted_empty_tasks": int(source["empty_tasks"]),
                "predicted_useful_tasks": int(source["useful_tasks"]),
                "present_columns": list(source["present_columns"]),
                "useful_layout_sha256": source["useful_layout_sha256"],
            }
            if selected != expected:
                raise ValueError(
                    "registry layout mismatch for {} {} MiB".format(name, mib)
                )
        by_episode[name] = actual

    treatments = [
        {
            "configured_mib": mib,
            "episode": name,
            "treatment_id": "{}:{}m".format(name, mib),
        }
        for name, _, candidates in EXPECTED_EPISODES
        for mib in candidates
    ]
    if registry.get("treatment_count") != 11:
        raise ValueError("registry treatment count must be 11")
    if schedule.get("treatment_count") != 11 or schedule.get("blocks") != 2:
        raise ValueError("schedule must contain 11 treatments and two blocks")
    expected_cpu_order = [item[0] for item in EXPECTED_EPISODES]
    if schedule.get("cpu_reference_order") != expected_cpu_order:
        raise ValueError("CPU reference order mismatch")
    if schedule.get("warmup_order") != treatments:
        raise ValueError("warmup allocation differs from frozen base treatment order")
    blocks = schedule.get("measured_blocks", [])
    if len(blocks) != 2:
        raise ValueError("expected exactly two measured blocks")
    rng = random.Random(int(schedule.get("seed")))
    for expected_block, block in enumerate(blocks):
        if block.get("block") != expected_block:
            raise ValueError("measured block index mismatch")
        expected_order = list(treatments)
        rng.shuffle(expected_order)
        if block.get("treatments") != expected_order:
            raise ValueError(
                "measured block {} differs from seeded order".format(expected_block)
            )

    identities = {
        "census_sha256": census_sha256,
        "registry_sha256": registry_sha256,
        "schedule_sha256": schedule_sha256,
    }
    return census, registry, schedule, by_episode, identities


def expected_planned_tasks(episode, configured_mib):
    for layout in episode["candidate_layouts"]:
        if int(layout["configured_mib"]) == configured_mib:
            return int(layout["planned_tasks"])
    raise ValueError(
        "unregistered candidate {} for {}".format(
            configured_mib, episode["episode"]
        )
    )


def expected_exact_layout(episode, configured_mib):
    for layout in episode["candidate_layouts"]:
        if int(layout["configured_mib"]) == configured_mib:
            return {
                "empty_ranges": int(layout["predicted_empty_ranges"]),
                "empty_tasks": int(layout["predicted_empty_tasks"]),
                "physical_layout_sha256": layout["physical_layout_sha256"],
                "planned_ranges": int(layout["planned_ranges"]),
                "planned_tasks": int(layout["planned_tasks"]),
                "useful_layout_sha256": layout["useful_layout_sha256"],
                "useful_tasks": int(layout["predicted_useful_tasks"]),
            }
    raise ValueError("unregistered exact layout")


def execute(
        spark,
        census,
        data_dir,
        episode,
        configured_mib,
        run_id,
        phase,
        block,
        journal,
        expected_sha=None,
        require_gpu=False):
    scope = {
        "block": block,
        "configured_mib": configured_mib,
        "episode": episode["episode"],
        "phase": phase,
        "query": episode["query"],
        "run_id": run_id,
    }
    journal.append({
        **scope,
        "event": "run_start",
        "timestamp_ns": time.time_ns(),
    })
    job_group_set = False
    try:
        spark.conf.set(
            "spark.sql.files.maxPartitionBytes",
            str(configured_mib * MIB),
        )
        paths = [os.path.join(data_dir, name) for name in episode["files"]]
        source, result = build_query(spark, paths, episode["query"])
        episode_names = set(episode["files"])
        episode_file_metadata = [
            item for item in census["files"] if item["file"] in episode_names
        ]
        actual_layout = exact_layout(source, episode_file_metadata)
        registered_layout = expected_exact_layout(episode, configured_mib)
        if actual_layout != registered_layout:
            raise RuntimeError(
                "exact physical layout mismatch in {}: {} != {}".format(
                    run_id, actual_layout, registered_layout
                )
            )
        planned_partitions = actual_layout["planned_tasks"]
        predicted_partitions = expected_planned_tasks(
            episode, configured_mib
        )

        spark.sparkContext.setJobGroup(
            run_id, run_id, interruptOnCancel=True
        )
        job_group_set = True
        started_ns = time.monotonic_ns()
        rows = result.collect()
        elapsed_ms = (time.monotonic_ns() - started_ns) / 1_000_000.0

        canonical_rows, result_sha256 = canonical(rows)
        plan = result._jdf.queryExecution().executedPlan().toString()
        record = {
            **scope,
            "actual_exact_layout": actual_layout,
            "elapsed_ms": elapsed_ms,
            "event": "run_result",
            "gpu_scan_in_plan": is_gpu_scan(plan),
            "plan": plan,
            "plan_sha256": hashlib.sha256(
                plan.encode("utf-8")
            ).hexdigest(),
            "planned_file_partitions": planned_partitions,
            "predicted_planned_file_partitions": predicted_partitions,
            "result_rows": len(canonical_rows),
            "result_sha256": result_sha256,
            "timestamp_ns": time.time_ns(),
        }
        journal.append(record)
        if expected_sha is not None and result_sha256 != expected_sha:
            raise RuntimeError("result mismatch in " + run_id)
        if require_gpu and not record["gpu_scan_in_plan"]:
            raise RuntimeError("GPU scan absent in " + run_id)
        journal.append({
            **scope,
            "event": "run_terminal",
            "status": "success",
            "timestamp_ns": time.time_ns(),
        })
        return record, canonical_rows
    except BaseException as error:
        journal.append({
            **scope,
            "error_message": str(error),
            "error_type": type(error).__name__,
            "event": "run_terminal",
            "status": "error",
            "timestamp_ns": time.time_ns(),
        })
        raise
    finally:
        if job_group_set:
            spark.sparkContext.setLocalProperty("spark.jobGroup.id", None)
            spark.sparkContext.setLocalProperty("spark.job.description", None)


def record_without_plan(record):
    return {key: value for key, value in record.items() if key != "plan"}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--census", required=True)
    parser.add_argument("--registry", required=True)
    parser.add_argument("--schedule", required=True)
    parser.add_argument("--event-log-dir", required=True)
    parser.add_argument("--journal", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--plans", required=True)
    parser.add_argument("--mode", choices=("cpu", "gpu"), required=True)
    parser.add_argument("--cpu-reference")
    args = parser.parse_args()

    for path in (args.output, args.plans, args.journal):
        if os.path.exists(path):
            raise FileExistsError("refusing to overwrite " + path)
    if args.mode == "gpu" and not args.cpu_reference:
        raise ValueError("--cpu-reference is required in GPU mode")

    census, registry, schedule, episodes, identities = load_and_validate_inputs(
        args.census, args.registry, args.schedule
    )
    cpu_reference = None
    if args.cpu_reference:
        with open(args.cpu_reference, encoding="utf-8") as stream:
            cpu_reference = json.load(stream)
        if cpu_reference.get("mode") != "cpu":
            raise ValueError("CPU reference file has the wrong mode")
        for key, value in identities.items():
            if cpu_reference.get(key) != value:
                raise ValueError("CPU reference {} mismatch".format(key))

    os.makedirs(args.event_log_dir, exist_ok=True)
    journal = Journal(args.journal)
    spark = None
    plans = {}
    records = []
    try:
        from pyspark.sql import SparkSession

        spark = (
            SparkSession.builder
            .appName("max-partition-bytes-longitudinal-" + args.mode)
            .config("spark.eventLog.enabled", "true")
            .config(
                "spark.eventLog.dir",
                "file://" + os.path.abspath(args.event_log_dir),
            )
            .config(
                "spark.rapids.sql.enabled",
                "true" if args.mode == "gpu" else "false",
            )
            .config("spark.rapids.sql.concurrentGpuTasks", "2")
            .config("spark.rapids.sql.concurrentGpuTasks.dynamic", "false")
            .config(
                "spark.rapids.sql.format.parquet.reader.type", "COALESCING"
            )
            .config(
                "spark.sql.files.openCostInBytes",
                str(registry["open_cost_bytes"]),
            )
            .config(
                "spark.sql.files.minPartitionNum",
                str(registry["min_partitions"]),
            )
            .config("spark.sql.adaptive.enabled", "false")
            .config("spark.sql.caseSensitive", "false")
            .config("spark.sql.shuffle.partitions", "32")
            .getOrCreate()
        )
        if spark.conf.get("spark.sql.files.maxPartitionNum", None) is not None:
            raise RuntimeError("spark.sql.files.maxPartitionNum must remain unset")
        application_id = spark.sparkContext.applicationId
        journal.append({
            **identities,
            "application_id": application_id,
            "event": "session_start",
            "mode": args.mode,
            "timestamp_ns": time.time_ns(),
        })
        if args.mode == "gpu":
            journal.append({
                "event": "allocation",
                "schedule": schedule,
                "timestamp_ns": time.time_ns(),
            })

        if args.mode == "cpu":
            references = {}
            for episode_name in schedule["cpu_reference_order"]:
                episode = episodes[episode_name]
                record, canonical_rows = execute(
                    spark,
                    census,
                    registry["data_dir"],
                    episode,
                    128,
                    "cpu-reference-" + episode_name,
                    "reference",
                    -1,
                    journal,
                )
                records.append(record_without_plan(record))
                plans[record["plan_sha256"]] = record["plan"]
                references[episode_name] = {
                    "canonical_rows": canonical_rows,
                    "result_sha256": record["result_sha256"],
                }
            output = {
                **identities,
                "application_id": application_id,
                "mode": "cpu",
                "records": records,
                "references": references,
            }
        else:
            references = cpu_reference.get("references", {})
            if sorted(references) != sorted(episodes):
                raise ValueError("CPU reference episode set mismatch")

            for treatment in schedule["warmup_order"]:
                episode_name = treatment["episode"]
                mib = int(treatment["configured_mib"])
                record, _ = execute(
                    spark,
                    census,
                    registry["data_dir"],
                    episodes[episode_name],
                    mib,
                    "warmup-{}-{}m".format(episode_name, mib),
                    "warmup",
                    -1,
                    journal,
                    expected_sha=references[episode_name]["result_sha256"],
                    require_gpu=True,
                )
                records.append(record_without_plan(record))
                plans[record["plan_sha256"]] = record["plan"]

            for block in schedule["measured_blocks"]:
                block_id = int(block["block"])
                for treatment in block["treatments"]:
                    episode_name = treatment["episode"]
                    mib = int(treatment["configured_mib"])
                    record, _ = execute(
                        spark,
                        census,
                        registry["data_dir"],
                        episodes[episode_name],
                        mib,
                        "block-{:02d}-{}-{}m".format(
                            block_id, episode_name, mib
                        ),
                        "measured",
                        block_id,
                        journal,
                        expected_sha=references[episode_name]["result_sha256"],
                        require_gpu=True,
                    )
                    records.append(record_without_plan(record))
                    plans[record["plan_sha256"]] = record["plan"]
            output = {
                **identities,
                "allocation": schedule,
                "application_id": application_id,
                "cpu_reference_file_sha256": file_sha256(args.cpu_reference),
                "mode": "gpu",
                "records": records,
            }

        write_json_exclusive(args.output, output)
        write_json_exclusive(args.plans, plans)
        journal.append({
            "event": "terminal",
            "status": "success",
            "timestamp_ns": time.time_ns(),
        })
    except BaseException as error:
        journal.append({
            "error_message": str(error),
            "error_type": type(error).__name__,
            "event": "terminal",
            "status": "error",
            "timestamp_ns": time.time_ns(),
        })
        raise
    finally:
        if spark is not None:
            spark.stop()
        journal.close()


if __name__ == "__main__":
    main()
