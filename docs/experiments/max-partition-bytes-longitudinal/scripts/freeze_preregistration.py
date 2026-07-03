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

"""Create the immutable Stage-1 preregistration before treatment execution."""

import argparse
import datetime
import hashlib
import json
import os
import subprocess


def sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def spark_dist_jar_manifest(spark_home):
    jar_dir = os.path.join(spark_home, "dist/jars")
    names = sorted(
        name for name in os.listdir(jar_dir)
        if name.endswith(".jar") and os.path.isfile(os.path.join(jar_dir, name))
    )
    if not names:
        raise RuntimeError("Spark dist/jars contains no JARs")
    return [
        {"name": name, "sha256": sha256(os.path.join(jar_dir, name))}
        for name in names
    ]


def output(command):
    return subprocess.run(
        command,
        check=True,
        stderr=subprocess.PIPE,
        stdout=subprocess.PIPE,
        universal_newlines=True,
    ).stdout.strip()


def java_identity():
    completed = subprocess.run(
        ["java", "-version"],
        check=True,
        stderr=subprocess.PIPE,
        stdout=subprocess.PIPE,
        universal_newlines=True,
    )
    return (completed.stdout + completed.stderr).strip()


def gpu_identity():
    return output([
        "nvidia-smi",
        "--query-gpu=name,memory.total,driver_version",
        "--format=csv,noheader",
    ])


def host_topology_identity():
    wanted = {
        "Architecture",
        "CPU(s)",
        "On-line CPU(s) list",
        "Model name",
        "Socket(s)",
        "Core(s) per socket",
        "Thread(s) per core",
        "NUMA node(s)",
    }
    lscpu = json.loads(output(["lscpu", "--json"]))["lscpu"]
    cpu = {
        item["field"].rstrip(":"): item["data"]
        for item in lscpu
        if item["field"].rstrip(":") in wanted
    }
    if set(cpu) != wanted:
        raise RuntimeError("lscpu omitted required stable topology fields")
    cpuset_path = "/sys/fs/cgroup/cpuset.cpus.effective"
    if not os.path.isfile(cpuset_path):
        cpuset_path = "/sys/fs/cgroup/cpuset/cpuset.cpus"
    with open(cpuset_path, encoding="utf-8") as stream:
        cgroup_cpuset = stream.read().strip()
    with open("/proc/meminfo", encoding="utf-8") as stream:
        mem_total = next(
            line.split(":", 1)[1].strip()
            for line in stream
            if line.startswith("MemTotal:")
        )
    return {
        "cgroup_cpuset": cgroup_cpuset,
        "cpu": cpu,
        "mem_total": mem_total,
        "process_affinity": sorted(os.sched_getaffinity(0)),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--experiment-root", required=True)
    parser.add_argument("--spark-home", required=True)
    parser.add_argument("--rapids-jar", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if os.path.exists(args.output):
        raise FileExistsError("refusing to overwrite " + args.output)
    root = os.path.abspath(args.experiment_root)
    paths = {
        "census": os.path.join(root, "analysis/stage0-census.json"),
        "source_hashes": os.path.join(root, "analysis/source-sha256.json"),
        "stage0_planning": os.path.join(
            root, "analysis/stage0-planning-compliance.json"
        ),
        "stage0_gpu_planning": os.path.join(
            root, "analysis/stage0-gpu-planning-compliance.json"
        ),
        "stage0_verdict": os.path.join(root, "analysis/stage0-verdict.json"),
        "registry": os.path.join(root, "preregistration/episode-registry.json"),
        "schedule": os.path.join(root, "preregistration/schedule.json"),
    }
    code = {
        name: os.path.join(root, relative)
        for name, relative in {
            "benchmark": "scripts/benchmark.py",
            "census": "scripts/census.py",
            "freeze_preregistration": "scripts/freeze_preregistration.py",
            "hash_sources": "scripts/hash_sources.py",
            "planning_validator": "scripts/validate_planning.py",
            "prepare_schedule": "scripts/prepare_schedule.py",
            "runner": "run_experiment.sh",
            "stage0_validator": "scripts/validate_stage0.py",
            "stage1_validator": "scripts/validate_experiment.py",
            "verify_preregistration": "scripts/verify_preregistration.py",
            "wrapper_validator": "scripts/validate_wrapper.py",
        }.items()
    }
    specs = {
        name: os.path.join(root, relative)
        for name, relative in {
            "manifest": "manifest.yaml",
            "model_card": "model-card.md",
            "readme": "README.md",
        }.items()
    }
    for path in list(paths.values()) + list(code.values()) + list(specs.values()):
        if not os.path.isfile(path):
            raise FileNotFoundError(path)
    with open(paths["registry"], encoding="utf-8") as stream:
        registry = json.load(stream)
    with open(paths["schedule"], encoding="utf-8") as stream:
        schedule = json.load(stream)
    repo_root = os.path.abspath(os.path.join(root, "../../.."))
    spark_home = os.path.realpath(args.spark_home)
    rapids_jar = os.path.realpath(args.rapids_jar)
    environment = {
        "gpu": gpu_identity(),
        "host_topology": host_topology_identity(),
        "java": java_identity(),
        "rapids_jar_path": rapids_jar,
        "rapids_jar_sha256": sha256(rapids_jar),
        "repo_parent_head": output(
            ["git", "-C", repo_root, "rev-parse", "HEAD"]
        ),
        "spark_dist_jars": spark_dist_jar_manifest(spark_home),
        "spark_home": spark_home,
    }
    result = {
        "experiment_id": "max-partition-bytes-longitudinal-feasibility",
        "frozen_at_utc": datetime.datetime.now(
            datetime.timezone.utc
        ).isoformat(),
        "lifecycle": "PREREGISTERED",
        "statement": (
            "Frozen before any Stage-1 CPU/GPU treatment execution or timing "
            "result was examined."
        ),
        "authority": {
            "environment": "local shared host, one RTX A6000",
            "external_spend": 0,
            "maximum_gpu_phase_seconds": 1230,
            "maximum_local_gpu_hours": 0.35,
            "maximum_total_wall_minutes": 30,
            "production_change": False,
        },
        "objective": {
            "claim_form": "estimation-only exploratory feasibility",
            "primary": (
                "mechanically validate schema-aware useful/empty task "
                "prediction for every accepted run"
            ),
            "performance": (
                "retain individual and median scan/query durations as "
                "descriptive evidence only; no ranking or interval claim"
            ),
        },
        "analysis_contract": {
            "policy": (
                "remove candidates only when the exact useful row-group grouping "
                "is identical and task/range/empty counts are no better with at "
                "least one strictly worse; choose a candidate only when one "
                "survivor remains, otherwise ABSTAIN to 128 MiB fallback"
            ),
            "outputs": [
                "surviving and dominated candidates by episode",
                "paired within-block descriptive differences against a dominator",
                "individual and median scan/query durations",
                "mechanism prediction coverage and errors",
            ],
            "performance_inference": "none",
        },
        "identities": {
            name: sha256(path) for name, path in paths.items()
        },
        "code_sha256": {
            name: sha256(path) for name, path in code.items()
        },
        "environment": environment,
        "spec_sha256": {
            name: sha256(path) for name, path in specs.items()
        },
        "stage1": {
            "episode_registry": registry,
            "schedule": schedule,
            "cpu_references": 3,
            "gpu_warmups": 11,
            "gpu_measured": 22,
            "global_cpu_timeout_seconds": 300,
            "global_gpu_timeout_seconds": 1200,
            "missing_failed_outlier_policy": (
                "abort accepted run package on timeout, failure, missing or "
                "extra attempt; preserve failed attempt separately"
            ),
        },
        "queries": {
            "common": {
                "schema": [
                    "passenger_count LONG nullable",
                    "trip_distance DOUBLE nullable",
                ],
                "aggregate": (
                    "row/passenger counts and trip-distance null/NaN/sign counts"
                ),
            },
            "variable_width": {
                "schema_addition": "payment_type STRING nullable",
                "group_by": "payment_type",
            },
            "missing_location": {
                "schema_addition": "PULocationID LONG nullable",
                "group_by": "PULocationID",
                "expected_evolution": (
                    "missing/materialized null in 2009-2010, present in 2011"
                ),
            },
        },
        "fixed_configuration": {
            "master": "local[8]",
            "spark.sql.adaptive.enabled": False,
            "spark.sql.caseSensitive": False,
            "spark.sql.files.maxPartitionNum": "unset",
            "spark.sql.files.minPartitionNum": 8,
            "spark.sql.files.openCostInBytes": 4194304,
            "spark.sql.shuffle.partitions": 32,
            "spark.rapids.sql.concurrentGpuTasks": 2,
            "spark.rapids.sql.concurrentGpuTasks.dynamic": False,
            "spark.rapids.sql.format.parquet.reader.type": "COALESCING",
        },
        "abort_conditions": [
            "CPU/GPU canonical result mismatch",
            "GPU scan absent",
            "planned partition mismatch",
            "predicted useful/empty task mismatch",
            "failed, killed, missing, or extra task",
            "fatal OOM or executor loss",
            "nonzero retry or spill",
            "identity, schedule, journal, plan, or checksum mismatch",
            "CPU or GPU phase timeout",
        ],
        "split_verdict_rules": {
            "stage0": "retain existing mechanically validated verdict",
            "stage1_correctness_and_mechanism": (
                "SUPPORTED only when every accepted GPU run passes all gates "
                "and predicted planned/useful/empty task counts match"
            ),
            "schema_aware_estimation": (
                "EXPLORATORY_ONLY; missing-column materialized bytes remain "
                "explicitly unmodeled"
            ),
            "performance_effect": "EXPLORATORY_INCONCLUSIVE unconditionally",
        },
    }
    os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
    with open(args.output, "x", encoding="utf-8") as stream:
        json.dump(result, stream, indent=2, sort_keys=True)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())


if __name__ == "__main__":
    main()
