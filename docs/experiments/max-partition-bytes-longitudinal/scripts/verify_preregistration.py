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

"""Verify the committed preregistration, code, inputs, software, and topology."""

import argparse
import hashlib
import json
import os
import subprocess


INPUTS = {
    "census": "analysis/stage0-census.json",
    "source_hashes": "analysis/source-sha256.json",
    "stage0_planning": "analysis/stage0-planning-compliance.json",
    "stage0_gpu_planning": "analysis/stage0-gpu-planning-compliance.json",
    "stage0_verdict": "analysis/stage0-verdict.json",
    "registry": "preregistration/episode-registry.json",
    "schedule": "preregistration/schedule.json",
}
CODE = {
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
}
SPECS = {
    "manifest": "manifest.yaml",
    "model_card": "model-card.md",
    "readme": "README.md",
}


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


def verify_hashes(root, frozen, mapping, label):
    actual = {}
    for name, relative in mapping.items():
        path = os.path.join(root, relative)
        actual[name] = sha256(path)
    if actual != frozen:
        raise RuntimeError(label + " hashes differ from preregistration")
    return actual


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--experiment-root", required=True)
    parser.add_argument("--spark-home", required=True)
    parser.add_argument("--rapids-jar", required=True)
    parser.add_argument("--snapshot", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if os.path.exists(args.output):
        raise FileExistsError("refusing to overwrite " + args.output)
    root = os.path.abspath(args.experiment_root)
    repo_root = os.path.abspath(os.path.join(root, "../../.."))
    with open(args.snapshot, encoding="utf-8") as stream:
        snapshot = json.load(stream)
    if snapshot.get("lifecycle") != "PREREGISTERED":
        raise RuntimeError("snapshot is not preregistered")

    verify_hashes(root, snapshot["identities"], INPUTS, "input")
    verify_hashes(root, snapshot["code_sha256"], CODE, "code")
    verify_hashes(root, snapshot["spec_sha256"], SPECS, "specification")

    spark_home = os.path.realpath(args.spark_home)
    rapids_jar = os.path.realpath(args.rapids_jar)
    environment = {
        "gpu": gpu_identity(),
        "host_topology": host_topology_identity(),
        "java": java_identity(),
        "rapids_jar_path": rapids_jar,
        "rapids_jar_sha256": sha256(rapids_jar),
        "repo_parent_head": output(
            ["git", "-C", repo_root, "rev-parse", "HEAD^"]
        ),
        "spark_dist_jars": spark_dist_jar_manifest(spark_home),
        "spark_home": spark_home,
    }
    if environment != snapshot["environment"]:
        raise RuntimeError("runtime software/topology differs from preregistration")

    relative_root = os.path.relpath(root, repo_root)
    diff = subprocess.run(
        ["git", "-C", repo_root, "diff", "--quiet", "HEAD", "--", relative_root]
    )
    if diff.returncode != 0:
        raise RuntimeError("tracked experiment files differ from committed HEAD")
    untracked = output([
        "git", "-C", repo_root, "ls-files", "--others", "--exclude-standard",
        "--", relative_root,
    ])
    if untracked:
        raise RuntimeError("untracked experiment files exist: " + untracked)

    result = {
        "environment": environment,
        "snapshot_sha256": sha256(args.snapshot),
        "status": "VERIFIED",
    }
    os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
    with open(args.output, "x", encoding="utf-8") as stream:
        json.dump(result, stream, indent=2, sort_keys=True)
        stream.write("\n")


if __name__ == "__main__":
    main()
