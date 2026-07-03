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

"""Mechanically validate allocation, correctness, plans, tasks, and split verdicts."""
import argparse
import gzip
import hashlib
import json
import random
import statistics


def load_json(path):
    with open(path, encoding="utf-8") as stream:
        return json.load(stream)


def load_journal(path):
    with open(path, encoding="utf-8") as stream:
        return [json.loads(line) for line in stream if line.strip()]


def canonical_sha(rows):
    payload = json.dumps(rows, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def percentile(values, fraction):
    ordered = sorted(values)
    position = fraction * (len(ordered) - 1)
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def exploratory_bootstrap_mean_ci(values, seed=20260704, draws=20000):
    rng = random.Random(seed)
    samples = [
        statistics.mean(rng.choice(values) for _ in values)
        for _ in range(draws)
    ]
    return [percentile(samples, 0.025), percentile(samples, 0.975)]


def validate_terminal(journal, mode):
    terminals = [item for item in journal if item.get("event") == "terminal"]
    if len(terminals) != 1 or terminals[0].get("status") != "success":
        raise RuntimeError(mode + " journal lacks exactly one success terminal")
    if journal[-1] != terminals[0]:
        raise RuntimeError(mode + " terminal is not the final journal record")


def parse_event_log(path, expected_groups):
    opener = gzip.open if path.endswith(".gz") else open
    application_ids = []
    group_stages = {group: set() for group in expected_groups}
    scan_stage_for_group = {}
    stage_info_from_start = {}
    stage_completed = {}
    task_ends = {}

    with opener(path, mode="rt", encoding="utf-8") as stream:
        for line in stream:
            event = json.loads(line)
            event_type = event.get("Event")
            if event_type == "SparkListenerApplicationStart":
                application_ids.append(event.get("App ID"))
            elif event_type == "SparkListenerJobStart":
                group = event.get("Properties", {}).get("spark.jobGroup.id")
                if group not in expected_groups:
                    continue
                for stage in event["Stage Infos"]:
                    stage_id = stage["Stage ID"]
                    group_stages[group].add(stage_id)
                    stage_info_from_start[stage_id] = stage
                    rdds = stage.get("RDD Info", [])
                    if any(
                        "GpuScan" in (rdd.get("Name", "") + rdd.get("Scope", ""))
                        for rdd in rdds
                    ):
                        prior = scan_stage_for_group.get(group)
                        if prior is not None and prior != stage_id:
                            raise RuntimeError(group + " maps to multiple scan stages")
                        scan_stage_for_group[group] = stage_id
            elif event_type == "SparkListenerStageCompleted":
                info = event["Stage Info"]
                stage_completed[info["Stage ID"]] = info
            elif event_type == "SparkListenerTaskEnd":
                task_ends.setdefault(event["Stage ID"], []).append(event)

    if len(application_ids) != 1 or not application_ids[0]:
        raise RuntimeError("event log lacks exactly one application ID")
    for group, stages in group_stages.items():
        if not stages:
            raise RuntimeError("no stages found for " + group)
        for stage_id in stages:
            info = stage_completed.get(stage_id)
            if info is None or info.get("Failure Reason"):
                raise RuntimeError("incomplete or failed stage {} for {}".format(stage_id, group))
            events = task_ends.get(stage_id, [])
            expected_tasks = stage_info_from_start[stage_id]["Number of Tasks"]
            if len(events) != expected_tasks:
                raise RuntimeError(
                    "task-end count {} != {} for stage {}".format(
                        len(events), expected_tasks, stage_id
                    )
                )
            for event in events:
                if (
                    event["Task End Reason"].get("Reason") != "Success"
                    or event["Task Info"]["Failed"]
                    or event["Task Info"]["Killed"]
                ):
                    raise RuntimeError("non-success task end in stage {}".format(stage_id))
    return {
        "application_id": application_ids[0],
        "group_stages": group_stages,
        "scan_stage_for_group": scan_stage_for_group,
        "stage_completed": stage_completed,
        "task_ends": task_ends,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cpu-journal", required=True)
    parser.add_argument("--gpu-journal", required=True)
    parser.add_argument("--cpu-output", required=True)
    parser.add_argument("--gpu-output", required=True)
    parser.add_argument("--cpu-plans", required=True)
    parser.add_argument("--gpu-plans", required=True)
    parser.add_argument("--cpu-event-log", required=True)
    parser.add_argument("--gpu-event-log", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    cpu_journal = load_journal(args.cpu_journal)
    gpu_journal = load_journal(args.gpu_journal)
    validate_terminal(cpu_journal, "CPU")
    validate_terminal(gpu_journal, "GPU")

    cpu_output = load_json(args.cpu_output)
    gpu_output = load_json(args.gpu_output)
    cpu_plans = load_json(args.cpu_plans)
    gpu_plans = load_json(args.gpu_plans)

    cpu_results = [item for item in cpu_journal if item.get("event") == "run_result"]
    gpu_results = [item for item in gpu_journal if item.get("event") == "run_result"]
    if len(cpu_results) != 1 or cpu_results[0]["run_id"] != "cpu-reference":
        raise RuntimeError("CPU journal must contain exactly one reference result")
    cpu_run_terminals = [
        item for item in cpu_journal if item.get("event") == "run_terminal"
    ]
    if (
        len(cpu_run_terminals) != 1
        or cpu_run_terminals[0].get("run_id") != "cpu-reference"
        or cpu_run_terminals[0].get("status") != "success"
    ):
        raise RuntimeError("CPU journal lacks one successful run-scoped terminal")
    cpu_result = cpu_results[0]
    cpu_sha = canonical_sha(cpu_output["canonical_rows"])
    if cpu_sha != cpu_result["result_sha256"]:
        raise RuntimeError("canonical CPU payload does not match journal hash")
    if cpu_output["cpu_reference"]["result_sha256"] != cpu_sha:
        raise RuntimeError("CPU output hash mismatch")
    if cpu_result["plan_sha256"] not in cpu_plans:
        raise RuntimeError("CPU plan missing from plan artifact")
    if hashlib.sha256(cpu_result["plan"].encode("utf-8")).hexdigest() != cpu_result["plan_sha256"]:
        raise RuntimeError("CPU journal plan hash mismatch")
    if cpu_plans[cpu_result["plan_sha256"]] != cpu_result["plan"]:
        raise RuntimeError("CPU plan artifact differs from journal")

    starts = [item for item in gpu_journal if item.get("event") == "session_start"]
    allocations = [item for item in gpu_journal if item.get("event") == "allocation"]
    if len(starts) != 1 or len(allocations) != 1:
        raise RuntimeError("GPU journal requires one session start and allocation")
    start = starts[0]
    allocation = allocations[0]
    values = start["values_mib"]
    blocks = start["blocks"]
    rng = random.Random(start["seed"])
    expected_schedule = []
    for _ in range(blocks):
        order = list(values)
        rng.shuffle(order)
        expected_schedule.append(order)
    if allocation["warmup_order"] != values:
        raise RuntimeError("warm-up allocation differs from declared values")
    if allocation["measured_schedule"] != expected_schedule:
        raise RuntimeError("measured allocation differs from seeded schedule")
    if gpu_output["schedule"] != expected_schedule:
        raise RuntimeError("GPU output schedule differs from journal allocation")

    expected_order = [
        ("warmup-{}m".format(mib), "warmup", -1, mib) for mib in values
    ]
    expected_order += [
        ("block-{:02d}-{}m".format(block, mib), "measured", block, mib)
        for block, order in enumerate(expected_schedule) for mib in order
    ]
    actual_order = [
        (item["run_id"], item["phase"], item["block"], item["configured_mib"])
        for item in gpu_results
    ]
    if actual_order != expected_order:
        raise RuntimeError("run-result order does not match exact allocation")
    starts_by_id = {
        item["run_id"]: item for item in gpu_journal if item.get("event") == "run_start"
    }
    if set(starts_by_id) != {item[0] for item in expected_order}:
        raise RuntimeError("run-start IDs do not match allocation")
    if len(starts_by_id) != len(expected_order):
        raise RuntimeError("duplicate run-start IDs")
    run_terminals = [
        item for item in gpu_journal if item.get("event") == "run_terminal"
    ]
    terminal_by_id = {}
    for item in run_terminals:
        run_id = item["run_id"]
        if run_id in terminal_by_id:
            raise RuntimeError("duplicate run terminal for " + run_id)
        terminal_by_id[run_id] = item
    if set(terminal_by_id) != {item[0] for item in expected_order}:
        raise RuntimeError("run-terminal IDs do not match allocation")
    if any(item.get("status") != "success" for item in run_terminals):
        raise RuntimeError("non-success run-scoped terminal in accepted journal")

    for result in gpu_results:
        if result["result_sha256"] != cpu_sha:
            raise RuntimeError("GPU/CPU result mismatch in " + result["run_id"])
        if not result["gpu_scan_in_plan"]:
            raise RuntimeError("GPU scan absent in " + result["run_id"])
        if hashlib.sha256(result["plan"].encode("utf-8")).hexdigest() != result["plan_sha256"]:
            raise RuntimeError("journal plan hash mismatch in " + result["run_id"])
        if gpu_plans.get(result["plan_sha256"]) != result["plan"]:
            raise RuntimeError("plan artifact mismatch in " + result["run_id"])
    compact_gpu_results = [
        {key: value for key, value in item.items() if key != "plan"}
        for item in gpu_results
    ]
    if gpu_output["records"] != compact_gpu_results:
        raise RuntimeError("GPU output records differ from journal")

    cpu_events = parse_event_log(args.cpu_event_log, {"cpu-reference"})
    gpu_groups = {item["run_id"] for item in gpu_results}
    gpu_events = parse_event_log(args.gpu_event_log, gpu_groups)
    if "cpu-reference" not in cpu_events["group_stages"]:
        raise RuntimeError("CPU reference stages missing")

    enriched = []
    for result in gpu_results:
        run_id = result["run_id"]
        stage_id = gpu_events["scan_stage_for_group"].get(run_id)
        if stage_id is None:
            raise RuntimeError("GPU scan stage missing for " + run_id)
        stage = gpu_events["stage_completed"][stage_id]
        events = gpu_events["task_ends"][stage_id]
        task_metrics = []
        for event in events:
            inputs = event["Task Metrics"]["Input Metrics"]
            task_metrics.append({
                "bytes_read": inputs["Bytes Read"],
                "duration_ms": (
                    event["Task Info"]["Finish Time"]
                    - event["Task Info"]["Launch Time"]
                ),
                "records_read": inputs["Records Read"],
            })
        useful = [task for task in task_metrics if task["records_read"] > 0]
        enriched.append({
            **{key: value for key, value in result.items() if key != "plan"},
            "empty_scan_tasks": len(task_metrics) - len(useful),
            "max_scan_task_duration_ms": max(task["duration_ms"] for task in task_metrics),
            "scan_bytes_read": sum(task["bytes_read"] for task in task_metrics),
            "scan_records_read": sum(task["records_read"] for task in task_metrics),
            "scan_stage_id": stage_id,
            "scan_stage_makespan_ms": stage["Completion Time"] - stage["Submission Time"],
            "scan_task_count": len(task_metrics),
            "useful_scan_tasks": len(useful),
        })

    measured = [run for run in enriched if run["phase"] == "measured"]
    grouped = {}
    for run in measured:
        grouped.setdefault(run["configured_mib"], []).append(run)
    if set(grouped) != set(values) or any(len(group) != blocks for group in grouped.values()):
        raise RuntimeError("measured block completeness failure")

    measured_summary = {}
    for mib, group in sorted(grouped.items()):
        measured_summary[str(mib)] = {
            "empty_scan_tasks_each_run": sorted(set(run["empty_scan_tasks"] for run in group)),
            "median_scan_stage_makespan_ms": statistics.median(
                run["scan_stage_makespan_ms"] for run in group
            ),
            "median_whole_query_elapsed_ms": statistics.median(
                run["elapsed_ms"] for run in group
            ),
            "planned_file_partitions": sorted(set(
                run["planned_file_partitions"] for run in group
            )),
            "scan_stage_makespan_ms": [
                run["scan_stage_makespan_ms"] for run in group
            ],
            "useful_scan_tasks_each_run": sorted(set(
                run["useful_scan_tasks"] for run in group
            )),
        }

    baseline_median = measured_summary["128"]["median_scan_stage_makespan_ms"]
    no_observed_5pct_improvement = all(
        summary["median_scan_stage_makespan_ms"] > 0.95 * baseline_median
        for mib, summary in measured_summary.items() if mib != "128"
    )
    by_block = {
        (run["block"], run["configured_mib"]): run for run in measured
    }
    exploratory_pairs = {}
    for mib in sorted(set(values) - {128}):
        percent_differences = []
        differences = []
        for block in range(blocks):
            candidate = by_block[(block, mib)]["scan_stage_makespan_ms"]
            baseline = by_block[(block, 128)]["scan_stage_makespan_ms"]
            differences.append(candidate - baseline)
            percent_differences.append(100.0 * (candidate - baseline) / baseline)
        exploratory_pairs[str(mib)] = {
            "exploratory_unadjusted_95pct_paired_bootstrap_ci_percent":
                exploratory_bootstrap_mean_ci(percent_differences),
            "mean_difference_ms": statistics.mean(differences),
            "mean_percent_difference": statistics.mean(percent_differences),
            "multiple_comparisons_adjusted": False,
            "per_block_difference_ms": differences,
        }

    mechanism_supported = all(
        run["useful_scan_tasks"] == 1
        and run["empty_scan_tasks"] == run["planned_file_partitions"] - 1
        for run in enriched
    )
    result = {
        "applications": {
            "cpu": cpu_events["application_id"],
            "gpu": gpu_events["application_id"],
        },
        "correctness_and_compliance": {
            "all_gpu_results_match_canonical_cpu_payload": True,
            "cpu_result_sha256": cpu_sha,
            "exact_allocation_complete": True,
            "gpu_scan_present_all_warmup_and_measured_runs": True,
            "non_success_task_end_reasons_in_all_relevant_stages": 0,
            "plans_and_hashes_match_journals": True,
        },
        "exploratory_performance": {
            "confidence_intervals_are_unadjusted_exploratory_not_confirmatory": True,
            "no_observed_5pct_improvement_in_seven_blocks": no_observed_5pct_improvement,
            "paired_scan_stage_vs_128m": exploratory_pairs,
            "verdict": "EXPLORATORY_INCONCLUSIVE",
        },
        "measured_summary": measured_summary,
        "row_group_empty_task_mechanism": {
            "supported": mechanism_supported,
            "verdict": "SUPPORTED" if mechanism_supported else "NOT_SUPPORTED",
        },
        "runs": enriched,
        "split_verdict": {
            "empty_task_mechanism": "SUPPORTED" if mechanism_supported else "NOT_SUPPORTED",
            "performance_effect": "EXPLORATORY_INCONCLUSIVE",
        },
    }
    with open(args.output, "x", encoding="utf-8") as stream:
        json.dump(result, stream, indent=2, sort_keys=True)
        stream.write("\n")


if __name__ == "__main__":
    main()
