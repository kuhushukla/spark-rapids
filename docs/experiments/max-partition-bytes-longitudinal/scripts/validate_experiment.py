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

"""Mechanically validate the longitudinal Stage-1 journals, logs, and claims."""

import argparse
import gzip
import hashlib
import json
import os
import re
import statistics

from benchmark import file_sha256, load_and_validate_inputs


RAPIDS_ZERO_COUNT_METRICS = {
    "gpuRetryCount",
    "gpuSplitAndRetryCount",
}
RAPIDS_ZERO_BYTE_METRICS = {
    "gpuSpillToHostBytes",
    "gpuSpillToDiskBytes",
}


def load_json(path):
    with open(path, encoding="utf-8") as stream:
        return json.load(stream)


def load_journal(path):
    with open(path, encoding="utf-8") as stream:
        return [json.loads(line) for line in stream if line.strip()]


def canonical_sha(rows):
    payload = json.dumps(
        rows, sort_keys=True, separators=(",", ":"), default=str
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def validate_overall_terminal(journal, name):
    terminals = [item for item in journal if item.get("event") == "terminal"]
    if len(terminals) != 1 or terminals[0].get("status") != "success":
        raise RuntimeError(name + " lacks exactly one successful overall terminal")
    if journal[-1] != terminals[0]:
        raise RuntimeError(name + " overall terminal is not final")


def metric_number(name, value):
    if isinstance(value, (int, float)):
        return value
    text = str(value)
    if name in RAPIDS_ZERO_BYTE_METRICS:
        match = re.search(r"\((-?\d+) bytes\)", text)
        if match:
            return int(match.group(1))
    match = re.search(r"-?\d+", text.replace(",", ""))
    return int(match.group(0)) if match else 0


def parse_event_log(path, expected_groups):
    opener = gzip.open if path.endswith(".gz") else open
    app_ids = []
    application_end_count = 0
    executor_removed = []
    group_stages = {group: set() for group in expected_groups}
    stage_started = {}
    stage_completed = {}
    task_ends = {}
    rapids_nonzero = []
    standard_spill_bytes = 0
    with opener(path, "rt", encoding="utf-8") as stream:
        for line in stream:
            event = json.loads(line)
            kind = event.get("Event")
            if kind == "SparkListenerApplicationStart":
                app_ids.append(event.get("App ID"))
            elif kind == "SparkListenerApplicationEnd":
                application_end_count += 1
            elif kind == "SparkListenerExecutorRemoved":
                executor_removed.append({
                    "executor_id": event.get("Executor ID"),
                    "reason": event.get("Removed Reason"),
                })
            elif kind == "SparkListenerJobStart":
                group = event.get("Properties", {}).get("spark.jobGroup.id")
                if group in expected_groups:
                    for stage in event.get("Stage Infos", []):
                        stage_id = stage["Stage ID"]
                        group_stages[group].add(stage_id)
                        stage_started[stage_id] = stage
            elif kind == "SparkListenerStageCompleted":
                info = event["Stage Info"]
                stage_completed[info["Stage ID"]] = info
            elif kind == "SparkListenerTaskEnd":
                stage_id = event["Stage ID"]
                task_ends.setdefault(stage_id, []).append(event)
                metrics = event["Task Metrics"]
                standard_spill_bytes += (
                    int(metrics["Memory Bytes Spilled"])
                    + int(metrics["Disk Bytes Spilled"])
                )
                for accumulator in event["Task Info"].get("Accumulables", []):
                    name = accumulator.get("Name")
                    if name in RAPIDS_ZERO_COUNT_METRICS | RAPIDS_ZERO_BYTE_METRICS:
                        value = metric_number(name, accumulator.get("Update", 0))
                        if value != 0:
                            rapids_nonzero.append({
                                "metric": name,
                                "stage_id": stage_id,
                                "task_id": event["Task Info"]["Task ID"],
                                "value": value,
                            })

    if len(app_ids) != 1 or not app_ids[0]:
        raise RuntimeError("event log lacks exactly one application ID")
    if application_end_count != 1:
        raise RuntimeError("event log lacks exactly one application end")
    if executor_removed:
        raise RuntimeError("event log contains executor removal: " + str(executor_removed))
    scan_stage_by_group = {}
    for group, stages in group_stages.items():
        if not stages:
            raise RuntimeError("no stages for " + group)
        scan_candidates = []
        for stage_id in stages:
            completed = stage_completed.get(stage_id)
            if completed is None or completed.get("Failure Reason"):
                raise RuntimeError("missing/failed stage {} for {}".format(stage_id, group))
            events = task_ends.get(stage_id, [])
            expected = stage_started[stage_id]["Number of Tasks"]
            if len(events) != expected:
                raise RuntimeError("task count mismatch in stage {}".format(stage_id))
            for event in events:
                reason = event["Task End Reason"].get("Reason")
                info = event["Task Info"]
                if reason != "Success" or info["Failed"] or info["Killed"]:
                    raise RuntimeError("non-success task in stage {}".format(stage_id))
            input_records = sum(
                int(event["Task Metrics"]["Input Metrics"]["Records Read"])
                for event in events
            )
            if input_records > 0:
                scan_candidates.append(stage_id)
        if len(scan_candidates) != 1:
            raise RuntimeError(
                "{} has {} input-reading stages".format(group, len(scan_candidates))
            )
        scan_stage_by_group[group] = scan_candidates[0]
    return {
        "application_end_count": application_end_count,
        "application_id": app_ids[0],
        "executor_removed": executor_removed,
        "group_stages": group_stages,
        "rapids_nonzero_retry_or_spill": rapids_nonzero,
        "scan_stage_by_group": scan_stage_by_group,
        "stage_completed": stage_completed,
        "stage_started": stage_started,
        "standard_spill_bytes": standard_spill_bytes,
        "task_ends": task_ends,
    }


def expected_gpu_order(schedule):
    values = []
    for treatment in schedule["warmup_order"]:
        values.append((
            "warmup-{}-{}m".format(
                treatment["episode"], treatment["configured_mib"]
            ),
            treatment["episode"],
            int(treatment["configured_mib"]),
            "warmup",
            -1,
        ))
    for block in schedule["measured_blocks"]:
        for treatment in block["treatments"]:
            values.append((
                "block-{:02d}-{}-{}m".format(
                    block["block"],
                    treatment["episode"],
                    treatment["configured_mib"],
                ),
                treatment["episode"],
                int(treatment["configured_mib"]),
                "measured",
                int(block["block"]),
            ))
    return values


def validate_run_records(journal, expected, episode_queries, name):
    expected_sequence = [("session_start", None)]
    if name == "GPU":
        expected_sequence.append(("allocation", None))
    for scope in expected:
        for event in ("run_start", "run_result", "run_terminal"):
            expected_sequence.append((event, scope))
    expected_sequence.append(("terminal", None))
    actual_events = [item.get("event") for item in journal]
    expected_events = [event for event, _ in expected_sequence]
    if actual_events != expected_events:
        raise RuntimeError(name + " journal exact event sequence mismatch")

    results = []
    for record, (expected_event, scope) in zip(journal, expected_sequence):
        if scope is None:
            continue
        expected_scope = (
            scope[0],
            scope[1],
            scope[2],
            scope[3],
            scope[4],
            episode_queries[scope[1]],
        )
        actual_scope = (
            record.get("run_id"),
            record.get("episode"),
            int(record.get("configured_mib")),
            record.get("phase"),
            int(record.get("block")),
            record.get("query"),
        )
        if actual_scope != expected_scope:
            raise RuntimeError(name + " run-record scope/query mismatch")
        if expected_event == "run_terminal" and record.get("status") != "success":
            raise RuntimeError(name + " contains unsuccessful run terminal")
        if expected_event == "run_result":
            results.append(record)
    return results


def layout_by_treatment(registry):
    layouts = {}
    for episode in registry["episodes"]:
        for layout in episode["candidate_layouts"]:
            layouts[(episode["episode"], int(layout["configured_mib"]))] = layout
    return layouts


def enrich_runs(results, events, layouts):
    enriched = []
    for result in results:
        run_id = result["run_id"]
        stage_id = events["scan_stage_by_group"][run_id]
        stage = events["stage_completed"][stage_id]
        tasks = events["task_ends"][stage_id]
        task_metrics = []
        for event in tasks:
            inputs = event["Task Metrics"]["Input Metrics"]
            task_metrics.append({
                "bytes_read": int(inputs["Bytes Read"]),
                "disk_spill_bytes": int(
                    event["Task Metrics"]["Disk Bytes Spilled"]
                ),
                "duration_ms": (
                    int(event["Task Info"]["Finish Time"])
                    - int(event["Task Info"]["Launch Time"])
                ),
                "memory_spill_bytes": int(
                    event["Task Metrics"]["Memory Bytes Spilled"]
                ),
                "records_read": int(inputs["Records Read"]),
            })
        useful = [item for item in task_metrics if item["records_read"] > 0]
        layout = layouts[(result["episode"], int(result["configured_mib"]))]
        actual = {
            "empty_tasks": len(task_metrics) - len(useful),
            "planned_tasks": len(task_metrics),
            "useful_tasks": len(useful),
        }
        expected = {
            "empty_tasks": int(layout["predicted_empty_tasks"]),
            "planned_tasks": int(layout["planned_tasks"]),
            "useful_tasks": int(layout["predicted_useful_tasks"]),
        }
        expected_exact = {
            "empty_ranges": int(layout["predicted_empty_ranges"]),
            "empty_tasks": int(layout["predicted_empty_tasks"]),
            "physical_layout_sha256": layout["physical_layout_sha256"],
            "planned_ranges": int(layout["planned_ranges"]),
            "planned_tasks": int(layout["planned_tasks"]),
            "useful_layout_sha256": layout["useful_layout_sha256"],
            "useful_tasks": int(layout["predicted_useful_tasks"]),
        }
        if result.get("actual_exact_layout") != expected_exact:
            raise RuntimeError("journal exact layout differs in " + run_id)
        if actual != expected:
            raise RuntimeError(
                "mechanism mismatch in {}: {} != {}".format(run_id, actual, expected)
            )
        enriched.append({
            **result,
            "empty_scan_tasks": actual["empty_tasks"],
            "max_scan_task_duration_ms": max(
                item["duration_ms"] for item in task_metrics
            ),
            "scan_bytes_read": sum(item["bytes_read"] for item in task_metrics),
            "scan_standard_spill_bytes": sum(
                item["disk_spill_bytes"] + item["memory_spill_bytes"]
                for item in task_metrics
            ),
            "scan_records_read": sum(item["records_read"] for item in task_metrics),
            "scan_stage_id": stage_id,
            "scan_stage_makespan_ms": (
                int(stage["Completion Time"]) - int(stage["Submission Time"])
            ),
            "scan_task_count": actual["planned_tasks"],
            "useful_scan_tasks": actual["useful_tasks"],
        })
    return enriched


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--census", required=True)
    parser.add_argument("--registry", required=True)
    parser.add_argument("--schedule", required=True)
    parser.add_argument("--cpu-journal", required=True)
    parser.add_argument("--gpu-journal", required=True)
    parser.add_argument("--cpu-output", required=True)
    parser.add_argument("--gpu-output", required=True)
    parser.add_argument("--cpu-plans", required=True)
    parser.add_argument("--gpu-plans", required=True)
    parser.add_argument("--cpu-event-log", required=True)
    parser.add_argument("--gpu-event-log", required=True)
    parser.add_argument("--stage0-verdict", required=True)
    parser.add_argument("--preregistration", required=True)
    parser.add_argument("--prereg-verification", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if os.path.exists(args.output):
        raise FileExistsError("refusing to overwrite " + args.output)

    _, registry, schedule, episodes, identities = load_and_validate_inputs(
        args.census, args.registry, args.schedule
    )
    stage0 = load_json(args.stage0_verdict)
    preregistration = load_json(args.preregistration)
    prereg_verification = load_json(args.prereg_verification)
    if preregistration.get("lifecycle") != "PREREGISTERED":
        raise RuntimeError("preregistration lifecycle is not frozen")
    if (
        prereg_verification.get("status") != "VERIFIED"
        or prereg_verification.get("snapshot_sha256")
        != file_sha256(args.preregistration)
        or prereg_verification.get("environment")
        != preregistration.get("environment")
    ):
        raise RuntimeError("preregistration verification does not bind the snapshot")
    if (
        stage0["sensor_feasibility"] != "SUPPORTED"
        or stage0["treatment_distinctness"] != "SUPPORTED"
    ):
        raise RuntimeError("Stage 0 gates did not pass")

    cpu_journal = load_journal(args.cpu_journal)
    gpu_journal = load_journal(args.gpu_journal)
    validate_overall_terminal(cpu_journal, "CPU journal")
    validate_overall_terminal(gpu_journal, "GPU journal")
    cpu_output = load_json(args.cpu_output)
    gpu_output = load_json(args.gpu_output)
    cpu_plans = load_json(args.cpu_plans)
    gpu_plans = load_json(args.gpu_plans)
    for output, mode in ((cpu_output, "cpu"), (gpu_output, "gpu")):
        if output.get("mode") != mode:
            raise RuntimeError(mode + " output mode mismatch")
        for key, value in identities.items():
            if output.get(key) != value:
                raise RuntimeError(mode + " output identity mismatch: " + key)

    cpu_expected = [
        (
            "cpu-reference-" + episode,
            episode,
            128,
            "reference",
            -1,
        )
        for episode in schedule["cpu_reference_order"]
    ]
    gpu_expected = expected_gpu_order(schedule)
    episode_queries = {
        episode["episode"]: episode["query"] for episode in registry["episodes"]
    }
    cpu_results = validate_run_records(
        cpu_journal, cpu_expected, episode_queries, "CPU"
    )
    gpu_results = validate_run_records(
        gpu_journal, gpu_expected, episode_queries, "GPU"
    )
    cpu_sessions = [
        item for item in cpu_journal if item.get("event") == "session_start"
    ]
    gpu_sessions = [
        item for item in gpu_journal if item.get("event") == "session_start"
    ]
    if len(cpu_sessions) != 1 or len(gpu_sessions) != 1:
        raise RuntimeError("each journal requires exactly one session start")
    for session, mode in ((cpu_sessions[0], "cpu"), (gpu_sessions[0], "gpu")):
        if session.get("mode") != mode:
            raise RuntimeError(mode + " session mode mismatch")
        for key, value in identities.items():
            if session.get(key) != value:
                raise RuntimeError(mode + " session identity mismatch: " + key)
    allocations = [
        item for item in gpu_journal if item.get("event") == "allocation"
    ]
    if len(allocations) != 1 or allocations[0].get("schedule") != schedule:
        raise RuntimeError("GPU journal allocation differs from frozen schedule")

    compact_cpu = [
        {key: value for key, value in item.items() if key != "plan"}
        for item in cpu_results
    ]
    compact_gpu = [
        {key: value for key, value in item.items() if key != "plan"}
        for item in gpu_results
    ]
    if cpu_output.get("records") != compact_cpu:
        raise RuntimeError("CPU output records differ from journal")
    if gpu_output.get("records") != compact_gpu:
        raise RuntimeError("GPU output records differ from journal")
    if gpu_output.get("allocation") != schedule:
        raise RuntimeError("GPU output allocation differs from schedule")
    if gpu_output.get("cpu_reference_file_sha256") != file_sha256(args.cpu_output):
        raise RuntimeError("GPU output CPU-reference hash mismatch")

    references = cpu_output["references"]
    if set(references) != set(episodes):
        raise RuntimeError("CPU reference set mismatch")
    for episode, reference in references.items():
        if canonical_sha(reference["canonical_rows"]) != reference["result_sha256"]:
            raise RuntimeError("canonical CPU payload mismatch for " + episode)
    for result in cpu_results:
        if result["result_sha256"] != references[result["episode"]]["result_sha256"]:
            raise RuntimeError("CPU journal/reference mismatch")
        plan = cpu_plans.get(result["plan_sha256"])
        if plan is None:
            raise RuntimeError("CPU plan missing")
        if plan != result["plan"]:
            raise RuntimeError("CPU plan artifact differs from journal")
        if hashlib.sha256(plan.encode("utf-8")).hexdigest() != result["plan_sha256"]:
            raise RuntimeError("CPU plan hash mismatch")
    for result in gpu_results:
        if result["result_sha256"] != references[result["episode"]]["result_sha256"]:
            raise RuntimeError("GPU/CPU result mismatch in " + result["run_id"])
        if not result["gpu_scan_in_plan"]:
            raise RuntimeError("GPU scan absent in " + result["run_id"])
        plan = gpu_plans.get(result["plan_sha256"])
        if plan is None:
            raise RuntimeError("GPU plan missing")
        if plan != result["plan"]:
            raise RuntimeError("GPU plan artifact differs from journal")
        if hashlib.sha256(plan.encode("utf-8")).hexdigest() != result["plan_sha256"]:
            raise RuntimeError("GPU plan hash mismatch")

    cpu_groups = {item[0] for item in cpu_expected}
    gpu_groups = {item[0] for item in gpu_expected}
    cpu_events = parse_event_log(args.cpu_event_log, cpu_groups)
    gpu_events = parse_event_log(args.gpu_event_log, gpu_groups)
    if (
        cpu_output.get("application_id") != cpu_events["application_id"]
        or cpu_sessions[0].get("application_id") != cpu_events["application_id"]
    ):
        raise RuntimeError("CPU event log application ID mismatch")
    if (
        gpu_output.get("application_id") != gpu_events["application_id"]
        or gpu_sessions[0].get("application_id") != gpu_events["application_id"]
    ):
        raise RuntimeError("GPU event log application ID mismatch")
    if cpu_events["standard_spill_bytes"] != 0:
        raise RuntimeError("CPU standard spill is nonzero")
    if gpu_events["standard_spill_bytes"] != 0:
        raise RuntimeError("GPU standard spill is nonzero")
    if gpu_events["rapids_nonzero_retry_or_spill"]:
        raise RuntimeError("RAPIDS retry/spill metric is nonzero")

    layouts = layout_by_treatment(registry)
    enriched = enrich_runs(gpu_results, gpu_events, layouts)
    measured = [item for item in enriched if item["phase"] == "measured"]
    summaries = {}
    for episode in schedule["cpu_reference_order"]:
        summaries[episode] = {}
        candidates = next(
            item["candidates_mib"] for item in registry["episodes"]
            if item["episode"] == episode
        )
        for mib in candidates:
            runs = [
                item for item in measured
                if item["episode"] == episode
                and int(item["configured_mib"]) == int(mib)
            ]
            if len(runs) != 2 or {item["block"] for item in runs} != {0, 1}:
                raise RuntimeError("incomplete measured blocks")
            summaries[episode][str(mib)] = {
                "empty_tasks": sorted({item["empty_scan_tasks"] for item in runs}),
                "individual_scan_stage_makespan_ms": [
                    item["scan_stage_makespan_ms"] for item in runs
                ],
                "individual_whole_query_ms": [
                    item["elapsed_ms"] for item in runs
                ],
                "median_scan_stage_makespan_ms": statistics.median(
                    item["scan_stage_makespan_ms"] for item in runs
                ),
                "median_whole_query_ms": statistics.median(
                    item["elapsed_ms"] for item in runs
                ),
                "useful_tasks": sorted({item["useful_scan_tasks"] for item in runs}),
            }

    policy = {}
    for episode in registry["episodes"]:
        name = episode["episode"]
        layouts_for_episode = {
            int(item["configured_mib"]): item
            for item in episode["candidate_layouts"]
        }
        survivors = sorted(
            mib for mib, layout in layouts_for_episode.items()
            if not layout["dominated_by_mib"]
        )
        action = (
            {"kind": "SELECT", "configured_mib": survivors[0]}
            if len(survivors) == 1
            else {
                "kind": "ABSTAIN",
                "fallback_mib": 128,
                "reason": "multiple nondominated physical layouts",
            }
        )
        paired = []
        by_block = {
            (item["block"], int(item["configured_mib"])): item
            for item in measured if item["episode"] == name
        }
        for mib, layout in sorted(layouts_for_episode.items()):
            for dominator in layout["dominated_by_mib"]:
                differences = [
                    by_block[(block, mib)]["scan_stage_makespan_ms"]
                    - by_block[(block, int(dominator))]["scan_stage_makespan_ms"]
                    for block in (0, 1)
                ]
                paired.append({
                    "dominated_mib": mib,
                    "dominator_mib": int(dominator),
                    "scan_stage_difference_ms_by_block": differences,
                })
        policy[name] = {
            "action": action,
            "dominance_pairs_descriptive_only": paired,
            "surviving_candidates_mib": survivors,
        }

    result = {
        "applications": {
            "cpu": cpu_events["application_id"],
            "gpu": gpu_events["application_id"],
        },
        "correctness_and_compliance": {
            "all_gpu_results_match_cpu": True,
            "all_gpu_scans_present": True,
            "exact_schedule_complete": True,
            "fatal_or_non_success_tasks": 0,
            "mechanism_predictions_match_all_gpu_runs": True,
            "nonzero_rapids_retry_or_spill_accumulator_updates": 0,
            "rapids_sparse_metric_semantics": (
                "absence means zero only for the preregistered pinned producer; "
                "nonzero updates are emitted and rejected"
            ),
            "standard_spill_bytes": 0,
        },
        "identities": {
            **identities,
            "cpu_event_log_sha256": file_sha256(args.cpu_event_log),
            "gpu_event_log_sha256": file_sha256(args.gpu_event_log),
            "preregistration_sha256": file_sha256(args.preregistration),
            "preregistration_verification_sha256":
                file_sha256(args.prereg_verification),
            "stage0_verdict_sha256": file_sha256(args.stage0_verdict),
        },
        "metadata_dominance_policy": {
            "episode_count": len(policy),
            "episodes": policy,
            "mechanism_prediction_coverage": 1.0,
            "rule": preregistration["analysis_contract"]["policy"],
        },
        "performance_effect": {
            "claim": "ESTIMATION_ONLY_EXPLORATORY",
            "inference": "INCONCLUSIVE",
            "reason": "two measured blocks and three episodes cannot support ranking",
        },
        "runs": enriched,
        "schema_aware_estimation_feasibility": {
            "status": "MEASURED_EXPLORATORY",
            "missing_column_materialization_bytes": "UNMODELED",
            "note": (
                "schema/query/layout strata executed correctly; performance "
                "generalization is not established"
            ),
        },
        "stage0": stage0,
        "summaries": summaries,
        "verdicts": {
            "performance_effect": "EXPLORATORY_INCONCLUSIVE",
            "schema_aware_estimation": "EXPLORATORY_ONLY",
            "stage0_sensor_and_exact_planning": "SUPPORTED_WITHIN_REGISTERED_CORPUS",
            "stage1_correctness_and_mechanism": "SUPPORTED",
        },
    }
    os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
    with open(args.output, "x", encoding="utf-8") as stream:
        json.dump(result, stream, indent=2, sort_keys=True)
        stream.write("\n")


if __name__ == "__main__":
    main()
