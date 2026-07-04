---
name: gpu-tuning-diagnose
description: Diagnoses performance and resource bottlenecks in GPU-accelerated Apache Spark jobs using repository code, plans, event logs, RAPIDS metrics, and explicit byte/work accounting. Use before proposing configuration changes, dynamic controllers, AQE extensions, or fleet/routing changes.
license: CC-BY-4.0 AND Apache-2.0
metadata:
  spdx-file-copyright-text: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
model: inherit
---

# Diagnose GPU Spark Tuning Opportunities

## Purpose

Produce an evidence-backed bottleneck diagnosis and a small set of falsifiable hypotheses. Do not implement a fix or change a running system unless the user separately authorizes it.

When working in the spark-rapids repository, consult [the dynamic tuning guide](../../docs/design/dynamic-gpu-job-tuning.md) for deeper context. The workflow below is self-contained when that repository-level document is not installed with the skill.

## Triage mode

For an explicitly read-only quick look, use a bounded triage instead of the full workflow:

1. capture the stated objective, plan/configuration evidence, and highest-signal metrics;
2. return a preliminary bottleneck statement that separates observations from hypotheses, plus confidence and evidence gaps;
3. make no configuration-value, architecture, or implementation recommendation;
4. route any consequential decision to the full workflow below.

Label the output **TRIAGE — NOT A TUNING DECISION**. Triage is not permitted for production changes, benchmark claims, controller promotion, or design prioritization.

## Workflow

- [ ] Step 1: State the optimization contract and evidence available.
- [ ] Step 2: Reconstruct workload identity, effective configuration, plan, topology, and timeline.
- [ ] Step 3: Trace logical, encoded, resident, transferred, shuffled, and spilled bytes.
- [ ] Step 4: Build the stage DAG and resource-constrained critical path.
- [ ] Step 5: Reconcile metrics with their exact scope, cadence, and semantics.
- [ ] Step 6: Rank at most three falsifiable hypotheses and define the next discriminating experiment.

**Before analysis, create a visible TODO checklist for these steps and keep it updated.** Do not produce a final diagnosis until every completed and blocked step is marked accurately.

## Step 1: Optimization Contract

Record:

- primary objective: latency percentile, throughput, cost, or another declared metric;
- correctness, OOM, retry, spill, cost, fairness, and overhead constraints;
- workload population to which the objective applies;
- comparison baseline;
- authority: diagnosis only, experiment, code change, or external deployment.

Discover missing context from code, saved artifacts, and authoritative documentation first. If the primary objective, workload identity, or correctness contract remains missing and different answers would change the diagnosis, stop and ask before ranking hypotheses. A bounded evidence inventory may still be delivered with the missing fields marked; do not infer values or substitute GPU utilization for the objective.

Classify important claims as `IMPLEMENTED`, `MEASURED`, `EXTERNALLY VALIDATED`, `HYPOTHESIS`, or `PROPOSAL`.

## Step 2: Identity, Configuration, and Plan

Capture or report missing:

- application/query identifier and normalized logical/physical plan;
- Spark/RAPIDS/cuDF/CUDA/JVM/platform versions and repository SHA;
- table/data snapshot, schema, projections, predicate values or selectivity, layout, scale, and skew;
- executor/GPU/CPU/memory/disk/network topology;
- effective Spark and RAPIDS configuration;
- shuffle manager implementation and transport, `spark.sql.shuffle.partitions`, AQE coalescing/skew/local-reader settings, exchange count/types, and whether the measured scan stage includes shuffle write;
- AQE initial and final plans;
- cache/warm-up/concurrent-workload conditions.

Trace a configuration to its consumer before saying it is active. In this repository inspect its `RapidsConf` declaration, startup/runtime metadata, snapshots, read sites, lazy/cached values, executor initialization, AQE stage conversion, tests, and shims.

For every candidate control surface, build a configured/effective/physical-work table:

| Configured input | Interacting settings/defaults | Effective value or rule | Consumer and lifecycle | Physical work created | Compliance evidence |
|---|---|---|---|---|---|

The physical-work column names the actual tasks, partitions, batches, buffers, requests, or other units affected, including indivisible layout or scheduling granularity. Two configured values are not distinct treatments when they resolve to the same effective value or create the same relevant physical work. Mark any unverified transition as a hypothesis.

## Step 3: Byte and Work Ledger

Start from [the byte-ledger template](templates/byte-ledger.csv) when useful. The bundled [admission extractor](scripts/extract_gpu_admission.py) can recover the per-stage `gpuMaxConcurrentGpuTasks` signal from Spark JSON event logs; inspect and extend it for other metrics rather than changing their semantics.

Use explicit units. Keep these distinct:

- logical/uncompressed input;
- encoded/compressed storage input;
- projected/decoded bytes;
- GPU-resident and peak temporary bytes;
- host-device bytes;
- logical and wire shuffle bytes;
- host and disk spill bytes in each direction;
- output logical and encoded bytes;
- rows, batches, tasks, and passes.

For each resource lane calculate both concepts when supported:

```text
time_lower_bound >= lane_demand / validated_capacity_ceiling
typical_time_estimate = lane_demand / measured_typical_rate
```

A measured sustainable rate is a calibrated estimate, not automatically a capacity ceiling; attach uncertainty. Do not use marketing peak rates as achieved rates. Do not use one operational-intensity ratio for lanes that process different bytes or passes.

## Step 4: Schedule and Critical Path

Build the Spark stage DAG. Mark:

- shuffle/barrier edges;
- narrow pipelined operators;
- independent branches that can overlap;
- materialization and retry boundaries;
- shared-resource contention;
- serial, overlapped, and fill/drain regions.

Use sums only for serial work and `max` only for demonstrated overlap. Unexplained time is a result to investigate, not overhead to silently distribute.

## Step 5: Metric Semantics

For every sensor record:

- producing symbol/tool;
- executor, task, stage, query, application, or cluster scope;
- live, release-time, task-end, stage-end, or cross-run cadence;
- counter, duration, high watermark, sample, or estimate;
- aggregation and retry/speculation behavior;
- clock/timebase, extraction transform/version, and missing/censored-value meaning;
- whether it is stable context, a slowly learned parameter, or volatile live state;
- known blind spots and overhead.

RAPIDS-specific cautions:

- semaphore holding time is not CUDA kernel time;
- GPU bubble time is based on semaphore waiters, not SM utilization;
- event-log accumulators arrive too late for a fast executor control loop;
- resident memory after spill is not counterfactual no-spill demand;
- retry and spill are safety signals, not evidence of optimal control;
- file partitions and GPU batches are different distributions.

## Step 6: Hypotheses

Return at most three ranked hypotheses. For each include:

- observation;
- causal mechanism;
- evidence for and against;
- predicted change in named metrics;
- decision boundary and plausible actuator, if one exists;
- smallest experiment that distinguishes it from alternatives;
- safety abort condition;
- current confidence and validity scope.

Do not recommend a configuration value merely because a metric is high. Link the action to a causal model and lifecycle.

## Output

Start with a one-sentence diagnosis or state that evidence is insufficient.

Then provide:

1. optimization contract;
2. evidence inventory and gaps;
3. configured/effective/physical-work table;
4. critical-path and byte/work ledger;
5. metric reconciliation;
6. ranked hypotheses;
7. next experiment;
8. claims that remain proposals rather than current capabilities.

Use exact repository paths/symbols and immutable artifact paths where available.
