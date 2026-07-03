---
name: gpu-tuning-controller-review
description: Performs a skeptical design and code review of dynamic GPU Spark tuning controllers, including actuator feasibility, metric semantics, model units, stability, safety, Spark/Databricks shims, GPU resource hygiene, retry behavior, experiments, and claims. Use before enabling, promoting, or merging a tuning loop.
license: CC-BY-4.0 AND Apache-2.0
metadata:
  spdx-file-copyright-text: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
model: inherit
---

# Review a Dynamic GPU Tuning Controller

## Purpose

Independently verify that a tuning proposal or implementation observes the claimed state, acts at a legal and effective boundary, remains safe and stable, and is supported by reproducible evidence.

When working in the spark-rapids repository, consult [the dynamic tuning guide](../../docs/design/dynamic-gpu-job-tuning.md) for deeper context. The workflow below is self-contained when that repository-level document is not installed with the skill.

## Authority

This skill authorizes read-only review and safe local checks only. External jobs, paid GPU/cluster time, deployments, feature/configuration changes, or other mutations require explicit authority and a declared budget. Without that authority, reproduce calculations from existing artifacts, mark external runs unverified, and use `INSUFFICIENT EVIDENCE` where the missing run is material.

## Workflow

- [ ] Step 1: Inventory claims, evidence state, objective, hard constraints, and review authority.
- [ ] Step 2: Trace sensors and actuators end to end.
- [ ] Step 3: Check model math, units, uncertainty, timing, and validity.
- [ ] Step 4: Check controller stability, composition, rollback, and failure behavior.
- [ ] Step 5: Review implementation, GPU resources, retry paths, shims, and tests.
- [ ] Step 6: Audit experiments and reproduce critical claims where feasible.
- [ ] Step 7: Issue a confidence-gated verdict with actionable findings.

**Create a visible TODO checklist before reviewing.** Review code and raw evidence rather than relying on design prose.

## Step 1: Contract and Claims

Require:

- primary objective and target population;
- correctness and resource constraints;
- evidence labels: implemented, measured, externally validated, hypothesis, proposal;
- current behavior versus proposed behavior;
- validity region and controller/model version.

Discover missing items from code, raw artifacts, and authoritative sources. If workload identity, correctness contract, or evidence needed for a safety/performance claim remains unavailable, do not infer it: issue `INSUFFICIENT EVIDENCE` for the affected scope. A partial review may continue only where the conclusion is independent of the missing input.

Flag utilization-only objectives, undefined “optimal,” unscoped benchmark claims, and analogy presented as parameter validation.

## Step 2: Sensor and Actuator Trace

For every sensor verify:

- producing symbol and measurement mechanism;
- exact units and semantics;
- executor/driver and task/stage/query/application scope;
- live/release/task-end/stage-end cadence;
- retry/speculation/aggregation behavior;
- overhead and missing state.

For every actuator verify:

- declaration and all read sites;
- startup/runtime metadata versus actual initialization;
- SQLConf/SparkConf snapshot and driver-executor propagation;
- cached/lazy values;
- earliest effective boundary;
- bounds, permissions, and rollback;
- applicable Spark and Databricks shims.

Fail claims of same-run control when the implementation merely changes a value that the remaining work never rereads.

## Step 3: Model Review

Check:

- dimensional consistency and named units;
- logical, encoded, resident, wire, and spill bytes are distinct;
- partitions, tasks, and batches are distinct;
- sums represent serial work and maxima represent demonstrated overlap;
- stage-DAG critical path and resource contention;
- priors, sample count, percentile definition, censoring, skew, and recency;
- prediction interval or confidence-based abstention;
- training/hold-out separation;
- assumptions and falsification test;
- model output maps to the actuator without double division or double counting.

Roofline/Ridgeline classification is not by itself a controller or end-to-end runtime prediction.

## Step 4: Control Safety and Composition

Check:

- observation and actuation delays;
- hard min/max and resource invariants;
- hysteresis, cooldown, and step limits;
- cold-start and low-confidence fallback;
- kill switch and automatic disable;
- shadow and canary modes;
- persistence identity, freshness, decay, and invalidation;
- other controllers that change the plant;
- precedence when safety and performance recommendations conflict;
- fairness and head-of-line blocking;
- retry, spill, executor loss, speculation, and dynamic-allocation behavior.

OOM/retry/spill must remain fallback safety mechanisms, not routine policy exploration.

## Step 5: Code and Tests

Apply project rules, including:

- all GPU resources use `withResource`, `closeOnExcept`, `safeClose`, or `safeMap` as appropriate;
- GPU-allocating retryable work uses the established retry wrappers;
- task completion and exception paths release admission state;
- no new user-facing configuration or integration contract without explicit authorization;
- all related Spark-version and Databricks shims are updated consistently;
- metrics do not create unacceptable event-log/driver volume;
- concurrency structures cannot deadlock or leak permits;
- unit tests cover estimators, bounds, interpolation, empty/prior state, skew/order, and concurrency;
- integration tests cover plans, metrics, failure injection, AQE, retries, and supported profiles;
- changes to a `pom.xml` include required Scala 2.13 synchronization.

## Step 6: Evidence Audit

Verify:

- immutable workload/data/software/hardware identity;
- complete effective configurations;
- raw runs, failures, checksums, and commands;
- randomized/counterbalanced order and warm-up policy;
- sufficient repetitions for observed variance;
- primary metric pre-registration and multiple-search accounting;
- correctness and safety outcomes;
- independent hold-out cases;
- conclusion no broader than measured evidence.

Reproduce the most consequential calculation or run when feasible. If raw evidence is unavailable, mark the claim unverified rather than accepting slide or summary values.

## Output

Begin with one verdict:

- `PASS`
- `PASS WITH NON-BLOCKING RISKS`
- `FAIL`
- `INSUFFICIENT EVIDENCE`

List findings in severity order. Each finding includes:

- file/symbol/artifact;
- issue;
- concrete evidence;
- impact on correctness, safety, stability, or claimed performance;
- required fix or experiment.

End with:

- verified claims;
- unverified/proposal claims;
- validity region approved, if any;
- required promotion gates.
