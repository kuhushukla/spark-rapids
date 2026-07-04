---
name: gpu-tuning-implement
description: Implements an authorized and experimentally justified dynamic tuning change in the RAPIDS Accelerator for Apache Spark, with observability, shadowing, safety bounds, GPU resource hygiene, retry semantics, Spark and Databricks shim coverage, and tests. Use after diagnosis and experiment design establish a bounded controller change.
license: CC-BY-4.0 AND Apache-2.0
metadata:
  spdx-file-copyright-text: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
model: inherit
---

# Implement a Dynamic GPU Tuning Change

## Purpose

Implement the smallest authorized change that connects a validated sensor, model, policy, and actuator at the correct Spark/RAPIDS decision boundary.

When working in the spark-rapids repository, the deeper design reference is [the dynamic tuning guide](../../docs/design/dynamic-gpu-job-tuning.md). The workflow below is self-contained when that repository-level document is not installed with the skill.

## Workflow

- [ ] Step 1: Confirm authority, inputs, experimental justification, and exact scope.
- [ ] Step 2: Trace the existing sensor, actuator, lifecycle, ownership, retry, and shim paths.
- [ ] Step 3: Write the controller/model card and implementation invariants.
- [ ] Step 4: Add observability and shadow behavior before active control.
- [ ] Step 5: Implement the smallest bounded control path with safe fallback.
- [ ] Step 6: Add unit, integration, failure, resource, shim, and metric tests.
- [ ] Step 7: Run proportionate validation and hand off for independent review.

**Create a visible TODO checklist before editing and keep it current.**

## Step 1: Authority and Required Inputs

Proceed only when the user has authorized code changes in the named repository. External cluster changes, paid runs, deployments, feature enablement, Git branches, and commits require their own explicit or clearly supplied authority.

Require or discover:

- optimization objective and hard safety/correctness constraints;
- approved hypothesis/model card and supporting experiment;
- validated [model lifecycle manifest](../gpu-tuning-model-lifecycle/templates/model-lifecycle.yaml), model payload hash, evidence-gate result, and append-only rollout/activation record, or an explicit reason they are not applicable;
- exact sensor and actuator;
- decision boundary and affected remaining work;
- validity region, fallback, and rollout mode;
- applicable Spark, Databricks, Scala, and platform profiles.

If workload identity, correctness contract, essential evidence, or external authority is missing and cannot be discovered, stop before the affected action and ask. A bounded code trace or implementation plan may still be delivered if it does not depend on the missing decision.

Do not invent user-facing configuration or integration contracts. New contracts require explicit instruction.

## Step 2: Trace the Existing Paths

Use code discovery before editing. Trace:

- RapidsConf or Spark config declaration, metadata, snapshots, and every consumer;
- driver-to-executor propagation and initialization;
- whole-plan versus AQE query-stage conversion;
- task admission, task completion, cancellation, and exception paths;
- RMM/task memory attribution and metric update cadence;
- spill framework and RmmRapidsRetryIterator wrappers;
- GpuSemaphore and PrioritySemaphore ownership and lock ordering;
- all related Spark and Databricks shims;
- current tests and generated documentation.

Classify the actuator as startup, query-plan, AQE-stage-plan, task-admission, retry-local, or cross-run. Do not implement same-run control through generic config mutation when consumers snapshot the value.

## Step 3: Controller Contract

Record in code comments or a colocated design/test artifact as appropriate:

- state/features and units;
- estimator, prior, uncertainty, and sample policy;
- objective and hard constraints;
- policy, bounds, hysteresis, cooldown, and abstention;
- observation and actuation delays;
- serial, parallel, pipelined, barrier, and shared-resource assumptions;
- fallback and automatic-disable condition;
- independent context, feature, metric, observation, model, policy, and persistence versions;
- immutable model payload hash, rollout transition state, activation authority, and rollback pointer;
- falsification test and validity region.

Model task waves and DAG barriers according to actual scheduling. Do not add serial times for parallel tasks or use a maximum for phases that do not overlap.

## Step 4: Observability and Shadowing

Before active decisions, expose enough evidence to audit:

- raw observation or reconciled summary;
- candidate action and confidence;
- actual action;
- abstention/fallback reason;
- safety margin;
- realized result at the correct attribution boundary;
- controller/model and all schema versions;
- stable observation, recommendation, actual-action, and result linkage IDs.

A realized outcome labels the actual action, not an unexecuted shadow recommendation. Active control is forbidden unless a valid rollout record places the immutable model in `canary` or `active` state and the activation authority is present; otherwise implement shadow or disabled behavior only.

Respect metric-volume limits. Event-log/task-end metrics are suitable for audit and cross-run learning, not a fast live control channel.

Default a new unvalidated policy to shadow or disabled behavior unless the user explicitly authorizes active-by-default semantics and evidence supports it.

## Step 5: Implementation Rules

- Keep the diff minimal and preserve established architecture.
- Use withResource and closeOnExcept for GPU resources, and safeClose or safeMap for collections as appropriate.
- Wrap GPU-allocating retryable work in established retry helpers.
- Release permits/resources on completion, cancellation, and exceptions.
- Preserve priority, fairness, lock ordering, and multi-thread-per-task semantics.
- Bound memory/task/partition actions and provide conservative fallback.
- Do not use routine OOM, retry, or spill as exploration.
- Do not resize already-admitted work unless ownership and accounting explicitly support it.
- Do not claim a setting is runtime effective merely because it lacks startupOnly metadata.
- Update every applicable shim with version-specific adaptations.
- After any pom.xml modification, run ./build/make-scala-version-build-files.sh 2.13.

## Step 6: Tests

Add tests for applicable surfaces:

- estimator math, dimensions, interpolation/percentile, priors, empty and capped histories;
- bounds, confidence abstention, hysteresis, cooldown, and fallback;
- parallel task waves, skew/order sensitivity, and head-of-line behavior;
- task completion, cancellation, exceptions, retry/split-and-retry OOM, and spill;
- no resource or permit leaks and no deadlock;
- metric scope, cadence, aggregation, and version tags;
- non-AQE and AQE final plans;
- Spark-version and Databricks shims;
- shadow versus active behavior and kill switch;
- correctness against CPU or previous behavior.

Use deterministic unit tests for policy logic. Use integration or GPU tests only where the boundary cannot be validated otherwise.

## Step 7: Validate and Hand Off

Run the narrowest relevant checks first, then proportionate suites. Record exact commands, profiles, results, and unavailable environments. Do not skip hooks or CI.

Inspect the final diff for unrelated changes. Do not commit unless authorized. If committing in this repository, use git commit -s and never bypass hooks.

Use [gpu-tuning-model-lifecycle](../gpu-tuning-model-lifecycle/SKILL.md) to create or update durable history, drift, and rollout artifacts, then hand off to [gpu-tuning-controller-review](../gpu-tuning-controller-review/SKILL.md) when available. The implementation is not promoted merely because tests pass.

## Output

Report:

- implemented sensor/model/policy/actuator path;
- decision boundary and effective scope;
- safety/fallback/shadow behavior;
- tests and exact results;
- untested profiles or blocked validation;
- experiment/controller versions;
- files changed;
- independent-review and rollout gates still required.
