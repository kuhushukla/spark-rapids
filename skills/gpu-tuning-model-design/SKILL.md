---
name: gpu-tuning-model-design
description: Builds and compares execution-aware models for new GPU Spark design and optimization ideas, including pipelining, background shuffle, MPP-style execution, compression, batching, and plan changes. Use before prioritizing or prototyping a new architecture.
license: CC-BY-4.0 AND Apache-2.0
metadata:
  spdx-file-copyright-text: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
model: inherit
---

# Model and Prioritize a GPU Optimization

## Purpose

Turn a design idea into a falsifiable counterfactual model that says when it should help, when it should not, what implementation authority it requires, and which experiment should come next. This complements feedback-loop design: a feedback loop tunes an implemented decision; this skill evaluates whether a new decision or execution structure deserves to exist.

When working in spark-rapids, use the [dynamic tuning guide](../../docs/design/dynamic-gpu-job-tuning.md) for repository-specific details. Start from [the model-card template](templates/counterfactual-model.md).

## Workflow

- [ ] Define the optimization contract, evidence state, and decision being considered.
- [ ] Reconstruct the baseline execution graph and critical path.
- [ ] Draw the candidate execution graph, including ownership and failure semantics.
- [ ] Build a byte/work ledger and per-resource demand model.
- [ ] Prove candidate treatments create distinct effective state or useful physical work.
- [ ] Compose durations according to the actual serial, parallel, or pipelined schedule.
- [ ] Derive break-even boundaries and sensitivity to uncertain inputs.
- [ ] Rank the idea and specify the smallest falsification experiment.

Create a visible checklist before doing substantial work. Record current and required authority plus evidence provenance. Verdicts are recommendations, not authority to edit code, run paid jobs, or mutate external systems. Do not recommend implementation until the graph, lifecycle, safety surface, and falsification test are explicit.

## 1. Define the decision

State the primary objective and constraints, candidate, baseline, decision boundary, responsible component, and lifecycle. Classify every material claim as Implemented, Measured, Externally validated, Hypothesis, or Proposal.

A proposal is not a knob. Pipelined shuffle, a background thread, or MPP routing changes the execution graph and therefore needs scheduling, memory ownership, backpressure, recovery, and observability contracts.

## 2. Model both execution graphs

For baseline and candidate, record:

- dependency edges and Spark barriers;
- task, executor, node, and fleet parallelism;
- shared GPU, CPU, memory, network, storage, and scheduler resources;
- buffers, queues, materialization, and spill points;
- startup, steady-state, fill, drain, and teardown;
- failure, retry, cancellation, and recomputation boundaries;
- the resource-constrained critical path.

Do not compare a detailed candidate to a simplified baseline. Preserve the same demand definitions and calibrated rates in both.

## 3. Account for work and bytes

For every phase, identify representation, logical and encoded bytes, passes, operations/rows, temporary memory, and resource lane. A compression ratio does not account for codec work; shuffle partition count does not determine shuffle bytes; file partition and GPU batch distributions are distinct.

Use a defensible capacity ceiling only for a lower bound. Use measured sustainable rates plus uncertainty for a prediction.

## 4. Check treatment distinctness

Before proposing a sweep, map each candidate through configured input → effective rule → physical work. Use metadata, source tracing, or a small compliance probe to identify indivisible units and treatment equivalence.

Do not spend performance runs on candidates that produce the same relevant execution graph, useful-work layout, or actuator state. If the proposed population cannot exercise the modeled mechanism, return **MEASURE** and specify the smallest workload or scale change that makes treatments distinguishable.

## 5. Compose the actual schedule

Serial phases add. Independent parallel branches complete according to their resource-constrained schedule. Fully overlapped phases use a maximum only when traces or an implementation contract demonstrate concurrency.

For a pipeline, include:

- producer and consumer rates;
- finite buffer capacity;
- fill and drain latency;
- backpressure and queueing;
- resource interference between stages;
- materialization/fallback paths;
- failure recovery when partially consumed data exists.

A background thread creates overlap only if it executes concurrently, has available CPU/device/network resources, and does not move the bottleneck through contention. MPP-style execution must model aggregate resource limits, data redistribution, skew, stragglers, and coordination—not ideal worker-count scaling.

## 6. Find break-even boundaries

Express the candidate as an inequality against the baseline and solve for the uncertain variable when possible. Sweep plausible ranges rather than hiding them in one point estimate. Report which inputs change the decision, measurement value, confidence, and validity region.

Separate:

- necessary conditions: without them the idea cannot win;
- sufficient conditions under the model;
- operational constraints that can veto an otherwise favorable estimate.

## 7. Decide the next investment

Choose one verdict:

- **REJECT**: cannot satisfy the objective or safety contract in the plausible region;
- **RETAIN AS HYPOTHESIS**: mechanism is plausible but essential evidence is missing;
- **MEASURE**: one or more inputs dominate uncertainty;
- **PROTOTYPE**: graph/lifecycle are viable and a minimal implementation can falsify the model;
- **IMPLEMENT**: supported in the declared validity region with integration and rollback plans.

Prioritize by expected decision value, not predicted speedup alone: benefit probability and magnitude must justify implementation cost, correctness surface, operational risk, and opportunity cost.

## Output

Return the completed model card, baseline/candidate graph comparison, treatment-distinctness/compliance matrix, equations with units, sensitivity or break-even boundary, evidence gaps, verdict, and next experiment. If a critical lifecycle or correctness input cannot be discovered, keep the idea labeled Proposal and request that evidence rather than assuming it.
