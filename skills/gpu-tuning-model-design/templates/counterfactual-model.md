# Counterfactual model card

## Decision

- Candidate:
- Decision boundary:
- Owner/actuator lifecycle:
- Baseline:
- Primary objective and hard constraints:
- Current authority:
- Additional authority required:
- Evidence provenance:

## Evidence state

Label each input as Implemented, Measured, Externally validated, Hypothesis, or Proposal.

## Baseline execution graph

Record dependency edges, resource ownership, materialization, barriers, failure/retry boundaries, and critical path.

## Candidate execution graph

State exactly which edges, buffers, resource consumers, and failure semantics change.

## Demand and byte ledger

| Phase | Representation | Operations/rows | Bytes and passes | Resource lane | Evidence/uncertainty |
|---|---|---:|---:|---|---|

## Schedule equations

Use sums for serial phases. Use maxima only for demonstrated overlap. Include pipeline fill/drain, backpressure, task waves, and shared-resource contention.

## Prediction and break-even boundary

- Baseline prediction and interval:
- Candidate prediction and interval:
- Break-even inequality:
- Sensitivity variables:
- Validity region:

## Risks and correctness

- Semantics:
- Resource ownership:
- OOM/retry/spill:
- Scheduler and backpressure:
- Recovery and recomputation:
- Shims/platforms:

## Falsification experiment

- Predicted observable changes:
- Smallest discriminating experiment:
- Controls/ablations:
- Abort criteria:
- Reject if:
- Promote if:

## Priority decision

Choose one: reject, retain as hypothesis, measure, prototype, implement.
