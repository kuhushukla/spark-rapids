---
name: gpu-tuning-model-lifecycle
description: Creates and maintains composable GPU Spark performance models from exact evidence provenance, transferable component features, historical priors, and live telemetry. Use when defining per-component fallback and uncertainty, calibrating on new data or environments, detecting drift, updating a deployed model, or deciding whether a challenger may progress through shadow and canary operation.
license: CC-BY-4.0 AND Apache-2.0
metadata:
  spdx-file-copyright-text: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
model: inherit
---

# Maintain a GPU Tuning Model

## Purpose

Keep a tuning model useful as data, queries, Spark/RAPIDS code, hardware, storage services, and contention change. Historical measurements are priors, not permanent facts. Live measurements describe current capacity, but may update durable history only through an explicit, auditable policy.

When working in the spark-rapids repository, consult [the dynamic tuning guide](../../docs/design/dynamic-gpu-job-tuning.md) for deeper context. This workflow is self-contained when that guide is unavailable.

## Authority

Read-only analysis, local artifact validation, and model comparison are allowed. Benchmark execution, cluster or cloud spending, deployments, changing an active policy, or writing production history require explicit authority and a declared budget. Never store credentials in model or experiment artifacts.

## Workflow

- [ ] Step 1: Freeze the objective, actuator boundary, safety constraints, and model version being challenged.
- [ ] Step 2: Define the mechanism model, context identity, feature schema, and metric provenance.
- [ ] Step 3: Separate immutable context, slowly learned parameters, and live state.
- [ ] Step 4: Build or reuse a portable calibration and validation protocol.
- [ ] Step 5: Fit a new immutable challenger and validate untouched contexts.
- [ ] Step 6: Run shadow and canary stages with valid outcome attribution.
- [ ] Step 7: Monitor residuals and safety, then reuse, decay, abstain, invalidate, or roll back.

Create a visible checklist before changing a model or policy. Preserve the incumbent until the challenger passes every declared gate.

## Step 1: Contract and Actuation Boundary

Record:

- objective and target population;
- correctness and resource constraints;
- actuator, bounds, and earliest point at which work rereads it;
- prediction horizon and decision latency;
- fallback, kill switch, and rollback owner;
- incumbent model, policy, feature, metric, context, and persistence versions.

Near-real-time measurement does not imply near-real-time actuation. A Spark file partition cannot be resized after its tasks exist, but a plugin hook immediately before a particular scan creates its file partitions can choose that scan's split limit using the selected files, schemas, predicates, configuration, history, and available live state. This is a per-read planning decision, not necessarily an application-wide launcher setting. Task admission and retry-local choices have different boundaries and require separate policies.

Do not build one monolithic query model merely because components share an objective. Decompose the prediction by mechanism. Shuffle manager/transport may condition a shuffle component; it need not invalidate a data-expansion component. Storage client changes may condition read service; they need not discard learned decoded row/byte ratios. Include a feature only in components whose causal behavior it changes.

## Step 2: Mechanism, Context, and Metric Contract

Start from resource-constrained execution graphs, not a black-box curve. Represent serial work by sums, demonstrated overlap by maxima, pipelines by fill/steady/drain terms, and barriers or task waves explicitly. Keep the smallest model that explains the decision and exposes a falsification test.

Create a versioned, structured contract using the [model lifecycle template](templates/model-lifecycle.yaml). Before production use, publish a schema and validator that reject missing, unknown, incompatible, or ill-typed fields. For every metric record:

- stable metric ID and schema version;
- producer symbol and software build;
- unit, representation, scope, and cadence;
- clock/timebase and attribution window;
- retry, speculation, aggregation, censoring, and missing-value semantics;
- extraction transform and code hash;
- expected overhead.

Keep reusable metric semantics separate from run-specific lineage. Observation batches record raw artifact locations/checksums, collection windows, and extraction-run IDs in training lineage.

A Spark event-log value available at task or stage end cannot drive a faster same-task loop. Use an executor-local signal for that loop and retain the event log for reconciliation.

Separate exact evidence provenance from prediction retrieval.

Evidence provenance records the complete observed query/plan, table snapshot, schemas, literal predicates, software, hardware, storage, reader, configuration, resource state, action, and result. It is immutable and checksummed so a claim can be reproduced. It does not define an all-or-nothing prediction key.

Prediction is a composition of component estimators. For each component declare:

- required and optional runtime inputs;
- the small feature subset that causally affects that component;
- compatible observations and hard semantic incompatibilities;
- an ordered fallback lattice from specific history through transferable priors;
- partial-pooling or shrinkage rule, age/sample weighting, interval, and abstention;
- behavior when an optional feature is missing.

For example, encoded-to-decoded rows/bytes may use file format, codec when known, projected column types, schema-evolution state, predicate/selectivity evidence, and optional table history. It must not require an exact query or full-plan match. Decode throughput may add GPU/software features. Read service may add connector/client and recent capacity. A task-wave component may use current slots, while the size/memory components continue when that live value is unavailable.

Treat query literals, snapshots, schema additions, and executor-count changes as feature values with uncertainty—not automatic new model families. A new column should borrow compatible column/type priors while known columns retain their learned estimates. Plan operators are runtime features when visible inside the current AQE boundary; unavailable future plan fragments disable only dependent components.

The modeled actuator is an action feature whose recommended and actual effective values are linked to each outcome. Unknown or incompatible metric/feature schema versions must not be silently pooled, but ordinary missing optional features invoke the declared fallback instead of global abstention.

## Step 3: Three Timescales

Classify every input per component:

1. **Runtime structural features**: available schemas, projected columns, predicate/operator structure, selected file/layout information, reader/configuration semantics, and compatible software epochs. These are inputs, not necessarily exact keys.
2. **Learned parameters**: per-column/type widths, expansion and selectivity distributions, fixed task cost, decode/filter/operator rates, footprint coefficients, and residuals. Estimate with hierarchical fallback, uncertainty, and recency.
3. **Live state**: available memory, admitted concurrency, current executor/slot estimate, queue depth, recent storage/network throughput and latency, throttling/retries, cache state, skew, spill, and competing load. Measure over a declared recent window and tolerate absence.

Use history for data description and mechanism parameters that cannot be calculated at planning time. Use current scan inputs wherever possible and live state for volatile capacity. Hard-invalidate only the affected component across an incompatible metric meaning or mechanism. Widen uncertainty, fall back, or disable one optional component when ordinary inputs are missing or changing.

Keep online observations in a quarantine buffer. They may update a durable prior only after correctness, identity, attribution, censoring, and safety checks pass. Raw observations are append-only; publish new model artifacts rather than mutating an active artifact in place.

## Step 4: Portable Calibration and Validation

A benchmark library is a falsification and calibration system, not a lookup table of global optimums. Use a shared experiment specification with independent registries for:

- datasets: logical snapshot and physical-layout digests, schema evolution, row/file/row-group census, URI aliases, reconstruction/source and license;
- queries: source/hash, parameters, correctness canonicalizer, expected plan mechanisms;
- runtime profiles: launcher adapter, master/deploy mode, Spark/RAPIDS config, executor topology, instance/GPU/storage expectations, and profiler capabilities;
- artifacts: append-only local or object-store destination.

Separate exact replay from replication:

- **replay** holds data, query, software, schedule, and runtime identity fixed;
- **replication** intentionally changes hardware, storage, topology, or software and creates a new context stratum.

The launcher owns experimental submit, status, cancel, log collection, remote paths, and cleanup. It exists to collect portable evidence; it is not the deployed controller. Runtime decisions belong at the product's legal actuation boundary—for scan sizing, normally a per-read plugin planning hook. The benchmark workload receives resolved dataset/query parameters and output locations; it must not hard-code local paths, master mode, credentials, or event-log discovery.

Start a new context with a small discriminating calibration set, not a full Cartesian sweep. Include enough low/mid/high action points and task waves to identify each mechanism, then validate an untouched query, snapshot, scale, or hardware stratum. Expand only when residuals or the decision boundary require it.

## Step 5: Challenger Fit and Evidence Gates

A model artifact is immutable and records:

- parent model ID plus manifest and payload hashes, and externally stored canonical hashes for the new immutable manifest and model payload;
- training experiment IDs with source manifest and validator-verdict hashes, selected run IDs, observation-batch checksums, and cutoff time;
- estimator code and parameter/calibration hashes;
- rejected/censored observations and reasons;
- supported feature range, residuals, uncertainty, and abstention behavior;
- independent context, feature, metric, model, policy, and persistence versions.

Pre-register controller-specific gates. At minimum define:

- minimum effective samples and independent runs per segment;
- maximum observation age and extrapolation distance;
- interval calibration or coverage target;
- maximum prediction error or bounded regret;
- benefit lower-confidence bound where promotion claims improvement;
- memory/safety upper-confidence margin and event budget;
- untouched hold-out strata;
- shadow duration/coverage, canary allocation, and rollback thresholds.

Prefer leave-one-query, leave-one-snapshot, leave-one-dataset, or leave-one-runtime-context-out validation over random row splits when transfer is the claim. Compare against simple baselines and the incumbent. If the decision is insensitive across a wide plateau, validate equivalence or bounded regret instead of selecting a noise-fit winner.

## Step 6: Shadow, Canary, and Attribution

The immutable model manifest records only its initial `candidate` state. Record later transitions in a separate append-only [rollout transition record](templates/rollout-transition.yaml):

`candidate -> offline_validated -> shadow -> canary -> active -> retired|invalid`

Each transition links both immutable lifecycle-manifest and model-payload hashes, the prior transition hash, authority, gate evidence, assignment, reason, and previous/rollback activation pointers. The manifest checksum is stored by the external artifact index using declared canonicalization; it is never a self-referential field inside the hashed manifest. An atomic external activation pointer selects an immutable model; activation never rewrites the model manifest. Promotion requires explicit authority. In shadow mode, log observation, prediction, confidence, candidate, actual incumbent action, realized result, abstention reason, safety margin, and all versions. The realized result labels the action actually taken; it is not a counterfactual label for an unexecuted candidate.

For canaries define eligibility, randomization unit, holdback/control, attribution window, interference exclusions, abort thresholds, and immutable assignment. Delayed outcomes must link observation, recommendation, actual action, and result by stable IDs. Preserve the incumbent and an atomic activation/rollback pointer.

## Step 7: Drift, Update, and Review

Define drift separately for:

- covariates/context features;
- prediction residuals and interval coverage;
- safety outcomes such as spill, retry, OOM, or fallback;
- volatile infrastructure rates and queues.

For each detector declare reference/current event-time windows, weighting, minimum samples, statistic/test, threshold, multiplicity handling, consecutive-window rule, and action. Actions are:

- **reuse**: compatible and calibrated;
- **soft decay/recalibrate**: same semantics, changed rates or load;
- **abstain**: insufficient, stale, out-of-range, or unstable evidence;
- **hard invalidate/fork**: incompatible semantics, schemas, mechanisms, or actuator lifecycle;
- **rollback**: safety or promotion gate violated.

Record reason codes, evidence, transition time, recovery/requalification criteria, and fallback. Review raw observations and code/config diffs before concluding that a model changed. A software/provider release is a trigger for targeted conformance and residual checks, not proof that every parameter changed.

## Recommended Maintenance Loop

1. Detect an identity, code/config, residual, safety, or capacity change.
2. Reconcile metric provenance and determine whether the change is semantic or transient.
3. Replay cheap correctness, treatment-compliance, and instrumentation tests.
4. Run only the experiments that distinguish competing mechanism hypotheses.
5. Fit an immutable challenger and update estimator unit tests and boundary tests.
6. Validate on untouched query/data/runtime strata.
7. Shadow, canary, independently review, and promote or abstain.
8. Continue live residual/safety monitoring with automatic fallback and rollback.

The agent proposes and analyzes changes. Before production use, mechanical validators must enforce schemas, identities, evidence gates, artifact lineage, and the activation-log hash chain. Human or delegated deployment authority promotes the model.

## Output

Produce:

- current-versus-proposed mechanism and version map;
- context/feature/metric contract;
- stable, learned, and live input classification;
- calibration and hold-out protocol;
- immutable challenger artifact and evidence-gate result;
- drift/invalidation matrix;
- shadow/canary/rollback plan;
- one verdict: `PROMOTE`, `KEEP INCUMBENT`, `RECALIBRATE`, `FORK MODEL FAMILY`, `ABSTAIN`, or `INSUFFICIENT EVIDENCE`.

State the approved validity region and the next legal actuation boundary. Do not generalize local-mode, one-storage-system, or one-shuffle-manager evidence to another context without replication.
