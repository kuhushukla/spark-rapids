---
name: gpu-tuning-model-lifecycle
description: Creates and maintains evidence-driven GPU Spark performance models from historical priors and live telemetry. Use when defining reusable context keys and metric provenance, calibrating a model on new hardware or storage, detecting drift, updating a deployed model, or deciding whether a challenger may progress from offline validation through shadow and canary operation.
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

Near-real-time measurement does not imply near-real-time actuation. For example, a planned Spark file partition normally cannot be resized after its tasks exist. Live evidence may tune later file groups only if the reader exposes such a boundary; otherwise it informs a later stage, query, or run. Task admission and retry-local choices have different boundaries and require separate policies.

Do not combine distinct plants under one model merely because they share a knob name. Shuffle manager and transport, AQE behavior, partition count, topology, storage connector, and downstream operator pipeline can change the mechanism. Split or explicitly condition the model when these change causal behavior.

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

The canonical context separates:

- semantic identity: normalized logical and physical/AQE-final plan, table snapshot, schema, projection, predicate family/selectivity, layout and statistics version, downstream operators;
- execution identity: Spark/RAPIDS/cuDF/connector versions, reader mode, shuffle manager/transport, AQE, effective configuration, executor and GPU topology;
- storage identity: endpoint/region/class, filesystem/connector/client version, sync/async behavior, configured request concurrency, range/coalescing/readahead policy;
- plant identity: identities and active policies of other controllers that change semantics.

The modeled actuator is not an exact-match context field: it is an action feature whose recommended and actual effective values are linked to each outcome. Treat other controllers as exact context, covariates, or interference exclusions according to their causal effect.

Define exact-match fields, similarity features, allowed interpolation ranges, and confidence penalties. Unknown or incompatible schema versions must not be silently pooled. Every exact-context artifact reference must carry schema version, immutable location, and checksum; literal and artifact forms are mutually exclusive.

## Step 3: Three Timescales

Classify every input:

1. **Stable context**: semantics and mechanisms that select a model family. A hard mismatch selects another family or invalidates reuse.
2. **Learned parameters**: expansion ratio, fixed task cost, decode/filter/operator rate, footprint coefficients, and residual distributions. Estimate over multiple independent observations with uncertainty and recency.
3. **Live state**: available memory, admitted concurrency, queue depth, current storage/network throughput and latency, throttling/retries, cache state, skew, spill, and competing load. Measure over a declared recent window.

Use history to initialize learned parameters and priors. Use live state for volatile service capacity. Do not create a permanent context key for every transient S3 slowdown; decay or abstain when capacity changes. Conversely, do not merely decay across a semantic code, plan, schema, metric, or actuator-lifecycle incompatibility—hard-invalidate it.

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

The launcher owns submit, status, cancel, log collection, remote paths, and cleanup. The workload receives resolved dataset/query parameters and output locations; it must not hard-code local paths, master mode, credentials, or event-log discovery.

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
