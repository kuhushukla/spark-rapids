---
name: gpu-tuning-experiment
description: Designs, versions, runs, and analyzes reproducible experiments for GPU-accelerated Spark tuning models and control policies. Use when validating a bottleneck hypothesis, configuration candidate, memory estimator, AQE target, compression choice, or dynamic controller.
license: CC-BY-4.0 AND Apache-2.0
metadata:
  spdx-file-copyright-text: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
model: inherit
---

# Run a Reproducible GPU Tuning Experiment

## Purpose

Turn one falsifiable hypothesis into versioned raw evidence and a conclusion whose validity is no broader than the experiment.

When working in the spark-rapids repository, consult [the dynamic tuning guide](../../docs/design/dynamic-gpu-job-tuning.md) for deeper context. The workflow below is self-contained when that repository-level document is not installed with the skill.

## Workflow

- [ ] Step 1: Confirm authority and freeze the hypothesis, objective, schedule model, safety constraints, and statistical decision rules.
- [ ] Step 2: Trace the actuator lifecycle and sensor semantics.
- [ ] Step 3: Create a versioned manifest and artifact layout.
- [ ] Step 4: Validate correctness and instrumentation with a small run.
- [ ] Step 5: Run randomized/repeated baseline and treatment cases with safety aborts.
- [ ] Step 6: Analyze raw runs, uncertainty, confounders, and hold-out behavior.
- [ ] Step 7: Preserve reproducible artifacts, commit only when authorized, and report the bounded conclusion.

**Before editing or running jobs, create a visible TODO checklist for all steps.** Do not produce a final answer until required steps are complete or explicitly blocked.

## Step 1: Authority and Pre-registration

Before any external execution or mutation, record the authorized environment, permitted actions, maximum GPU-hours/runtime/dollars, credential mechanism/reference and availability, sensitive-data/log-redaction policy, cleanup obligations, and whether Git branch/commit creation is authorized. Never record credential values, tokens, passwords, private keys, kubeconfig contents, or other secrets in artifacts, logs, or Git. Do not infer authority from access. If authority or an essential input remains unknown after safe discovery, prepare only a bounded manifest/plan and ask before the affected action.

Write before examining treatment results:

- one-sentence hypothesis;
- primary metric, estimand, experimental unit, aggregation, and confidence level/interval method;
- claim form: superiority, noninferiority, equivalence, or estimation-only;
- exact statistic/estimand, effect direction, practical margin, one- or two-sided error rate, and decision rule;
- candidate family and multiplicity/simultaneous-inference method when more than one comparison can support the conclusion;
- paired/blocking/randomization design and schedule topology: serial phases, parallel task waves/branches, pipelines, barriers, and shared-resource contention;
- sample-size power or precision rule, stopping rule, and treatment of missing, failed, aborted, and outlier runs;
- hard constraints and abort thresholds;
- baseline and treatment delta;
- minimum meaningful effect;
- rejection criterion;
- training and hold-out cases;
- expected causal chain and secondary metrics;
- known confounders;
- allowed environment, cost/runtime budget, credential reference/mechanism (never a secret value), redaction, cleanup, and commit authorization.

Failure to reject superiority is not evidence of noninferiority or equivalence. Those claims require their own pre-registered margins and confidence-bound or test rule.

Discover workload identity and the correctness contract from available code/artifacts. If either remains missing and would change the test, stop and ask before executing. Change one causal factor at a time for attribution. If coupled settings must change together, define the complete candidate as one action and add ablations.

## Step 2: Validate Actuator and Sensors

Trace the exact code path. Classify the actuator as:

- executor startup;
- query planning;
- AQE stage planning;
- task admission;
- retry-local;
- cross-run launcher/router.

Prove it can affect the measured work before running a costly experiment. `spark.conf.set` is not evidence that already-running operators or executors observe a value.

For each metric document producer, scope, cadence, aggregation, units, retries/speculation, and blind spots. Add instrumentation validation cases when counters do not reconcile.

## Step 3: Versioned Artifacts

Use explicit lifecycle states: `DRAFT`, `PREREGISTERED`, `EXECUTED`, and `VALIDATED` (or `BLOCKED`). Preserve an immutable preregistration snapshot and checksum before treatment results are visible. Record amendments separately with timestamp, reason, changed fields, and whether treatment data had been examined; do not overwrite the preregistration snapshot. Finalization must remove or resolve every `pending` field.

Place experiment-only material outside the final design document. Use the user-specified directory. If none is specified, use a clearly named project-local directory such as `artifacts/tuning/<experiment-id>/` only when repository policy permits it.

Keep:

```text
<experiment-id>/
├── README.md             # hypothesis, procedure, commands, result, limitations
├── manifest.yaml         # immutable identity and pre-registered criteria
├── scripts/              # generation, launch, collection, analysis
├── queries/              # immutable query text or hashes
├── schedule.json         # allocation/order frozen before first treatment
├── attempts/             # harness/instrumentation failures and diagnostics
├── raw/                  # raw logs/metrics or durable external references
│   └── run-journal.jsonl # append-only start/terminal records for every attempt
└── analysis/             # derived tables/plots and machine-readable summary
```

Never overwrite prior raw runs. Use stable run IDs. If raw data is too large for git, version checksums, schemas, collection commands, and durable immutable locations.

For experiments intended to transfer beyond one workstation, separate protocol from execution. Give the dataset, query, runtime profile, and artifact store stable versioned IDs. Dataset identity contains logical snapshot and physical-layout digests plus location aliases; a local path or cloud URI is a resolved location, not the identity. The runtime profile declares launcher adapter, master/deploy mode, executor topology, effective Spark/RAPIDS configuration, storage/client context, and profiler capabilities.

Use a small launcher boundary:

```text
probe() -> discovered software, hardware, and capabilities
prepare(resolved run) -> immutable inputs and artifact locations
submit(resolved run) -> external application handle
wait/cancel(handle) -> terminal state
collect(handle) -> normalized artifact index
```

Begin with a local `spark-submit` adapter and a generic command adapter for cluster wrappers. Keep CSP SDK details out of workloads. Workloads receive resolved dataset/query parameters and output URIs; they must not hard-code the master, developer paths, event-log discovery, credentials, or local-only profiler assumptions.

Declare the run mode. **Replay** requires the same dataset, query, software, schedule, and runtime identity. **Replication** deliberately changes data, query, schema, hardware, storage, topology, or software and creates a declared result stratum. This distinction protects causal evidence; it does not imply that a deployed predictor requires exact identity. Transfer experiments should deliberately vary one feature family and test the relevant component's fallback. Use versioned common envelopes for the run journal, environment, normalized observations, artifact index, and verdict so experiment-specific analysis can coexist with common validators.

Write the allocation schedule durably before the first treatment. Maintain an append-only journal: write and flush a start record before each attempt and a terminal record after it with run ID, phase, treatment/config hash, timestamps, status, artifact references, and error/abort reason. Persist partial results incrementally so interruption cannot erase completed or failed attempts. Keep harness/instrumentation failures in `attempts/`; exclude them from the estimand unless pre-registered, but do not erase them.

Copy [the bundled manifest template](templates/experiment-manifest.yaml) into the experiment directory and fill every applicable field. It is the authoritative field list and separates typed experiment identity, derivation, workload, expected runtime profile, discovered environment, metric contract, procedure, and artifact references.

For a replay, record the base manifest hash and any experiment-specific invariant extensions. Regardless of that extension list, mandatory replay invariants are the resolved dataset snapshot/layout, query source and parameters, software/build artifacts, frozen schedule, runtime-profile definition, complete expected and observed effective Spark/RAPIDS configuration, and relevant storage/cache policy. The validator must fail replay on any mandatory or declared invariant mismatch. For a replication, record intentional deltas, create a new result stratum, and state the pooling policy. Record expected runtime constraints separately from the probed environment artifact. The metric contract must include event-time-aligned volatile storage/network capacity observations when those lanes can affect the result.

Before production or shared-cluster use, publish a versioned schema and validator for the manifest and common artifact envelopes. The validator must reject unknown or ill-typed fields, unresolved artifact references, replay identity mismatches, and replication results without a distinct stratum.

## Step 4: Small Validation

Before performance runs verify:

- CPU/GPU or accepted semantic comparison;
- intended physical and AQE-final plan;
- treatment is actually consumed;
- traces validate every claimed serial edge, parallel task wave/branch, pipeline overlap, barrier, and shared-resource interaction;
- metrics appear once at the expected scope;
- byte/counter reconciliation is within a declared tolerance;
- resource cleanup and retry/failure behavior;
- scripts fail loudly on missing data or unsuccessful jobs.

## Step 5: Execute

- Preserve warm-ups as separate run IDs.
- Randomize or counterbalance baseline/treatment ordering.
- Keep background workload, cache policy, cluster allocation, and data snapshot fixed.
- Choose repetitions from variance and target effect; three is only a smoke-test minimum.
- Capture every warm-up, measured, failed, and aborted attempt through the append-only journal; never rely on an end-of-experiment write for preservation.
- Apply pre-registered aborts immediately for correctness, fatal OOM, executor loss, runaway retry/spill, cost, or timeout.
- Do not use normal OOM/retry as an exploration strategy.

If GPU or cluster access is unavailable, finish the manifest/scripts and mark execution blocked. Do not invent measurements.

## Step 6: Analyze

Report individual runs and distributions. Include:

- effect size and confidence interval;
- primary and safety metrics;
- warm versus cold behavior;
- critical-path and resource-lane changes compared with the pre-registered resource-constrained schedule; use sums only for serial elapsed phases and maxima only for measured overlap;
- task waves, last-finisher/straggler behavior, pipeline fill/drain, and shared-resource contention;
- failed/aborted runs;
- treatment compliance;
- unexplained residuals;
- sensitivity to skew/order/scale;
- hold-out result;
- the pre-registered multiplicity or simultaneous-interval method for every family of candidate comparisons; if it was not pre-registered, label the family-wise conclusion exploratory;
- for noninferiority/equivalence, the pre-registered bound relative to the practical margin—never infer “no meaningful benefit/difference” only because superiority was not detected.

Separate exploratory findings from pre-registered confirmation. A model fit on a case is not validated on that same case.

## Step 7: Conclusion

Run a mechanical validator before issuing a verdict. It must verify manifest lifecycle/completeness, immutable identities and checksums, exact schedule/allocation, treatment compliance, expected run/block counts, the declared missing/failed/aborted policy, correctness and safety gates, and the pre-registered statistical decision rule. It writes the machine-readable verdict and exits nonzero on any failed gate. Human summaries must not claim a stronger result than this output.

Use one verdict:

- `SUPPORTED WITHIN VALIDITY REGION`
- `REJECTED`
- `INCONCLUSIVE`
- `EXECUTION BLOCKED`

State the exact validity region and next decision. Include the validator output and exact replay command in the handoff. Do not generalize from a microbenchmark, one query, one GPU, or one scale to Spark workloads broadly.

If files were changed, list versioned working-tree artifacts separately from large external raw data. Commit only when explicitly authorized; in this repository use `git commit -s` and never bypass hooks. Otherwise leave a complete working-tree handoff.
