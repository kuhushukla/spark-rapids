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

Place experiment-only material outside the final design document. Use the user-specified directory. If none is specified, use a clearly named project-local directory such as `artifacts/tuning/<experiment-id>/` only when repository policy permits it.

Keep:

```text
<experiment-id>/
├── README.md             # hypothesis, procedure, commands, result, limitations
├── manifest.yaml         # immutable identity and pre-registered criteria
├── scripts/              # generation, launch, collection, analysis
├── queries/              # immutable query text or hashes
├── raw/                  # raw logs/metrics or durable external references
└── analysis/             # derived tables/plots and machine-readable summary
```

Never overwrite prior raw runs. Use stable run IDs. If raw data is too large for git, version checksums, schemas, collection commands, and durable immutable locations.

Copy [the bundled manifest template](templates/experiment-manifest.yaml) into the experiment directory and fill every applicable field. The manifest must include:

```yaml
experiment_id: "..."
hypothesis: "..."
objective:
  primary_metric: "..."
  estimand_and_experimental_unit: "..."
  aggregation: "..."
  confidence_level_and_interval_method: "..."
  minimum_effect: "..."
  sample_size_power_or_precision_rule: "..."
  stopping_and_missing_failed_outlier_policy: "..."
safety:
  abort_conditions: []
authority:
  environment_and_actions: "..."
  max_gpu_hours_runtime_dollars: "..."
  credential_reference_or_mechanism_never_secret_value: "..."
  sensitive_data_and_log_redaction: "..."
  cleanup_obligations: "..."
  git_branch_and_commit: "..."
baseline: {}
treatment_delta: {}
workload:
  query_or_hash: "..."
  data_snapshot: "..."
  seed: "..."
  scale_logical_and_encoded: "..."
software:
  repo_sha_build_profile_spark_rapids_cudf_cuda_jvm: "..."
hardware:
  gpu_cpu_memory_disk_network_topology: "..."
procedure:
  warmups: 0
  repetitions: 0
  paired_blocking_randomization_and_allocation: "..."
  cache_policy: "..."
  schedule_topology_serial_parallel_pipeline_barrier_contention: "..."
artifacts:
  raw_locations_and_checksums: []
```

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
- Capture every run, including failures and aborted runs.
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
- multiple-comparison correction when searching many candidates.

Separate exploratory findings from pre-registered confirmation. A model fit on a case is not validated on that same case.

## Step 7: Conclusion

Use one verdict:

- `SUPPORTED WITHIN VALIDITY REGION`
- `REJECTED`
- `INCONCLUSIVE`
- `EXECUTION BLOCKED`

State the exact validity region and next decision. Do not generalize from a microbenchmark, one query, one GPU, or one scale to Spark workloads broadly.

If files were changed, list versioned working-tree artifacts separately from large external raw data. Commit only when explicitly authorized; in this repository use `git commit -s` and never bypass hooks. Otherwise leave a complete working-tree handoff.
