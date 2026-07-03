<!--
Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Stage 1 results: longitudinal and schema-aware `maxPartitionBytes`

## Evidence status

The preregistered experiment at commit `0983a70fb` completed successfully as run
`stage1-20260703-1432-0983a70fb`.

- Wrapper: `SUPPORTED`, 144 seconds total and 115 seconds for the GPU phase.
- GPU executions: 11 warm-ups and 22 measured runs in two randomized complete blocks.
- Correctness: every GPU result matched its canonical CPU result.
- Mechanism: every run matched the frozen seven-field physical-layout prediction.
- Plans: every GPU run contained the intended GPU scan.
- Failures, executor removals, standard spill, and nonzero RAPIDS retry/spill: zero.
- Stage 1 correctness and mechanism verdict: `SUPPORTED`.
- Performance verdict: `EXPLORATORY_INCONCLUSIVE`.

The machine-readable result is
[analysis/validated-analysis.json](analysis/validated-analysis.json), and the wrapper
verdict is [analysis/wrapper-verdict.json](analysis/wrapper-verdict.json). Raw event
logs and journals are under [raw](raw), and
[provenance/checksums.txt](provenance/checksums.txt) authenticates the accepted
runtime evidence files plus the frozen-input and executed-code hash manifests. This
post-run narrative is outside that checksum.

## What the fixed policy did

The preregistered rule removes a candidate only when it has the same useful row-group
grouping as another candidate and no better task/range/empty-work counts, with at least
one count strictly worse. It is a metadata-dominance filter, not a timing model.

| Episode | Policy action | Median whole-query time by candidate (ms) | Lowest observed two-run median |
|---|---|---|---|
| Fixed 2009, common columns | `SELECT(512)` | 128: 1744.5; 256: 1503.4; 512: 1388.3 | 512 |
| Variable-width 2010, group by `payment_type` | `SELECT(1024)` | 128: 2367.4; 256: 2152.1; 512: 2267.3; 1024: 2355.0 | 256 |
| Evolution through 2011, group by nullable `PULocationID` | `ABSTAIN(fallback=128)` | 128: 4951.0; 256: 4742.5; 512: 4043.5; 1024: 3458.2 | 1024 |

These are two-run medians, not stable rankings. They show that the structural rule did
not identify the lowest observed two-run median in every episode; they do not establish
a stable timing optimum.

In the fixed-schema case, 512 MiB preserved the same useful row-group grouping while
removing all 34/12 empty tasks present at 128/256 MiB. It also happened to have the
lowest observed median; the structural mechanism did not predict that timing result.

The variable-width case is the important counterexample. The rule selected 1024 MiB,
but the structurally dominated 256 MiB candidate had a roughly 9% lower median
whole-query time in this run. All candidates had the same 12 useful tasks and useful
layout. This experiment does not isolate whether the difference is noise, scheduling,
range-level effects, contention, or another runtime mechanism.

The evolution case shows why abstention is correct but the fixed fallback is not yet an
optimizer. Multiple useful layouts survived, so metadata dominance could not decide.
The 128 MiB fallback was roughly 43% slower than the observed 1024 MiB median. Fallback
selection therefore needs its own evidence-backed policy.

## What schema evolution established

During preparation, Spark's automatic merged-schema path could not reconcile the
observed `INT/DOUBLE` and `STRING/BIGINT` conflicts. That preparation error was not
preserved as raw Stage 1 evidence. The registered minimal explicit schemas did produce
CPU/GPU-equivalent results across all three query shapes in the accepted run.

For `PULocationID`, 339,897,217 rows in the older files have no physical column and
were materialized as null. File/range/task prediction remained exact across that
evolution. The materialized GPU byte cost of the absent column remains
`UNMODELED`; this experiment must not be cited as a memory-sizing validation for
missing columns.

## Practical conclusion

The metadata rule is implementable as a deterministic structural descriptor and
empty-work preference, but this run shows it is not performance-safe as a hard
candidate-pruning layer:

- its inputs come from Spark configuration/planning, file status, and Parquet footers;
- the local 36-file census cost 917.145066 ms without decoding row data;
- CPU and RAPIDS planners both exposed the exact task/range layout;
- it identifies avoidable empty work without using same-run timing feedback.

It is not a sufficient model for ideal `maxPartitionBytes`. Query shape, useful-work
distribution, parallel scheduling, physical ranges, contention, and downstream
aggregation/shuffle are hypotheses for timing variation that a future experiment must
separate; this run does not establish their individual causal effects.

## Next model to test

The next runtime model must rank every distinct safe physical layout. Exact
physical-layout hashes may deduplicate equivalent treatments, but metadata dominance
cannot be a hard runtime filter: the accepted variable-width run's lowest observed
median was the structurally dominated 256 MiB candidate. Task/range/empty counts
remain useful features and may support a separate overhead policy.

For candidate c, extend the decision-time collector to persist:

- N_plan(c), N_useful(c), and N_empty(c): planned, useful, and empty tasks from the
  deterministic file/range planner;
- for every planned task j, the task index, assigned file ranges and row groups,
  selected compressed bytes B_j(c), rows R_j(c), projected types, and schema/query
  fingerprint;
- P_task: effective overlapping Spark task capacity from prior event-log launch/finish
  intervals under the same executor/GPU configuration; this is task concurrency, not
  GPU kernel concurrency;
- L_empty and L_useful: prior task setup distributions for empty and useful tasks;
- per-task byte and row throughput distributions for comparable plan/read-schema
  fingerprints;
- aggregate input bandwidth and scheduler task-issue rate from prior episodes;
- the analyzed stage DAG, projected types, aggregate/grouping shape, and available
  Spark statistics.

The accepted census contains layout hashes and aggregate/quantile features, not the
required per-task B_j/R_j vectors. The collector must therefore add and validate the
mapping from task index to ranges, row groups, selected bytes, rows, and types. After
execution, join that frozen map to SparkListenerTaskEnd by input-reading stage and
Task Info.Index. Event logs alone do not identify file/range membership.

A simple first counterfactual assigns a heterogeneous service time to each planned
task:

```text
if B_j = 0 and R_j = 0:
  d_hat_j = L_empty
else:
  d_hat_j = L_useful + f(B_j, R_j, projected_types, plan_fingerprint)
```

Fit f only from earlier completed episodes. Simulate list scheduling of all empty and
useful task estimates over P_task slots. To compare with event-log stage
submission/completion, bound the input-reading-stage estimate by aggregate I/O and
scheduler issue capacity:

```text
T_hat_input_stage(c) = max(
  list_schedule_makespan({d_hat_j}, P_task),
  sum(B_j) / aggregate_byte_rate,
  N_plan(c) / task_issue_rate
)
```

Empty tasks are members of the parallel task set; their executor durations are not
summed as serial work. The standard event-log input-reading stage can include fused
projection and partial aggregation, so its task duration is not labeled pure Parquet
scan time. Use operator SQL metrics if a pure scan component is required.

The benchmark reports action time from immediately before `collect()` until it
returns. Build one capacity-aware estimate per barrier-separated stage and use the same
timer boundary:

```text
T_hat_timed_query(c) =
  T_action_driver_serial(c) + critical_path(stage_estimates, stage_DAG)
```

Serial dependent stages add along a path; independent branches take a max subject to
shared-resource contention. Metadata collection, file listing, DataFrame construction,
and physical planning belong to a separate end-to-end decision-latency objective and
must be added exactly once outside `T_hat_timed_query`.

The next harness must record driver timestamps for metadata start/end, file
listing/DataFrame construction end, physical-planning end, action start, job/stage
submission, and action end. `SparkListenerTaskEnd` alone cannot fit the serial driver
terms. If a future design pipelines I/O, decode, GPU work, or shuffle concurrently,
model them as distinct capacity-limited service centers rather than blindly summing
their elapsed times.

This model remains a hypothesis. Evaluate it with leave-one-month or
leave-one-quarter-out prediction: train only on earlier periods, predict every
distinct safe candidate for the next period, then compare predicted and observed
rankings. Treat a query-shape change as a held-out stratum, not a random row split.
The selector should ABSTAIN when its plan/schema stratum lacks history or candidate
prediction intervals overlap materially.

## Exact reproduction

The immutable snapshot expects the preregistration commit topology. For an exact rerun,
check out `0983a70fb`, provide the source files matching
[analysis/source-sha256.json](analysis/source-sha256.json), set `SPARK_HOME`,
`RAPIDS_JAR`, and a fresh `RUN_ID`, then invoke `run_experiment.sh`.

The later results commit packages this accepted run but is intentionally not the
preregistration commit.
