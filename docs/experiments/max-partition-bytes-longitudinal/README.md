# Longitudinal and schema-aware `maxPartitionBytes` feasibility pilot

## Status

**STAGE 0 COMPLETE — STAGE 1 NOT EXECUTED**

The immutable Stage 1 snapshot is
[preregistration/preregistration.json](preregistration/preregistration.json). It is
generated only after this specification is final. No Stage 1 CPU or GPU treatment may
run until the committed snapshot's verifier passes.

The user authorized local GPU experimentation and versioning on branch
`experiment/max-partition-bytes-longitudinal`. The complete wrapper has a global
1,800-second timeout, and the GPU phase must remain at or below 1,230 seconds and
0.35 local GPU-hours. There is no external spend, production-change, or deployment
authority.

## Claim boundary

This pilot tests schema-aware mechanism compliance and an exact metadata-dominance
filter for a newly planned file scan. Stage 1 performance output is limited to raw
timings and paired, within-block descriptive differences for preregistered dominance
pairs.

There is no performance ranking, regression-model claim, superiority,
noninferiority, equivalence, confidence interval, or production tuning claim.

## Completed Stage 0

The corpus has 36 source files totaling 13,580,215,763 bytes and 516,784,476 rows.
All 36 files contain exactly one Parquet row group, across six physical schema
fingerprints. Source identities are in
[analysis/source-sha256.json](analysis/source-sha256.json).

Exact metadata collection took 917.145066 ms. The six registered episodes are:

- `fixed_2009_first_3`
- `fixed_2009_all_12`
- `variable_2010_first_3`
- `variable_2010_all_12`
- `evolution_before_2011`
- `evolution_through_2011`

For the Stage 0 candidates 64, 128, 256, 512, 1024, and 2048 MiB, all 36
episode/candidate comparisons matched Spark exactly for:

- planned tasks and planned ranges;
- useful tasks;
- empty tasks and empty ranges;
- physical-layout SHA-256;
- useful-layout SHA-256.

The same 36 comparisons also passed through the pinned RAPIDS
`GpuFileSourceScanExec` planning path; its nested `DataSourceRDDPartition` layout is
recorded in
[analysis/stage0-gpu-planning-compliance.json](analysis/stage0-gpu-planning-compliance.json).

This supports the metadata-to-Spark physical-work mechanism within the registered
corpus. It is not a Stage 1 performance result.

## Decision-time sensors and leakage cutoff

Before the treatment schedule is used or any timed query executes, the runner must
serialize and checksum:

- source-file identities, bytes, rows, and one-row-group-per-file layout;
- selected-column chunk metadata;
- physical schema fingerprints and schema-evolution features;
- normalized query and minimal explicit read-schema identities;
- configured and effective split inputs;
- predicted planned tasks/ranges, useful tasks, and empty tasks/ranges;
- physical- and useful-layout hashes;
- candidate dominance relationships;
- pinned software, code, JVM, GPU, and topology identities.

The current episode's event-log metrics, useful tasks, batches, runtime, throughput,
memory, retry, spill, semaphore time, utilization, and kernel counters are outcomes.
They cannot enter the same episode's selector.

### Implementable collection path

The current selector requires no profiler, GPU kernel counter, learned coefficient, or
row-data sampling. Its inputs have direct collection paths:

| Input | Collection path | Required cost |
|---|---|---|
| File path and encoded length | Spark file index / filesystem status already used to plan the scan | Metadata listing |
| Row-group offset, compressed bytes, rows, and selected-column chunks | Parquet footer | Footer read; no row decode |
| Required columns and explicit read schema | Analyzed Spark query plus the registered coercion policy | Driver-side analysis |
| Open cost, min/max partition settings, and configured candidate | `SQLConf` | In-process lookup |
| Exact task/range grouping | Spark or RAPIDS scan's materialized input RDD partitions | Driver-side planning |
| Useful/empty prediction | Row-group midpoint ownership over the materialized ranges | Linear metadata pass |

On this local 36-file corpus, the complete footer census took 917.145066 ms. That is
evidence for local feasibility, not a remote-object-store latency claim. A production
implementation should cache footer identities by immutable file identity, invalidate
them when files change, and record listing/footer latency as model-operating cost.
If required metadata or a supported schema coercion is unavailable, the action is
`ABSTAIN`, not an inferred replacement value.

The exact physical-layout readback is a validation oracle in this pilot. A production
selector can run the same deterministic packing calculation before planning and sample
the readback for drift detection; the pinned CPU and RAPIDS probes demonstrate that the
oracle is accessible in both scan implementations.

## Schema evolution and missing-column scope

Schema evolution is a first-class model feature. The registry records physical schema
fingerprints, field presence, type families, and observed `INT/DOUBLE` and
`STRING/BIGINT` conflicts.

Spark automatic merge failed on those conflicts. Stage 1 therefore uses a minimal,
preregistered explicit read schema for the normalized query fields. If a required field
cannot be represented without ambiguous or unsupported coercion, the episode must
`ABSTAIN`.

The `evolution_through_2011` query includes nullable `PULocationID`. For files where
that column is absent, the model records the missing row values and Spark materializes
nulls. The bytes used to materialize that missing column are explicitly
`UNMODELED`; neither zero nor a derived byte estimate may be substituted. The Stage 1
schema-aware result remains exploratory because this byte demand is not modeled.

## Exact metadata-dominance policy

The implemented selector does not fit B0/B1/M1/M2 regressions and does not choose the
fastest measured candidate.

For each episode, candidate A is dominated by candidate B only when:

1. A and B have the exact same useful row-group grouping
   (`useful_layout_sha256`);
2. A has no fewer planned tasks or empty tasks;
3. A has no fewer planned ranges or empty ranges; and
4. at least one of those task/range/empty counts is strictly worse for A.

Exact physical-layout equality is separately recorded as treatment equivalence.
The selector removes candidates only by the dominance rule above.

- If exactly one nondominated candidate remains, emit `SELECT` for that candidate.
- If zero or multiple candidates remain, emit `ABSTAIN` and use the 128 MiB fallback.

The selector is computed from frozen metadata. Stage 1 timings do not change the
survivor set or action.

## Fixed Stage 1 design

| Episode | Candidates (MiB) |
|---|---|
| `fixed_2009_all_12` | 128, 256, 512 |
| `variable_2010_all_12` | 128, 256, 512, 1024 |
| `evolution_through_2011` | 128, 256, 512, 1024 |

The fixed schedule contains:

- three separate canonical CPU references;
- eleven GPU warm-ups;
- two randomized complete measured blocks;
- twenty-two measured GPU executions.

The schedule is already materialized at
[preregistration/schedule.json](preregistration/schedule.json). The episode and
candidate contract is at
[preregistration/episode-registry.json](preregistration/episode-registry.json).

The wrapper has one global 1,800-second deadline. The complete GPU phase is checked
against the 1,230-second budget. There is no per-query timeout claim.

## Per-run compliance and correctness

Before every CPU or GPU run, the benchmark reconstructs the scan and revalidates the
exact physical layout against the frozen registry: configured value, planned
tasks/ranges, useful tasks, empty tasks/ranges, and physical/useful hashes. A mismatch aborts
the accepted package before its timing can be used.

The Stage 1 validator also requires:

- every GPU output to equal the corresponding canonical CPU payload;
- the intended GPU scan in every warm-up and measured plan;
- exact schedule, journal, plan, registry, identity, and checksum agreement;
- one start, result, and successful terminal for every accepted attempt;
- every relevant task end reason to be success, with no missing or extra tasks;
- no executor removal, fatal OOM, standard Spark spill, or nonzero RAPIDS retry/spill;
- validator replay to reproduce the machine-readable verdict byte-for-byte;
- the wrapper and GPU budgets to pass.

RAPIDS retry/spill accumulators are sparse. Absence is interpreted as zero only under
the pinned producer semantics embodied by the frozen RAPIDS JAR and code identities.
Every observed nonzero retry/spill update is rejected. Standard Spark memory/disk spill
bytes are checked separately and must also be zero.

Failed, aborted, or harness-validation attempts remain evidence but do not enter
descriptive timing summaries.

## Stage 1 output contract

For each episode, the validator reports:

- dominated and surviving candidates from the frozen metadata policy;
- `SELECT` only for one survivor, otherwise `ABSTAIN` with fallback 128 MiB;
- raw scan-stage and whole-query timings for every accepted run;
- individual and median durations;
- paired within-block descriptive timing differences for each frozen
  dominated/dominator pair;
- planned/useful/empty mechanism coverage and errors;
- missing-column row values and
  `missing_column_materialization_bytes: UNMODELED`.

All paired differences are descriptive only. Performance inference remains
`EXPLORATORY_INCONCLUSIVE` regardless of their sign or magnitude.

## Preregistration freeze and verification

The snapshot `preregistration/preregistration.json` is generated only after these
specifications are final. The freeze and verifier pin:

- census, source hashes, Stage 0 planning evidence, Stage 0 verdict, registry, and
  schedule;
- README, model card, and manifest;
- runner and every Stage 0/Stage 1 preparation, freeze, verification, benchmark,
  validation, and wrapper-validation script;
- a sorted filename/SHA-256 manifest of every Spark 3.5.5 `dist/jars` JAR;
- RAPIDS assembly JAR path and hash;
- repository HEAD;
- Java identity;
- GPU model, memory, and driver identity;
- stable CPU topology, process/cgroup CPU affinity, and total host memory.

The verifier must pass immediately before execution. The snapshot itself records the
hashes of these final documents.

## Wrapper finalization and package integrity

The wrapper records its final budget check and terminal status, then
`validate_wrapper.py` emits a wrapper verdict. The finalized wrapper journal, wrapper
verdict, Stage 1 validator output, replay evidence, raw logs, plans, stdout/stderr,
identities, and checksums are packaged together. The final wrapper journal is
checksummed; package verification must fail if it changes.

## Split verdicts

- **STAGE 0 SENSOR/PHYSICAL-MECHANISM:** supported within the registered corpus.
- **STAGE 1 CORRECTNESS/MECHANISM:** pending execution.
- **SCHEMA-AWARE ESTIMATION:** exploratory; missing-column materialized bytes remain
  unmodeled.
- **METADATA-DOMINANCE ACTION:** pending validated Stage 1 package.
- **PERFORMANCE EFFECT:** `EXPLORATORY_INCONCLUSIVE` unconditionally.

## Current artifacts

Current specification and Stage 0 assets include:

- `README.md`, `model-card.md`, `manifest.yaml`;
- `run_experiment.sh`;
- `analysis/stage0-census.json`;
- `analysis/stage0-planning-compliance.json`;
- `analysis/stage0-gpu-planning-compliance.json`;
- `analysis/stage0-verdict.json`;
- `analysis/source-sha256.json`;
- `preregistration/episode-registry.json`;
- `preregistration/schedule.json`;
- `scripts/benchmark.py`;
- `scripts/census.py`;
- `scripts/freeze_preregistration.py`;
- `scripts/hash_sources.py`;
- `scripts/prepare_schedule.py`;
- `scripts/validate_experiment.py`;
- `scripts/validate_planning.py`;
- `scripts/validate_stage0.py`;
- `scripts/validate_wrapper.py`;
- `scripts/verify_preregistration.py`.

Stage 1 may execute only from the committed snapshot after verification succeeds.
The wrapper then packages the resulting evidence.
