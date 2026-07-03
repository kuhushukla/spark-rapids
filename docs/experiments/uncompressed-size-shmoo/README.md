# Uncompressed-size and row-count shmoo for GPU Parquet scans

## Status

**CORPUS READY — INSTRUMENTATION COMPILES — PERFORMANCE PROTOCOL NOT YET FROZEN**

This experiment tests whether a model available at Spark planning time can predict the
decoded/projected GPU volume, and whether decoded bytes, rows, or both predict the
performance knee. Configured `spark.sql.files.maxPartitionBytes` is the treatment;
realized GPU bytes and rows are mediators and the quantities used to explain performance.

## Measurement boundaries

Every ratio names its exact numerator and boundary:

- `C_split`: compressed file-range bytes assigned by Spark. This is actuator-facing and
  includes bytes for columns that projection may later prune.
- `C_projected`: compressed Parquet column-chunk bytes for projected columns in selected
  row groups. This is model-facing when footer metadata can provide it.
- `U_projected_task`, `R_projected_task`: cumulative cuDF device bytes and rows emitted
  by the GPU scan in one task, after schema evolution and projection and before a
  downstream filter.
- `U_survive_task`, `R_survive_task`: cumulative cuDF device bytes and rows emitted by
  a downstream GPU filter in one task.
- `U_projected_batch_max`, `R_projected_batch_max` and their post-filter counterparts:
  largest emitted batch in one task. These are boundary footprints, not simultaneous
  resident memory, allocation traffic, or RMM peak memory.

The primary query-conditioned feedback ratio is, for example:

```text
q_projected = C_split / U_projected_task
U_hat       = C_split / q_projected
C_next      = q_projected * desired_uncompressed_target
```

`C_next` is only a proposed Spark split target. Spark then packs whole files/ranges and
Parquet row groups, so the realized workload must be measured. This multiplication
corrects REPORT_2's formula for a ratio defined as compressed over uncompressed.
A footer model may instead use `C_projected`; its ratio and predictions are labeled
`q_projected_chunks` so the two numerators cannot be mixed.

Feedback applies **between queries** because Spark fixes file partitions during planning.
A learned record is keyed by exact table snapshot/epoch, read schema, projection, and
predicate. Reuse across a normalized or similar predicate is a separately tested
near-match policy, never assumed equivalence. `q_survive` is undefined when no rows
survive; such observations are censored from ratio fitting and retained as zero-output
evidence.

## Models to compare

All model inputs must be obtainable from Spark planning, Parquet footers, or prior
instrumented executions:

- M0 footer-bytes: sum projected Parquet footer uncompressed column-chunk bytes.
- M1 rows-only: footer row count times historically measured GPU bytes per row.
- M2 combined: compressed/projected footer bytes and rows plus physical/logical types,
  nullability, casts, missing columns, schema epoch, projection, and exact predicate.
- M3 feedback ratio: prior matching-query `C_split/U_projected_task`, with explicit
  abstention when the key or confidence requirement is not met.

Filtered-output models separately estimate selectivity. Unsupported variable-width
evidence, unseen schema evolution, and insufficient history produce abstention or a
conservative upper bound rather than an invented point estimate.

Model fitting is chronological: train on 2009, rolling validation on 2010, and untouched
test on 2011. Query-shape holdouts are separate from time holdouts.

## Performance question

The controlled treatment is configured `maxPartitionBytes`. The explanatory axes are
the resulting decoded bytes and rows per task and maximum emitted batch. Responses are
whole-query and input-stage time, rows/s, decoded GiB/s, decode/scan time, RMM task peak,
retry, spill, OOM, semaphore pressure, and corroborating 100 ms GPU-utilization samples.
GPU utilization is corroboration, not a substitute for SM-occupancy profiling.

The initial candidate family is 32, 64, 128, 256, 512, 1024, and 2048 MiB. Reader
batch-byte/row limits and the general RAPIDS batch target remain fixed in the primary
sweep. A secondary partition-size × reader-batch experiment tests whether reader
batching mediates the result. All Spark file-packing controls are pinned, and a planning
census must show distinct realized workloads before a treatment is admitted.

The exact repetition, blocking, fit, confidence interval, plateau, and memory-quantile
rules will be frozen in the preregistration before timed runs. The intended decision is
the smallest **configured maxPartitionBytes candidate** whose conservative throughput
bound is within the frozen plateau tolerance and whose frozen peak-memory bound fits the
per-task budget without retry or spill in the tested lane. Absence of observed failures
is tested evidence, not a proof of safety.

## Derived real-data corpus

The 36 original monthly taxi files each contained one row group, so a direct split sweep
was degenerate. A controlled rewrite produced 825 sub-32-MiB, one-row-group Snappy files
across six independently rewritten schema strata.

- Source footer rows: 516,784,476.
- Derived footer rows: 516,784,476.
- Derived encoded bytes: 22,104,073,052.
- File size: median 26,165,203 bytes; maximum 30,729,458 bytes.
- Row-group compressed size: median 26,145,760 bytes; maximum 30,684,229 bytes.

The rewrite uses real row values but a balanced synthetic physical layout created by
Spark `repartition`; it does not preserve original file locality or task skew. Current
evidence proves matching footer row counts per source month, not row-multiset identity.
Physical Parquet schemas were rewritten by Spark; the corpus retains six separately
rewritten schema strata rather than byte-identical source schemas. Row fingerprints,
logical-schema mapping, manifest authentication, column-level footer metadata, and the
seven-treatment planner census are required before timed runs.

The external files are ignored by Git. Source and derived SHA-256 identities are recorded
in `analysis/derived-manifest.json`; footer observations are in
`analysis/derived-footer-census.json`. The derivation's nominal 16-MiB input is a
shard-count heuristic, not the achieved file size.

## Instrumentation

The RAPIDS scan and filter expose DEBUG SQL metrics:

- `outputBatchBytes`: cumulative deduplicated cuDF device footprint emitted at that
  operator boundary;
- `maxOutputBatchBytes`: per-task maximum emitted batch footprint;
- `maxOutputBatchRows`: per-task maximum emitted batch rows.

Task-level event-log updates preserve the per-task maxima; driver aggregation of maximum
metrics is a sum across tasks and must not be interpreted as a global maximum. Existing
metrics provide rows, batches, decode/scan time, task peak memory/footprint, retry, spill,
semaphore time, and task timing. Extractors select scan and filter plan nodes separately;
they never sum both boundaries. Device-footprint traversal is disabled below DEBUG.

## Evidence boundary

Corpus derivation and compilation are not performance results. Failed pilot attempts
remain recorded. The preregistered planning census, correctness checks, treatment order,
and analysis code will be versioned before measured treatments execute.
