# Uncompressed-size and row-count shmoo for GPU Parquet scans

## Status

**MEASURED LOCALLY — 628 GPU RUNS, 12 CPU REFERENCES, FOUR STUDIES COMPLETE**

This experiment asks two separate questions:

1. Can Spark planning metadata and prior executions predict decoded GPU rows and bytes?
2. Do decoded rows, task bytes, or emitted-batch bytes predict a useful performance region
   without violating an independent RMM memory bound?

Configured `spark.sql.files.maxPartitionBytes` is a treatment used to move the realized
workload. It is not itself the modeled answer.

## What was run

- Primary: 264 GPU runs; 252 measured runs across 84 cells and three annual epochs.
- High-size sequential extension: 120 GPU runs; 108 measured.
- Batch-control factorial: 164 GPU runs; 162 measured across partition size, general
  RAPIDS batch target, and reader soft limit.
- Cumulative growth and mixed-schema study: 80 GPU runs; 72 measured.
- CPU correctness: 12 references, one for every annual episode/query.
- Every timed study used three seeded randomized complete blocks. Exact schedules,
  failed attempts, raw event logs, task metrics, analysis, and lifecycle checks are
  versioned under `attempts/` and `preregistration/`.

All CPU reference hashes match the corresponding GPU hashes. All measured result hashes
are stable within their episode/query. Lifecycle validation found no failed tasks,
executor removal, retry, split-retry, or spill in these lanes.

## Findings

### Planning-time byte prediction works extremely well for the fixed-width lane

For two nullable 64-bit columns, Spark's actual `FilePartition` ranges were joined to
Parquet footer row counts before execution. Footer-predicted task rows matched emitted
scan rows exactly. Given those planned rows,

```text
U_hat = 2 * align64(8 * rows) + 2 * align64(ceil(rows / 8))
```

predicts one-batch cuDF boundary footprint with 0.000439% MAPE and at most 112 bytes
absolute error. The task observations reduce to 1,639 unique planned layouts after
removing repeated blocks and the duplicate common/filtered scan boundary.

The proposed generalization for other fixed-width projections is a sum over columns,
using each column's cuDF
physical width and a validity-mask term only when a mask is present or conservatively
predicted:

```text
U_hat = Σ [align64(cudf_type_width_j * rows)
           + mask_present_or_predicted_j * align64(ceil(rows / 8))]
```

Missing columns requested by the read schema must be modeled explicitly; they are not
necessarily free in GPU output.

### The useful performance region is query- and control-dependent

The primary range did not bracket a turnover: 2,048 MiB configured was fastest in every
annual query cell.

The high-size extension observed a turnover or flattening in several cells, but did not
establish a universal knee. For the 2009 common query:

| Configured MiB | Median task bytes | Median rows/task | Median query time |
|---:|---:|---:|---:|
| 2,048 | 560,258,624 (534 MiB) | 34,477,452 | 400 ms |
| 4,096 | 1,124,430,448 (1,072 MiB) | 69,195,711 | 369 ms |
| 8,192 | 1,388,530,552 (1,324 MiB) | 85,448,028 | 394 ms |

The 8,192-MiB cell was skewed: its p95 task volume was 2,261,826,256 bytes, so median
task size is not a safety bound. Across all annual query cells, the smallest configured
candidate within 5% of the best three-run median was 2,048 or 4,096 MiB. “Within 5%” is
a descriptive heuristic, not a confidence interval.

A candidate selected from the 2009 high-size results had 2011 median-time regret of
0.00% for common, 14.98% for filtered, 0.08% for variable-width, and 3.15% for the
schema-evolution query. No single candidate was within 5% of the best median in every
annual query cell; this exploratory result motivates query/epoch-specific policies but
does not establish that a global default can never be adequate.

![Annual GPU scan shmoo](analysis/annual-shmoo.svg)

### Batch controls causally change batching and performance in this lane

The full factorial independently varied:

- configured partition size: 2,048 / 4,096 / 8,192 MiB;
- `spark.rapids.sql.batchSizeBytes`: 512 / 1,024 / 2,048 MiB;
- `spark.rapids.sql.reader.batchSizeBytes`: 1,024 / 2,048 / 4,096 MiB.

Holding partition layout fixed, changing the general RAPIDS target moved the maximum
emitted batch boundary to approximately 0.5, 1, or 2 GiB. A lower reader soft limit also
capped the emitted batch. Thus both controls mediate batching.

For the 4,096-MiB common cell, median time was about 367 ms at a 1-GiB general target
and 2-GiB reader limit, versus 330 ms at a 2-GiB general target and 4-GiB reader limit.
At the original 1-GiB target, the largest batch was 1,073,643,664 bytes (about
0.9999 GiB), while cumulative task output was larger and split across batches.

This factorial supports batching mediation; it still does not isolate GPU occupancy,
filesystem cache, scheduling waves, or every reader internal. The application environment
event records the central 1-GiB general target and 2-GiB reader limit; the randomized
per-run mutations are bound by the frozen schedule and journal, then corroborated by the
observed batch response. Spark does not emit a new environment event for each mutation.

![Batch-control mediation](analysis/batch-mediation.svg)

### Feedback ratios require the correct numerator and an epoch key

Three byte quantities are distinct:

- `C_split`: sum of Spark-assigned `PartitionedFile.length` values; actuator-facing.
- `C_projected`: projected Parquet column-chunk bytes; footer/model-facing.
- `C_read_task`: Spark TaskMetrics `Bytes Read`; runtime physical read accounting.

The experiment now captures `C_split` from exact planned partitions. A 2009
`C_split / U_projected` ratio transferred with 2.62% MAPE to 2010 common scans but
57.07% MAPE to 2011. Runtime `C_read_task / U_projected` transferred within about
1–2% for the common projection, but it is diagnostic evidence and cannot be inverted
directly into `maxPartitionBytes`.

For the filtered query, the 2009 survivor model transferred to 2010 with 0.96% byte
MAPE and 3.40% `C_split / U_survive` MAPE. In 2011 those errors were 1.03% and 56.76%.
This is why a record must be keyed by table identity, physical schema/codec epoch, read
schema/projection, and predicate family, with footer-domain and drift checks. Requiring
an exact snapshot would prevent useful day/week reuse; reusing across an out-of-domain
epoch would be unsafe.

### Rows versus bytes is not a settled universal choice

A task-level association model trained on 2009 decode time produced 2011 MAPE of 13.19%
for bytes only, 13.55% for rows only, and 13.22% for bytes plus rows. Those models are
descriptive task decode-time associations, not independent observations or an
end-to-end setting selector. Within a fixed schema, bytes and rows are nearly collinear.

The evidence supports retaining both:

- bytes for decoded-volume prediction and as an input to a separate RMM footprint bound;
- rows for saturation, variable-width interpretation, and schema/missing-column effects.

Emitted boundary bytes do not themselves enforce memory safety. The safety actuator must
use a separately calibrated RMM task-footprint upper bound.

### Growth changes waves and sometimes the preferred setting

The cumulative study used 1, 3, 6, 12, 24, and 36 months with a common projection, plus
24/36-month mixed reads where `PULocationID` is absent in older files and present in
2011. The mixed explicit-schema queries completed with stable GPU result hashes.

Small windows collapsed multiple configured settings onto the same physical layout. At
12 months the unique best observed median was 4,096 MiB; at 24 months 4,096 and 8,192
were within 5%; at 36 months all three high-size candidates were within 5%. The
per-task target remained usable, while the whole-query curve changed as task counts,
waves, and tails changed. This study does not isolate those from cache, data/schema drift,
reader behavior, or other scale effects.

![Cumulative table-growth shmoo](analysis/table-growth.svg)

This is an explicit-schema missing-column study. It is not evidence that Parquet
automatically up-casts incompatible physical types. The 2011 `payment_type` INT64 epoch
required an explicit read-as-long then cast-to-string adapter above the scan.

## Controller blueprint and implementable signals

This is not yet an end-to-end production selector: the variable-width, RMM upper-bound,
and makespan models remain open. It is a blueprint whose fixed-width inputs, planning
boundary, and feedback signals were exercised locally. For every candidate next-query
layout:

1. Enumerate Spark `FilePartition` / `PartitionedFile` ranges.
2. Map ranges to selected Parquet row groups and sum footer `NumRows`.
3. Predict fixed-width GPU output from requested read-schema widths, validity masks,
   allocator alignment, and explicit missing-column behavior.
4. Predict strings/variable-width columns from projected chunk metadata plus
   schema-epoch history. Abstain or use a conservative bound for unseen epochs.
5. Predict `U_projected` and `R_projected` separately from predicate selectivity and
   `U_survive` / `R_survive`.
6. Predict batches/task from the general RAPIDS target and reader soft limit.
7. Predict RMM task footprint and reject candidates whose upper bound exceeds the
   per-task budget.
8. Predict makespan from task count/waves, task rows/bytes, batches/task, and the
   query-family performance model.
9. Choose the smallest safe candidate in a measured flat region. If uncertainty or
   drift is high, retain the fallback and collect a new observation.
10. Apply feedback only between queries. For a structurally matching record, compute the
    desired assigned bytes `C_assigned_target = q * U_target` when
    `q = C_split / U_projected`. This is not directly the configuration value: inverse-search
    candidate `maxPartitionBytes` values through the pinned Spark packing simulation,
    including open cost and file/range quantization.

Production integration still requires shim/API design and measurement of driver-side
footer access, metadata caching, packing-simulation cost, and DEBUG metric overhead.
The required signals exist; operational ease and cost are not yet demonstrated.

## Measurement boundaries

- `U_projected_task` / `R_projected_task`: cumulative GPU scan output per task.
- `U_survive_task` / `R_survive_task`: cumulative downstream GPU filter output per task.
- `U_projected_batch_max` / `R_projected_batch_max`: largest emitted scan batch per task.
- `gpuMaxTaskFootprint`: observed task-level GPU footprint used for safety analysis.

Boundary footprint is not simultaneous residency, allocation traffic, or physical VRAM.

## Corpus and reproducibility

The original 36 monthly files each had one row group, so a direct split sweep was
degenerate. The controlled rewrite contains 825 sub-32-MiB, one-row-group Snappy files
and 516,784,476 rows across six independently rewritten schema strata.

The rewrite preserves row values with high confidence: for every month, source and
derived data match on schema, row count, `bit_xor(xxhash64(all columns))`,
`sum(xxhash64(all columns))`, and every column's null/NaN count. This is a collision-
bounded multiset check, not a mathematical proof.

Spark `repartition` created a balanced synthetic physical layout. It does not preserve
the original file locality or production task skew. Source/derived SHA-256 identities,
footer census, logical schemas, planner census, multiset validation, raw logs, and
replayable analyses are versioned in this experiment directory. External Parquet data
remains ignored by Git.

## Completed versus still open

| Area | Status |
|---|---|
| Exact fixed-width footer-row → GPU-byte chain | Measured and validated |
| Scan and post-filter feedback boundaries | Measured and chronologically scored |
| Exact assigned `C_split` and runtime `C_read_task` | Separated and scored |
| Primary/high-size shmoo | Complete, three descriptive blocks |
| General-target × reader-limit mediation | Complete |
| 1→36-month cumulative growth | Complete |
| Mixed missing/present column read | Complete with explicit schema |
| Annual CPU/GPU correctness and all-study lifecycle checks | Complete |
| M0 projected footer-uncompressed model | Not yet scored |
| M2 combined type/null/chunk feature model | Not yet fitted |
| Confirmatory confidence intervals / more repeats | Not done; n=3 is exploratory |
| GPU utilization / SM occupancy corroboration | Not measured |
| Automatic compatible Parquet up-casts | Not isolated |
| Multi-executor, multi-GPU, and production skew | Not tested |
| RMM footprint upper-bound model | Metrics captured; predictive model still open |
| Universal bytes-vs-rows winner | Not supported |

## Artifacts

- Primary: `attempts/full-shmoo-002/RESULTS.md`
- High-size extension: `attempts/high-size-extension-001/RESULTS.md`
- Batch mediation: `attempts/batch-mediation-001/RESULTS.md`
- Growth/mixed schema: `attempts/growth-001/RESULTS.md`
- Annual CPU/GPU correctness: `attempts/cpu-reference-001/analysis/cpu-gpu-correctness.json`
- Planner census: `analysis/planner-census.json`
- Row multiset validation: `analysis/row-multiset-validation.json`
- Generated plots: `analysis/annual-shmoo.svg`, `analysis/batch-mediation.svg`, and
  `analysis/table-growth.svg`
- Reproducibility manifest: `manifest.yaml`
