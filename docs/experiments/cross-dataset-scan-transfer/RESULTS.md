# Results: yellow-to-high-volume-for-hire transfer

## Frozen verdict

**NOT SUPPORTED** under the preregistered all-gates rule.

The workload trained only on yellow-taxi months 2024-01 through 2024-06 and
then evaluated high-volume-for-hire months 2024-01 through 2024-06. Both scans
projected two nullable integers and one nullable double. No holdout measurement
was used to fit the frozen model.

| Gate | Result |
|---|---:|
| decoded bytes median APE <= 20% | fail: 31.23% |
| decoded bytes p90 APE <= 35% | pass: 31.26% |
| decoded rows median APE <= 20% | fail: 31.23% |
| decoded rows p90 APE <= 35% | pass: 31.26% |
| footprint median APE <= 30% | pass: 0.000051% |
| footprint empirical-upper coverage >= 90% | fail: 0% |
| no retry or spill | pass |
| CPU/GPU correctness | pass: 12/12 |

The machine-readable frozen result is [analysis/result.json](analysis/result.json).

## What transferred and what did not

The direct POC data-shape feature was decoded output per Spark input byte.
That feature did not transfer. Yellow training scans contained a median
0.55377 decoded rows per input byte; high-volume-for-hire held out scans
contained 0.42198. Consequently both rows and bytes were overpredicted by
about 31.2%.

A post-hoc decomposition, explicitly excluded from the frozen verdict, found
that decoded width transferred almost perfectly:

| Component | Yellow training median | HVFHV holdout median |
|---|---:|---:|
| decoded rows/input byte | 0.5537745 | 0.4219764 |
| decoded GPU bytes/row | 16.3750411 | 16.3750083 |

Using the yellow median decoded width on holdout rows had median relative error
0.0002007%. Therefore the failed byte prediction is almost entirely a failed
row-density/compression prediction, not a failed projected-width prediction.

This supports the decomposed model:

```text
decodedRows = encoded/projected input * data-density/selectivity component
decodedBytes = decodedRows * projected decoded-width component
```

The width component can transfer by projected physical schema. The row-density
component needs table/data statistics, Parquet footer information, or a
table-specific learned prior. A same-schema generic prior is not sufficient for
the preregistered 20% target on this holdout.

## Footprint interpretation

The median batch-normalized footprint prediction was extremely accurate, but
the training p90 sat slightly below every holdout observation. The maximum
additional relative margin required for all six tasks was only
0.0000409%; a relative margin of 0.0001% produced 100% post-hoc coverage.

The strict preregistered coverage gate remains failed. The result shows why an
empirical p90 must be combined with measurement resolution, model residual,
and an operational safety margin before it controls admission or split sizing.

Only one output-producing scan task occurred per monthly run. This validates
cross-dataset consistency at the observed batch scale; it does not validate
footprint scaling across many task or batch sizes.

## Correctness and safety

- All 12 CPU and GPU canonical aggregation hashes matched.
- GPU retry, split-and-retry, host spill, disk spill, Spark memory spill, and
  Spark disk spill were all zero.
- Spark local, shuffle, event-log, and temporary storage were placed under
  `/data/tmp/cross-dataset-scan-transfer-run-001`.

## Required model change

Do not add full table identity as a mandatory match. Instead:

1. Keep decoded width transferable by projected column/type shape.
2. Split row-density/selectivity into its own component.
3. Prefer current Parquet/catalog statistics when row counts or projected
   compressed sizes are cheaply available.
4. Otherwise use a table/column prior and widen uncertainty for a new table.
5. Retain the batch-normalized footprint component, but add a calibrated safety
   margin and validate it across task sizes and additional datasets.

The exploratory decomposition is
[analysis/exploratory.json](analysis/exploratory.json) and is permanently
labeled post hoc. It does not change the frozen verdict.

## Harness amendment

The initial analyzer used Python `str.removeprefix`, which is unavailable in
the container Python. All CPU/GPU queries had completed, but analysis stopped
before constructing predictions. Amendment 001 replaced only that operation
with `startswith` plus slicing and replayed the immutable raw artifacts.
No query, allocation, model equation, threshold, or observation changed.
