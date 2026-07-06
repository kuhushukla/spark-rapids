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

# Rolling split autotuning handoff

## Read this first

The current branch does **not** contain an end-to-end in-plugin scan-split
autotuner.

It contains two separate prototypes:

1. `PerformanceHistory.scala` is Scala code in the plugin with an abstract
   history/prediction API, a local append-only file implementation, component
   predictors, and unit tests. It has no production caller.
2. This rolling experiment is a PySpark benchmark controller. It changes
   `spark.sql.files.maxPartitionBytes` before constructing each DataFrame.
   It validates a sequential historical policy through a session-wide
   actuator, but it does not exercise `PerformanceHistory` or make a
   per-table-read decision inside the plugin.

Do not cite the rolling experiment as validation of the Scala implementation.
Do not describe the Scala implementation as integrated with scan planning.

## What the rolling experiment established

The controller used real metrics from completed GPU scans. For each logical
table it began with an empty in-memory history and processed the frozen
12-month windows chronologically:

1. Before window zero, select the 128 MiB cold-start fallback.
2. Compute the enabled decision before running any measured treatment in the
   window.
3. Run enabled, fixed-128, and fixed-1024 in the frozen randomized order.
4. Require identical result hashes.
5. Read decoded bytes and rows from the completed enabled `GpuScan`.
6. Append only that enabled observation to history.
7. Use only completed earlier enabled observations for the next window.

Warm-ups and fixed-control outcomes never entered history. The frozen policy
used a recency-weighted median of at most 12 earlier enabled observations. The
later exploratory analysis favored the latest compatible observation, and the
Scala data-shape predictor was changed accordingly, but that revised Scala
policy has not received the rolling runtime validation.

The current listed encoded bytes were obtained from normal file listing. They
were not a historical measurement and were not used to choose window
boundaries. Taxi windows contained 12 monthly files. The Freddie workload
selected the intersecting year directories and applied the exact `period`
predicate during the scan. That coarse physical layout explains its
one-directory/two-directory prediction regimes.

## Required next milestone: an in-plugin POC

Build the smallest end-to-end path that proves a plugin can tune one table read
without changing the session-wide Spark configuration.

### Decision path

At the point where Spark has completed normal file listing but before scan
partitions are finalized:

1. Construct a request using only information already available to the read:
   logical table identity when available, projected read schema, projection,
   pushed predicates and their shapes, file format, selected paths and lengths,
   current RAPIDS/Spark settings, and cheap runtime capacity hints.
2. Ask the abstract history API for compatible same-table data-shape evidence.
3. Predict decoded bytes and rows from listed encoded bytes.
4. Derive a candidate encoded split size from the decoded-byte batch target.
5. Apply safety bounds from the available footprint/admission evidence.
6. Fall back to ordinary Spark behavior when evidence is missing, incompatible,
   stale, divergent, or too uncertain.
7. Apply the decision to this read's partition construction. Do not mutate
   `spark.sql.files.maxPartitionBytes` for the session.

Multiple table reads in one query must be allowed to receive different
decisions.

### Observation path

After a scan completes successfully:

1. Record the exact decision inputs and selected split.
2. Record low-overhead scan outcomes, including listed bytes, decoded rows and
   bytes, output batches, maximum batch size, useful and launched tasks, and
   the available maximum task-footprint/admission measurement.
3. Persist the observation through the abstract API.
4. Do not promote failed, cancelled, retry-corrupted, or incomplete reads into
   trusted history.
5. Make the append durable before relying on it after an application restart.

The local append-only implementation is sufficient for this POC. A distributed
history service, external auto-tuner integration, and a durable public storage
contract are later work.

### Information boundary

The decision must not require opening Parquet footers for tuning, catalog
column statistics, pre-read row sampling, results from the current read, or
future/control outcomes. Reader execution may use normal Parquet metadata as it
already does; the tuner may not add a separate metadata query.

The history match must be component-specific. A changing date literal or table
snapshot must not force cold start when the projected schema and predicate
shape remain compatible. A schema or projection change may reuse narrower
compatible evidence with reduced confidence rather than pretending it is an
exact match.

## Acceptance tests for the in-plugin POC

The milestone is complete only when all of the following are demonstrated:

- The first read with empty history uses the declared fallback.
- A later read uses a real observation written by the earlier successful read.
- Restarting the application reloads the local history and produces the same
  prediction.
- Two table scans in one query can receive different split decisions.
- The decision is visible in the actual scan partition count or partition
  descriptors, not merely in a reported recommendation.
- No session-wide max-partition-bytes mutation is used as the actuator.
- No footer-only or post-decision information enters the prediction.
- Failed and cancelled reads do not contaminate trusted history.
- CPU and GPU query results remain identical.
- With the same observation sequence, the Scala policy can be compared
  mechanically with the committed Python reference decisions.
- Unit tests cover compatibility fallback, stale/divergent history, persistence
  truncation, concurrency, and numeric overflow/clamping.

A useful integration test should execute two consecutive reads against a small
generated table, inspect the first observation persisted by the plugin, and
prove that the second read's physical partitioning reflects that observation.
A second test should include two logical tables in one query and prove the
decisions remain independent.

## Evidence and spreadsheet exports

Authoritative experiment evidence remains in:

- `../raw/<dataset>/results.jsonl`: every warm-up and measured treatment.
- `../raw/<dataset>/history.json`: enabled observations in chronological
  order.
- `../analysis/result.json`: frozen analysis and per-window trajectories.
- `../analysis/exploratory.json`: explicitly post-hoc estimator and drift
  analysis.
- `../schedule.json`: frozen windows, selected paths and listed file lengths.

Spreadsheet-friendly derived exports are in `../raw/spreadsheet/`:

- `run-results.csv`: one row per treatment run, including warm-ups.
- `window-summary.csv`: one row per rolling window with predictions, errors,
  timings, regret, and task counts.

Regenerate both files with:

```bash
python3 docs/experiments/rolling-split-autotuning/scripts/export_spreadsheet.py
```

The CSV files are convenience views. The committed JSON/JSONL evidence remains
authoritative.

## Portability gaps for a new developer

A clean checkout contains the model code, tests, protocol, scripts, compact
results, and analysis, but not the approximately 154 GiB taxi and Freddie
datasets or the complete external event-log bundle. The frozen schedule also
contains absolute `/data` paths, and the runner defaults to a developer-local
Spark installation and built RAPIDS JAR.

NYC taxi Parquet files can be downloaded publicly. Freddie Mac CRT disclosure
downloads require individual Clarity registration. Do not commit or share
credentials. Confirm the applicable download and redistribution terms before
placing raw or converted Freddie data in a shared artifact store.

The Freddie text-to-Parquet converter is currently outside this branch. Its
source, dependency versions, validation output, source archive checksums, and
licensing must be resolved before claiming clean-checkout data reproducibility.
