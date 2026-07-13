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

# POC integration: in-plugin scan split autotuner

## Status

First-pass implementation is committed. Not yet compiled or tested.
The Python rolling experiment validated the algorithm; this integrates it into the plugin.

## What was implemented

### New file
`sql-plugin/src/main/scala/com/nvidia/spark/rapids/perf/ScanSplitAutotuner.scala`

- `ScanSplitRecord` — one observation: tableLabel, listedBytes, decodedBytes, decodedRows, timestampMs
- `ScanSplitStore` — append-only local file, tab-separated, tableLabel base64-encoded.
  Truncates incomplete trailing lines on load. File-level lock on every write.
- `ScanSplitAutotuner` object — singleton holding the optional store. Exposes `init`, `close`,
  `decide`, `record`, `tableLabel`.
- `ScanObservationListener` — `QueryExecutionListener` that walks the executed plan after
  `onSuccess`, reads `filesSize` and `outputBatchBytes` metrics from each `GpuFileSourceScanExec`,
  and calls `ScanSplitAutotuner.record`. `onFailure` is a no-op.

### Modified files

| File | Change |
|---|---|
| `RapidsConf.scala` | Added `SCAN_SPLIT_AUTOTUNER_HISTORY_PATH` (internal, optional String) and `scanSplitAutotunerHistoryPath` lazy val |
| `Plugin.scala` | `init()`: opens store and registers listener if path is set. `shutdown()`: calls `ScanSplitAutotuner.close()` |
| `GpuFileSourceScanExec.scala` | `createNonBucketedReadRDD`: calls `ScanSplitAutotuner.decide()` instead of using `maxSplitBytes` directly |

## How to enable

```
spark.rapids.sql.scan.splitAutotuner.historyPath=/tmp/scan-split-history.tsv
```

Empty (default) = disabled, ordinary Spark behavior unchanged.

## Decision logic

```
expansionRatio = latestObservation.decodedBytes / latestObservation.listedBytes
splitBytes     = clamp(batchSizeBytes / expansionRatio, min=64MiB, max=batchSizeBytes)
```

Ceiling is `spark.rapids.sql.batchSizeBytes` (default 1 GiB). This scales automatically
if the user changes batchSizeBytes.

## Log events (all INFO)

```
[ScanSplitAutotuner] COLD_START  table=<label> listed_bytes=<n> split_bytes=<n> history_count=0
[ScanSplitAutotuner] DECIDED     table=<label> listed_bytes=<n> expansion_ratio=<r> predicted_decoded_bytes=<n> split_bytes=<n> spark_default=<n> history_count=1
[ScanSplitAutotuner] SKIPPED     table=<label> reason=<arithmetic_overflow|invalid_history> split_bytes=<n>
[ScanSplitAutotuner] RECORDED    table=<label> listed_bytes=<n> decoded_bytes=<n> decoded_rows=<n>
```

`table=` is the catalog name (`db.table`) when available, else the first root path.

## Table identity

- Catalog table present: `tableIdentifier.unquotedString` → `db.table_name`
- Path-only reads (NDS without catalog): first root path string

## Metrics used

| Metric key | Description | Level |
|---|---|---|
| `filesSize` | Listed encoded bytes (post dynamic pruning) | ESSENTIAL |
| `outputBatchBytes` | Total decoded GPU batch bytes | DEBUG |
| `numOutputRows` | Total decoded rows | ESSENTIAL |

`outputBatchBytes` is DEBUG level and will be zero if `spark.rapids.sql.metrics.level`
is set to ESSENTIAL or MODERATE. In that case `record()` skips the observation silently.

## What is NOT done yet

- No unit or integration tests
- Bucketed scans are not tuned (go through `createBucketedReadRDD`, unmodified)
- AQE with dynamic partition pruning: metrics may not be finalized at listener call time in all
  Spark versions — needs verification
- The Scala policy (latest observation) differs from the Python experiment (weighted median of 12).
  These have not been compared mechanically
- No predicate shape or schema fingerprinting — table identity is name/path only, so a schema
  change or projection change is not detected
- `ScanSplitAutotuner` is a global singleton; concurrent Spark applications in the same JVM
  would share it (not expected in production but worth noting)

## First test to run

Run the same single-table scan+aggregate query twice over NDS Parquet paths:

```sql
SELECT count(*), sum(ss_sales_price), sum(ss_quantity) FROM store_sales
```

Check:
1. First run logs `COLD_START` and writes the history file
2. Second run logs `DECIDED` with a non-zero expansion ratio
3. Partition count differs between the two runs
4. Query results are identical
5. No `spark.sql.files.maxPartitionBytes` mutation in session conf

## Files for context

- `README.md` (this directory) — authoritative handoff state
- `../RESULTS.md` — experiment results and algorithm evidence
- `ScanSplitAutotuner.scala` — the implementation
- `PerformanceHistory.scala` — the fuller history API (not yet used by the autotuner)
