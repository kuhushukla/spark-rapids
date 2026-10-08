# History-backed planning heuristics

Planning decisions that learn from earlier runs. A heuristic reads history when a query plans
and records what the query did when it ends. If history is off, missing, stale or unusable, the
static decision is used unchanged; the two are never blended. The one heuristic today sizes scan
splits for Iceberg and catalog file-source tables.

## Enabling

| Key | Default | Meaning |
|---|---|---|
| `spark.rapids.sql.history.metrics.provider` | `none` | any other value enables history-backed planning |
| `spark.rapids.sql.history.planningTimeoutMillis` | 100 | budget for one history lookup while planning |

Planning only uses history once a provider store is actually installed. This package talks to
the history metrics API (`MetricStores.current()`) only; the driver plugin's
`HistoryMetricsManager` owns the provider, and an enforcer rule bans provider dependencies.

## How scan splits are sized

- **Measure**: when a query ends, each scan records its decode-expansion ratio, GPU output batch
  bytes / on-disk bytes, keyed by table name. It is recorded only if every planned split was read
  and no job or stage attempt failed.
- **Decide**: the next scan of that table reads the latest ratio (at most 7 days old) and sets
  `split = targetBatchBytes / ratio`, clamped to `[64 MiB, min(4 GiB, listedBytes / minPartitionNum)]`.
- **minPartitionNum**: `spark.sql.files.minPartitionNum`, `spark.sql.leafNodeDefaultParallelism`,
  `spark.default.parallelism`, executor instances x cores, then registered cores.
- **Iceberg precedence**: DataFrame `split-size` option > learned split >
  `spark.rapids.iceberg.*-setting.*.read-split-target-size` > table properties > Iceberg default.
  On Iceberg `listedBytes` is the unpruned table size, so the cap is loose for small slices.

| Step | Where |
|---|---|
| decide (Iceberg) | `RapidsSparkTable.newScanBuilder`, via `IcebergSplitAdvisor` |
| observe (Iceberg) | `GpuSparkScan.toBatch` |
| decide (file source) | `GpuFileSourceScanExec.createNonBucketedReadRDD` |
| observe (file source) | `GpuFileSourceScanExec.getFinalRDD` |

Driver log lines, kept stable for tooling:

```
scan.split: table=<t> ratio=<r> targetBatch=<b> rawSplit=<s>
scan.split: table=<t> listed=<l> -> split=<n> bytes
scan.split: table=<t> listed=<l> -> no history, iceberg default
scan.split: table=<t> not recorded: <reason>
```

Store errors are logged once per JVM per status code and the decision falls back to static.

## Adding a heuristic

Code layers: `HistoryMetric` (family contract) -> `MetricHistory` (store access, `HistoryPolicy`)
-> `HistoryHeuristic` (decide/observe cycle, lifecycle) -> `ScanSplitHeuristic`.

1. Register a family id and name in the API's `HistoryMetricCatalog`.
2. Define it: `object X extends HistoryMetric`.
3. Extend `HistoryHeuristic` (key, static decision, formula, bounds, observation) as a singleton
   over `MetricStores.current()`, and add it to `HistoryHeuristics`.
4. Call `decide` and `register` from the planning site.
