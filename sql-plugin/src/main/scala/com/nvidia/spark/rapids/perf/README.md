# History-backed planning heuristics

Planning decisions that learn from what earlier runs actually did.

A heuristic reads one or more measured quantities at planning time, decides, and records what the
query really did once it ends. If history is missing, stale, unavailable or unusable the static
decision stands whole - a learned value is never blended with it.

Today there is one heuristic: Iceberg and file-source scan splits sized from decode expansion.

## Layers

Contract (`HistoryMetric`, `ScanExpansionRatio`) -> pure sizer (`ScanSplitSizer`) -> history
access through `MetricStore` only (`MetricHistory`, `HistoryPolicy`) -> decide/observe cycle
(`HistoryHeuristic`, `ScanSplitHeuristic`) -> planning hooks.

The code compiles against the history metrics API alone. The driver plugin's
`HistoryMetricsManager` selects, opens, installs and shuts down the provider; this package only
ever calls `MetricStores.current()`. An enforcer rule and `HistoryBoundarySuite`, which checks
the compiled classes, keep it that way.

## The scan heuristic

| Step | Where | What |
|---|---|---|
| decide (Iceberg) | `RapidsSparkTable.newScanBuilder` | sets `read.split.target-size` before Iceberg plans tasks |
| observe (Iceberg) | `GpuSparkScan.toBatch` | registers each planning; only the latest is read |
| decide (file source) | `GpuFileSourceScanExec.createNonBucketedReadRDD` | catalog tables only |
| observe (file source) | `GpuFileSourceScanExec.getFinalRDD` | catalog tables, every reader |

- **Formula**: `split = targetBatchBytes / ratio`, clamped to
  `[64 MiB, min(4 GiB, listedBytes / minPartitionNum)]`.
- **Ratio**: GPU output batch bytes over on-disk bytes, summed over the splits that ran, from the
  table's most recent observation (`limit(1)`, `Summary.mean` of that one reading) within the
  family's planning age, 7 days. That age is part of the contract's retention, which a store
  fixes at the first declaration, so it is not configurable.
- **Key**: the table name only, exactly as the hook sees it - Iceberg's `Table.name()` on both
  the decide and observe sides, and the catalog identifier for file-source tables. Values are
  compared as exact bytes. This is the same string the `scan.split: table=<t>` lines print.
  Names longer than 253 UTF-8 bytes cannot be a dimension value and are not tracked.
- **minPartitionNum**: `spark.sql.files.minPartitionNum`, then
  `spark.sql.leafNodeDefaultParallelism`, then `spark.default.parallelism`, then the configured
  `spark.executor.instances` x `spark.executor.cores`; registered cores only when none is set.
- **Known looseness**: on the Iceberg path `listedBytes` for the cap is the whole table from the
  snapshot summary, before pruning, because the decision runs before planning. The ratio itself
  uses the pruned bytes the scan reports.
- **Precedence on Iceberg**: explicit DataFrame `split-size` option > learned split >
  `spark.rapids.iceberg.*-setting.*.read-split-target-size` > table properties > Iceberg default.
- **Per split**: each split read to the end adds (index, decoded bytes, on-disk bytes); the first
  read of a split wins. Cleared after the query's observation; a re-run of an executed plan is
  not re-registered, leaving one unread entry per split.
- **Observation guard**: recorded only if exactly splits 0..planned-1 were read and no job or
  stage attempt failed; otherwise one INFO `scan.split: table=<t> not recorded: <reason>` line.

The Iceberg table wrapper lives at the distribution root and cannot reference shim-loaded
classes, so the plugin hands it the decision function through `IcebergSplitAdvisor`.

### Driver log lines

Kept stable for tooling:

```
scan.split: table=<t> ratio=<r> targetBatch=<b> rawSplit=<s>
scan.split: table=<t> listed=<l> -> split=<n> bytes
scan.split: table=<t> listed=<l> -> no history, iceberg default
```

When history cannot be used at all - the declaration was rejected, a lookup failed or timed
out - one line per distinct status code is logged once per JVM, and every affected decision falls
back to the static split. Provider text is never logged.

## Lifecycle

```
Plugin.init            provider requested?  enable heuristics, add the execution-end listener,
                                            install the Iceberg advisor
Plugin.registerMetrics after the manager ran: a provider store installed?  declare every family;
                                            otherwise stop again
Plugin.shutdown        stop heuristics and remove the advisor, then the manager shuts down
```

Recording requires a declaration accepted in this process, so each family declares once per
store and caches the outcome. Any non-`ACCEPTED` status disables only that family.

## Configuration

| Key | Default | Meaning |
|---|---|---|
| `spark.rapids.sql.history.metrics.provider` | `none` | enables history-backed planning when not `none` |
| `spark.rapids.sql.history.planningTimeoutMillis` | 100 | budget for one planning lookup |

Declaration uses a fixed 5 second budget, once per store.

## Adding a heuristic

The plug point is `HistoryHeuristic`:

1. Get a governed id and name in the API's `HistoryMetricCatalog`.
2. Define the contract: `object X extends HistoryMetric`.
3. Implement `class XHeuristic extends HistoryHeuristic` (key, static decision, formula, bounds,
   observation) with a singleton over `MetricStores.current()`.
4. Add the singleton to `HistoryHeuristics`, and call `decide` and `register` from the planning
   site.

## Future work

- **Finer keys**: table plus decoded columns and pushed filters move the ratio too. That needs a
  new version of the family and a hook after pushdown; it is a separate change.
- **Functions over history**: an exponentially weighted mean, a median or a trend needs an
  addition to the history metrics API; today a heuristic can read the last N readings and their
  plain mean, min and max.
- **Reducer setting**: the same machinery can size shuffle partitions from observed output.
- **Unknown table size**: when the snapshot reports no file size (listed bytes <= 0) the cap
  collapses and the split takes the 64 MiB floor, as the reference implementation did; skipping
  the cap in that case is a follow-up.
