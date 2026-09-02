# Split-learning transfer: across queries and across data windows

Does a split learnt by one scan reach a different one? Two runners answer that:
`run_learning_bench.sh` varies the query, `run_window_bench.sh` varies the data window of one table.
Both feed `build_ledger.py`, which emits a TSV the report generators render.

The scan-split kit itself lives in `../rolling-split-autotuning/handoff`; these runners source its
`bench_common.sh` and use its bench queries.

## 1. What the key does

The plugin keys history on **`(table, columns, filters)`**, hashed. `filters` is `pushedDownFilters`,
i.e. predicates on non-partition columns only (`GpuFileSourceScanExec.pushedDownFilters`). Three
consequences drive the whole design:

| pair | same key? | so |
|---|---|---|
| two queries projecting different columns | no | no transfer, by construction |
| one query, two partition-pruned windows | **yes** | windows of a table share one slot |
| two queries differing only in a data filter | no | filters are in the key |

This is a change. Earlier revisions keyed on the table label alone, which made cross-query transfer
the default; results recorded before 2026-09 measured that behaviour, not this one.

**Measured 2026-09-02** on the current key, clickstream csH3 then cs02 sharing one history file:

```
core1-iso-cs02     it1  4M  spark-maxSplitBytes    (no history)
core1-shared-cs02  it1  4M  prev-writer:csH3@-     (csH3 had written 64M)
```

Identical. cs02 planned its cold iteration at the fallback despite csH3's record being present — the
key isolated them. Ledger: `results/ledger-smoke2.tsv`.

## 2. Status of each half

**Cross-query: currently a negative control.** With different columns there is nothing to transfer,
so the `shared` arm measures isolation. It is worth keeping — it guards the keying — but it does not
measure what it was built to measure. Restoring a positive transfer test needs either a key that
generalises across column sets, or query pairs that project identically and differ only in shape.

**Data windows: intact.** Partition predicates never reach the key, so `ym < '2022-01'` and
`ym >= '2022-01'` on the same table share one slot with different `listedBytes`. Last writer wins, so
a window that learnt on 57 GB can size the split for a run over 88 GB. That is the risk this half
exists to measure and it is unaffected by the key change.

## 3. Split formula

```
ratio   = decodedBytes / listedBytes          from this scan's previous run
raw     = batchSizeBytes / ratio              batchSizeBytes = spark.rapids.sql.batchSizeBytes
ceiling = min(4 GiB, listedBytes / minPartitionNum)
floor   = 64 MiB
split   = max(floor, min(ceiling, raw))
```

No cold record means Spark's `FilePartition.maxSplitBytes`.

The floor is applied **last**, so when `listedBytes / minPartitionNum` falls below 64 MiB the floor
overrides the ceiling and the one-task-per-core intent is dropped. Measured: a 33.5 MiB table on 16
cores wanted a 2.10 MiB ceiling and got 64 MiB — one task instead of sixteen. Only reachable below
roughly 1 GiB per 16 cores.

`minPartitionNum` comes from `spark.sql.files.minPartitionNum`, else `defaultParallelism`. It is the
only remaining lever on the ceiling; `ceiling_conf` in `bench_common.sh` maps the old `core<N>` onto
it, and `core1` is what the formula does with the conf unset.

## 4. Arms

| arm | history file | measures |
|---|---|---|
| `off` | none | the baseline; no learning at all |
| `shared` | one for both queries | what production would do |
| `iso` | one per query | a query learning only from itself |

One application per query in every arm. This matters: the first query in an application pays a large
scan-task semaphore-wait penalty the second does not (~43 s on the NDS 20260820b run), so an arm that
runs a query second is not comparable to one that runs it first. Separate applications equalise that
and also make `shared` a genuine cross-JVM test — app 2 reads a record app 1 wrote.

File cache is off everywhere. It transfers across queries exactly as the split history does and would
confound the measurement.

## 5. Reading the ledger

`learnt_from` names whichever query last **wrote** the history file. It is reconstructed from run
order, not observed, and under the current key the previous writer is usually *not* the record the
scan read. Use the `split_mb` column as the evidence: a cold split equal to the fallback means no
history was applied, whoever wrote last.

Cold (iteration 1) and warm (2..N) are separate fields and never combined. A cold value is one
measurement with no noise estimate behind it.

## 6. Running

```bash
export RAPIDS_JAR=/path/to/rapids-4-spark_2.12-<ver>-cuda12.jar
export BENCH_SPARK_HOME=/path/to/spark-3.5.3-bin-hadoop3

DATASET=clickstream QUERIES="csH3 cs02" OUTROOT=/data/xq bash run_learning_bench.sh
python3 build_ledger.py /data/xq ../results/ledger-xq.tsv
python3 gen_learning_report.py --ledger ../results/ledger-xq.tsv --out ../results/report-xq
```

Windows are declared in `windows.yaml`, never passed on the command line, so the sweep and the
learning runs always use identical data:

```bash
OUTROOT=/data/win bash run_window_bench.sh check     # precheck first
OUTROOT=/data/win bash run_window_bench.sh all       # sweep -> learn -> refine -> report
```

`DATASET=nds` routes through the `ab` repo against the sparkh cluster instead of local spark-shell;
`make_templates.py` generates its templates. Set `BENCH_SHIM=spark357` for that cluster.

## 7. Files

| file | role |
|---|---|
| `run_learning_bench.sh` | vary the query |
| `run_window_bench.sh` | vary the window; `windows.yaml` declares them |
| `build_ledger.py` | event logs -> ledger TSV |
| `gen_learning_report.py`, `gen_window_report.py` | reports |
| `wincfg.py` | turns a declared window into a predicate |
| `make_partitioned_clickstream.sh` | builds the (wiki, ym)-partitioned copy |
| `precheck.sh` | GPU/jar/Spark checks before a matrix |
| `make_templates.py` | `ab` templates for the NDS backend |

## 8. Earlier results

`WINDOW-LEARNING-RESULTS-20260824.md` and `CLICKSTREAM-QUERY-PAIR-CHARACTERISATION-20260821.md`
were measured against the **table-only key**. Their numbers stand; their mechanism sections describe
behaviour the current key no longer has. The query-pair characterisation is about the data
(read selectivity, compression, ratio) and is unaffected.
