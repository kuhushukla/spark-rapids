# Scan-split benchmark kit

Measures the history-learnt scan split against a swept baseline, on real analytical queries over
public datasets.

The plugin learns one number per scan: `split = batchSizeBytes / decodeRatio`, clamped to
`[64 MiB, min(4 GiB, listedBytes / minPartitionNum)]`. History is keyed on
`(table, columns, filters)` and persisted to `spark.rapids.sql.historyPath`.

## The pipeline

```
download_*.sh ──► /data/<set>/parquet
                        │
                        ▼
        ┌──────── run_scan_bench.sh ────────┐
        │  smoke  GPU-fallback check        │
        │  off    sweep maxPartitionBytes ──┼──► baseline = sweep winner
        │  ratio  history-learnt split   ───┼──► compared against baseline
        │  parts  2x2 with partition rule   │
        └───────────────┬───────────────────┘
                        │  one event log per arm
                        ▼
        gen_partition_rule_report.py ─┐
        gen_ratio_report.py ──────────┴─► gen_clean_2x2_report.py ──► report.html

        run_learning_bench.sh  (vary the query)  ─┐
        run_window_bench.sh    (vary the window) ─┴─► build_ledger.py ─► gen_*_report.py
```

Each stage reads the previous stage's event logs; nothing is carried between them by hand. The
baseline is always the sweep winner, never a value someone picked.

## Requirements

- A RAPIDS jar built from a branch that has the split heuristic. Verify with
  `unzip -l $JAR | grep ScanSplitHeuristic`.
- Spark matching the jar's `buildver` (default `spark353`; set `BENCH_SHIM` otherwise).
- One GPU, pinned by UUID.

```bash
export RAPIDS_JAR=/path/to/rapids-4-spark_2.12-<ver>-cuda12.jar
export BENCH_SPARK_HOME=/path/to/spark-3.5.3-bin-hadoop3
export BENCH_GPU=$(nvidia-smi --query-gpu=uuid --format=csv,noheader | head -1)
```

Build a jar: `mvn -Dbuildver=353 -DskipTests package -pl dist -am`

## Get data

Each script takes `--out` and downloads a slice; `--max-files=N` keeps a run small.

```bash
bash download_clickstream.sh --out=/data/clickstream --start-month=2025-01 --end-month=2025-01
bash download_pageviews.sh   --out=/data/pageviews   --start-month=2025-01 --end-month=2025-01
bash download_overture.sh    --out=/data/overture
```

Overture needs `awscli`; the others need `curl`, `wget` and a Spark dist for the parquet conversion.
`bench_overture.scala` registers all five Overture tables at load, so download all of them.

## Run

`run_scan_bench.sh` is the only runner for this half. Stages run in order and each depends on the
previous, so `--only` exists to resume, not to skip:

| stage | what it does |
|---|---|
| `smoke` | one iteration with `explain=NOT_ON_GPU` — catches CPU fallback |
| `off` | sweeps `maxPartitionBytes`; the winner is the baseline every comparison uses |
| `ratio` | history-learnt split, compared against that baseline |
| `parts` | 2x2 of the shuffle-partition rule against the split heuristic |

```bash
A="--out=/data/run1 --iters=5 --jar=$RAPIDS_JAR"
J="--job=cs02 cs03|/data/clickstream/parquet|$PWD/bench_clickstream.scala"
bash run_scan_bench.sh $A "$J" --only=off
bash run_scan_bench.sh $A "$J" --only=ratio
python3 gen_clean_2x2_report.py --run-dir=/data/run1 --queries="cs02 cs03" --force
```

Repeat `--job='QUERIES|DATA|BENCH'` to cover several datasets in one invocation. `--only=all` is the
default and runs every stage.

Queries carry their analytical question in each `bench_*.scala` header: clickstream `cs01-cs05 csH
csH3`, pageviews `pv02 pv03g pv05g pv06 pv09g pvH pvU1 pvU2`, Overture `gf1-3 hs1-3 rw6-9`.
`pv07g` crashes on iteration 2 and `ovJ1` has never been run.

## Cross-query and data windows

Two further runners live in `../../table-split-learning/handoff`. They ask whether a split learnt by
one scan reaches a different one:

```bash
DATASET=clickstream QUERIES="csH3 cs02" OUTROOT=/data/xq bash run_learning_bench.sh
python3 build_ledger.py /data/xq ../results/ledger-xq.tsv
python3 gen_learning_report.py --ledger ../results/ledger-xq.tsv --out ../results/report-xq
```

Arms are `off` / `shared` (one history file) / `iso` (one each). `run_window_bench.sh` does the same
across data windows of one table, declared in `windows.yaml`; run `check` before a full matrix.

**Status: incomplete.** With the key on `(table, columns, filters)`, two queries projecting different
columns never share a split, so the `shared` arm currently measures isolation rather than transfer.
The ledger's `learnt_from` column names the previous *writer* of the history file, which is not
evidence the scan read it — read the split value instead. Window arms are unaffected: partition
predicates never enter the key, so windows of one table do share a slot.

## Reading a report

- Baseline is always the sweep winner. `parts` refuses to run without a completed sweep.
- Iteration 1 is dropped everywhere. It plans at the fallback split because history is empty.
- Bands (`<=+3%`, `+3..10%`, `>=+10%`) group the gap against the baseline. They are reporting
  conventions and say nothing about whether a gap exceeds run-to-run noise — compare against the
  spread of the warm iterations first.
- Time columns are elapsed time on a shared GPU. Read the byte columns for work.
- A sweep cannot discriminate on a table small enough that `listedBytes / cores` falls below
  `maxPartitionBytes`; every point collapses to the same split.

## Layout

| file | role |
|---|---|
| `bench_common.sh` | the one Spark invocation; every runner sources it |
| `run_scan_bench.sh` | sweep / ratio / parts |
| `eventlog_metrics.py` | metric names, shared by every parser |
| `gen_ratio_report.py` | event-log parsing, ratio-vs-baseline report |
| `gen_partition_rule_report.py` | scan-stage task metrics |
| `gen_clean_2x2_report.py` | the combined report |
| `partition_rule_full.py` | the partition rule; `--json` for machines |
| `download_*.sh`, `bench_*.scala` | data and queries |

Configure through the environment rather than editing scripts: `BENCH_JAVA_HOME`, `BENCH_SPARK_HOME`,
`BENCH_GPU`, `BENCH_CORES`, `BENCH_SHIM`, `BENCH_DRIVER_MEM`, `BENCH_PINNED`, `BENCH_CONC_GPU`,
`BENCH_FREE_PATH`, `BENCH_LOCALDIR`, `BENCH_DATA`, `BENCH_OUT`.

## Failure modes worth knowing

- Every arm runs with `spark.rapids.sql.metrics.level=DEBUG`; the split metric is DEBUG-only. A run
  without it aborts at the first arm rather than reporting a blank split column.
- An arm is reused only when its `maxPartitionBytes` and `shuffle.partitions` match what the stage
  wants. Mismatches are reported, never silently reused.
- Queries writing more than ~50 GiB per arm are not repeatable on a single NVMe: sustained write
  speed falls once past the drive's SLC cache, penalising later arms.
- No query here exercises parquet schema evolution; all files in each dataset share one schema.
