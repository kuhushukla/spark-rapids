# Scan Split Autotuner — Mechanics & GPU Routing (durable notes)

Written 2026-07-13. Captures two things that were expensive to (re)discover:
1. **How the split override actually works** (clamp, override point, per-table keying).
2. **The GPU-routing gotcha** on this box (CUDA device ordering).

All statements below are grounded in the source as of branch `trials_zero_conf`:
- `sql-plugin/src/main/scala/com/nvidia/spark/rapids/perf/ScanSplitAutotuner.scala`
- `sql-plugin/src/main/scala/org/apache/spark/sql/rapids/GpuFileSourceScanExec.scala`

---

## 1. What the autotuner overrides — and what it does NOT touch

**It does NOT change `spark.sql.files.maxPartitionBytes`.** That conf is left alone.
Changing the conf would be global (every scan, and it feeds Spark's own planning);
we deliberately avoid that.

Instead, the override happens **per-scan, at RDD-creation time**, inside
`GpuFileSourceScanExec.createNonBucketedReadRDD` (line ~579–594):

```scala
val sparkDefault =
  FilePartition.maxSplitBytes(fsRelation.sparkSession, dynamicallySelectedPartitions)
val label = ScanSplitAutotuner.tableLabel(
  tableIdentifier.map(_.unquotedString),
  relation.location.rootPaths.headOption.map(_.toString))
val listedBytes = dynamicallySelectedPartitions.flatMap(_.files.map(_.getLen)).sum
val maxSplitBytes = ScanSplitAutotuner.decide(          // <-- our override
  label, listedBytes, sparkDefault, rapidsConf.gpuTargetBatchSizeBytes)
ScanSplitAutotuner.registerPendingScan(label, listedBytes, this)
val splitFiles = FilePartitionShims.splitFiles(dynamicallySelectedPartitions, fsRelation,
  maxSplitBytes)
FilePartitionShims.getFilePartitions(fsRelation, splitFiles, maxSplitBytes)
```

So the knob we turn is the **`maxSplitBytes` local** — the bin-packing target that
drives `splitFiles` / `getFilePartitions`. It is *downstream* of `maxPartitionBytes`:
Spark computes `sparkDefault` from `maxPartitionBytes`, open-cost, parallelism and total
bytes; we then **replace that value** with our decision for this one scan only. Nothing
else in the session sees a different config.

`sparkDefault` is the fallback: `decide()` returns it unchanged whenever there is no
history, the history is invalid, or the arithmetic doesn't produce a usable number.

---

## 2. Per-table logic — yes, it is keyed per table

The decision is keyed on a **table label**, not global:

```scala
def tableLabel(catalogName: Option[String], firstRootPath: Option[String]): String =
  catalogName.orElse(firstRootPath).getOrElse("")
```

- Preferred key: the catalog identifier (`db.table`, via `tableIdentifier.unquotedString`).
- Fallback key: the first root path string (used in these NDS file-based tests, since the
  tables aren't registered in a catalog — that's why labels look like
  `file:/.../parquet_sf100.../store_sales`).

`decide()` looks up `store.latestFor(label)` — the **latest observation for that exact
table** — and computes an expansion ratio from *that table's* prior scan. Each table gets
its own ratio and its own split. Proof from the 2026-07-13 A5000 run, same session:

| Table       | Expansion ratio | Autotuned split |
|-------------|-----------------|-----------------|
| store_sales | 1.146           | 894 MiB         |
| web_sales   | 0.760           | 1024 MiB        |

Two tables, one run, two different splits. History is persisted append-only in
`data/scan-split-history.tsv`, one line per observation, `base64(label)` as the key.

---

## 3. Why the clamp — and why "1 GB" specifically

The decision formula (`ScanSplitAutotuner.decide`, lines ~167–195):

```scala
val maxSplit = math.max(batchSizeBytes, MIN_SPLIT_BYTES)   // ceiling
val expansionRatio = rec.decodedBytes.toDouble / rec.listedBytes
val raw = (batchSizeBytes / expansionRatio).toLong
val splitBytes = math.max(MIN_SPLIT_BYTES, math.min(maxSplit, raw))   // clamp
```

- **`batchSizeBytes` = `rapidsConf.gpuTargetBatchSizeBytes`** = `spark.rapids.sql.batchSizeBytes`,
  whose **default is 1 GiB**. So the "1 GB" ceiling is **not hardcoded** — it *is* the GPU
  target batch size. Change `spark.rapids.sql.batchSizeBytes` and the ceiling moves with it.

- **Why ceil at the batch size:** the split sizes the *encoded* bytes read per task; those
  decode (× expansionRatio) into a GPU batch. Making a split decode to *more* than one
  target batch buys nothing — the reader still emits multiple batches — while forcing a
  larger host/GPU buffer allocation per task. So the useful maximum is "one split ≈ one
  target batch's worth of decoded data." That's exactly `raw = batchSizeBytes / ratio`.
  - web_sales: ratio 0.76 → `raw = 1 GiB / 0.76 = 1.34 GiB`, clamped **down** to the 1 GiB
    ceiling → 1024 MiB.
  - store_sales: ratio 1.146 → `raw = 1 GiB / 1.146 = 894 MiB`, under the ceiling → 894 MiB.

- **Why floor at 64 MiB** (`MIN_SPLIT_BYTES = 64L * 1024 * 1024`): never shrink so far that
  we explode task count for a high-expansion table. (No table hit the floor in these runs.)

**Intuition:** the split is chosen so one task's files decode to roughly one GPU batch —
big enough to avoid tiny-task overhead, capped so we don't over-allocate per task.

---

## 4. GPU routing gotcha — the one that cost a whole session

**Symptom:** Spark GPU jobs kept landing on the T400 (4 GB display GPU) and OOM-crashing,
even with `CUDA_VISIBLE_DEVICES=1` exported *and* set in `spark-env.sh`.

**Root cause: CUDA device ordering.** CUDA's default `CUDA_DEVICE_ORDER=FASTEST_FIRST`
**reverses** nvidia-smi's PCI-bus ordering. On this box:

| nvidia-smi index | GPU        | UUID              | CUDA index (FASTEST_FIRST) |
|------------------|------------|-------------------|----------------------------|
| 0                | T400 4 GB  | `GPU-09455c63...` | **1**                      |
| 1                | RTX A5000  | `GPU-1aaa66fd...` | **0**                      |

So `CUDA_VISIBLE_DEVICES=1` selected **CUDA index 1 = the T400**, the exact opposite of
what was intended. `nvidia-smi` itself ignores `CUDA_VISIBLE_DEVICES`, so a naive
`nvidia-smi` check always *looked* fine while the JVM ran on the wrong card.

**Fix: reference the A5000 by UUID (unambiguous under any ordering):**

```bash
export CUDA_VISIBLE_DEVICES=GPU-1aaa66fd-0c1e-935b-fe65-2c9ca7357504
```

Applied in both `handoff/run-autotuner-test.sh` (inline on the `spark-shell` line) and
`$SPARK_HOME/conf/spark-env.sh`.

**Verify which GPU a running JVM actually uses** (this is the authoritative check —
it maps process → physical GPU UUID, unaffected by CUDA_VISIBLE_DEVICES):

```bash
nvidia-smi --query-compute-apps=pid,used_memory,gpu_uuid --format=csv,noheader
# A5000 = GPU-1aaa66fd...   T400 = GPU-09455c63...
```

**Corollary that resolved an old red herring:** once correctly on the A5000, store_sales
decoded at an 894 MiB split (and web_sales at 1024 MiB) **with no SIGSEGV**. The earlier
"chunked reader crashes at ~937 MiB" was a **T400 OOM** (a 4 GB card can't hold a ~1 GiB
coalesced host/GPU buffer), *not* an A5000 or cuDF chunked-reader bug.

---

## 5. Environment — which Spark, which GPU, exact launch

### Spark
- **Use Spark 3.5.3** at `/home/kuhu/Downloads/spark-3.5.3-bin-hadoop3/`.
- The `SPARK_HOME` env var on this box defaults to a **3.3.3** install — always override
  it explicitly, or the plugin will load against the wrong Spark shim:
  ```bash
  export SPARK_HOME=/home/kuhu/Downloads/spark-3.5.3-bin-hadoop3
  export JAVA_HOME=/usr/lib/jvm/java-1.17.0-openjdk-amd64
  ```
- Plugin JAR (built with `-Dbuildver=353`):
  `dist/target/rapids-4-spark_2.12-26.08.0-SNAPSHOT-cuda12.jar`

### Selecting the GPU (A5000) — by UUID, see §4 for why
```bash
# unset any stale guard so spark-env.sh is re-sourced, then select A5000 by UUID
unset SPARK_ENV_LOADED
export CUDA_VISIBLE_DEVICES=GPU-1aaa66fd-0c1e-935b-fe65-2c9ca7357504   # A5000, NOT numeric "1"
```
`$SPARK_HOME/conf/spark-env.sh` also exports this UUID as a backstop. **Never** use
`CUDA_VISIBLE_DEVICES=1` — that lands on the T400 (§4).

### The shell command (what the run script executes)
`CUDA_VISIBLE_DEVICES` is set **inline** on the launch so the JVM inherits it before it
initializes CUDA (setting it after CUDA init has no effect):

```bash
CUDA_VISIBLE_DEVICES=GPU-1aaa66fd-0c1e-935b-fe65-2c9ca7357504 \
"$SPARK_HOME/bin/spark-shell" \
  --master local[4] \
  --driver-memory 4g \
  --conf spark.plugins=com.nvidia.spark.SQLPlugin \
  --conf spark.rapids.sql.enabled=true \
  --conf spark.rapids.sql.scan.splitAutotuner.historyPath="$REPO/data/scan-split-history.tsv" \
  --conf spark.local.dir="$REPO/data/spark-tmp" \
  --conf spark.sql.shuffle.partitions=200 \
  --conf spark.rapids.sql.metrics.level=DEBUG \          # REQUIRED: OUTPUT_BATCH_BYTES is DEBUG-level
  --conf spark.sql.catalogImplementation=in-memory \     # no Hive/Derby
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=/home/kuhu/logdir \          # History Server location
  --jars "$REPO/dist/target/rapids-4-spark_2.12-26.08.0-SNAPSHOT-cuda12.jar" \
  -i docs/experiments/rolling-split-autotuning/handoff/nds-autotuner-test.scala
```

### Where things go
- Event logs → `/home/kuhu/logdir` (the Spark History Server reads from here).
- NDS SF100 data → `/home/kuhu/Reps/ab/nds_sf100/parquet_sf100_decimal_fresh_20260623/`.
- History TSV → `data/scan-split-history.tsv` (append-only; cleared per run for a cold start).

---

## 6. What was changed (this branch, `trials_zero_conf`)

**New files**
- `sql-plugin/.../com/nvidia/spark/rapids/perf/ScanSplitAutotuner.scala` — the autotuner:
  `ScanSplitStore` (append-only TSV), `decide()` / `record()`, the pending-scan queue, and
  `ScanObservationListener` (a `QueryExecutionListener` that drains metrics on query success).

**Modified files**
- `sql-plugin/.../org/apache/spark/sql/rapids/GpuFileSourceScanExec.scala` — in
  `createNonBucketedReadRDD`: build the table label, sum listed bytes, call
  `ScanSplitAutotuner.decide(...)` to get `maxSplitBytes`, and `registerPendingScan(...)`
  (see §1). **This is the only place the split value is overridden.**
- `sql-plugin/.../com/nvidia/spark/rapids/RapidsConf.scala` — adds the internal config
  `spark.rapids.sql.scan.splitAutotuner.historyPath` (empty ⇒ autotuner disabled).
- `sql-plugin/.../com/nvidia/spark/rapids/Plugin.scala` — driver plugin lifecycle: on init,
  if the history path is set, `ScanSplitAutotuner.init(path)`; on shutdown,
  `ScanSplitAutotuner.close()`.

**Enable/disable:** set `spark.rapids.sql.scan.splitAutotuner.historyPath` to a local path
to enable; leave it unset to disable (default). No other config is required to turn it on.

---

## 7. Reproduce

```bash
bash docs/experiments/rolling-split-autotuning/handoff/run-autotuner-test.sh
```

- Targets the A5000 by UUID, disables Hive/Derby (`spark.sql.catalogImplementation=in-memory`),
  sets `spark.rapids.sql.metrics.level=DEBUG` (required — `OUTPUT_BATCH_BYTES` is DEBUG-level;
  without it `decodedBytes=0` and every scan logs `SKIPPED_RECORD`).
- Event logs → `/home/kuhu/logdir` (History Server location).
- Parsed results → `docs/experiments/rolling-split-autotuning/results/run-<timestamp>.md`.
- Override an experiment via `EXTRA_CONFS`, e.g.
  `EXTRA_CONFS="--conf spark.rapids.sql.batchSizeBytes=536870912" bash .../run-autotuner-test.sh`
  (this also lowers the clamp ceiling to 512 MiB, per §3).

### Reference result — 2026-07-13, A5000, local[4], defaults (128 MiB start, 1 GiB batch)

| Table       | Run 1 (cold, 128 MiB) | Run 2 (autotuned) | Speedup | Split         | Ratio |
|-------------|-----------------------|-------------------|---------|---------------|-------|
| store_sales | 44.5s                 | 24.1s             | 1.85×   | → 894 MiB     | 1.146 |
| web_sales   | 40.0s                 | 19.2s             | 2.08×   | → 1024 MiB    | 0.760 |

### 2 GiB batch experiment — 2026-07-13, A5000, `EXTRA_CONFS="--conf spark.rapids.sql.batchSizeBytes=2147483648"`

Raising the batch ceiling from 1 GiB to 2 GiB scales the autotuned splits proportionally
(§3): store_sales `2 GiB / 1.146 = 1788 MiB`; web_sales `raw = 2 GiB / 0.76 = 2.68 GiB`
**clamped to exactly 2 GiB** (`split_bytes=2147483648`). No crash, no cuDF column overflow —
confirms 2 GiB is safe on this all-decimal (no wide-string) dataset.

| Table       | Split @1 GiB | Split @2 GiB      | Runtime @1 GiB | Runtime @2 GiB | Cold (128 MiB) |
|-------------|--------------|-------------------|----------------|----------------|----------------|
| store_sales | 894 MiB      | 1788 MiB          | 24.1s          | 23.0s          | 44.1s          |
| web_sales   | 1024 MiB     | 2048 MiB (clamped)| 19.2s          | 18.8s          | 39.0s          |

**Finding:** doubling the batch (1→2 GiB) **halved scan-stage task count** (store_sales
180→90, web_sales 148→74) but moved wall-clock only ~1s (2–5%). CORRECTION: later GPU-util
sampling (`../results/nds-gpu-util-20260713.md`) showed the GPU is only ~38% utilized during
warm queries — so this is NOT GPU decode+aggregate bound as originally inferred; the limiter is
the non-GPU path (CPU-side small-file coalescing / shuffle). The
entire win is the first jump (128 MiB → autotuned = 1.85–2.08×); pushing the ceiling higher
gives diminishing returns for this dataset. A dataset with smaller files or higher per-task
open cost would benefit more from the larger ceiling. **2 GiB is the safe upper bound** —
it's the cuDF 32-bit-offset column limit (§3); do not exceed it for string/list-bearing data.
