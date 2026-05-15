# Scan batch sizing — requirement + benchmark

## Origin

Started from a question: is there a correlation between scan time and MPB, batches-per-task, or output batch size relative to target? Working hypothesis: `B ≈ f(MPB / compression_ratio × filter_selectivity)` — output batch size is roughly proportional to MPB, modulated by codec inflation and selectivity. Column pruning is the first lever to understand: spark-rapids inherits Spark's approach — read the Parquet footer, keep only the column-chunk pages of interest, rewrite a trimmed footer, hand the buffer to cuDF. To validate the hypothesis we want a plot with MPB on x (128m → 2g) and stage time, output-batch-vs-target ratio, and batches-per-task (intuition: should be 1 in the best case) on y. Two new metrics make this measurable: `outputBatchBytesAfterFilter` (per-task decoded GPU bytes) and a compression ratio (`readBufferSize / outputBatchBytesAfterFilter`). One filesystem only (HDFS); minimal code change.

## Problem

On HDFS Parquet, the scan's output batch size today is decided by `populateCurrentBlockChunk` (`GpuParquetScan.scala:2202`) using `estimateGpuMemory(schema, rowCount)` — a schema-only estimator that is codec-blind and data-blind (`StringType.defaultSize = 20` always). The actual output batch size on GPU depends on MPB, column pruning, and codec, none of which feed into the sizing decision. Result: output batches drift from the target, tasks emit multiple batches, scan time pays per-batch overhead.

**Goal:** pick `spark.sql.files.maxPartitionBytes` at plan time so each task produces one output batch sized close to `spark.rapids.sql.batchSizeBytes`.

## What's missing from the metrics today

| Have | Need |
|---|---|
| `readBufferSize` — on-disk bytes after column pruning (`:2147-2153`) | — |
| `NUM_OUTPUT_BATCHES`, `BUFFER_TIME`, `GPU_DECODE_TIME` | — |
| — | **`outputBatchBytesAfterFilter`** — decoded GPU bytes per task |
| — | **`gpuExpansionRatio`** — per-task ratio of the above two, as `average` SQLMetric |

Without these, we cannot tell whether MPB landed us close to the target.

## Read-side variables, mapped to code

```
MPB ──Spark FilePartition.maxSplitBytes──▶ on_disk_partition_bytes (1 task = 1 FilePartition)
   ─ column pruning (clipParquetSchema :810, clipBlocksToSchema :816)
                                                 │
                                                 ▼
                              readBufferSize   [existing metric]
                                                 │
                                                 ▼
                              cuDF Table.readParquet :3410
                                                 │
                                                 ▼
                              outputBatchBytesAfterFilter   [new metric]
```

## Heuristic

```
B  = spark.rapids.sql.batchSizeBytes
r  = gpu_expansion_ratio   (from footer sample, fallback to codec table)
kc = kept_cols_size / all_cols_size

MPB = B / (r × kc)
```

Focus is column pruning (`kc`). The expansion ratio `r` is a follow-up concern.

## Two new metrics

**`outputBatchBytesAfterFilter`** — SUM, `ESSENTIAL_LEVEL`.
Source: `Σ table.getColumn(i).getDeviceMemorySize()`.
Sites: `MakeParquetTableProducer.apply()` `:3434` (non-chunked); `ParquetTableReader.next()` `:3463+` per yielded Table (chunked — required, else undercounts).

**`gpuExpansionRatio`** — AVERAGE, `DEBUG_LEVEL`. Monitoring only; not consumed by any sizing decision in this PR.
Computed once per task in reader `close()`:
```
ratio = outputBatchBytesAfterFilter_local / readBufferSize_local
metrics(GPU_EXPANSION_RATIO).set(ratio)
```

## Benchmark

### Hypotheses

- **H1** — Output batch bytes are roughly linear in MPB for fixed (query, codec, projection).
- **H2** — `batches_per_task → 1` when MPB is sized to fill `batchSizeBytes`.
- **H3** — Scan stage time drops monotonically as `output_vs_target_ratio → 1`.
- **H4** — `gpuExpansionRatio` is roughly constant in MPB for fixed (query, codec).

### Setup

**Setup:** HDFS, Parquet/snappy, COALESCING reader (HDFS default), chunked reader on (default), Spark 3.3.0. Suite: `nvidia/spark-rapids-benchmarks` modified TPC-DS at SF1000. Queries: **q9** (multi-aggregate over store_sales), **q67** (rollup, large coalesced batches), **q76** (union of 3 fact tables with NULL-channel filter).

**Sweep:** `spark.sql.files.maxPartitionBytes ∈ {128m, 256m, 512m, 1g, 2g}`. Hold `batchSizeBytes=1g`, `concurrentGpuTasks=2`, `metrics.level=DEBUG`. **3 queries × 5 MPB × 3 iters = 45 executions.**

**Captured per cell** (one CSV row from Spark REST `applications/{id}/sql/{exec_id}`):
`mpb_bytes`, `wall_clock_ms`, `scan_stage_time_ms`, `num_partitions`, `num_output_batches`, `read_buffer_bytes`, `output_batch_bytes_after_filter`, `gpu_expansion_ratio_mean`, `buffer_time_ns`, `gpu_decode_time_ns`. Derived: `batches_per_task`, `mean_batch_bytes`, `output_vs_target_ratio`.

### Plots

X = MPB on log scale, one line per query.

| Plot (y-axis) | Tests |
|---|---|
| `output_vs_target_ratio` | H1 |
| `batches_per_task` | H2 |
| `scan_stage_time_ms` vs MPB | H3 |
| `gpu_expansion_ratio_mean` | H4 |
| `scan_stage_time_ms` vs `output_vs_target_ratio` (scatter, colored by query) | H3 |

**Run:**

```bash
git clone https://github.com/nvidia/spark-rapids-benchmarks
cd spark-rapids-benchmarks
./nds_gen_data.sh hdfs 1000 100 hdfs:///nds/sf1000/

for mpb in 128m 256m 512m 1g 2g; do
  for q in q9 q67 q76; do
    ./nds_power.sh hdfs:///nds/sf1000/ ./query_streams/$q.sql time.csv \
      --property-file confs/sweep.template \
      --conf spark.sql.files.maxPartitionBytes=$mpb \
      --conf spark.rapids.sql.batchSizeBytes=1g \
      --conf spark.rapids.sql.metrics.level=DEBUG \
      --json-summary results/$q-mpb${mpb}.json
  done
done

python tools/plot.py results/*.json --out plots/
```

## Risks

- **Chunked-reader undercount** if `outputBatchBytesAfterFilter` is incremented only at `MakeParquetTableProducer.apply()` and not also inside `ParquetTableReader.next()`.
- **stock-Spark MPB cap:** `FilePartition.maxSplitBytes` may clamp below configured MPB when `totalBytes/parallelism` is small. Log the effective `maxSplitBytes` (`GpuFileSourceScanExec.scala:561`) per run.
- **HDFS skew:** use median per cell, capture stddev.

## Out of scope

ORC/CSV/JSON; S3/GCS/ABFS (MULTITHREADED path); PERFILE reader; cross-scan memory budgeting; AQE-style adaptive resizing; write-side codec/RG tuning.
