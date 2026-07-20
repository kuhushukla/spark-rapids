# NDS SF3k sparkh — scan-split sizing-basis POC (query9 / store_sales, 2026-07-20)

Single-query proof-of-concept comparing **how the scan split is sized** for the `store_sales` table.
Question: does sizing the split from **actual bytes read** (or a **read budget**) beat the current
`decoded/listed` sizing? Run on the shared sparkh cluster at SF3k, one query (**query9** — store_sales
only, so the split change is isolated), autotuner off vs each sizing mode.

Cluster: sparkh standalone, **8 executors × 16 cores = 128 cores**, `maxPartitionBytes=2gb`,
`concurrentGpuTasks=4`, AQE on, `metrics.level=DEBUG`. Data (read-only):
`hdfs://sparkh-nn1:8020/user/nvidia/nds/parquet_sf3k_decimal`. Jar: `data/jars/rapids-ratiobasis-357.jar`
(357 shim, this session's build). 5 iterations per mode in one session: **iter1 cold** (autotuner learns
the ratio), **iters 2–5 warm** (mode applies). Warm = mean of iters 2–5. All autotuner modes share
**ceiling = 8 GiB**.

---

## 1. The sizing modes (from `ScanSplitAutotuner.scala`)

`splitBytes = max(floor, min(ceiling, raw))` (line 281). `raw` is sized per mode (lines 235–242); the
split bin-packs on **listed** bytes (`getLen`), so `tasks ≈ listed / split`.

| mode | how `raw` is sized | flag |
|---|---|---|
| **listed** (current) | `batchSize / (decoded/listed)` → decoded/task ≈ one 1 GiB batch | `ratioBasis=listed` |
| **bytesread** | `batchSize / (decoded/readBytes)` = `batchSize / decode_expansion` (projection removed) | `ratioBasis=bytesread` |
| **readbudget** | `readBudget / read_selectivity`, `read_selectivity = readBytes/listed` (bound compressed reads/task) | `ratioBasis=readbudget`, `readBudgetBytes=N` |

**Floor** (line 279–280): default `max(64MiB, sparkDefault)`; `sparkDefault` = Spark's own `maxSplitBytes`
= **2 GiB** here (driven by `maxPartitionBytes=2gb`). A new `-Drapids.autotuner.floor=min` drops the floor
to 64 MiB so a split **smaller** than Spark's default can take effect. **Ceiling** = `8g` for this POC.

For query9's projection of store_sales (measured, from the `DECIDED`/`RECORDED` logs):
`listed = 370.5 GiB`, `read_selectivity = 0.150`, `decode_expansion = 1.79`, `decoded/listed = 0.269`,
`decoded ≈ 99.6 GiB`, `readBytes ≈ 55.6 GiB`.

---

## 2. Results (query9, warm = mean of iters 2–5)

| mode | store_sales split | raw target | bound by | ≈tasks | scan-out batches | batch fullness (÷1 GiB) | decode tput¹ | warm | speedup vs OFF |
|---|---|---|---|---|---|---|---|---|---|
| **OFF** | 2.0 GiB (Spark default) | — | — | 185 | 202² | 49%² | 7.0 GiB/s | 2.18 s | **1.00×** |
| **listed** | **3.72 GiB** | 3.72 GiB | ratio | 100 | 112 | **89%** | 8.2 GiB/s | **1.99 s** | **1.09×** |
| **bytesread** | 2.0 GiB *(floored)* | 0.57 GiB *(discarded)* | **floor** | 185 | 202 | 49% | 6.9 GiB/s | 2.00 s | 1.09× |
| **rb1g** (readbudget 1 GiB) | **6.66 GiB** | 6.66 GiB | ratio | 56 | 176 | 57% | **10.8 GiB/s** | 2.28 s | **0.95×** |
| **brfloor** (bytesread, floor=min) | **0.57 GiB** | 0.57 GiB | ratio | 663 | 825 | 12% | 3.7 GiB/s | 2.25 s | 0.97× |

Per-iteration warm times (ms): OFF `2383/2042/2134/2144` · listed `2436/2136/1734/1665` ·
bytesread `2162/1618/2157/2074` · rb1g `2658/1905/2205/2370` · brfloor `2603/2103/2379/1907`.
Cold (iter1, ms): OFF `5753` · listed `5690` · bytesread `5826` · rb1g `5644` · brfloor `5699`.

¹ Aggregate device decode-time: `Σ "GPU decode time"` over all tasks / all 5 iters, vs ~498 GiB decoded.
It is a **per-byte decode-efficiency** figure, not wall-clock. ² OFF has no autotuner record; its split is
the same 2 GiB default as floored-bytesread, so its batch stats are taken as equal (≈202 / 49%).

---

## 3. Findings

1. **`listed` (current sizing) is the sweet spot.** It is *designed* so decoded/task ≈ the 1 GiB target,
   and it lands there: **89%-full** scan batches with a healthy 100 tasks. Fastest warm (1.09×).

2. **bytesRead is inert under the default floor.** Its raw target (0.57 GiB) is **below** the 2 GiB floor
   (= `maxPartitionBytes`), so it is clamped **up to 2 GiB** and the ratio is discarded — identical to OFF
   (`bound_by=floor`). The floor was using maxPartitionBytes, not the ratio.

3. **Letting the bytesRead ratio through (brfloor, floor=min) hurts.** The 0.57 GiB split makes **663 tiny
   tasks**, **12%-full** batches, the **slowest decode (3.7 GiB/s)**, and **0.97×** — slower than both OFF
   and listed.

4. **read-budget makes the fewest, biggest tasks (rb1g: 56 tasks, 6.66 GiB split).** It has the **best
   per-byte decode efficiency (10.8 GiB/s)** but the **worst runtime (0.95×)**.

5. **Decode efficiency anti-correlates with runtime.** Bigger batches decode faster per byte (rb1g 10.8 vs
   brfloor 3.7 GiB/s) yet run slower, because fewer/larger tasks under-parallelize. Same pattern as the
   SF3k full-suite breakdowns (`nds-sf3000-sparkh-breakdowns-20260719.md`).

**Net:** among the three sizings, the existing `decoded/listed` (batches at target) is best; moving the
split either **smaller** (bytesRead / brfloor) or **bigger** (read-budget) is equal-or-worse on runtime for
store_sales at SF3k.

---

## 4. Caveats

- **Runtimes are noisy.** query9 warm is ~2 s over only 4 samples that swing 1665–2658 ms, so the ±10%
  speedups are near the noise floor. **The splits, task counts, batch fullness, and decode throughput are
  ground truth** (from the `DECIDED`/`RECORDED` logs and event-log metrics); the runtime *ranking* is
  directional, not a precise measurement.
- Batch counts/fullness are the **scan node's own** `NUM_OUTPUT_BATCHES` / `OUTPUT_BATCH_BYTES`
  (`ScanSplitAutotuner.scala:146,154`) — read-stage output only, not downstream operators.
- Decode throughput is aggregate device-time summed across tasks and all 5 iterations (cold + warm mixed),
  not warm-only wall-clock.
- Task counts are `listed / split` (deterministic for a fixed dataset), not counted from stages.
- One query, one table, one projection (query9's). read_selectivity here is 0.150 (not the 0.039 of the
  most-projected scan in the full-suite doc) — the store_sales ratio is projection-specific.

---

## 5. Sources

- Run dirs: `data/poc-{off,listed,bytesread,rb1g,brfloor}-results/` (per-query CSV + `eventlog-test-1`).
- Jar: `data/jars/rapids-ratiobasis-357.jar`. Templates: `ab/templates/onprem-h/gpu-poc-{listed,bytesread,rb1g,rb2g,rb4g,brfloor}.template`.
- Runner: `docs/experiments/rolling-split-autotuning/handoff/run-poc-ratiobasis.sh <mode>`.
- Code: `ScanSplitAutotuner.scala` — `ratioBasis` (lines 228–242), `floor` knob (279–284), `readBytes`
  persisted in `ScanSplitRecord`. Decode-throughput parse: `handoff/decode_tput.py`.
- Not run: rb2g / rb4g (both would pin to the 8 GiB ceiling: raw 13.3 / 26.6 GiB).
