# Road full-profile: geometry complexity + attribute completeness + integrity — maxPartitionBytes sweep + fill-to-target (local, Spark 3.5.3) — 2026-07-23

Query **GF1** from `overture-realworld-2.scala` (56.4 GiB scanned (geometry + ~15 attribute columns)). Scan-heavy: one scan of <b>segment</b> reading the geometry (WKB) column <b>plus every attribute column</b> (~56 GiB / 348.7 M rows) + a tiny GROUP BY class. Heaviest per-task work of any query here. All ops (<code>length</code>/<code>md5</code> on binary, GPU HyperLogLog for <code>approx_count_distinct</code>) run on GPU (0 CPU fallbacks, verified post-AQE).

**Headline:** GF1 is the clean **goal-met** case: ftt picks a **small 0.45 GB split** (because it reads geometry + all ~15 columns → high selectivity → high decoded/listed ratio → small split) → 171 tasks, **within +0.9% of the 256m optimum on wall** while cutting **gpuTime 14% (404→348 s) and decode 11%**. It's the counterexample to the rw7/rw8/gf2 overshoot: when a query reads a lot, ftt correctly sizes <i>down</i> and lands on the optimum. (Wall rises steadily with bigger splits here — 42→63 s — as fewer, heavier tasks starve the 16 cores; low skew ≤1.5× means it's pure parallelism, not stragglers.)

## The query & the question

**Question:** A complete data-quality + geometry profile of the world road network, by class: how complete is each class's attribution (fill rates), how geometrically complex are its shapes (WKB byte size ~ vertex count), and are there duplicate/copied geometries (integrity)? The profile a team builds before trusting a layer.

```sql
SELECT class, COUNT(*) AS segments,
    ROUND(AVG(length(geometry)),1) AS avg_wkb_bytes, MAX(length(geometry)) AS max_wkb_bytes,
    (COUNT(*) - approx_count_distinct(md5(geometry))) AS approx_dup_geoms,   -- HLL, see caveat
    ROUND(100.0*AVG(CASE WHEN names.primary IS NOT NULL THEN 1 ELSE 0 END),1) AS pct_named,
    ... 12 more fill-rate columns (connectors/speed/access/surface/flags/width/turns/routes/…) ...
  FROM segment WHERE class IS NOT NULL GROUP BY class ORDER BY segments DESC
```

**Result (real insight):**

| class | segments | avg WKB bytes | % named | % speed |
|---|---|---|---|---|
| residential | 127.9 M | 94.1 | 42.0 | 8.8 |
| service | 61.5 M | 117.4 | 4.1 | 1.8 |
| unclassified | 30.2 M | 238.8 | 18.0 | 5.1 |
| track | 26.4 M | 347.8 | 4.8 | 0.6 |
| footway | 24.3 M | 131.2 | 3.4 | 0.0 |
| tertiary | 20.8 M | 154.1 | 55.1 | 20.6 |
| secondary | 11.4 M | 134.2 | 68.9 | 33.3 |
| primary | 7.4 M | 124.9 | 69.2 | 41.3 |
| trunk | 4.2 M | 138.3 | 61.6 | 38.8 |


Genuine insight: **geometry complexity varies by class** — **track (348 B) and unclassified (239 B) have the most complex shapes** (long winding rural paths → more vertices), while residential (94 B) is simplest; and higher road classes are best-attributed (primary/secondary ~69% named, ~40% speed vs track ~5%). **Honest caveat:** the `approx_dup_geoms` column uses `approx_count_distinct` (GPU HyperLogLog, ~1–2% error), so on tens of millions of rows it is **noisy and can go slightly negative** (e.g. service −0.76 M) — only <i>large</i> duplicate counts are meaningful; small ones are within the sketch's error band.

## Data read (per execution)
| stage | bytes | note |
|---|---|---|
| on disk (segment, listed) | 66.3 GiB | 128 files |
| scanned off disk | 56.4 GiB | measured (Spark input) — geometry + all ~15 attribute columns (read_selectivity 0.85) |
| decoded on GPU | ~68 GiB | decode_expansion ~1.2× (reads almost everything, little to expand) |
| rows | 348.7 M | segments |


## Setup
Local Spark 3.5.3 + spark353 jar, **RTX A5000 only**, `local[16]`, driver 32G, `concurrentGpuTasks=2`, `filecache=false`, `metrics.level=DEBUG`, 5 iters (iter1 cold / COLD_START, 2–5 warm). **One query per session** (`BENCHMARK-METHOD.md`). Fully on GPU (post-AQE plan: GpuScan/GpuGenerate/GpuHashAggregate/GpuUnion; zero CPU fallback).

## 1. Sweep (autotuner OFF) → optimum 256m
Warm iters 2–5. | mpb | tasks | byte skew | avg batch | scan | decode | gpuTime | wall ms | note |
|---|---|---|---|---|---|---|---|---|
| 128m | 550 | 1.16× | 295M | 366.7s | 97.0s | 575.8s | 43617 |  |
| 256m | 286 | 1.08× | 403M | 432.2s | 76.0s | 404.0s | 42161 | ← optimum |
| 512m | 143 | 1.08× | 445M | 527.9s | 60.7s | 294.4s | 44165 |  |
| 1g | 99 | 1.52× | 445M | 549.1s | 54.6s | 252.9s | 45392 |  |
| 2g | 40 | 1.24× | 537M | 555.5s | 44.1s | 209.6s | 48616 |  |
| 4g | 18 | 1.10× | 567M | 561.3s | 32.6s | 161.2s | 62750 |  |


**byte skew** = max÷median bytes-read per scan task (1.0 = balanced; high → straggler).

## 2. Autotuner ON (fill-to-target)
Converges **start-independently** (128m and 4g starts → same split): **0.45 GB** (`bound_by=ratio`). ftt warm: 171 tasks, skew 1.07×, scan 470.6s, decode 67.5s, gpuTime 347.6s, wall 42541 ms.

**ftt vs fixed settings** (Δ = ftt − baseline):
| baseline | wall | Δ wall | scan | Δ scan | gpuTime | Δ gpuTime |
|---|---|---|---|---|---|---|
| 128m | 43617 | -2% | 366.7s | +28% | 575.8s | -40% |
| 256m | 42161 | +1% | 432.2s | +9% | 404.0s | -14% |
| 512m | 44165 | -4% | 527.9s | -11% | 294.4s | +18% |
| 1g | 45392 | -6% | 549.1s | -14% | 252.9s | +37% |
| 2g | 48616 | -12% | 555.5s | -15% | 209.6s | +66% |
| 4g | 62750 | -32% | 561.3s | -16% | 161.2s | +116% |


Vs the **256m optimum**: wall +1%, scan +9%, gpuTime -14%; byte skew 1.08× → 1.07×. Verdict: **a tie** (lands in the flat region; no harm).

## 3. Conclusion
GF1 is the clean **goal-met** case: ftt picks a **small 0.45 GB split** (because it reads geometry + all ~15 columns → high selectivity → high decoded/listed ratio → small split) → 171 tasks, **within +0.9% of the 256m optimum on wall** while cutting **gpuTime 14% (404→348 s) and decode 11%**. It's the counterexample to the rw7/rw8/gf2 overshoot: when a query reads a lot, ftt correctly sizes <i>down</i> and lands on the optimum. (Wall rises steadily with bigger splits here — 42→63 s — as fewer, heavier tasks starve the 16 cores; low skew ≤1.5× means it's pure parallelism, not stragglers.)


## Sources
Runs: `data/overture-rw2-gf1-{256m,512m,1g,2g,4g,ftt-128m,ftt-4g}/`. Harness: `overture_rw2_bench.scala`, `run-rw2-sweep.sh`, `run-rw2-ftt.sh`, `rw2_warm_parse.py`. Method: `BENCHMARK-METHOD.md`. Query source: `docs/experiments/overture-analytics/overture-realworld-2.scala`.
