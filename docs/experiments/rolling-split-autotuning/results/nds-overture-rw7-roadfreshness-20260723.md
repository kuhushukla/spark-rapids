# Road-network freshness by class — maxPartitionBytes sweep + fill-to-target (local, Spark 3.5.3) — 2026-07-23

Query **RW7** from `overture-realworld-2.scala` (2.11 GiB scanned). Scan-heavy: one exploded scan of <code>segment.sources</code> (348.7 M segments) + a tiny GROUP BY class.

**Headline:** ftt sizes a 4.99 GB split (segment's class+sources columns are highly compressible, decoded/listed 0.22) → 16 fuller tasks that **cut gpuTime 45% (22.8 → 12.7 s) and decode 43% (6.3 → 3.6 s)** vs the 2g optimum — a real GPU-efficiency win. The trade is **+6% wall**: with only 16 tasks this query is mildly parallelism-bound, so less GPU work doesn't translate to faster wall here.

## The query & the question

**Question:** How stale is each part of the road network? For each road class, what share of its source records were updated 2024+ vs frozen before 2022 — which classes are actively maintained?

```sql
WITH s AS ( SELECT seg.class, SUBSTRING(src.update_time,1,4) AS yr
    FROM segment seg LATERAL VIEW explode(seg.sources) t AS src WHERE seg.class IS NOT NULL )
  SELECT class, COUNT(*) AS source_records,
    ROUND(100.0*AVG(CASE WHEN yr>='2024' THEN 1 ELSE 0 END),1) AS pct_2024plus,
    ROUND(100.0*AVG(CASE WHEN yr<'2022' THEN 1 ELSE 0 END),1) AS pct_before_2022,
    MIN(yr) AS oldest_year, MAX(yr) AS newest_year
  FROM s GROUP BY class ORDER BY source_records DESC
```

**Result (real insight):**

| class | source records | % 2024+ | % before 2022 | oldest | newest |
|---|---|---|---|---|---|
| service | 67.0 M | 34.1 | 46.9 | 2006 | 2026 |
| unclassified | 34.5 M | 35.4 | 43.7 | 2006 | 2026 |
| footway | 32.5 M | 55.9 | 23.9 | 2006 | 2026 |
| track | 30.0 M | 30.6 | 49.6 | 2006 | 2026 |
| tertiary | 25.8 M | 56.2 | 19.4 | 2007 | 2026 |
| secondary | 17.0 M | 57.5 | 10.2 | 2008 | 2026 |
| primary | 13.7 M | 51.7 | 6.7 | 2008 | 2026 |
| trunk | 9.7 M | 41.1 | 5.9 | 2008 | 2026 |
| motorway | 3.3 M | 55.3 | 9.5 | 2008 | 2026 |
| cycleway | 2.2 M | 68.2 | 13.2 | 2006 | 2026 |


Genuine insight: higher-class roads are **freshest** (cycleway 68%, secondary 58%, tertiary 56% updated 2024+), while **service/track** are stalest (~47–50% frozen before 2022). All classes span 2006–2026. A real maintenance-coverage signal, no artifacts.

## Data read (per execution)
| stage | bytes | note |
|---|---|---|
| on disk (segment, listed) | 66.3 GiB | 128 files — whole segment dataset is the scan unit |
| scanned off disk | 2.11 GiB | measured (Spark input) — only class + sources columns |
| decoded on GPU | ~14.3 GiB | explode expands sources (decode_expansion 13.6×) |
| rows after explode | ~330 M | source records |


## Setup
Local Spark 3.5.3 + spark353 jar, **RTX A5000 only**, `local[16]`, driver 32G, `concurrentGpuTasks=2`, `filecache=false`, `metrics.level=DEBUG`, 5 iters (iter1 cold / COLD_START, 2–5 warm). **One query per session** (`BENCHMARK-METHOD.md`). Fully on GPU (post-AQE plan: GpuScan/GpuGenerate/GpuHashAggregate/GpuUnion; zero CPU fallback).

## 1. Sweep (autotuner OFF) → optimum 2g
Warm iters 2–5. | mpb | tasks | byte skew | avg batch | scan | decode | gpuTime | wall ms | note |
|---|---|---|---|---|---|---|---|---|
| 256m | 286 | 1.62× | 51M | 19.7s | 11.2s | 33.5s | 3063 |  |
| 512m | 143 | 1.24× | 102M | 15.9s | 8.9s | 27.5s | 2602 |  |
| 1g | 99 | 2.06× | 148M | 14.6s | 8.3s | 26.4s | 2490 |  |
| 2g | 40 | 1.48× | 365M | 13.6s | 6.3s | 22.8s | 2339 | ← optimum |
| 4g | 18 | 1.18× | 812M | 16.4s | 3.8s | 13.6s | 2484 |  |


**byte skew** = max÷median bytes-read per scan task (1.0 = balanced; high → straggler).

## 2. Autotuner ON (fill-to-target)
Converges **start-independently** (128m and 4g starts → same split): **4.99 GB** (`bound_by=ratio`). ftt warm: 16 tasks, skew 1.25×, scan 15.6s, decode 3.7s, gpuTime 12.6s, wall 2495 ms.

**ftt vs fixed settings** (Δ = ftt − baseline):
| baseline | wall | Δ wall | scan | Δ scan | gpuTime | Δ gpuTime |
|---|---|---|---|---|---|---|
| 256m | 3063 | -19% | 19.7s | -21% | 33.5s | -63% |
| 512m | 2602 | -4% | 15.9s | -2% | 27.5s | -54% |
| 1g | 2490 | +0% | 14.6s | +7% | 26.4s | -52% |
| 2g | 2339 | +7% | 13.6s | +15% | 22.8s | -45% |
| 4g | 2484 | +0% | 16.4s | -5% | 13.6s | -8% |


Vs the **2g optimum**: wall +7%, scan +15%, gpuTime -45%; byte skew 1.48× → 1.25×. Verdict: **gpu-lean, +7% wall** — cuts GPU work (-45% gpuTime) at a small wall cost (parallelism-bound).

## 3. Conclusion
ftt sizes a 4.99 GB split (segment's class+sources columns are highly compressible, decoded/listed 0.22) → 16 fuller tasks that **cut gpuTime 45% (22.8 → 12.7 s) and decode 43% (6.3 → 3.6 s)** vs the 2g optimum — a real GPU-efficiency win. The trade is **+6% wall**: with only 16 tasks this query is mildly parallelism-bound, so less GPU work doesn't translate to faster wall here.


## Sources
Runs: `data/overture-rw2-rw7-{256m,512m,1g,2g,4g,ftt-128m,ftt-4g}/`. Harness: `overture_rw2_bench.scala`, `run-rw2-sweep.sh`, `run-rw2-ftt.sh`, `rw2_warm_parse.py`. Method: `BENCHMARK-METHOD.md`. Query source: `docs/experiments/overture-analytics/overture-realworld-2.scala`.
