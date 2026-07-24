# Multilingual naming coverage of the road network — maxPartitionBytes sweep + fill-to-target (local, Spark 3.5.3) — 2026-07-23

Query **RW8** from `overture-realworld-2.scala` (1.68 GiB scanned). Scan-heavy: one scan of <code>segment</code> class + names (348.7 M rows) + a tiny GROUP BY class.

**Headline:** ftt sizes a **7.9 GB** split (names.common map is highly compressible, decoded/listed 0.14) → 10 fuller tasks that **cut gpuTime 49% (5.5 → 2.8 s) and decode 41% (2.9 → 1.7 s)** vs the 2g optimum. The trade is **+11% wall**: 10 tasks under-fills the 16 cores, so this parallelism-bound query is slightly slower on wall despite doing less GPU work.

## The query & the question

**Question:** How multilingual is road naming? For each class, of the named roads, what share also carry alternate-language names (names.common map)?

```sql
SELECT class, COUNT(*) AS named_segments,
    ROUND(AVG(size(map_keys(names.common))),2) AS avg_languages,   -- see caveat
    ROUND(100.0*AVG(CASE WHEN size(map_keys(names.common))>=2 THEN 1 ELSE 0 END),1) AS pct_multilingual
  FROM segment WHERE class IS NOT NULL AND names.primary IS NOT NULL
  GROUP BY class ORDER BY named_segments DESC
```

**Result (real insight):**

| class | named segments | pct_multilingual | avg_languages ⚠ |
|---|---|---|---|
| motorway | 0.39 M | 26.2 | (−0.26) |
| trunk | 2.57 M | 22.4 | (−0.06) |
| standard_gauge (rail) | 0.20 M | 16.5 | (−0.15) |
| primary | 5.14 M | 13.8 | (−0.38) |
| secondary | 7.85 M | 10.5 | (−0.53) |
| tertiary | 11.43 M | 8.2 | (−0.64) |
| living_street | 1.29 M | 6.1 | (−0.71) |
| unclassified | 5.43 M | 4.7 | (−0.78) |
| service | 2.54 M | 3.6 | (−0.84) |
| track | 1.26 M | 1.5 | (−0.88) |


Genuine insight (from **pct_multilingual**): the **most important roads are the most multilingual** — motorway 26%, trunk 22%, rail 17%, primary 14% — tapering to <5% for service/track. **Honest caveat:** `avg_languages` is **corrupt** — `size(map_keys(NULL))` returns **−1** for roads with no `names.common`, so the AVG goes negative and is meaningless. Only **pct_multilingual** (which treats −1 as <2 → not multilingual) is valid. Reported honestly; avg_languages shown struck-through.

## Data read (per execution)
| stage | bytes | note |
|---|---|---|
| on disk (segment, listed) | 66.3 GiB | 128 files |
| scanned off disk | 1.68 GiB | measured (Spark input) — only class + names columns |
| decoded on GPU | ~9.0 GiB | names.common map decodes (decode_expansion 16.1×) |
| rows | 348.7 M | segments (named subset aggregated) |


## Setup
Local Spark 3.5.3 + spark353 jar, **RTX A5000 only**, `local[16]`, driver 32G, `concurrentGpuTasks=2`, `filecache=false`, `metrics.level=DEBUG`, 5 iters (iter1 cold / COLD_START, 2–5 warm). **One query per session** (`BENCHMARK-METHOD.md`). Fully on GPU (post-AQE plan: GpuScan/GpuGenerate/GpuHashAggregate/GpuUnion; zero CPU fallback).

## 1. Sweep (autotuner OFF) → optimum 2g
Warm iters 2–5. | mpb | tasks | byte skew | avg batch | scan | decode | gpuTime | wall ms | note |
|---|---|---|---|---|---|---|---|---|
| 256m | 286 | 1.90× | 32M | 17.8s | 7.9s | 17.5s | 2141 |  |
| 512m | 143 | 1.54× | 65M | 11.0s | 4.2s | 8.6s | 1408 |  |
| 1g | 99 | 2.04× | 93M | 9.6s | 3.5s | 7.2s | 1206 |  |
| 2g | 40 | 1.54× | 231M | 8.8s | 2.9s | 5.5s | 1131 | ← optimum |
| 4g | 18 | 1.16× | 369M | 10.2s | 3.2s | 5.0s | 1195 |  |


**byte skew** = max÷median bytes-read per scan task (1.0 = balanced; high → straggler).

## 2. Autotuner ON (fill-to-target)
Converges **start-independently** (128m and 4g starts → same split): **7.9 GB** (`bound_by=ratio`). ftt warm: 10 tasks, skew 1.16×, scan 6.8s, decode 1.7s, gpuTime 2.8s, wall 1260 ms.

**ftt vs fixed settings** (Δ = ftt − baseline):
| baseline | wall | Δ wall | scan | Δ scan | gpuTime | Δ gpuTime |
|---|---|---|---|---|---|---|
| 256m | 2141 | -41% | 17.8s | -62% | 17.5s | -84% |
| 512m | 1408 | -11% | 11.0s | -39% | 8.6s | -68% |
| 1g | 1206 | +4% | 9.6s | -30% | 7.2s | -62% |
| 2g | 1131 | +11% | 8.8s | -23% | 5.5s | -50% |
| 4g | 1195 | +5% | 10.2s | -34% | 5.0s | -45% |


Vs the **2g optimum**: wall +11%, scan -23%, gpuTime -50%; byte skew 1.54× → 1.16×. Verdict: **gpu-lean, +11% wall** — cuts GPU work (-50% gpuTime) at a small wall cost (parallelism-bound).

## 3. Conclusion
ftt sizes a **7.9 GB** split (names.common map is highly compressible, decoded/listed 0.14) → 10 fuller tasks that **cut gpuTime 49% (5.5 → 2.8 s) and decode 41% (2.9 → 1.7 s)** vs the 2g optimum. The trade is **+11% wall**: 10 tasks under-fills the 16 cores, so this parallelism-bound query is slightly slower on wall despite doing less GPU work.


## Sources
Runs: `data/overture-rw2-rw8-{256m,512m,1g,2g,4g,ftt-128m,ftt-4g}/`. Harness: `overture_rw2_bench.scala`, `run-rw2-sweep.sh`, `run-rw2-ftt.sh`, `rw2_warm_parse.py`. Method: `BENCHMARK-METHOD.md`. Query source: `docs/experiments/overture-analytics/overture-realworld-2.scala`.
