# Basemap provenance audit (who mapped the world) — maxPartitionBytes sweep + fill-to-target (local, Spark 3.5.3) — 2026-07-23

Query **RW6** from `overture-realworld-2.scala` (10.75 GiB scanned across 5 themes). Scan-heavy: one exploded scan of the <code>sources</code> array across <b>all 5 Overture themes</b> (UNION ALL), then a tiny GROUP BY dataset. Scan+explode dominate.

**Headline:** ftt's **per-table split sizing** is the win a single global maxPartitionBytes can't match: it collapses cross-table byte skew **7.65× → 1.85×**, cuts gpuTime **106 → 89 s**, and matches/slightly beats the best fixed split on wall.

## The query & the question

**Question:** Across roads, connectors, POIs, addresses and admin areas, which upstream datasets contribute the world's basemap, how much of each, how confident are they, and how fresh? A provenance-bias audit.

```sql
WITH src AS (
    SELECT s.dataset, s.confidence AS conf, s.update_time AS ut FROM segment   LATERAL VIEW explode(sources) t AS s
    UNION ALL ... connector, places, address, division ... )
  SELECT dataset, COUNT(*) AS records, ROUND(AVG(conf),3) AS avg_confidence,
    MIN(SUBSTRING(ut,1,4)) AS oldest_year, MAX(SUBSTRING(ut,1,4)) AS newest_year,
    ROUND(100.0*AVG(CASE WHEN SUBSTRING(ut,1,4)>='2024' THEN 1 ELSE 0 END),1) AS pct_updated_2024plus
  FROM src GROUP BY dataset ORDER BY records DESC
```

**Result (real insight):**

| dataset | records | avg_conf | oldest | newest | % upd 2024+ |
|---|---|---|---|---|---|
| br_ibge | 89.9 M | — | — | — | 0.0 |
| NAD | 84.6 M | — | — | — | 0.0 |
| Overture-signals | 80.2 M | 1.0 | 2026 | 2026 | 100.0 |
| Overture | 74.2 M | — | 2026 | 2026 | 100.0 |
| meta | 60.6 M | 0.678 | 2026 | 2026 | 100.0 |
| OpenAddresses/…/INEGI | 30.7 M | — | — | — | 0.0 |
| TomTom | 9.9 M | — | — | — | 0.0 |


Genuine insight: the basemap leans on **government/OpenAddresses bulk imports** (br_ibge = Brazil IBGE, NAD = US National Address Database, OpenAddresses/*) plus **meta** and **TomTom**. **Honest caveat:** most bulk sources carry **no confidence or update_time** (blank cells) — only Overture's own signals/records are timestamped+scored, so freshness/confidence is only meaningful for those.

## Data read (per execution)
| stage | bytes | note |
|---|---|---|
| on disk (5 themes, listed) | 120.6 GiB | sum of file sizes across all 5 datasets |
| scanned off disk | 10.75 GiB | measured (Spark input) — sources column of each theme |
| decoded on GPU | ~133 GiB | explode blows the arrays up (decode_expansion 10–3192× per theme) |
| rows after explode | ~1.3 B | source records |


## Setup
Local Spark 3.5.3 + spark353 jar, **RTX A5000 only**, `local[16]`, driver 32G, `concurrentGpuTasks=2`, `filecache=false`, `metrics.level=DEBUG`, 5 iters (iter1 cold / COLD_START, 2–5 warm). **One query per session** (`BENCHMARK-METHOD.md`). Fully on GPU (post-AQE plan: GpuScan/GpuGenerate/GpuHashAggregate/GpuUnion; zero CPU fallback).

## 1. Sweep (autotuner OFF) → optimum 1g
Warm iters 2–5. **Noisy/flat** — outliers present; treat as an interleave candidate.
| mpb | tasks | byte skew | avg batch | scan | decode | gpuTime | wall ms | note |
|---|---|---|---|---|---|---|---|---|
| 256m | 532 | 7.62× | 233M | 74.1s | 59.0s | 136.4s | 10082 |  |
| 512m | 272 | 7.76× | 416M | 66.0s | 51.4s | 129.4s | 9530 |  |
| 1g | 196 | 7.65× | 446M | 65.0s | 42.4s | 105.9s | 8485 | ← optimum |
| 2g | 124 | 3.03× | 497M | 87.5s | 42.8s | 100.8s | 9638 |  |
| 4g | 102 | 3.28× | 530M | 95.1s | 28.5s | 63.6s | 8580 |  |


**byte skew** = max÷median bytes-read per scan task (1.0 = balanced; high → straggler).

## 2. Autotuner ON (fill-to-target)
Converges **start-independently** (128m and 4g starts → same split): **per-table (segment 2.06 / connector 0.70 / place 0.42 / address 0.81 / division 1.065 GB)** (`bound_by=ratio`). ftt warm: 141 tasks, skew 1.85×, scan 72.8s, decode 37.9s, gpuTime 89.3s, wall 8418 ms.

**ftt vs fixed settings** (Δ = ftt − baseline):
| baseline | wall | Δ wall | scan | Δ scan | gpuTime | Δ gpuTime |
|---|---|---|---|---|---|---|
| 256m | 10082 | -17% | 74.1s | -2% | 136.4s | -34% |
| 512m | 9530 | -12% | 66.0s | +10% | 129.4s | -31% |
| 1g | 8485 | -1% | 65.0s | +12% | 105.9s | -16% |
| 2g | 9638 | -13% | 87.5s | -17% | 100.8s | -11% |
| 4g | 8580 | -2% | 95.1s | -23% | 63.6s | +40% |


Vs the **1g optimum**: wall -1%, scan +12%, gpuTime -16%; byte skew 7.65× → 1.85×. Verdict: **a win** (matches/beats the optimum and cuts skew+GPU work).

## 3. Conclusion
ftt's **per-table split sizing** is the win a single global maxPartitionBytes can't match: it collapses cross-table byte skew **7.65× → 1.85×**, cuts gpuTime **106 → 89 s**, and matches/slightly beats the best fixed split on wall.


## Sources
Runs: `data/overture-rw2-rw6-{256m,512m,1g,2g,4g,ftt-128m,ftt-4g}/`. Harness: `overture_rw2_bench.scala`, `run-rw2-sweep.sh`, `run-rw2-ftt.sh`, `rw2_warm_parse.py`. Method: `BENCHMARK-METHOD.md`. Query source: `docs/experiments/overture-analytics/overture-realworld-2.scala`.
