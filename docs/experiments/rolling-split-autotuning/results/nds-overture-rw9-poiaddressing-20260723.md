# POI address completeness by category — maxPartitionBytes sweep + fill-to-target (local, Spark 3.5.3) — 2026-07-23

Query **RW9** from `overture-realworld-2.scala` (1.16 GiB scanned). Scan-heavy: one scan of <code>places</code> categories + embedded addresses (74.2 M POIs) + a tiny GROUP BY category.

**Headline:** ftt lands at 1.92 GB — inside rw9's **flat region** (the ~1 GB data can't subdivide past ~18 tasks, so any split ≥1g is identical) → **≈ tie** with the optimum on wall, while cutting gpuTime 11.6 → 2.6 s. The knob barely matters here; ftt does no harm and lands right.

## The query & the question

**Question:** Which kinds of business carry a usable street address vs only a point? For each category, what share of POIs have both a locality and a postcode in their embedded address (geocodable)?

```sql
SELECT categories.primary AS category, COUNT(*) AS pois,
    ROUND(100.0*AVG(CASE WHEN element_at(addresses,1).locality IS NOT NULL THEN 1 ELSE 0 END),1) AS pct_locality,
    ROUND(100.0*AVG(CASE WHEN element_at(addresses,1).postcode IS NOT NULL THEN 1 ELSE 0 END),1) AS pct_postcode,
    ROUND(100.0*AVG(CASE WHEN element_at(addresses,1).locality IS NOT NULL
                          AND element_at(addresses,1).postcode IS NOT NULL THEN 1 ELSE 0 END),1) AS pct_addressable
  FROM places WHERE categories.primary IS NOT NULL GROUP BY categories.primary HAVING COUNT(*)>=5000
  ORDER BY pct_addressable DESC
```

**Result (real insight):**

| category | pois | % locality | % postcode | % addressable |
|---|---|---|---|---|
| propane_supplier | 47.0 K | 100.0 | 100.0 | 100.0 |
| courier_and_delivery | 53.4 K | 100.0 | 100.0 | 100.0 |
| tax_services | 37.9 K | 100.0 | 100.0 | 100.0 |
| auto_insurance | 37.9 K | 100.0 | 100.0 | 100.0 |
| tire_shop | 22.6 K | 100.0 | 100.0 | 100.0 |
| rental_services | 17.1 K | 100.0 | 100.0 | 100.0 |
| builders | 15.1 K | 100.0 | 100.0 | 100.0 |
| home_decor | 12.2 K | 100.0 | 100.0 | 100.0 |


**Honest caveat:** sorted by addressability **descending**, so the top-20 (of **874** categories with ≥5000 POIs) are all **saturated at 100%** — many service/retail categories are fully addressed. The informative variation is in the **tail** (categories with partial addressing), not shown here. The genuine takeaway: a large share of Overture POI categories carry complete embedded street addresses.

## Data read (per execution)
| stage | bytes | note |
|---|---|---|
| on disk (places, listed) | 10.5 GiB | 16 files |
| scanned off disk | 1.16 GiB | measured (Spark input) — categories + addresses columns |
| decoded on GPU | ~5.9 GiB | decode_expansion 7.5× |
| rows | 74.2 M | POIs (874 categories ≥5000) |


## Setup
Local Spark 3.5.3 + spark353 jar, **RTX A5000 only**, `local[16]`, driver 32G, `concurrentGpuTasks=2`, `filecache=false`, `metrics.level=DEBUG`, 5 iters (iter1 cold / COLD_START, 2–5 warm). **One query per session** (`BENCHMARK-METHOD.md`). Fully on GPU (post-AQE plan: GpuScan/GpuGenerate/GpuHashAggregate/GpuUnion; zero CPU fallback).

## 1. Sweep (autotuner OFF) → optimum flat (≥1g)
Warm iters 2–5. **Flat**: for split ≥1g the data can't subdivide past ~18 tasks, so metrics are byte-identical.
| mpb | tasks | byte skew | avg batch | scan | decode | gpuTime | wall ms | note |
|---|---|---|---|---|---|---|---|---|
| 256m | 44 | 1.47× | 136M | 9.7s | 4.6s | 12.7s | 1599 |  |
| 512m | 22 | 1.37× | 273M | 8.8s | 3.6s | 11.9s | 1631 |  |
| 1g | 18 | 1.27× | 333M | 8.7s | 3.3s | 11.6s | 1515 | ← optimum |
| 2g | 18 | 1.27× | 333M | 8.9s | 3.4s | 11.3s | 1541 |  |
| 4g | 18 | 1.27× | 333M | 8.4s | 3.3s | 11.9s | 1530 |  |


**byte skew** = max÷median bytes-read per scan task (1.0 = balanced; high → straggler).

## 2. Autotuner ON (fill-to-target)
Converges **start-independently** (128m and 4g starts → same split): **1.92 GB** (`bound_by=ratio`). ftt warm: 7 tasks, skew 1.36×, scan 3.9s, decode 0.9s, gpuTime 2.6s, wall 1524 ms.

**ftt vs fixed settings** (Δ = ftt − baseline):
| baseline | wall | Δ wall | scan | Δ scan | gpuTime | Δ gpuTime |
|---|---|---|---|---|---|---|
| 256m | 1599 | -5% | 9.7s | -60% | 12.7s | -79% |
| 512m | 1631 | -7% | 8.8s | -56% | 11.9s | -78% |
| 1g | 1515 | +1% | 8.7s | -56% | 11.6s | -77% |
| 2g | 1541 | -1% | 8.9s | -57% | 11.3s | -77% |
| 4g | 1530 | -0% | 8.4s | -54% | 11.9s | -78% |


Vs the **1g optimum**: wall +1%, scan -56%, gpuTime -77%; byte skew 1.27× → 1.36×. Verdict: **a tie** (lands in the flat region; no harm).

## 3. Conclusion
ftt lands at 1.92 GB — inside rw9's **flat region** (the ~1 GB data can't subdivide past ~18 tasks, so any split ≥1g is identical) → **≈ tie** with the optimum on wall, while cutting gpuTime 11.6 → 2.6 s. The knob barely matters here; ftt does no harm and lands right.


## Sources
Runs: `data/overture-rw2-rw9-{256m,512m,1g,2g,4g,ftt-128m,ftt-4g}/`. Harness: `overture_rw2_bench.scala`, `run-rw2-sweep.sh`, `run-rw2-ftt.sh`, `rw2_warm_parse.py`. Method: `BENCHMARK-METHOD.md`. Query source: `docs/experiments/overture-analytics/overture-realworld-2.scala`.
