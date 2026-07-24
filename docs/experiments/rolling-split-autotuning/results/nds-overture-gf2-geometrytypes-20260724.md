# Geometry types by theme (WKB header, no coordinate decode) — maxPartitionBytes sweep + fill-to-target (local, Spark 3.5.3) — 2026-07-23

Query **GF2** from `overture-realworld-2.scala` (48.1 GiB scanned (geometry column, 5 themes)). Scan-heavy: one scan of the <code>geometry</code> (WKB binary) column across <b>all 5 themes</b> (~48 GiB) + a tiny GROUP BY theme,header. All geometry ops (<code>substring</code>/<code>hex</code> on binary) run on GPU (0 CPU fallbacks, verified post-AQE).

**Headline:** ftt sizes each of the 5 themes' geometry scan **separately** (1.3–6.0 GB, per its decoded/listed ratio) → 96 balanced tasks that **cut gpuTime 26% (126→94 s) and decode 23% (43→33 s)** vs the 512m optimum, at **+5.7% wall**. The real win is **skew control**: per-theme sizing holds byte skew to **1.50×**, while a naive global 4g split (the other way to get low gpuTime) explodes skew to **13.3×** — one 2.4 GB / 18.9 s straggler task — and wall to **+49%**. On a big geometry scan, ftt is the safe way to harvest the GPU savings.

## The query & the question

**Question:** What geometry TYPES make up each Overture theme? Read straight from the 5-byte WKB header (byte-order + type code) WITHOUT decoding any coordinates — the shape vocabulary of the basemap.

```sql
WITH g AS (
    SELECT 'segment' AS theme, hex(substring(geometry,1,5)) AS hdr FROM segment
    UNION ALL SELECT 'connector', hex(substring(geometry,1,5)) FROM connector
    UNION ALL ... address, place, division ... )
  SELECT theme, hdr AS wkb_header,
    CASE hdr WHEN '0101000000' THEN 'Point' WHEN '0102000000' THEN 'LineString' ... END AS geometry_type,
    COUNT(*) AS features
  FROM g GROUP BY theme, hdr ORDER BY theme, features DESC
```

**Result (real insight):**

| theme | wkb_header | geometry_type | features |
|---|---|---|---|
| segment | 0102000000 | LineString | 348.7 M |
| address | 0101000000 | Point | 472.7 M |
| connector | 0101000000 | Point | 416.8 M |
| place | 0101000000 | Point | 74.2 M |
| division | 0101000000 | Point | 4.66 M |


Clean result: the basemap's geometry vocabulary is just **two shapes** — **roads (segment) are LineStrings**, everything else (addresses, connectors, POIs, division points) is a **Point**. No non-standard/EWKB headers appeared (all standard OGC little-endian WKB). **Honest note:** `division` here is the type=division <i>reference point</i>, not the boundary polygon (those live in a separate division_area theme).

## Data read (per execution)
| stage | bytes | note |
|---|---|---|
| on disk (5 themes, listed) | 120.6 GiB | sum of all 5 datasets |
| scanned off disk | 48.1 GiB | measured (Spark input) — the geometry (WKB) column across 5 themes |
| decoded on GPU | ~77 GiB | decode_expansion 1.4–2.1× per theme (WKB binary is already compact) |
| rows | 1.32 B | features across 5 themes |


## Setup
Local Spark 3.5.3 + spark353 jar, **RTX A5000 only**, `local[16]`, driver 32G, `concurrentGpuTasks=2`, `filecache=false`, `metrics.level=DEBUG`, 5 iters (iter1 cold / COLD_START, 2–5 warm). **One query per session** (`BENCHMARK-METHOD.md`). Fully on GPU (post-AQE plan: GpuScan/GpuGenerate/GpuHashAggregate/GpuUnion; zero CPU fallback).

## 1. Sweep (autotuner OFF) → optimum 512m
Warm iters 2–5. | mpb | tasks | byte skew | avg batch | scan | decode | gpuTime | wall ms | note |
|---|---|---|---|---|---|---|---|---|
| 256m | 532 | 2.18× | 141M | 218.8s | 53.6s | 140.2s | 19633 |  |
| 512m | 272 | 2.00× | 275M | 203.5s | 42.8s | 126.5s | 18352 | ← optimum |
| 1g | 196 | 2.81× | 380M | 223.4s | 37.9s | 106.1s | 18585 |  |
| 2g | 124 | 6.40× | 411M | 312.5s | 19.4s | 60.7s | 22304 |  |
| 4g | 102 | 13.33× | 396M | 400.8s | 10.8s | 50.6s | 27279 |  |


**byte skew** = max÷median bytes-read per scan task (1.0 = balanced; high → straggler).

## 2. Autotuner ON (fill-to-target)
Converges **start-independently** (128m and 4g starts → same split): **per-theme (segment 1.32 / connector 2.34 / address 1.84 / place 6.03 / division 4.93 GB)** (`bound_by=ratio`). ftt warm: 96 tasks, skew 1.50×, scan 238.9s, decode 33.0s, gpuTime 93.7s, wall 19392 ms.

**ftt vs fixed settings** (Δ = ftt − baseline):
| baseline | wall | Δ wall | scan | Δ scan | gpuTime | Δ gpuTime |
|---|---|---|---|---|---|---|
| 256m | 19633 | -1% | 218.8s | +9% | 140.2s | -33% |
| 512m | 18352 | +6% | 203.5s | +17% | 126.5s | -26% |
| 1g | 18585 | +4% | 223.4s | +7% | 106.1s | -12% |
| 2g | 22304 | -13% | 312.5s | -24% | 60.7s | +54% |
| 4g | 27279 | -29% | 400.8s | -40% | 50.6s | +85% |


Vs the **512m optimum**: wall +6%, scan +17%, gpuTime -26%; byte skew 2.00× → 1.50×. Verdict: **gpu-lean, +6% wall** — cuts GPU work (-26% gpuTime) at a small wall cost (parallelism-bound).

## 3. Conclusion
ftt sizes each of the 5 themes' geometry scan **separately** (1.3–6.0 GB, per its decoded/listed ratio) → 96 balanced tasks that **cut gpuTime 26% (126→94 s) and decode 23% (43→33 s)** vs the 512m optimum, at **+5.7% wall**. The real win is **skew control**: per-theme sizing holds byte skew to **1.50×**, while a naive global 4g split (the other way to get low gpuTime) explodes skew to **13.3×** — one 2.4 GB / 18.9 s straggler task — and wall to **+49%**. On a big geometry scan, ftt is the safe way to harvest the GPU savings.


## Sources
Runs: `data/overture-rw2-gf2-{256m,512m,1g,2g,4g,ftt-128m,ftt-4g}/`. Harness: `overture_rw2_bench.scala`, `run-rw2-sweep.sh`, `run-rw2-ftt.sh`, `rw2_warm_parse.py`. Method: `BENCHMARK-METHOD.md`. Query source: `docs/experiments/overture-analytics/overture-realworld-2.scala`.
