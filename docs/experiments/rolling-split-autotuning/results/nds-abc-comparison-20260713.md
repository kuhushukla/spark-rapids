# NDS Autotuner — A/B/C Comparison (2026-07-13)

> ⛔ **RETRACTED conclusions (2026-07-14).** This doc's "split barely matters / cold→warm is
> caching" reasoning measured the wrong stages. Grounded fact: query9 store_sales scan (stage 31,
> sqlExec 24) went **cold 1232 → warm 174 tasks** — the autotuner split works. The wall-clock
> "within noise" comparison here was never a clean split A/B (all runs used
> `spark.sql.files.maxPartitionBytes=128m`; only the autotuner override varied, and its effect on
> the scans wasn't isolated). Re-run with the `scanMaxSplitBytes` metric before trusting any timing
> conclusion. See `nds-time-breakdown-20260713.md` (retraction banner).

NDS SF100, queries `query9,query67,query76`, A5000, `local[16]`, batchSizeBytes=1 GiB,
filecache off. Each run is cold (128 MiB) → warm (autotuned). All on the same rebuilt JAR.

## The three configs

| Run | Config | listedBytes | Split ceiling |
|---|---|---|---|
| **A** | baseline | full file (all columns) | 1 GiB (= batchSizeBytes) |
| **B** | projected change | scaled to projected columns | 1 GiB |
| **C** | raised ceiling | full file (all columns) | **2 GiB** |

Formula (all runs): `split = batchSizeBytes / ratio`, clamped to `[64 MiB, ceiling]`,
where `ratio = decodedBytes / listedBytes`.

## Warm splits (store_sales, ratio_full ≈ 0.30)

| Run | store_sales split | Effect |
|---|---|---|
| A | 1024 MiB (clamped) | tasks decode ~0.30 GiB — under-fill the 1 GiB batch |
| B | 379–588 MiB | ratio_proj > 1 → split shrinks → **more tasks** |
| C | 2048 MiB (clamped higher) | bigger split, fewer tasks |

Wanted split to fill a 1 GiB batch: `1 GiB / 0.30 ≈ 3.5 GiB` — still clamped even at C.

## Warm runtimes

| Query | A (1 GiB) | B (projected) | C (2 GiB) |
|---|---|---|---|
| query9 | 5.96s | 7.89s | **5.46s** |
| query67 | 8.25s | 8.48s | **7.62s** |
| query76 | 6.89s | 8.30s | 6.92s |
| **Power Test** | 21.0s | 24.0s | **20.0s** |

## Findings

1. **B (projected listedBytes) is wrong and slower.** The split bins *full-file* bytes, so the
   ratio must be `decoded / full-listed`. Projecting the denominator makes the ratio > 1 →
   smaller splits → more tasks → 24s. Reverted.
2. **The split is measured in full-file bytes**, so heavy projection (small ratio_full) correctly
   wants a *larger* split. A/C do this; the clamp is what limits it.
3. **Raising the ceiling (C) works but pays nothing here.** CORRECTIONS from follow-up analysis:
   (a) GPU-util sampling (`nds-gpu-util-20260713.md`) shows the GPU is only ~38% utilized during
   warm queries — NOT GPU-decode-bound as originally inferred. (b) `nds-time-breakdown-20260713.md`
   shows the split is **INERT** — cold and warm scan task counts are identical (store_sales = 1824 =
   its date-partition count; the split can't coalesce across date-partition dirs). So the "128 MiB →
   1 GiB ~1.8×" is page-cache/JIT warming, not the split. The limiter is per-task overhead from
   one-task-per-date-partition, plus aggregate/shuffle compute.
4. **Even C still clamps store_sales** (wants ~3.5 GiB). Chasing it further has flat returns on
   this workload, but would matter for smaller files / higher per-task open cost.

## Direction

Rather than hand-tuning a constant ceiling (1/2/4 GiB), derive it from a **predicted,
memory-safe per-task footprint** — the committed `PerformanceHistory` model
([[project-consider-performancehistory]]).
