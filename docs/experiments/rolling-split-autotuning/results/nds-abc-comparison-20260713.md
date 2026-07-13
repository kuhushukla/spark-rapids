# NDS Autotuner — A/B/C Comparison (2026-07-13)

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
3. **Raising the ceiling (C) works but pays little here.** Splits doubled, task counts ~halved,
   wall-clock improved only ~1s (~5%). Past ~1 GiB the scan is GPU-decode-bound, not
   task-overhead-bound. The dominant win is 128 MiB → 1 GiB (~1.8×); 1 → 2 GiB is diminishing.
4. **Even C still clamps store_sales** (wants ~3.5 GiB). Chasing it further has flat returns on
   this workload, but would matter for smaller files / higher per-task open cost.

## Direction

Rather than hand-tuning a constant ceiling (1/2/4 GiB), derive it from a **predicted,
memory-safe per-task footprint** — the committed `PerformanceHistory` model
([[project-consider-performancehistory]]).
