# NDS Autotuner — Self-Sizing Ceiling + Parallelism Cap (2026-07-13)

> ⛔ **RETRACTED timing conclusions (2026-07-14).** The "split is inert / cold→warm is caching /
> autotuner no-op" claims are wrong — they measured the "load at" schema-inference stages, not the
> "collect at" query scans. Grounded: query9 store_sales scan (stage 31) = cold 1232 → warm 174
> tasks; the autotuner works. The split/decision numbers below (per-table DECIDED values) are still
> valid; the wall-clock attribution is not. See `nds-time-breakdown-20260713.md`.

NDS SF100, `query9,query67,query76`, A5000, `local[16]`, batchSizeBytes=1 GiB, filecache off.
Cold (128 MiB) → warm (autotuned). Extends the A/B/C runs
(`nds-abc-comparison-20260713.md`).

## Design under test
```
target         = batchSizeBytes / ratio
memCeiling     = perTaskMemBudget / ratio       # perTaskMemBudget = getMemorySize / concurrentGpuTasks
parallelismCap = listedBytes / minPartitionNum  # Spark's bytesPerCore, no 128 MiB lid
split          = max(64 MiB, min(target, memCeiling, parallelismCap))
```

## Self-sizing ceiling alone (no parallelism cap)
Splits reached the fill-batch target: store_sales **3.3 GiB**, web_sales **18.7 GiB**,
catalog_sales 4.6 GiB. No OOM. But query76 ran ~1s slower — heavily-projected tables
(web_sales ratio 0.053) collapsed into 1–2 tasks, starving scan parallelism. mem_ceiling was
huge (web_sales 227 GiB) but never bound — the *target* overshot the table size.

## + Parallelism cap
Every big table now caps at `listedBytes/16` → ~16 tasks; tiny tables hit the 64 MiB floor:

| Table | listed | split | bound |
|---|---|---|---|
| store_sales | 15.5 GiB | 924 MiB | parallelismCap |
| catalog_sales | 10.8 GiB | 646 MiB | parallelismCap |
| web_sales | 5.8 GiB | 348 MiB | parallelismCap (was 18.7 GiB) |
| item/store/reason/date_dim | tiny | 64 MiB | floor |

The 18.7 GiB web_sales runaway is gone; splits land at the ~1 GiB-for-big-tables that was
already near-optimal — *derived* from parallelism, not hand-picked.

## Timing (warm Power Test)
| Config | cold | warm |
|---|---|---|
| 1 GiB clamp | 38s | 21s |
| 2 GiB clamp | 35s | 20s |
| self-sizing | 36s | 21s |
| parallelism cap | 43s | 25s |

**Inconclusive by design-effect:** the parallelism-cap run's *cold* was also elevated (43s vs
~36–38s), and cold uses the fixed 128 MiB COLD_START split (no autotuner effect) — so the whole
machine was slower that run (warm/cold ratio held ~0.58 across all configs). The 4s delta is
machine variance, not the cap.

## Summary-level takeaways
1. **CORRECTION (see `nds-time-breakdown-20260713.md`): the split is INERT here** — cold (128 MiB)
   and warm (924 MiB) produce identical scan task counts (store_sales = 1824 = its date-partition
   count). The cold→warm ~1.7× is page-cache/JIT warming, NOT the autotuner. The "128 MiB →
   autotuned ≈ 1.8×" claim does not hold for this dataset.
2. All four autotuned variants land at **20–25s warm — within ~20% run-to-run noise.** Split size
   past ~1 GiB doesn't move wall-clock — but **not** because the GPU is saturated: measured GPU
   util during the warm queries averaged **38%** (median 29%, <50% for 70% of samples; see
   `nds-gpu-util-20260713.md`). Neither GPU nor IO is the bottleneck; the limiter is the non-GPU
   path (CPU-side coalescing of thousands of tiny files per task, plus shuffle/aggregate).
3. **Parallelism cap's value is structural, not speed:** it prevents split runaway/scan
   serialization (would matter more where a collapse actually serializes a large scan). Reuses
   Spark's own `minPartitionNum` — no new knob.
4. To compare the autotuned variants cleanly would need back-to-back trials / repeats to beat the
   noise floor; not done here.
