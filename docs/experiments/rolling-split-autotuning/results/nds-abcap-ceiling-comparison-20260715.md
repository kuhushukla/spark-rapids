# NDS SF100 — scan-split ceiling A/B (4 GiB vs parallelism cap) + cap sweep

**Date:** 2026-07-15 · **Machine:** A5000 (24 GiB), `local[16]`, Spark 3.5.3 · **Data:** NDS SF100 parquet (decimal)

## One-paragraph summary

With the scan-split autotuner learning per-table GPU decode ratios and overriding `maxSplitBytes`, we
A/B'd two ceilings on the chosen split — a flat **4 GiB** of encoded input vs a **parallelism cap**
(`listedBytes/minPartitionNum`, ≈ one wave of `#cores` tasks) — and swept the flat cap **4→6→8 GiB**,
all same-session against a same-day OFF (128 MiB) baseline. **What we learned:** the 4 GiB
batch-size-factor cap wins (**1.80×** over OFF) and the parallelism cap loses (**1.33×**), because the
parallelism cap collapses the smaller fact tables (returns / web) back to ~baseline splits and forfeits
their win; raising the flat cap above 4 GiB is a **plateau** — splits demonstrably grow but runtime is
flat and nothing OOMs, so 4 GiB already sits past the performance knee. Mechanically the split works by
**inverse-scaling the scan task count** (median **23×** fewer tasks, e.g. query28 **1232→71**), because
`getFilePartitions` bin-packs by **full file length (all columns)**, not the projected bytes a query
reads; and task count is *not* the runtime constraint here — the GPU, fed through a dynamic
memory-based semaphore, saturates on a handful of big batches, which is why halving the tasks again
(8 GiB) changes nothing.

## Ceiling A/B: 4 GiB vs parallelism cap (same session, vs same-day OFF)

| config | total (95 q) | speedup | faster / slower / ~same | median |
|---|---|---|---|---|
| OFF (128 MiB) | 528.5 s | — | — | — |
| **4 GiB cap** | **293.2 s** | **1.80×** | 70 / 19 / 6 | 1.37× |
| parallelism cap | 397.5 s | 1.33× | 51 / 21 / 23 | 1.07× |

Positive control: 4 GiB pass ceiling always `4294967296`; parcap pass = 155 distinct
`listedBytes/minPartitionNum` values (each equal to the logged `parallelism_cap`).

**Why parcap loses** — it ties the ceiling to a table's on-disk size ÷ 16 cores. For store_sales
(big) that's ~923 MiB, still a win; for the smaller fact tables it's ≈ the 128 MiB baseline, so the
win is forfeited. Examples (`4GiB→parcap` split, MiB): query66 `web_sales 2534→128` (3.28×→1.00×),
query85 `web_sales 4096→128` (2.21×→1.01×), query75 all `4096→~128` (1.95×→1.02×).

Plots: `nds-abcap-plot-runtime-20260715.png` (per-query runtime OFF / 4 GiB / parcap),
`nds-abcap-plot-splits-20260715.png` (per-query max fact split 4 GiB vs parcap).
Full per-query tables: `nds-abcap-ceiling-comparison-20260715.txt` (fixed-width, per-table splits) and
`nds-abcap-compact-20260715.txt`.

## Cap sweep: 4 → 6 → 8 GiB (plateau)

| ceiling | total | speedup | splits |
|---|---|---|---|
| OFF | 528.5 s | 1.00× | — |
| 4 GiB | 292.9 s | 1.80× | max split 4096 MiB |
| 6 GiB | 294.6 s | 1.79× | 189 tables > 4 GiB, max 6144 |
| 8 GiB | 292.8 s | 1.81× | 190 tables > 4 GiB, max 8192 |

Splits genuinely grew (204 tables coalesced further; e.g. query5 store_sales 2664→8192), but runtime is
flat within ~0.6% noise, **no OOM / no failures** at 6 or 8 GiB (the lone log "failure" was a benign
Spark startup heartbeat NPE, pre-query). Detail: `nds-capsweep-trend-20260715.md`.

## Task-count mechanism (split → tasks)

The split controls parallelism by inverse-scaling scan task count. Measured on query28 store_sales
(same 14.8 GiB of files both runs):

| | split | scan tasks |
|---|---|---|
| OFF | 128 MiB | 1232 |
| 4 GiB | 2272 MiB | 71 |

`2272/128 = 17.8×` bigger split → `1232/71 = 17.4×` fewer tasks. Across all 95 queries the median
OFF→4 GiB scan-task reduction is **23×**. Plot: `nds-abcap-plot-tasks-20260715.png`; data:
`nds-abcap-taskcounts-20260715.csv`.

Task count is `fullFileBytes / split`, where `fullFileBytes` is `f.getLen` — the whole file on disk,
all columns — because `getFilePartitions` bin-packs on full file length
(`GpuFileSourceScanExec.scala:586-605`), not on the projected bytes a query reads. One task spans
2272 MiB of files but reads only the projected columns (~81 MiB for query28), so per-task read bytes
are much smaller than the split.

**Cores:** 67 of 95 queries have their largest 4 GiB scan stage below 16 tasks, but those are the small
queries (little data → few full files); the time-dominant scans stay well above (query28 = 71, i.e.
~4–5 waves on 16 cores). The cap sweep independently shows task count is not the runtime constraint —
halving tasks again (8 GiB) does not change runtime — so the big scans are GPU-bound, not core-starved.

## What else could be meaningful (not yet run)

- **GPU-underloaded time:** the scan node exposes `I/O schedule time (GPU underloaded)` and
  `GPU decode time` metrics — summing these per query would directly show whether the GPU is the
  bottleneck (the current claim rests on the cap-sweep plateau, which is indirect).
- **Learned-ratio distribution per table** — which tables drive big splits and why (store_sales low
  ratio → big split; returns tables vary).
- **Task-count vs runtime scatter** — would confirm the win is GPU batch efficiency, not fewer tasks
  per se (the cap sweep already implies this).
- **Decouple the autotuner target** from the global `batchSizeBytes` to a GPU-memory-derived value —
  a code change, separate experiment.

## Artifacts
- Runs: `data/nds-abcap-20260715_151451/` (A/B), `data/nds-capsweep-20260715_183357/` (sweep),
  `data/nds-allq-off-day2-20260715_144317/` (OFF baseline).
- Ceiling switch: `-Drapids.autotuner.ceiling={parcap, none, <N>g}` (default 4 GiB).
- Plots: `nds-abcap-plot-{runtime,splits,tasks}-20260715.png`.
- Tables/CSVs: `nds-abcap-{compact,ceiling-comparison}-20260715.txt`,
  `nds-abcap-{batch4g,parcap}-splits-20260715.csv`, `nds-abcap-taskcounts-20260715.csv`.
