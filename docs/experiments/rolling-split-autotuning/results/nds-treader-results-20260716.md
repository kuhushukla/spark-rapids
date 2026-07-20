# Reader-scoped batch size (T knob) — results

**Date:** 2026-07-16 · NDS SF100 · A5000 · 16 cores · run dir `data/nds-treader-20260716_144316/`

## What this run tested

A new reader-scoped knob `spark.rapids.sql.scan.targetDecodedBytesPerTask` (T). When set it drives,
**for the scan/reader only** (joins and aggregates stay on the global 1 GiB batch):
- the reader's **emitted** batch size (`targetSizeBytes`),
- the reader's **read/decode cap** (`maxReadBatchSizeBytes`, raised to ≥ T — the fix that was missing
  before), and
- the autotuner **split** target (`split = T / decodeRatio`), clamped to the default 4 GiB ceiling.

Sweep: T = 1 GiB (anchor), 2 GiB, 4 GiB. All passes completed, **no OOM**.

## Positive control — did the reader batch grow?

| T | max emitted GPU batch / task |
|---|---|
| 1 GiB | 1024 MiB |
| 2 GiB | **1274 MiB** |
| 4 GiB | 1274 MiB |

Raising the read cap with T pushed the batch **past the old 1024 MiB wall** (it was pinned there
before), but it **plateaus at ~1.3 GiB** — well short of 2 GiB. So the read cap was *a* limiter but not
the only one; another cap holds it at ~1.3 GiB (still to be identified — candidates: cudf per-column
limit, row limit, or simply data-limited by `split × ratio`).

## Runtime

| T | total (95 q) | speedup vs OFF |
|---|---|---|
| OFF (128 MiB) | 526.2s | — |
| **T = 1 GiB** | **295.0s** | **1.78×** |
| T = 2 GiB | 307.0s | 1.71× |
| T = 4 GiB | 306.5s | 1.72× |

**Feeding the reader a bigger batch (reader-only) does not help — it is ~4% slower at 2/4 GiB.** This
matches the earlier finding that the scan is footer/filter-bound (~85% of scan time, CPU, per file),
so a bigger GPU batch doesn't move it.

## Files
- Per-query runtime + split: `nds-treader-perquery-20260716.txt`
- Per-table splits: `nds-treader-{t1g,t2g,t4g}-splits-20260716.csv`
- Run: `data/nds-treader-20260716_144316/`

## Open item
Why the emitted batch plateaus at ~1.3 GiB despite the read cap being raised — to be checked against
`maxGpuColumnSizeBytes` (cudf ~2 GiB), `maxReadBatchSizeRows`, and the per-task decoded volume.
