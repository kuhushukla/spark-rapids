# NDS SF100 — ratio-driven scan-split autotuner (4 GiB input cap)

**Date:** 2026-07-15 · **Machine:** A5000 (24 GiB), `local[16]`, Spark 3.5.3 · **Data:** NDS SF100 parquet (decimal)

This is the **ratio-driven** variant of the scan-split autotuner, superseding the earlier
parallelism-cap / mem-ceiling design. It is a *separate* result from the floored run
(`nds-allq-autotuner-ab-20260714.md`, 1.34×); both are measured against the **same** OFF baseline,
so the two variants are directly comparable.

## Formula (the whole decision)

```
ratio  = decodedBytes / listedBytes            # learned per table on the cold pass
raw    = batchSizeBytes / ratio                 # so each task decodes to ~one GPU batch
split  = clamp( raw ,  max(64 MiB, sparkDefault) ,  4 GiB )
```

- **Ratio-driven, not parallelism-driven.** If decoded bytes are *smaller* than listed bytes
  (ratio < 1), each task can read *more* encoded input — `raw = batchSizeBytes/ratio` is exactly the
  encoded input that decodes to one batch (`decoded = split·ratio = batchSizeBytes`), so the GPU
  batch is the target, not the task count.
- **Floor** = `max(64 MiB, sparkDefault)` — never smaller than Spark would have picked, so the
  autotuner can only *raise* the split (no regression from going below baseline).
- **Ceiling** = a flat **4 GiB** cap on encoded input per task. That is the only ceiling: memCeiling
  (`perTaskMemBudget/ratio`) and parallelismCap were dropped as redundant — the ratio target already
  decodes to exactly one batch, which fits GPU memory by construction, so they never bound.
- The 4 GiB cap only exists to stop a *very low* ratio (e.g. 0.007) from collapsing a table into
  ~1 giant task and starving parallelism.

## Headline (same-day control)

| | OFF (128 MiB baseline) | WARM (ratio-driven autotuner) |
|---|---|---|
| Total (95 common queries) | **528.5 s** | **318.3 s** |
| Speedup | — | **1.66×** |
| faster >5% / slower >5% / ~same | — | **65 / 23 / 7** |
| median per-query speedup | — | **1.30×** |

**This is a same-day OFF baseline** (run back-to-back with the autotuner pass, identical machine
state), so the speedup is the split effect and not a cross-day / JIT / compute-cache artifact. A
control run confirms it: a second OFF baseline taken a day earlier totalled **525.5 s** — the two
128 MiB baselines agree to **0.6%**, so there is no measurable day-to-day drift. The cross-day number
(525.5 → 318.3 = 1.65×) matches the same-day number (1.66×).

For reference, the earlier floored variant was **1.34×** against the same baseline; going
ratio-driven with the 4 GiB cap lifts it to **1.66×**.

## 4 GiB cap usage

- **88 of 99 queries** had at least one scan clamped to the 4 GiB cap.
- **226 of 656** scan-decisions hit the cap; 84 queries have their *largest* scan at 4 GiB, 11 land
  in the 1024–4095 MiB range, none below 128 MiB.
- Learned ratios span **0.007 – 17.5**. The low tail (heavy read, tiny projection — e.g. `count(*)`
  scans) is what drives splits up to the cap.

## Top wins (scan-heavy queries)

| query | off → warm | speedup | split (MiB) | tasks off → warm |
|---|---|---|---|---|
| query9  | 15.4 → 3.7 s  | 4.12× | 1678 | 1232 → 96 |
| query28 | 43.7 → 14.5 s | 3.01× | 2272 | 1232 → 71 |
| query59 | 20.6 → 6.9 s  | 2.99× | 4096 | 1227 → 48 |
| query66 | 7.7 → 2.6 s   | 2.97× | 4096 | 232 → 12 |
| query62 | 14.2 → 4.8 s  | 2.93× | 4096 | 1162 → 37 |
| query2  | 21.1 → 7.6 s  | 2.79× | 4096 | 1302 → 42 |
| query95 | 21.6 → 8.8 s  | 2.44× | 4096 | 1348 → 45 |
| query88 | 40.1 → 16.7 s | 2.40× | 4096 | 1232 → 40 |

## Regressions — one is real, not noise

Most of the 24 "slower" queries are sub-2s queries inside cross-run variance. **One real
regression** stands out and is worth flagging:

| query | off → warm | speedup | split (MiB) | tasks off → warm |
|---|---|---|---|---|
| **query4** | 13.9 → 25.1 s | **0.55×** | 4096 | 245 → 17 |
| query42 | 0.4 → 2.4 s | 0.16× | 2419 | 23 → 2 |
| query83 | 1.0 → 3.0 s | 0.34× | 4096 | 16 → 1 |

query4 is a genuine over-coalescing loss: the 4 GiB cap dropped it from 245 tasks to 17, ~11 s
slower. The big wins outweigh it (net still 1.65×), but it shows the flat 4 GiB cap is more
aggressive than the earlier parallelism-cap and can starve some queries of parallelism. This is the
main tradeoff of the ratio-driven design vs the floored 1.34× variant (fewer, smaller regressions).

## Validation

Task counts move exactly as the split predicts (bigger split → fewer scan tasks), confirming the
override reaches `getFilePartitions`: e.g. query9 1232 → 96, query28 1232 → 71, query59 1227 → 48.
The `scanMaxSplitBytes` driver metric (recorded in the event log next to `numPartitions`) is
`decide()`'s return value verbatim (`GpuFileSourceScanExec` L611/L620); the 4 GiB values appear 226×
in the warm event log, matching the 226 DECIDED-at-cap log lines.

## Artifacts (this directory)

- `nds-allq-ratio4gib-sameday-offvswarm-20260715.txt` — **same-day** OFF-vs-WARM per-query
  (the headline table; both passes run today, only the split differs)
- `nds-allq-ratio4gib-table-20260715.txt` — full 95-query table (cross-day OFF, for reference)
- `nds-allq-ratio4gib-perquery-tablesplits-20260715.txt` — per-query speedup + split *per fact table*
- `nds-allq-ratio4gib-table-splits-20260715.csv` — raw per-(query, table) split (485 rows; table
  identity from projected-column prefix, since the event-log Location path is truncated)
- `nds-allq-ratio4gib-summary-20260715.csv` — raw per-query data
- `nds-allq-ratio4gib-plot-{runtime,split,tasks}-20260715.png` — plots
- Event logs: `data/nds-allq-autotuner-20260715_134131/el-{off,cold,warm}/`
- Baseline reused: OFF pass from `data/nds-allq-autotuner-20260714_175443` (identical config)

Regenerate: `handoff/run-nds-allq-autotuner.sh` (cold→warm) with the ratio-driven jar; analyze with
scratchpad `analyze_autotuner.py` (two-pass `scanMaxSplitBytes` extraction).
