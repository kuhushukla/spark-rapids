# Scan Split Autotuner — Self-Sizing Ceiling (design)

Grounded in code (`ScanSplitAutotuner.scala`, `GpuFileSourceScanExec.scala`, `GpuSemaphore.scala`,
`GpuDeviceManager.scala`) and the NDS SF100 A/B/C runs.

## Objective
Each scan task should decode ~`batchSizeBytes` (one GPU batch). The split bins files by **full
on-disk length**, so a task decodes `split × ratio` where `ratio = decodedBytes / listed_full`.
Therefore:

```
target         = batchSizeBytes / ratio        # split that fills one batch
memCeiling     = perTaskMemBudget / ratio       # split whose decode fits the per-task GPU budget
parallelismCap = listedBytes / minPartitionNum  # keep ~minPartitionNum tasks (Spark's bytesPerCore)
split          = max(64 MiB, min(target, memCeiling, parallelismCap))
```

`target` wins normally; `memCeiling` only binds when filling a batch would exceed memory;
`parallelismCap` only binds when the target would overshoot a small/projected table and collapse
its scan into too few tasks.

## Why self-sizing (not a constant)
`perTaskMemBudget = GpuDeviceManager.getMemorySize / concurrentGpuTasks` — the same budget
`GpuSemaphore.computeDefaultMemory` uses. `getMemorySize` is the RMM pool
(`rmmAllocFraction × (free − reserve)`), so it tracks the actual GPU: A5000 → high ceiling
(store_sales reaches its ~3.5 GiB target); T400 → low ceiling automatically. No per-GPU tuning.

## No regression / no prediction
On fixed data (NDS) the ratio is deterministic per (table, projection) — store_sales query9
reproduced 0.30127418 vs 0.30127362 across runs. Nothing to regress; `latestFor` memoization is
exact. Percentiles / evidence-tiers / footprint prediction (`PerformanceHistory`) only earn their
keep when data changes (the taxi transfer case) — deliberately not used here. Footprint stays
**measured** (`GpuTaskMetrics.maxGpuFootprint`), never predicted.

## Known limitations
- The split is decided **driver-side at plan time**; `getMemorySize`/`concurrentGpuTasks` are
  executor/runtime facts. Accurate in **local mode** (driver == executor). In **cluster mode** the
  driver may have no GPU → `getMemorySize == 0` → decide() falls back to a `batchSizeBytes` ceiling.
  Concurrency is the config/static estimate, not the live permit-based value.
- No safety haircut on the ceiling: the RMM pool already reserves (`rmmAllocReserve` + fraction)
  and admission uses per-task permits. Add one flat `SAFETY_FRACTION` only if a real run OOMs at
  the ceiling — not speculatively.

## Empirical backing (NDS SF100, query9/67/76, A5000)
**The autotuner controls scan parallelism (grounded 2026-07-14).** query9 store_sales scan
(stage 31, sqlExec 24) went **cold 1232 → warm 174 tasks** — the ~924 MiB autotuner split cut it
~7× (1232 = 128 MiB count; 174 ≈ 924 MiB count; plugin probe: 128m→1232, 1024m→157, 4096m→40).
The design here is therefore live on this dataset. NOTE: an earlier version of this doc wrongly
called the split "inert" (it measured schema-inference "load" stages, not the query scans) — that
is retracted. Whether the resulting wall-clock change is net positive (fewer tasks) or offset by
larger per-task work still needs a clean A/B using the new `scanMaxSplitBytes` scan metric.

## Implementation
- `decide(label, listedBytes, sparkDefault, batchSizeBytes, perTaskMemBudget, minPartitionNum)` —
  computes target, memCeiling, parallelismCap, 64 MiB floor. Logs `mem_ceiling` + `parallelism_cap`.
- `createNonBucketedReadRDD` computes `perTaskMemBudget` from `getMemorySize` / `concurrentGpuTasks`
  and `minPartitionNum` from Spark's `filesMinPartitionNum`/`leafNodeDefaultParallelism`.
- No extra config: ceiling self-sizes from GPU memory, parallelism cap reuses Spark's own
  `minPartitionNum`. (No static knobs — they would duplicate computed values.)
