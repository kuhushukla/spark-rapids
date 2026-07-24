# Overture scan-heavy query — fill-to-target vs tuned baseline (local, Spark 3.5.3) — 2026-07-23

Does the projection-aware **fill-to-target** split (`ftt` = `ratioBasis=listed`,
`maxSplitBytes = batchSize / (decodedBytes/listedBytes)`) beat a tuned `maxPartitionBytes` on a **single-scan,
wide-nested-column** query, and can it **self-tune** to a near-optimal split without a sweep?

**Answer — the goal is met:** fill-to-target **self-tunes to near-optimal automatically**. From *any* starting
`maxPartitionBytes` it converges to ~741 MB and lands within **~2% of the hand-tuned optimum** (inside the
iteration noise) — and **1.27–1.39× faster than a mistuned baseline**, which is the realistic case since finding
the optimum otherwise needs the per-dataset sweep that ftt eliminates. It does not *beat* an already-perfectly-tuned
`maxPartitionBytes` because its ~741 MB split lands in the same batch-size **plateau** (see below) — i.e. there is
no fullness left to gain, so ftt correctly stops there rather than mis-sizing.

## Setup
- Local Spark **3.5.3** + `dist/target/…cuda12.jar` (spark353, ratioBasis code), **RTX A5000 only**
  (`CUDA_VISIBLE_DEVICES=GPU-1aaa66fd…`, never the T400). `local[16]`, driver 32G, `concurrentGpuTasks=2`,
  `pinnedPool=8g`, `filecache=false`, `metrics.level=DEBUG`. 5 iterations (iter1 cold, 2–5 warm mean).
- Data: `overture_2026-07-22/transportation/type=segment`.
- Harness: `handoff/overture_scanheavy.scala` + `run-overture-{mpb-sweep,ftt,ftt-mpbcheck}.sh`. Parser: `handoff/mpb_parse.py`.

## The query & the question it poses
**Question:** *profile the global Overture transportation-segment network* — across all 348.7 M segments: how big
is it, how connected (connector refs), how much regulatory metadata (access restrictions, speed limits) does it
carry, how many roads are named, how much provenance exists, and how large is a typical segment geographically.

```sql
SELECT
  COUNT(*)                        AS segments,             -- how many road/path segments
  SUM(size(connectors))           AS total_connector_refs, -- total junction/connection points in the network
  AVG(size(access_restrictions))  AS avg_access_restr,     -- avg regulatory restrictions per segment
  AVG(size(speed_limits))         AS avg_speed_limits,     -- avg speed-limit records per segment
  COUNT(names.primary)            AS named_segments,       -- segments with a primary name
  SUM(size(sources))              AS total_sources,        -- total provenance/source records
  AVG(bbox.xmax - bbox.xmin)      AS avg_bbox_width_deg     -- avg geographic span of a segment (degrees)
FROM segment
```

It is a **scan-heavy query**: a single Parquet scan, **no join and no GROUP BY**, over deeply nested array/struct
columns (5× decode expansion). Essentially all the work is the scan + decode (only a trivial 1-row final
aggregate) — so it is ~100% scan-dominated, exactly the case the split lever is meant to affect.

### Query result (measured, across all 348.7 M segments) — with honest caveats
| profile | result | reading |
|---|---|---|
| segments | 348,672,901 | 348.7 M road/path segments (cross-checks the autotuner's `decoded_rows`) |
| connectors | 897,200,683 | ~**2.57 per segment** — well connected |
| named | 94,113,420 | **27%** of segments are named (73% unnamed) |
| sources | 407,039,736 | ~**1.17 provenance records** per segment |
| avg access restrictions | **−0.68** | **artifact** — `size()` returns −1 for NULL; a negative average means most segments have *none* |
| avg speed limits | **−0.84** | **artifact** — same; most segments carry *no* speed-limit metadata |
| avg bbox width | **0.00183°** | bounding-box longitude span (~200 m), **not** a true segment length |

**Honest read:** the valid outputs (count, connectivity, 27% named, ~1.2 sources/segment) profile the network
*shape*; but **3 of the 7 aggregates are artifacts** — the two "avg regulatory" numbers are negative from
`size(NULL) = −1`, and bbox-width-in-degrees isn't a real length. It's a fine **benchmark** probe (heavy scan +
decode, which is all the split study needs) but a **weak analytical query**. A null-safe, `GROUP BY class` rewrite
that answers a genuine coverage question is in `handoff/overture-realworld.scala`.

## Data read (from the autotuner `RECORDED`, per query execution)
| stage | bytes | note |
|---|---|---|
| on disk (listed, `getLen`) | **66.3 GiB** | 128 files, 332–761 MB |
| read off disk (`input_bytes`) | **18.2 GiB** | read_selectivity **0.275** (6 nested columns projected + row-group pruning) |
| decoded on GPU (`decoded_bytes`) | **91.6 GiB** | decode_expansion **5.03×** (array/struct decode) |
| rows | **348.7 M** | segment rows |

## Sanity checks (GPU coverage)
Executed plan (event-log `sparkPlanInfo`) — **scan pipeline is fully GPU**:
`GpuScan parquet → GpuProject → GpuHashAggregate (partial) → GpuColumnarExchange → GpuShuffleCoalesce`. The
`GpuScan` does the real work (348.7 M rows, 91.6 GiB decoded, GPU-decode-time metric present). The **only CPU work**
is the final `GpuColumnarToRow → HashAggregate` global reduction (~127 partial-agg rows → 1 result row), trivial and
not scan-related. The CPU `Scan parquet` node is the `CreateViewCommand` schema read (metadata). No OOM.

## 1. Baseline maxPartitionBytes sweep (autotuner OFF) → optimal = 1g
| maxPartitionBytes | tasks | warm mean (ms) |
|---|---|---|
| 128m | 550 | 9165 |
| 256m | 286 | 7888 |
| 512m | 143 | 6844 |
| **1g** | **99** | **6558**¹ ← optimal |
| 2g | 40 | 7405 |
| 4g | 18 | 8552 |

Clean U-shape on `local[16]`: below 1g too many small tasks, above 1g under-parallelization. **Optimal = `1g`.**
¹Same-window re-run; the sweep's own 1g iter was 6644, within noise.

## 2. Self-tuning — ftt converges to ~741 MB from any start, landing near-optimal
ftt sizes the split from the learned ratio (`decoded/listed = 1.38`, from decode_expansion 5.03×) →
`1 GiB / 1.38 ≈ 741 MB`, clamped by `floor=min` / `ceiling`. Run from two **suboptimal** starts:

| start `maxPartitionBytes` | iter1 cold | converged DECIDED split (warm) | warm mean |
|---|---|---|---|
| **128m** (worst-low) | 10947 ms | **743 MB** (`bound_by=ratio`) | **6576 ms** |
| **4g** (worst-high) | 9943 ms | **741 MB** (`bound_by=ratio`) | **6760 ms** |

Both converge to **~741–743 MB** (<0.2% apart) and **6.58–6.76 s warm** — only iter1 (cold) reflects the start.
**ftt rescues a mistuned setting** and lands within ~0.3–3% of the tuned optimum (6558 ms):

| you set maxPartBytes to… | fixed OFF warm | ftt warm (→~742 MB) | **ftt vs your fixed setting** |
|---|---|---|---|
| 128m (mistuned) | 9165 ms | 6576 ms | **1.39× faster** |
| 4g (mistuned) | 8552 ms | 6760 ms | **1.27× faster** |
| 1g (already optimal) | 6558 ms | ~6.6–6.8 s | ≈ tie, ~2% slower |

## 3. Warm-to-warm: ftt ties the optimum on GPU work (the batch plateaus)
Apples-to-apples, **iters 2–5 only** (iter1 = COLD_START, autotuner has no memory yet → runs at the default split;
excluded). Scan metrics attributed per SQL execution:

| metric (warm mean/iter) | off-1g (1 GiB) | ftt (→741 MB) |
|---|---|---|
| scan-stage tasks | 99 | 127 (+28%) |
| avg output batch | 413 MB (40%) | 401 MB (39%) |
| max output batch/task | **722 MB** | **722 MB** |
| GPU decode | 42.1 s | 41.5–43.4 s |
| scan-stage gpuTime | 72.0 s | 72.6–73.6 s |

**ftt matches the tuned optimum on decode, gpuTime, and batch** — the only difference is +28% tasks, which costs
the ~2% runtime. ftt's lever is batch fullness, but here the batch **grows with the split only up to ~512m, then
plateaus at 722 MB** (71% of the 1 GiB target — see Appendix). ftt's ~741 MB sits **in the plateau**, so there is
no fullness left to gain; it correctly stops there rather than mis-sizing.

**Mechanism (code-grounded, `GpuParquetScan.scala`):** `maxSplitBytes` only decides *which row groups* land in each
task. Host-side `populateCurrentBlockChunk` (lines 2140–2174) accumulates row groups until `estimateGpuMemory` ≥
`maxReadBatchSizeBytes` (2 GiB); the cuDF chunked reader then emits output batches ≤ `targetSizeBytes`
(`gpuTargetBatchSize` = 1 GiB) on row-group/page boundaries — whose largest aligned chunk on this data is 722 MB.
So once the split is big enough to hold ≥ one such chunk (~512m), growing it further can't grow the batch. To exceed
722 MB you tune `spark.rapids.sql.batchSizeBytes` / `reader.batchSizeBytes` (or the row-group size), **not** the split.

## 4. Conclusion
- **ftt does not beat a tuned `maxPartitionBytes` here.** Its ~741 MB split is in the batch plateau (same 722 MB as
  the 1g optimum), so no fuller batches / no GPU-work cut; ~2% slower from extra tasks.
- **ftt's value is self-tuning robustness:** it converges to the same near-optimal split from any `maxPartitionBytes`,
  so no per-dataset sweep — **1.27–1.39× over a mistuned baseline**, ≈ tie vs an already-optimal one.
- Contrast NDS (`nds-sf3k-scandominance-poc-final-20260722.md`), where the split *did* control fullness on
  scan-dominated queries — the difference here is the wide array/struct columns capping the batch at 722 MB.

## Appendix — batch size vs maxPartitionBytes (OFF sweep, all iters same split)
| maxPartitionBytes | tasks | avg batch | max batch/task |
|---|---|---|---|
| 128m | 550 | 170 MB (17%) | **273 MB (27%)** |
| 256m | 286 | 327 MB (32%) | **524 MB (51%)** |
| 512m | 143 | 460 MB (45%) | **722 MB (71%)** |
| 1g | 99 | 413 MB (40%) | 722 MB (71%) |
| 2g | 40 | 423 MB (41%) | 725 MB (71%) |
| 4g | 18 | 427 MB (42%) | 722 MB (71%) |

Max batch **grows with the split up to ~512m, then plateaus at ~722 MB** (71% of the 1 GiB target) — split-limited
below ~512m, reader-capped above. ftt's ~741 MB is in the plateau (max batch 722 MB, same as 1g). % of the 1 GiB
`gpuTargetBatchSize`.

## Sources
Runs: `data/overture-{mpb-*,ftt-mpb128m,ftt-mpb4g}/` (per-iter `run.log` + event logs). Scripts:
`handoff/overture_scanheavy.scala`, `run-overture-{mpb-sweep,ftt-mpbcheck}.sh`, `mpb_parse.py`. Resume:
`handoff/RESUME-overture-benchmark-20260723.md`. Code: `GpuParquetScan.scala`, `ScanSplitAutotuner.scala`.
