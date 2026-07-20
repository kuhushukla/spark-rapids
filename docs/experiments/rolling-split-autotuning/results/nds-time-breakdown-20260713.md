> ⛔ **RETRACTED (2026-07-14). The central conclusion of this doc is WRONG.** The autotuner is NOT
> inert. Grounded evidence:
> - Plugin honors the split (probe: 128m→1232, 1024m→157, 4096m→40 partitions on store_sales;
>   `getFilePartitions` → vanilla global bin-packing).
> - The real query scans are the **"collect at"** stages, not the "load at" stages I measured (those
>   are schema-inference / `createOrReplaceTempView` reads and are identical by construction).
> - **query9 (sqlExec 24) store_sales scan = stage 31: cold 1232 tasks → warm 174 tasks** — the
>   autotuner's ~924 MiB split cut it ~7× (1232 = 128 MiB count, 174 ≈ 924 MiB count).
>
> So "split is inert", "cold→warm 1.8× is caching", "autotuner is a no-op", and the GPU-38%
> non-GPU-coalescing bottleneck story below were all built on measuring the WRONG stages. Ignore the
> conclusions in this doc. Whether the cold→warm speedup is autotuner vs caching vs both needs a
> clean A/B using the new `scanMaxSplitBytes` scan metric.

# NDS — Where the Time Actually Goes (2026-07-13)

Follow-up to the GPU-util finding (GPU only ~38% during warm queries). Parses the warm event log
(`local-1783970681952`, query9/67/76, A5000, local[16]) to locate the real bottleneck. **The
result overturns the "128 MiB → autotuned ≈ 1.8×" headline.**

## Headline: `maxSplitBytes` is INERT on this dataset
Cold (128 MiB split) and warm (924 MiB split) produce **byte-for-byte identical scan task counts**:
```
scan-stage task counts, both runs: [2181, 2100, 2004, 1837, 1824, 1824, 261, 1, 1, ...]
```
`store_sales` has **exactly 1824 date-partition directories** (`ss_sold_date_sk=…`) and the scan
runs **exactly 1824 tasks** — one per date partition, regardless of split. Each date partition is
~8.5 MB (< every split we tested), and file bin-packing does not coalesce across partition dirs, so
task count is pinned to the partition granularity. **The split override changes nothing.**

## Time breakdown (warm, 27s Power Test)
| Stage class | wall (sum) | tasks | run_sum | deser_sum | per-task |
|---|---|---|---|---|---|
| **scan** (6 "load" stages) | 16.1s | 12,055 | 10s | **212s** | 0.8ms work / **17.6ms deserialize** |
| **compute** (aggregate/join/shuffle) | 24.1s | 1,909 | 347s | — | real work |

- **Scan = pure task overhead.** 12,055 tiny tasks each do ~1 ms of actual work but cost ~18 ms to
  deserialize/schedule. Deserialize alone = 212s / 16 cores ≈ **13s wall**. Actual scan work is
  ~0.6s. The GPU starves (38% util) because tasks are tiny and overhead-bound.
- **Compute = the real work.** 347s / 16 ≈ 22s of aggregate/join/shuffle (biggest single stage:
  the final collect/aggregate ≈ 10s wall).

## Consequences (corrections)
1. **The autotuner is effectively a no-op on this NDS layout.** Split is inert → cold and warm
   execute identically. The observed cold→warm ~1.7× is therefore **page-cache + JIT warming, not
   the split.** Prior docs that credited "128 MiB → autotuned ≈ 1.8×" to the autotuner are wrong
   for this dataset; the speedup is cache/JIT.
2. **Neither GPU nor IO is the limiter** (confirmed: GPU 38%, NVMe idle). The limiter is
   **per-task scheduling/deserialize overhead from one-task-per-date-partition**, plus the
   aggregate/shuffle compute.
3. **What would actually move the needle here:** reduce task count by coalescing across date
   partitions (so the split can bind), or reduce per-task overhead — not resizing splits that
   never take effect.

## Confirming test (not yet run)
An **A/A run** — the same query twice with an *identical* split — would isolate cache/JIT from the
autotuner. Expectation: run2 still ~1.7× faster than run1 with no split change, proving the
speedup is warming, not the autotuner.

## Corrects
`nds-abc-comparison-20260713.md`, `nds-selfsizing-parcap-20260713.md`,
`../handoff/split-ceiling-design.md`, `../handoff/autotuner-mechanics-and-gpu-routing.md`.
