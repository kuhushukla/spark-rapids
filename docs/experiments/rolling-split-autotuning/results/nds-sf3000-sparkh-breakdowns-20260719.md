# NDS SF3000 on sparkh — scan-split autotuner breakdowns (2026-07-19)

Cluster: sparkh standalone, **8 executors × 16 cores = 128 cores**, `maxPartitionBytes=2gb`,
`concurrentGpuTasks=4`, AQE on, metrics level DEBUG. Data: `hdfs://sparkh-nn1:8020/user/nvidia/nds/parquet_sf3k_decimal`.
Baseline = autotuner **off** (Spark's own `maxSplitBytes`). All figures below are from this session's runs;
each section notes its source.

> Every run compared here used the **same 8 executors / 8 hosts** and ran the **same 539 SQL executions**
> (99 queries × 5 iterations + temp-view setup). So the comparisons are not confounded by node count or
> query set. (Verified from the event logs.)

---

## 1. The heuristic (from `ScanSplitAutotuner.scala`)

```
ratio = decoded / listed            # decoded GPU-output bytes / full file length (getLen)
raw   = batchSize / ratio           # split that decodes to ~one target batch (batchSize = 1 GiB)
split = clamp( raw, low, high )

Earlier ("4 GiB cap"):  low = max(64MiB, sparkDefault)   high = 4 GiB
Current ("core1"):      low = 64 MiB                     high = min(4 GiB, listed / (N × cores))   # N=1
```
`core<N>` caps the split at `listed/(N×cores)` so a table never runs on fewer than N×cores tasks.
Switch via `-Drapids.autotuner.ceiling=core<N>|batch4g|parcap|none`.

---

## 2. Runtime sweep (warm, OFF vs each; source: per-query CSVs)

| config | ceiling | tasks/table behavior | WARM vs baseline |
|---|---|---|---|
| flat 4 GiB | batch4g | fewest tasks | **0.84×** (slower)¹ |
| **core1** | core1 (1 task/core) | ≈ baseline parallelism | **0.99×** (about even) |
| core2 | core2 | 2 tasks/core | **0.93×** |
| core3 | core3 | 3 tasks/core | **0.86×** |
| decouple | batch4g + 80 read threads | fewest tasks | **0.82×** (slowest) |

Warm = mean of iterations 2–5. core1/2/3/decouple share one baseline (176.4 s warm). ¹The 4 GiB number
is vs its own same-window baseline (173.2 s) from the first SF3000 A/B — comparable but a different run.

**Read:** core1 is the peak; worse on both sides (fewer tasks → 4 GiB/decouple; more tasks → core2/core3).

---

## 3. How often the ratio actually drives the split (source: core1 driver log, 3,298 decisions)

| `bound_by` | count | % | which tables |
|---|---|---|---|
| floor (64 MiB) | 2,371 | 72% | small/dim tables |
| parallelism_ceiling (`listed/cores`) | 853 | 26% | big tables — **= the split Spark's default already picks** |
| **ratio** | **74** | **2.2%** | **store_sales only** |

So at SF3000 the ratio changes the split on ~2% of scans; the rest match the autotuner-off baseline by
construction. This is why core1 ≈ baseline.

---

## 4. Scan batch fullness (exact, from `NUM_OUTPUT_BATCHES` in the event logs)

`avg batch = Σ scan output bytes / Σ scan output batches`, target batch = 1 GiB.

| config | avg scan batch | % of target |
|---|---|---|
| core1 (1 task/core) | 212 MiB | **21%** |
| decouple (fewest tasks) | 737 MiB | **72%** |

Both decode the same ~32,900 GiB of scan output; decouple emits 1/3.5 the batches → 3.5× larger batches.
(An earlier "% full" I quoted was from a per-task *proxy* that assumed one batch per task and **overstated**
fullness; these come from real batch counts.)

---

## 5. GPU decode throughput (source: `GPU decode time` + decoded bytes, event logs)

All three decode the **same ~67,250 GiB** of data.

| config | decode time | decode throughput |
|---|---|---|
| core2 (most tasks, smallest batches) | 8,497 s | **7.9 GiB/s** |
| core1 | 5,974 s | **11.3 GiB/s** |
| decouple (fewest tasks, largest batches) | 3,487 s | **19.3 GiB/s** |

**Key contradiction:** decode throughput rises with fuller batches, but the fastest-decoding config
(decouple, 19.3 GiB/s) has the **slowest** runtime (0.82×). Decode efficiency and runtime are anti-correlated.

---

## 6. Where scan time goes (source: one core1 event log, aggregate task-seconds)

| scan phase | task-seconds | % of scan time |
|---|---|---|
| GPU decode | 5,974 | **36%** |
| residual (semaphore wait + host footer/setup, not separated) | ~5,070 | ~30% |
| buffer (host→device) | 2,713 | 16% |
| filter (row-group/predicate) | 2,138 | 13% |
| **disk read (HDFS/footer bytes)** | **910** | **5.4%** |
| scan time (parent) | 16,803 | 100% |

The scan is **GPU-decode-heavy, not disk-IO-heavy** (disk read only 5.4%). *Caveat: one config, aggregate
task-seconds (proportions, not wall-clock); residual is inferred, not measured separately.*

---

## 7. Parallelism — exact per-stage task counts (source: `Number of Tasks` per stage, event logs)

| tasks / stage | core1 | decouple |
|---|---|---|
| **≥128 (can fill all cores)** | **1,118 stages** | **302 stages** |
| 64–127 | 291 | 314 |
| 16–63 | 703 | 749 |
| <16 | 3,672 | 4,457 |
| total stages | 5,784 | 5,822 |

decouple has **3.7× fewer stages that can occupy all 128 cores** and more tiny stages. The fewest-task
config under-parallelizes at the stage level — that's the direct evidence for why fuller batches run slower.
*Caveat: this is stage COUNT, not time-weighted; the runtime-relevant version (weight by stage task-time)
is still to compute. The 0.7% difference in total stage count is AQE re-planning (3,000+ adaptive updates/run),
not a query difference (539 SQL executions each).*

---

## 8. Projection vs compression per table (source: core2 run, `readBufferSize` = actual bytes read)

`read_selectivity = bytesRead/listed` (projection + row-group pruning); `decode_expansion = decoded/bytesRead`
(decompression). Largest scan per table:

| table | listed | actually read | read_selectivity | decode_expansion |
|---|---|---|---|---|
| store_sales | 370 G | 14.5 G | **0.039** (reads 3.9%) | 6.89 |
| catalog_sales | 292 G | 38 G | 0.130 | 1.31 |
| web_sales | 131 G | 22 G | 0.165 | 2.68 |
| inventory | 5.1 G | 5.1 G | 1.000 (all columns) | 3.10 |

Low overall ratios are **projection-dominated** (store_sales reads only 3.9% of the file). `readBufferSize`
is currently **recorded only** — the split still uses `decoded/listed`, not real read bytes.

---

## Conclusion

- **The ratio-driven split does not improve SF3000 runtime.** Best case (core1) = 0.99× (matches baseline);
  it only undoes the flat-4 GiB regression (0.84×). More tasks are worse. The ratio binds on ~2% of scans,
  so core1 ≈ baseline by construction.
- **Bigger/fuller batches decode faster but run slower** (decouple: 72% full, 19.3 GiB/s decode, 0.82×
  runtime), because they make far fewer partitions (302 vs 1,118 stages ≥128 tasks) and can't fill the cores.
  **Partition size trades parallelism for decode efficiency; at SF3000 parallelism wins.**
- **Partition/split size is not the SF3000 lever.** The scan is GPU-decode-heavy (36%) not IO (5%); if there's
  headroom it's on the GPU-decode/concurrency side.

**Not isolated (open):** (a) the 80 read threads (decouple changed ceiling AND threads at once); (b) how much
of "fuller batches slower" is under-parallelization vs GPU-decode-bound; (c) time-weighted parallelism.

## Next tests
1. **readBytes in the ratio** — expected ≈ no-op at SF3000 for repeating queries (`decoded/listed` already
   equals `projection × decodeExpansion`), so test at SF100 / varying projections where it should matter.
2. Isolate the 80 read threads: `<80` on batch4g, and `core1 + 80 threads`.
3. Time-weight the parallelism (per-query, same queryN both configs).
4. GPU side: sweep `concurrentGpuTasks`, nsys traces.

**Sources:** run dirs `data/nds-sparkh-pcap-core{1,2,3}-20260719_*`, `data/nds-sparkh-decouple-20260719_*`
(per-query CSVs + DEBUG event logs). Jars `data/jars/rapids-{bytesread,batchtrack}-357.jar`.
