# NDS — GPU Utilization During Warm Queries (2026-07-13)

Direct measurement to settle "is the GPU pegged or is it IO?" — correcting the earlier *inferred*
"GPU-decode-bound" claim (which came only from "bigger splits didn't help").

## Method
NDS SF100 `query9,query67,query76`, A5000, `local[16]`, parallelism-cap build. Sampled
`nvidia-smi --query-gpu=utilization.gpu -i 1` every 0.5s throughout the run, then filtered to the
**warm Power-Test window** (epoch 1783970715–1783970742, 27s, 47 samples).

## GPU util during the warm queries
```
0 0 0 0 0 52 49 29 16 31 30 2 0 0 39 83 94 68 96 84 85 66 83 0 100 79 100 81 56 0 15 34 24 27 27 27 27 27 27 25 29 26 28 29 37 31 27
```
| stat | value |
|---|---|
| mean | **38%** |
| median | 29% |
| p75 | 66% |
| < 50% | 70% of samples (33/47) |
| ≥ 90% | 9% (4/47) |
| max | 100% (brief) |

## Storage / IO context
- Data on `/dev/nvme0n1p2` (NVMe); fio single-thread sequential = **7.4 GB/s**.
- Dataset 38 GB fits in 125 GB RAM → page-cached; scans move ~2.5 GB/s effective. IO has large
  headroom.

## File/task breakdown
- store_sales / web_sales: **36,480 files each**, ~160–240 KB, partitioned by date.
- catalog_sales: 1,875 files (~5.8 MB).
- With a ~924 MiB split over ~16 tasks, each task coalesces **~2,000–4,000 tiny files** (not one
  file per task; one-file-per-task would be 36,480 tasks).

## Conclusion
**Neither GPU nor IO is the bottleneck.** GPU ~38% mean (idle/0% for long stretches, brief bursts
to 90–100%); IO idle. The limiter is the **non-GPU path** — most likely CPU-side coalescing of
thousands of ~240 KB files per task, plus shuffle/aggregate. This is why split size past ~1 GiB
doesn't move wall-clock: the limiter was never scan throughput.

Corrects: `nds-abc-comparison-20260713.md`, `nds-selfsizing-parcap-20260713.md`,
`../handoff/split-ceiling-design.md`, `../handoff/autotuner-mechanics-and-gpu-routing.md`.

## Next
Profile where the non-GPU time actually goes (coalescing vs shuffle vs AQE) — the thing that would
actually move the needle.
