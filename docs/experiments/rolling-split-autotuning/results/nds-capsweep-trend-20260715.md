# NDS SF100 cap sweep: split-ceiling 4 -> 6 -> 8 GiB

Same session, same jar/history, A5000, 2026-07-15. Baseline OFF (128 MiB) = 528.5s.
Ceiling set via -Drapids.autotuner.ceiling={4g,6g,8g}; positive control verified each pass used
the intended ceiling (4294967296 / 6442450944 / 8589934592).

| ceiling | total | speedup | splits changed |
|---|---|---|---|
| OFF (128 MiB) | 528.5s | 1.00x | - |
| 4 GiB | 292.9s | 1.80x | max split 4096 MiB, 0 tables >4 GiB |
| 6 GiB | 294.6s | 1.79x | 189 tables >4 GiB, max 6144 MiB |
| 8 GiB | 292.8s | 1.81x | 190 tables >4 GiB, max 8192 MiB |

**PLATEAU.** Raising the cap 4->8 GiB demonstrably grows splits (204 tables coalesced further;
e.g. query5 store_sales 2664->8192, query53 item 904->8192) but runtime is flat (292.9/294.6/292.8s,
within ~0.6% noise). No OOM, no task/query failures at 6 or 8 GiB.

Conclusion: 4 GiB is past the performance knee and below any failure. Not artificially limiting
(no gain above it) and not OOM-prone. Empirically confirms: bigger splits don't raise peak decode
memory (reader chunks by maxReadBatchSizeBytes; GpuSemaphore auto-scales concurrency), they just make
fewer/bigger tasks at the same GPU throughput.

Data: data/nds-capsweep-20260715_183357/ ; splits nds-capsweep-cap{4,6,8}g-splits-20260715.csv
