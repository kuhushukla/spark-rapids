# NDS SF3k sparkh — projection-aware ratio (`listed`) vs 2g baseline (2026-07-21)

**What is compared** (same cluster, same 99 NDS queries x 5 iterations, warm = mean of iters 2-5, 8 executors x 16 cores):

- **2g baseline**: autotuner OFF, `spark.sql.files.maxPartitionBytes=2gb` (Spark's own split). Event log: `data/mpb-2g-results/eventlog-test-1`.
- **listed**: autotuner ON, `-Drapids.autotuner.ratioBasis=listed -Drapids.autotuner.ceiling=8g`, jar `data/jars/rapids-ratiobasis-357.jar`. Split = `batchSize / (decodedBytes/listedBytes)` per scan (fills one ~1 GiB GPU batch per task, projection-aware), learned on iter1 and applied iters 2-5. Event log: `data/mpb-listedfull-results/eventlog-test-1`.

## Columns and how they were measured

Per query, values are the mean over warm iterations 2-5. The two `_s` columns are **sums over the query's parallel tasks within one iteration** (aggregate task-time), so they exceed the query's wall-clock time; only `warm_ms` is wall-clock.

| column | definition | measurement |
|---|---|---|
| warm_ms | query wall-clock runtime (ms) | benchmark per-query results file, mean of iterations 2-5 |
| scan_s | Parquet read+decode time, summed over the query's scan tasks (task-seconds) | `scan time` metric from the Spark event log, summed over tasks attributed to this query (task stage -> SQL execution -> query name) |
| gpuTime_s | GPU-semaphore hold time (task actively on GPU), summed over all the query's tasks | `gpuTime` task metric from the event log, summed per query |
| batch_full_% | mean scan output batch size as % of the 1 GiB GPU target batch | (scan output bytes / batch count) / 1 GiB x 100 |

Lower `scan_s`/`gpuTime_s` = less total GPU work; higher `batch_full_%` = fuller batches. A query can have lower `_s` but higher `warm_ms` when it runs on fewer parallel tasks — the trade-off shown below.

## Result (aggregate over 103 queries)

| metric | 2g | listed | change |
|---|---|---|---|
| scan time (s) | 3,247 | 1,879 | -42% |
| GPU decode (s) | 1,248 | 635 | -49% |
| gpuTime (s) | 4,849 | 3,687 | -24% |
| WARM runtime (s) | 172.9 | 215.9 | +25% |

listed lowers gpuTime on **98/103** queries and fills batches on **101/103**, but overall runtime is **+25%**. The goal (fuller batches + lower gpuTime + runtime within 5%) is met on **42/103** queries — the scan-dominated ones; the rest lose runtime to reduced parallelism.

## Queries FASTER under listed (warm_listed < warm_2g), with metrics

| query | warm_2g_ms | warm_listed_ms | speedup | gpuTime_2g_s | gpuTime_listed_s | scan_2g_s | scan_listed_s | batch_full_2g_% | batch_full_listed_% |
|---|---|---|---|---|---|---|---|---|---|
| query77 | 771 | 523 | 1.47x | 5.8 | 0.4 | 11.9 | 0.6 | 2 | 44 |
| query66 | 1964 | 1392 | 1.41x | 111.4 | 15.8 | 39.8 | 5.7 | 11 | 82 |
| query52 | 360 | 262 | 1.37x | 2.5 | 0.2 | 5.2 | 0.4 | 3 | 53 |
| query53 | 521 | 388 | 1.34x | 8.1 | 2.5 | 14.3 | 2.3 | 16 | 84 |
| query89 | 780 | 604 | 1.29x | 16.4 | 5.4 | 19.2 | 2.6 | 16 | 84 |
| query43 | 584 | 462 | 1.26x | 13.6 | 3.5 | 15.9 | 1.6 | 12 | 82 |
| query37 | 441 | 353 | 1.25x | 6.1 | 2.0 | 13.6 | 3.7 | 10 | 42 |
| query47 | 1342 | 1093 | 1.23x | 38.4 | 11.3 | 21.1 | 3.5 | 17 | 86 |
| query3 | 353 | 288 | 1.23x | 6.0 | 0.9 | 12.6 | 1.4 | 10 | 80 |
| query21 | 343 | 280 | 1.23x | 0.4 | 0.1 | 0.7 | 0.1 | 5 | 14 |
| query55 | 332 | 274 | 1.21x | 2.4 | 0.2 | 4.9 | 0.3 | 3 | 53 |
| query32 | 605 | 501 | 1.21x | 3.6 | 0.2 | 8.6 | 0.4 | 2 | 47 |
| query71 | 2625 | 2182 | 1.20x | 14.5 | 0.6 | 14.4 | 0.7 | 3 | 58 |
| query48 | 645 | 568 | 1.14x | 13.4 | 4.5 | 22.0 | 7.6 | 28 | 84 |
| query63 | 556 | 490 | 1.13x | 8.6 | 2.7 | 14.6 | 2.7 | 16 | 84 |
| query83 | 536 | 474 | 1.13x | 1.5 | 0.1 | 2.7 | 0.1 | 0 | 2 |
| query27 | 697 | 618 | 1.13x | 25.1 | 6.5 | 24.1 | 8.7 | 32 | 83 |
| query90 | 519 | 468 | 1.11x | 9.4 | 3.2 | 22.2 | 6.1 | 19 | 86 |
| query92 | 437 | 397 | 1.10x | 3.1 | 0.1 | 8.3 | 0.2 | 1 | 35 |
| query59 | 1356 | 1234 | 1.10x | 45.3 | 31.6 | 48.9 | 43.4 | 48 | 91 |
| query26 | 542 | 498 | 1.09x | 13.3 | 2.8 | 18.6 | 3.0 | 16 | 80 |
| query57 | 974 | 898 | 1.08x | 20.3 | 3.8 | 18.4 | 1.3 | 9 | 80 |
| query61 | 1067 | 988 | 1.08x | 7.9 | 1.1 | 17.2 | 1.3 | 5 | 64 |
| query12 | 414 | 384 | 1.08x | 0.7 | 0.1 | 1.6 | 0.1 | 1 | 10 |
| query2 | 977 | 913 | 1.07x | 35.7 | 12.1 | 38.8 | 18.5 | 17 | 84 |
| query70 | 1070 | 1000 | 1.07x | 35.5 | 8.4 | 32.9 | 3.0 | 12 | 88 |
| query39_part1 | 1099 | 1030 | 1.07x | 1.6 | 0.4 | 0.6 | 0.2 | 4 | 8 |
| query9 | 1425 | 1354 | 1.05x | 57.4 | 47.2 | 62.0 | 46.6 | 49 | 88 |
| query98 | 909 | 864 | 1.05x | 0.7 | 0.1 | 1.7 | 0.1 | 3 | 32 |
| query28 | 3704 | 3586 | 1.03x | 203.8 | 175.8 | 275.0 | 269.6 | 66 | 87 |
| query31 | 1367 | 1328 | 1.03x | 20.6 | 2.2 | 40.2 | 1.7 | 2 | 67 |
| query58 | 535 | 520 | 1.03x | 0.7 | 0.2 | 2.8 | 0.1 | 2 | 6 |
| query88 | 2835 | 2769 | 1.02x | 179.8 | 158.9 | 219.7 | 205.8 | 49 | 90 |
| query99 | 1148 | 1122 | 1.02x | 57.9 | 33.1 | 29.9 | 19.5 | 52 | 89 |
| query96 | 1035 | 1032 | 1.00x | 22.3 | 16.0 | 32.2 | 23.8 | 49 | 88 |

**35 of 103 queries are faster under listed.**

## Where to verify (event logs + intermediate CSVs)

- listed run: `data/mpb-listedfull-results/eventlog-test-1` (+ `kuhu-*-test-1.csv` per-query times)
- 2g baseline: `data/mpb-2g-results/eventlog-test-1`; 4g: `data/mpb-4g-results/eventlog-test-1`
- query9 single-query POC (all modes): `data/poc-{off,listed,bytesread,rb1g,brfloor}-results/eventlog-test-1`
- parsed per-query metrics: `data/mpb-perquery-2g.csv`, `data/mpb-perquery-4g.csv`, `data/mpb-perquery-listedfull.csv`
- parser: `docs/experiments/rolling-split-autotuning/handoff/mpb_perquery.py`; this doc: `mpb_writedoc_listed.py`
- cross-check: per-query warm sum equals the nds_power CSV total for both runs (2g 172.9s, listed 215.9s).
