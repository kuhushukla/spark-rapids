# NDS SF3k — per-scan split / fullness / decode, 2g vs 4g vs listed (2026-07-21)

One row per `(query, table)` scan. Compared runs (same cluster, 99 queries x 5 iterations, warm = mean of iterations 2-5, 8 executors x 16 cores):

- **2g** / **4g**: autotuner OFF, `maxPartitionBytes=2gb` / `4gb` (Spark's own split).
- **listed**: autotuner ON, split = `batchSize / (decodedBytes/listedBytes)` per scan (projection-aware, fills ~one 1 GiB GPU batch), jar `rapids-ratiobasis-357.jar`.
Event logs: `data/mpb-2g-results/eventlog-test-1`, `data/mpb-4g-results/eventlog-test-1`, `data/mpb-listedfull-results/eventlog-test-1`. Full CSV: `data/mpb-perscan-2g-4g-listed.csv`.

## Columns

| column | meaning |
|---|---|
| split_MiB | the effective `maxSplitBytes` chosen for that scan (bytes per scan task), from the `scanMaxSplitBytes` driver metric. Note: varies per query even at a fixed config because dynamic partition pruning changes each scan's total bytes. |
| full% | mean scan output batch size as % of the 1 GiB GPU target batch (fuller = closer to 100%) |
| decode_s | **GPU decode time** for that scan = time decompressing/decoding Parquet to GPU columnar, summed over the scan's tasks (task-seconds). One part of the scan's GPU work. |
| scanOp_s | **GPU op time of the scan operator** = the scan's own total GPU compute time, summed over its tasks (task-seconds). This is the per-scan GPU-time metric; decode_s is a subset of it. |
| qtime_x | query wall-clock speedup, 2g / listed (>1 = listed faster; <1 = listed slower). Query-level, same for all of a query's scans. |

**Neither decode_s nor scanOp_s is the task `gpuTime`.** `gpuTime` = time a task holds the GPU semaphore across ALL operators (decode+filter+project+join+agg); it is task/query-level and cannot be split cleanly per scan node, so it lives in the per-query CSVs (`data/mpb-perquery-*.csv`, column `wq_gpu_s`). `scanOp_s` here is the **scan operator's** GPU op time (a per-scan quantity); `decode_s` is the decode part of it.

## store_sales scans (largest table), top 18 queries by 2g runtime

split in MiB, full% of the 1 GiB target, decode and scanOp in task-seconds; each metric shown 2g / 4g / listed.

| query | qtime_x | split 2g/4g/lst | full% 2g/4g/lst | decode 2g/4g/lst | scanOp 2g/4g/lst |
|---|---|---|---|---|---|
| query93 | 0.98 | 2048/3021/2286 | 79/62/88 | 23.8/22.8/20.1 | 50.3/55.3/54.2 |
| query67 | 0.87 | 583/583/2321 | 20/20/90 | 6.7/6.6/1.8 | 19.3/19.2/4.2 |
| query64 | 0.9 | 587/587/965 | 49/49/86 | 8.1/8.4/5.2 | 16.8/16.8/61.2 |
| query50 | 0.94 | 2048/2914/2321 | 81/90/91 | 19.1/15.9/20.2 | 42.1/45.3/45.2 |
| query78 | 0.84 | 580/580/1656 | 29/29/92 | 10.6/10.5/3.8 | 25.6/25.3/10.0 |
| query24_part2 | 0.68 | 2048/3021/2286 | 79/62/88 | 20.7/21.7/21.2 | 38.0/46.6/39.2 |
| query24_part1 | 0.7 | 2048/3021/2286 | 79/62/88 | 19.7/21.6/20.5 | 37.3/45.4/39.7 |
| query23_part2 | 0.97 | 2048/3021/2908 | 64/72/94 | 15.3/13.5/9.5 | 37.4/35.4/31.1 |
| query23_part1 | 0.95 | 2048/3021/2908 | 64/72/94 | 15.2/13.2/9.5 | 37.3/34.9/30.7 |
| query14_part1 | 0.87 | 94/94/3888 | 41/41/94 | 6.0/6.0/3.9 | 20.8/20.8/11.0 |
| query14_part2 | 0.9 | 22/22/3888 | 41/41/94 | 6.0/6.0/3.9 | 20.7/20.8/10.7 |
| query75 | 0.73 | 587/587/2323 | 20/20/90 | 4.7/4.6/1.7 | 11.7/11.4/10.2 |
| query28 | 1.03 | 2048/3021/2858 | 66/64/87 | 18.3/17.3/14.8 | 44.4/42.8/42.3 |
| query4 | 0.83 | 583/578/1933 | 24/24/90 | 7.8/7.2/2.7 | 22.4/21.8/13.2 |
| query88 | 1.02 | 2048/3021/3810 | 49/73/91 | 12.8/9.9/10.5 | 27.4/27.4/25.8 |
| query65 | 0.74 | 583/583/2905 | 16/16/93 | 4.8/4.7/2.3 | 7.6/7.9/8.7 |
| query71 | 1.2 | 98/98/2906 | 4/4/85 | 2.9/2.9/0.1 | 5.2/5.2/0.4 |
| query80 | 0.62 | 65/65/1659 | 6/6/82 | 1.6/1.6/0.2 | 4.7/4.5/0.5 |

## Two findings

1. **Split of the same table varies per query at a fixed config** (store_sales at 2g: 2048 MiB for query93, 94 MiB for query14, 65 MiB for query80) — dynamic partition pruning changes each scan's total bytes, so `bytesPerCore` and the split are per-scan, not one global value.
2. **listed fills batches (to ~82-94%) and lowers decode almost everywhere, but query time mostly regresses** (qtime_x < 1) because the larger split cuts scan-task parallelism. Fuller batches and shorter decode do not translate to shorter runtime unless the query is scan-dominated.

