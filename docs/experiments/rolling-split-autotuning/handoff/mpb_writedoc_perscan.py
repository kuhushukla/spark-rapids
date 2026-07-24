#!/usr/bin/env python3
# Wrap the per-scan CSV into a short doc.
import csv
BASE="/home/kuhu/Reps/spark-rapids"
SRC=f"{BASE}/data/mpb-perscan-2g-4g-listed.csv"
OUT=f"{BASE}/docs/experiments/rolling-split-autotuning/results/nds-sf3k-perscan-2g-4g-listed-20260721.md"
rows=list(csv.DictReader(open(SRC)))
L=[]
L.append("# NDS SF3k — per-scan split / fullness / decode, 2g vs 4g vs listed (2026-07-21)\n")
L.append("One row per `(query, table)` scan. Compared runs (same cluster, 99 queries x 5 iterations, "
         "warm = mean of iterations 2-5, 8 executors x 16 cores):\n")
L.append("- **2g** / **4g**: autotuner OFF, `maxPartitionBytes=2gb` / `4gb` (Spark's own split).\n"
         "- **listed**: autotuner ON, split = `batchSize / (decodedBytes/listedBytes)` per scan "
         "(projection-aware, fills ~one 1 GiB GPU batch), jar `rapids-ratiobasis-357.jar`.\n"
         "Event logs: `data/mpb-2g-results/eventlog-test-1`, `data/mpb-4g-results/eventlog-test-1`, "
         "`data/mpb-listedfull-results/eventlog-test-1`. Full CSV: `data/mpb-perscan-2g-4g-listed.csv`.\n")
L.append("## Columns\n")
L.append("| column | meaning |")
L.append("|---|---|")
L.append("| split_MiB | the effective `maxSplitBytes` chosen for that scan (bytes per scan task), from the `scanMaxSplitBytes` driver metric. Note: varies per query even at a fixed config because dynamic partition pruning changes each scan's total bytes. |")
L.append("| full% | mean scan output batch size as % of the 1 GiB GPU target batch (fuller = closer to 100%) |")
L.append("| decode_s | **GPU decode time** for that scan = time decompressing/decoding Parquet to GPU columnar, summed over the scan's tasks (task-seconds). One part of the scan's GPU work. |")
L.append("| scanOp_s | **GPU op time of the scan operator** = the scan's own total GPU compute time, summed over its tasks (task-seconds). This is the per-scan GPU-time metric; decode_s is a subset of it. |")
L.append("| qtime_x | query wall-clock speedup, 2g / listed (>1 = listed faster; <1 = listed slower). Query-level, same for all of a query's scans. |")
L.append("\n**Neither decode_s nor scanOp_s is the task `gpuTime`.** `gpuTime` = time a task holds the GPU "
         "semaphore across ALL operators (decode+filter+project+join+agg); it is task/query-level and "
         "cannot be split cleanly per scan node, so it lives in the per-query CSVs "
         "(`data/mpb-perquery-*.csv`, column `wq_gpu_s`). `scanOp_s` here is the **scan operator's** GPU op "
         "time (a per-scan quantity); `decode_s` is the decode part of it.\n")
# store_sales sample
ss=[r for r in rows if r["table"]=="store_sales"]
ss.sort(key=lambda r:-float(r["warm2g_ms"]))
L.append("## store_sales scans (largest table), top 18 queries by 2g runtime\n")
L.append("split in MiB, full% of the 1 GiB target, decode and scanOp in task-seconds; each metric shown 2g / 4g / listed.\n")
L.append("| query | qtime_x | split 2g/4g/lst | full% 2g/4g/lst | decode 2g/4g/lst | scanOp 2g/4g/lst |")
L.append("|---|---|---|---|---|---|")
for r in ss[:18]:
    L.append(f"| {r['query']} | {r['qtime_x']} | {r['split2g_MiB']}/{r['split4g_MiB']}/{r['splitL_MiB']} | "
             f"{r['full2g%']}/{r['full4g%']}/{r['fullL%']} | {r['decode2g_s']}/{r['decode4g_s']}/{r['decodeL_s']} | "
             f"{r['scanOp2g_s']}/{r['scanOp4g_s']}/{r['scanOpL_s']} |")
L.append("\n## Two findings\n")
L.append("1. **Split of the same table varies per query at a fixed config** (store_sales at 2g: 2048 MiB "
         "for query93, 94 MiB for query14, 65 MiB for query80) — dynamic partition pruning changes each "
         "scan's total bytes, so `bytesPerCore` and the split are per-scan, not one global value.\n"
         "2. **listed fills batches (to ~82-94%) and lowers decode almost everywhere, but query time "
         "mostly regresses** (qtime_x < 1) because the larger split cuts scan-task parallelism. Fuller "
         "batches and shorter decode do not translate to shorter runtime unless the query is scan-dominated.\n")
open(OUT,"w").write("\n".join(L)+"\n")
print("wrote",OUT,f"({len(rows)} scans total, {len(ss)} store_sales)")
