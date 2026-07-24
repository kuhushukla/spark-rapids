#!/usr/bin/env python3
# Generate the concise listed-vs-2g findings doc from the per-query CSVs (no hand transcription).
import csv
BASE="/home/kuhu/Reps/spark-rapids/data"
OUT="/home/kuhu/Reps/spark-rapids/docs/experiments/rolling-split-autotuning/results/nds-sf3k-listed-vs-2g-20260721.md"
def load(p): return {r["query"]:r for r in csv.DictReader(open(p))}
g2=load(f"{BASE}/mpb-perquery-2g.csv"); lf=load(f"{BASE}/mpb-perquery-listedfull.csv")
qs=[q for q in g2 if q in lf]
def f(q,k,src): return float((g2 if src=='2g' else lf)[q][k])
def tots(src,k): return sum(f(q,k,src) for q in qs)

L=[]
L.append("# NDS SF3k sparkh — projection-aware ratio (`listed`) vs 2g baseline (2026-07-21)\n")
L.append("**What is compared** (same cluster, same 99 NDS queries x 5 iterations, warm = mean of iters 2-5, "
         "8 executors x 16 cores):\n")
L.append("- **2g baseline**: autotuner OFF, `spark.sql.files.maxPartitionBytes=2gb` (Spark's own split). "
         "Event log: `data/mpb-2g-results/eventlog-test-1`.\n"
         "- **listed**: autotuner ON, `-Drapids.autotuner.ratioBasis=listed -Drapids.autotuner.ceiling=8g`, "
         "jar `data/jars/rapids-ratiobasis-357.jar`. Split = `batchSize / (decodedBytes/listedBytes)` per "
         "scan (fills one ~1 GiB GPU batch per task, projection-aware), learned on iter1 and applied iters 2-5. "
         "Event log: `data/mpb-listedfull-results/eventlog-test-1`.\n")

L.append("## Columns and how they were measured\n")
L.append("Per query, values are the mean over warm iterations 2-5. The two `_s` columns are **sums over "
         "the query's parallel tasks within one iteration** (aggregate task-time), so they exceed the "
         "query's wall-clock time; only `warm_ms` is wall-clock.\n")
L.append("| column | definition | measurement |")
L.append("|---|---|---|")
L.append("| warm_ms | query wall-clock runtime (ms) | benchmark per-query results file, mean of iterations 2-5 |")
L.append("| scan_s | Parquet read+decode time, summed over the query's scan tasks (task-seconds) | `scan time` metric from the Spark event log, summed over tasks attributed to this query (task stage -> SQL execution -> query name) |")
L.append("| gpuTime_s | GPU-semaphore hold time (task actively on GPU), summed over all the query's tasks | `gpuTime` task metric from the event log, summed per query |")
L.append("| batch_full_% | mean scan output batch size as % of the 1 GiB GPU target batch | (scan output bytes / batch count) / 1 GiB x 100 |")
L.append("\nLower `scan_s`/`gpuTime_s` = less total GPU work; higher `batch_full_%` = fuller batches. "
         "A query can have lower `_s` but higher `warm_ms` when it runs on fewer parallel tasks — the "
         "trade-off shown below.\n")

warm2=tots('2g','warm_ms')/1000; warmL=tots('listed','warm_ms')/1000
L.append("## Result (aggregate over 103 queries)\n")
L.append("| metric | 2g | listed | change |")
L.append("|---|---|---|---|")
for k,lab,div in [("scan_s","scan time (s)",1),("decode_s","GPU decode (s)",1),("wq_gpu_s","gpuTime (s)",1)]:
    a=tots('2g',k); b=tots('listed',k); L.append(f"| {lab} | {a:,.0f} | {b:,.0f} | {(b-a)/a*100:+.0f}% |")
L.append(f"| WARM runtime (s) | {warm2:,.1f} | {warmL:,.1f} | {(warmL-warm2)/warm2*100:+.0f}% |")
gd=sum(1 for q in qs if f(q,'wq_gpu_s','listed')<f(q,'wq_gpu_s','2g'))
fu=sum(1 for q in qs if f(q,'pct_target','listed')>f(q,'pct_target','2g'))
met=sum(1 for q in qs if f(q,'wq_gpu_s','listed')<f(q,'wq_gpu_s','2g') and f(q,'pct_target','listed')>f(q,'pct_target','2g') and f(q,'warm_ms','listed')<=1.05*f(q,'warm_ms','2g'))
L.append(f"\nlisted lowers gpuTime on **{gd}/103** queries and fills batches on **{fu}/103**, but overall runtime "
         f"is **+{(warmL-warm2)/warm2*100:.0f}%**. The goal (fuller batches + lower gpuTime + runtime within 5%) "
         f"is met on **{met}/103** queries — the scan-dominated ones; the rest lose runtime to reduced parallelism.\n")

L.append("## Queries FASTER under listed (warm_listed < warm_2g), with metrics\n")
L.append("| query | warm_2g_ms | warm_listed_ms | speedup | gpuTime_2g_s | gpuTime_listed_s | scan_2g_s | scan_listed_s | batch_full_2g_% | batch_full_listed_% |")
L.append("|---|---|---|---|---|---|---|---|---|---|")
faster=[q for q in qs if f(q,'warm_ms','listed')<f(q,'warm_ms','2g')]
faster.sort(key=lambda q: f(q,'warm_ms','listed')/f(q,'warm_ms','2g'))
for q in faster:
    L.append(f"| {q} | {f(q,'warm_ms','2g'):.0f} | {f(q,'warm_ms','listed'):.0f} | "
             f"{f(q,'warm_ms','2g')/f(q,'warm_ms','listed'):.2f}x | {f(q,'wq_gpu_s','2g'):.1f} | "
             f"{f(q,'wq_gpu_s','listed'):.1f} | {f(q,'scan_s','2g'):.1f} | {f(q,'scan_s','listed'):.1f} | "
             f"{f(q,'pct_target','2g'):.0f} | {f(q,'pct_target','listed'):.0f} |")
L.append(f"\n**{len(faster)} of 103 queries are faster under listed.**\n")

L.append("## Where to verify (event logs + intermediate CSVs)\n")
L.append("- listed run: `data/mpb-listedfull-results/eventlog-test-1` (+ `kuhu-*-test-1.csv` per-query times)\n"
         "- 2g baseline: `data/mpb-2g-results/eventlog-test-1`; 4g: `data/mpb-4g-results/eventlog-test-1`\n"
         "- query9 single-query POC (all modes): `data/poc-{off,listed,bytesread,rb1g,brfloor}-results/eventlog-test-1`\n"
         "- parsed per-query metrics: `data/mpb-perquery-2g.csv`, `data/mpb-perquery-4g.csv`, `data/mpb-perquery-listedfull.csv`\n"
         "- parser: `docs/experiments/rolling-split-autotuning/handoff/mpb_perquery.py`; this doc: `mpb_writedoc_listed.py`\n"
         "- cross-check: per-query warm sum equals the nds_power CSV total for both runs (2g 172.9s, listed 215.9s).")
open(OUT,"w").write("\n".join(L)+"\n")
print("wrote",OUT,f"({len(faster)} faster queries)")
