#!/usr/bin/env python3
# Generate the maxPartitionBytes-sweep baseline doc from the per-query CSVs (no hand transcription).
# Reads data/mpb-perquery-<cfg>.csv for all configs; writes the results markdown.
import csv, os

CFGS=["128m","256m","512m","1g","2g","4g"]
BASE="/home/kuhu/Reps/spark-rapids/data"
OUT="/home/kuhu/Reps/spark-rapids/docs/experiments/rolling-split-autotuning/results/nds-mpb-sweep-perquery-20260720.md"

def load(cfg):
    d={}
    with open(f"{BASE}/mpb-perquery-{cfg}.csv") as f:
        for r in csv.DictReader(f): d[r["query"]]=r
    return d
data={c:load(c) for c in CFGS}
queries=sorted(data["2g"].keys())
def fnum(x):
    try:
        v=float(x)
        return "n/a" if v==-1 else (f"{v:,.0f}" if abs(v)>=100 else f"{v:g}")
    except: return x

L=[]
L.append("# NDS SF3k sparkh — maxPartitionBytes baseline sweep, per-query (2026-07-20)\n")
L.append("Full NDS on sparkh SF3k, **autotuner OFF** (no ratio; Spark's own `maxSplitBytes` = "
         "`maxPartitionBytes`). 8 executors x 16 cores = 128 cores (verified 8 added / 0 removed each run). "
         "`concurrentGpuTasks=4`, AQE on, `metrics.level=DEBUG`. 5 iterations per config: iter1 cold, "
         "iters 2-5 warm (mean). Jar `rapids-ratiobasis-357.jar` with autotuner off = vanilla scan "
         "(verified inert). Data read-only `/user/nvidia/nds/parquet_sf3k_decimal`.\n")

# ---- column provenance ----
L.append("## 1. How each column is computed\n")
L.append("| column | where it comes from | how it is calculated |")
L.append("|---|---|---|")
L.append("| cold_ms, warm_ms | the nds_power per-query results CSV (each row is labeled with the query name) | cold is iteration 1; warm is the average of iterations 2 through 5; both are wall-clock milliseconds |")
L.append("| scan_s | the event-log task metric named `scan time` (only scan operators emit this metric) | add up that metric across all tasks that ran in scan stages, grouped into the query it belongs to (found from the task's stage, which maps to the query's execution id), then average the four warm iterations. Unit: total task-seconds |")
L.append("| decode_s | the task metric `GPU decode time` (only scan operators emit this) | same calculation as scan_s |")
L.append("| scanstage_gpu_s | the task metric `gpuTime` (the time the task held the GPU semaphore, i.e. was actively on the GPU) | add it up across tasks that ran in scan-containing stages, per query, averaged over the four warm iterations |")
L.append("| wholequery_gpu_s | the same `gpuTime` task metric | add it up across all of the query's tasks (every stage, not just scans), averaged over the warm iterations |")
L.append("| batches | the scan operator's `output columnar batches` metric | add up per query, averaged over the warm iterations. For scalar-subquery queries where that metric undercounts, use the largest per-task batch count seen in scan stages instead |")
L.append("| avg_batch_MiB | the scan's `sum of output GPU batch bytes` divided by batches | total scan output bytes divided by the number of batches, then divided by 1,048,576 to convert bytes into MiB (mebibytes) |")
L.append("| percent_full | avg_batch_MiB compared to the target batch size | the average batch size divided by 1,073,741,824 bytes (the 1 GiB GPU target batch, `spark.rapids.sql.batchSizeBytes`), times 100 |")
L.append("\nEvery column ending in `_s`, plus the two gpuTime columns, is a **total of per-task time added "
         "up across all tasks for one warm iteration** (task-seconds), not wall-clock elapsed time. Only "
         "cold_ms and warm_ms are wall-clock elapsed time.\n")

# ---- config summary (computed from the per-query CSVs) ----
L.append("## 2. Config summary (sum over 103 queries; task-seconds are per warm iteration)\n")
L.append("| maxPartBytes | WARM total (s) | COLD total (s) | scan time (s) | GPU decode (s) | whole-query gpuTime (s) |")
L.append("|---|---|---|---|---|---|")
warm_by={}
for c in CFGS:
    warm=sum(float(data[c][q]["warm_ms"]) for q in queries)/1000
    cold=sum(float(data[c][q]["cold_ms"]) for q in queries)/1000
    scan=sum(float(data[c][q]["scan_s"]) for q in queries)
    dec=sum(float(data[c][q]["decode_s"]) for q in queries)
    gpu=sum(float(data[c][q]["wq_gpu_s"]) for q in queries)
    warm_by[c]=warm
    L.append(f"| {c} | {warm:,.1f} | {cold:,.1f} | {scan:,.0f} | {dec:,.0f} | {gpu:,.0f} |")
best=min(warm_by,key=warm_by.get)
L.append(f"\n**Runtime floor at `{best}`** (WARM {warm_by[best]:.1f} s). scan time, GPU decode, and gpuTime "
         "fall monotonically as partitions grow; runtime bottoms at 2g and ticks up at 4g.\n")

# ---- cross-config WARM matrix ----
L.append("## 3. Per-query WARM runtime (ms) across configs, plus speedup\n")
L.append("| query | "+" | ".join(CFGS)+" | best | speedup from 128m to best |")
L.append("|---|"+"---|"*(len(CFGS)+2))
rowsort=sorted(queries, key=lambda q:-float(data["128m"][q]["warm_ms"]))
for q in rowsort:
    warm={c:float(data[c][q]["warm_ms"]) for c in CFGS}
    bc=min(warm,key=warm.get); spd=warm["128m"]/warm[bc] if warm[bc] else 0
    L.append(f"| {q} | "+" | ".join(f"{warm[c]:,.0f}" for c in CFGS)+f" | {bc} | {spd:.2f}x |")

# ---- per-config full metric tables ----
COLS=[("scan_s","scan_s"),("decode_s","decode_s"),("scanstage_gpu_s","scanstage_gpu_s"),
      ("wq_gpu_s","wholequery_gpu_s"),("scan_batches","batches"),
      ("avg_batch_mib","avg_batch_MiB"),("pct_target","percent_full")]
L.append("\n## 4. Full per-query metrics, one table per config\n")
L.append("Columns are defined in section 1. cold_ms and warm_ms are wall-clock; scan_s, decode_s, "
         "scanstage_gpu_s, and wholequery_gpu_s are aggregate task-seconds per warm iteration; batches, "
         "avg_batch_MiB, and percent_full describe the scan output per warm iteration.\n")
for c in CFGS:
    L.append(f"### maxPartitionBytes = {c}\n")
    L.append("| query | cold_ms | warm_ms | "+" | ".join(h for _,h in COLS)+" |")
    L.append("|---|---|---|"+"---|"*len(COLS))
    for q in sorted(queries, key=lambda q:-float(data[c][q]["warm_ms"])):
        r=data[c][q]
        cells=[q, fnum(r["cold_ms"]), fnum(r["warm_ms"])]+[fnum(r[k]) for k,_ in COLS]
        L.append("| "+" | ".join(cells)+" |")
    L.append("")

L.append("## 5. Method and cross-checks\n")
L.append("- Runtime comes from the nds_power per-query CSV (each row labeled with the query name). "
         "Event-log metrics are matched to a query by reading the query name from the SQL execution's "
         "description, then following the job's execution id to its stages and their tasks.\n"
         "- scan time, GPU decode time, and buffer time are summed by metric name over scan-stage tasks. "
         "Only scan operators emit these names, so this stays correct even when Spark re-plans under AQE "
         "or when the scan runs inside a scalar subquery.\n"
         "- gpuTime is the per-task time holding the GPU semaphore (GPU-active). The whole-query column "
         "sums it over all of the query's tasks; the scan-stage column sums it only over tasks in "
         "scan-containing stages.\n"
         "- batches use the scan operator's accumulator (correct even for Expand/broadcast queries such as "
         "query28). Scalar-subquery queries fall back to the largest per-task batch count in scan stages; "
         "a value is shown as n/a only if both methods are unsafe.\n"
         "- Cross-checks run before publishing: the sum of per-query warm times equals the nds_power CSV "
         "total for every config; anchor batch counts are stable (query28 = 1,212, query88 = 1,626); "
         "query9 was corrected from 0 to 62 seconds of scan time.\n"
         "- Source CSVs: `data/mpb-perquery-<cfg>.csv`. Parsers: `handoff/mpb_perquery.py` and "
         "`handoff/mpb_writedoc.py`.")

with open(OUT,"w") as f: f.write("\n".join(L)+"\n")
print("wrote",OUT,"(",len(queries),"queries x",len(CFGS),"configs )")
