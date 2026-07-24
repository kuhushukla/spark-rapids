#!/usr/bin/env python3
# Warm-only, per-execution metrics for the Overture runs (removes cold-iter contamination).
# Attributes tasks to their SQL execution (JobStart 'spark.sql.execution.id' -> stageId -> TaskEnd), sums
# scan-node metrics per query execution, drops iter1 (cold), averages iters 2..N (warm at the converged split).
import json, glob, sys, re
from collections import defaultdict
TARGET=1024**3
def pv(v):
    if v is None: return None
    s=str(v)
    if ":" in s:
        try: h,m,r=s.split(":"); return int((int(h)*3600+int(m)*60+float(r))*1e9)
        except: return None
    try: return int(s)
    except: return None
SCAN={"scan time","GPU decode time","output columnar batches","sum of output GPU batch bytes",
      "maximum output GPU batch bytes per task"}
def parse(path):
    scan_acc={}                 # accId -> metric name (scan-node metrics)
    exec_of_scanacc={}          # accId -> execId
    stage_exec={}               # stageId -> execId
    exec_has_scan=set()         # execIds whose plan has a GpuScan
    order=[]                    # query execIds in start order
    per_exec=defaultdict(lambda: defaultdict(int))   # execId -> metric -> sum (scan by accId)
    per_exec_max=defaultdict(int)                     # execId -> max batch
    per_exec_gpu=defaultdict(int)                     # execId -> gpuTime (via stage)
    per_exec_tasks=defaultdict(int)                   # execId -> scan-stage task count
    scan_stage=set()            # stageIds that carry scan tasks (have a scan accId update)
    def walk(n, eid, hasscan):
        nm=n.get("nodeName","")
        if "Scan" in nm:
            hasscan[0]=True
            for me in n.get("metrics",[]):
                if me.get("name") in SCAN:
                    aid=me["accumulatorId"]; scan_acc[aid]=me["name"]; exec_of_scanacc[aid]=eid
        for c in n.get("children",[]): walk(c, eid, hasscan)
    for line in open(path):
        if 'SparkListenerSQLExecutionStart' in line:   # NOTE: bare name (log has ...ui.<name>, no leading quote)
            e=json.loads(line); eid=e["executionId"]; hs=[False]
            if e.get("sparkPlanInfo"): walk(e["sparkPlanInfo"], eid, hs)
            if hs[0] and eid not in exec_has_scan: exec_has_scan.add(eid); order.append(eid)
        elif 'SparkListenerSQLAdaptiveExecutionUpdate' in line:
            # the final (post-AQE) plan carries the GpuScan accumulator ids
            e=json.loads(line); eid=e.get("executionId"); hs=[False]
            if eid is not None and e.get("sparkPlanInfo"):
                walk(e["sparkPlanInfo"], eid, hs)
                if hs[0] and eid not in exec_has_scan:
                    exec_has_scan.add(eid); order.append(eid)
        elif 'SparkListenerJobStart' in line:
            e=json.loads(line); props=e.get("Properties",{})
            eid=props.get("spark.sql.execution.id")
            if eid is not None:
                eid=int(eid)
                for sid in e.get("Stage IDs",[]): stage_exec[sid]=eid
        elif '"Accumulables"' in line and "SparkListenerTaskEnd" in line:
            e=json.loads(line); sid=e.get("Stage ID"); eid=stage_exec.get(sid)
            is_scan_task=False
            for a in e.get("Task Info",{}).get("Accumulables",[]):
                aid=a.get("ID"); nm=a.get("Name")
                if aid in scan_acc:
                    v=pv(a.get("Update"));
                    if v is None: continue
                    is_scan_task=True
                    ex=exec_of_scanacc[aid]
                    mn=scan_acc[aid]
                    if mn=="maximum output GPU batch bytes per task": per_exec_max[ex]=max(per_exec_max[ex],v)
                    else: per_exec[ex][mn]+=v
                elif nm=="gpuTime" and eid is not None:
                    v=pv(a.get("Update"));
                    if v is not None: per_exec_gpu[eid]+=v
            if is_scan_task and eid is not None: per_exec_tasks[eid]+=1
    # keep only execs that actually produced GPU scan output (the 5 query iters; drops CreateView/explain)
    q=[e for e in order if per_exec[e].get("output columnar batches",0)>0]
    warm=q[1:] if len(q)>1 else q   # drop iter1 = COLD_START (autotuner has no memory yet -> default split)
    n=len(warm)
    agg=defaultdict(int); mx=0; gpu=0; tasks=0
    for e in warm:
        for k,v in per_exec[e].items(): agg[k]+=v
        mx=max(mx, per_exec_max[e]); gpu+=per_exec_gpu[e]; tasks+=per_exec_tasks[e]
    nb=agg.get("output columnar batches",0); ob=agg.get("sum of output GPU batch bytes",0)
    return dict(iters=n, tasks_per_it=tasks/n if n else 0,
        avg=ob/nb if nb else 0, maxb=mx,
        decode_per_it=agg.get("GPU decode time",0)/1e9/n if n else 0,
        scan_per_it=agg.get("scan time",0)/1e9/n if n else 0,
        gpu_per_it=gpu/1e9/n if n else 0)

print(f"{'run':14s}|{'warmIt':>6s}|{'tasks/it':>8s}|{'avgBatch':>9s}|{'%full':>6s}|{'maxBatch':>9s}|{'scanTime/it':>11s}|{'decode/it':>9s}|{'gpuTime/it':>10s}")
for arm in sys.argv[1:]:
    els=[e for e in glob.glob(f"data/overture-{arm}/el/*") if "inprogress" not in e]
    if not els: print(arm,"no el"); continue
    r=parse(els[0])
    print(f"{arm:14s}|{r['iters']:6d}|{r['tasks_per_it']:8.0f}|{r['avg']/2**20:8.0f}M|{r['avg']/TARGET*100:5.0f}%|{r['maxb']/2**20:8.0f}M|{r['scan_per_it']:10.1f}s|{r['decode_per_it']:8.1f}s|{r['gpu_per_it']:9.1f}s")
