#!/usr/bin/env python3
# Warm-only metrics for the RW2 benchmark. ONE run dir = ONE query x N iters (one-query-per-session, per
# BENCHMARK-METHOD.md). Attributes tasks to their SQL execution, drops iter1 (cold / COLD_START), averages warm
# iters 2..N. Emits: tasks / avg+max batch / scan time / GPU decode / gpuTime / byte-skew + wall (RW2_ITER lines).
# Usage: rw2_warm_parse.py <iters> <run_dir> [<run_dir> ...]
import json, glob, sys, re, statistics
from collections import defaultdict
TARGET=1024**3
SCAN={"scan time","GPU decode time","output columnar batches","sum of output GPU batch bytes",
      "maximum output GPU batch bytes per task"}
def pv(v):
    if v is None: return None
    s=str(v)
    if ":" in s:
        try: h,m,r=s.split(":"); return int((int(h)*3600+int(m)*60+float(r))*1e9)
        except: return None
    try: return int(s)
    except: return None

def wall_warm(run_log):
    its=[]
    for line in open(run_log,encoding="utf-8",errors="replace"):
        m=re.search(r"(?:RW2|GF)_ITER \w+ (\d+) (\d+)", line)
        if m: its.append((int(m.group(1)),int(m.group(2))))
    its=sorted(its); warm=[ms for i,ms in its if i>=2]
    return statistics.mean(warm) if warm else (its[0][1] if its else 0)

def parse(rundir):
    el=[e for e in glob.glob(f"{rundir}/el/*") if "inprogress" not in e]
    if not el: return None
    path=el[0]
    scan_acc={}; exec_of_scanacc={}; stage_exec={}; order=[]; seen=set()
    per_exec=defaultdict(lambda: defaultdict(int)); per_exec_max=defaultdict(int)
    per_exec_gpu=defaultdict(int); per_exec_tasks=defaultdict(int); per_exec_bytes=defaultdict(list)
    def walk(n,eid):
        if "Scan" in n.get("nodeName",""):
            for me in n.get("metrics",[]):
                if me.get("name") in SCAN:
                    aid=me["accumulatorId"]; scan_acc[aid]=me["name"]; exec_of_scanacc[aid]=eid
        for c in n.get("children",[]): walk(c,eid)
    for line in open(path,encoding="utf-8",errors="replace"):
        if 'SparkListenerSQLExecutionStart' in line:
            e=json.loads(line); eid=e["executionId"]
            if e.get("sparkPlanInfo"): walk(e["sparkPlanInfo"],eid)
            if eid not in seen: seen.add(eid); order.append(eid)
        elif 'SparkListenerSQLAdaptiveExecutionUpdate' in line:
            e=json.loads(line); eid=e.get("executionId")
            if eid is not None and e.get("sparkPlanInfo"):
                walk(e["sparkPlanInfo"],eid)
                if eid not in seen: seen.add(eid); order.append(eid)
        elif 'SparkListenerJobStart' in line:
            e=json.loads(line); eid=e.get("Properties",{}).get("spark.sql.execution.id")
            if eid is not None:
                eid=int(eid)
                for sid in e.get("Stage IDs",[]): stage_exec[sid]=eid
        elif '"Accumulables"' in line and "SparkListenerTaskEnd" in line:
            e=json.loads(line); sid=e.get("Stage ID"); eid=stage_exec.get(sid)
            rb=e.get("Task Metrics",{}).get("Input Metrics",{}).get("Bytes Read")
            is_scan=False
            for a in e.get("Task Info",{}).get("Accumulables",[]):
                aid=a.get("ID"); nm=a.get("Name")
                if aid in scan_acc:
                    v=pv(a.get("Update"))
                    if v is None: continue
                    is_scan=True; ex=exec_of_scanacc[aid]; mn=scan_acc[aid]
                    if mn=="maximum output GPU batch bytes per task": per_exec_max[ex]=max(per_exec_max[ex],v)
                    else: per_exec[ex][mn]+=v
                elif nm=="gpuTime" and eid is not None:
                    v=pv(a.get("Update"))
                    if v is not None: per_exec_gpu[eid]+=v
            if is_scan and eid is not None:
                per_exec_tasks[eid]+=1
                if rb: per_exec_bytes[eid].append(rb)
    qexecs=[e for e in order if per_exec[e].get("output columnar batches",0)>0]
    warm=qexecs[1:] if len(qexecs)>1 else qexecs   # drop iter1 cold
    n=len(warm); agg=defaultdict(int); mx=0; gpu=0; tasks=0; allbytes=[]
    for e in warm:
        for k,v in per_exec[e].items(): agg[k]+=v
        mx=max(mx,per_exec_max[e]); gpu+=per_exec_gpu[e]; tasks+=per_exec_tasks[e]; allbytes+=per_exec_bytes[e]
    nb=agg.get("output columnar batches",0); ob=agg.get("sum of output GPU batch bytes",0)
    scanned=sum(allbytes)/n if n else 0   # total bytes read off disk per iter (scan volume)
    allbytes.sort()
    skew=(allbytes[-1]/allbytes[len(allbytes)//2]) if allbytes else 0
    return dict(iters=n, tasks=tasks/n if n else 0, avg=ob/nb if nb else 0, maxb=mx,
        decode=agg.get("GPU decode time",0)/1e9/n if n else 0,
        scan=agg.get("scan time",0)/1e9/n if n else 0, gpu=gpu/1e9/n if n else 0, skew=skew, scanned=scanned)

if __name__=="__main__":
    iters=int(sys.argv[1]); dirs=sys.argv[2:]
    print(f"{'rundir':26s}|{'wIt':>3s}|{'tasks':>6s}|{'skew':>5s}|{'scanned':>8s}|{'avgB':>6s}|{'scan':>6s}|{'decode':>7s}|{'gpuTime':>8s}|{'wall':>6s}")
    for d in dirs:
        tag=d.rstrip("/").split("/")[-1].replace("overture-rw2-","")
        r=parse(d)
        if not r: print(f"{tag:26s}| no el"); continue
        w=wall_warm(f"{d}/run.log")
        print(f"{tag:26s}|{r['iters']:3d}|{r['tasks']:6.0f}|{r['skew']:4.2f}x|{r['scanned']/2**30:7.2f}G|{r['avg']/2**20:5.0f}M|{r['scan']:5.1f}s|{r['decode']:6.1f}s|{r['gpu']:7.1f}s|{w:5.0f}")
