#!/usr/bin/env python3
# Per-QUERY breakdown for one maxPartitionBytes config. Attributes event-log task metrics to the
# specific query+iteration via: SQLExecutionStart.description == query name; JobStart Properties
# spark.sql.execution.id -> Stage IDs -> execId; scan-node metric accIds mapped per execution
# (initial plan + AQE plan updates). Cold = each query's first (smallest) execId; warm = mean of the
# rest. Writes data/mpb-perquery-<cfg>.csv and prints validation totals.
# Usage: mpb_perquery.py <results_dir>
import json, sys, os, csv, re
from collections import defaultdict

TARGET = 1024**3

def parse_val(v):
    if v is None: return None
    if isinstance(v,(int,float)): return int(v)
    s=str(v)
    if ":" in s:
        try:
            hh,mm,rest=s.split(":"); return int((int(hh)*3600+int(mm)*60+float(rest))*1e9)
        except: return None
    try: return int(s)
    except: return None

def load_csv(d):
    path=None
    for fn in os.listdir(d):
        if fn.endswith("-test-1.csv"): path=os.path.join(d,fn)
    t=defaultdict(list)
    if path:
        with open(path) as f:
            for row in csv.reader(f):
                if len(row)<3: continue
                q=row[1].strip()
                if not q.startswith("query"): continue
                try: t[q].append(float(row[2]))
                except: pass
    return t

# scan-ONLY metric names (emitted only by scan operators) -> sum by NAME via stage->execId (robust
# to AQE/subquery accId churn). batches/bytes names appear on every operator, so those must be
# attributed by the scan node's accId instead.
SCAN_NAME_METRICS={"scan time","GPU decode time","buffer time"}
SCAN_ACC_METRICS={"output columnar batches","sum of output GPU batch bytes"}

def parse(path):
    # scanacc maps a scan-node metric accId -> metric NAME, harvested from EVERY plan (main query,
    # AQE re-plans, and scalar-subquery executions). Each accId is then attributed to a query by the
    # TASK's stage -> execId (JobStart), NOT by which plan the accId came from. This is robust to AQE
    # accId churn and to subquery scans running under their own execution.
    exec_query={}
    stage_exec={}
    scanacc={}                     # accId -> metric name  (scan-node batch/bytes accIds, any plan)
    scan_stage_ids=set()
    exec_gpu=defaultdict(int)      # execId -> whole-query task gpuTime
    exec_ssgpu=defaultdict(int)    # execId -> scan-stage task gpuTime
    exec_scan=defaultdict(lambda: defaultdict(int))  # execId -> scan metric -> sum (name & acc mixed)
    exec_scanmax=defaultdict(lambda:[0,0])  # execId -> [sum per-task MAX batches, MAX bytes] over scan stages
                                             # (fallback batch count for subquery scans; scan is the biggest
                                             #  batch emitter in a stage that has no Expand/broadcast explosion)
    def walk(n):
        if "Scan" in n.get("nodeName",""):
            for me in n.get("metrics",[]):
                if me.get("name") in SCAN_ACC_METRICS:
                    scanacc[me.get("accumulatorId")]=me.get("name")
        for c in n.get("children",[]): walk(c)
    with open(path) as f:
        for line in f:
            if 'SparkListenerSQLExecutionStart' in line:
                e=json.loads(line); desc=e.get("description","")
                if re.fullmatch(r'query\d+(_part\d+)?', desc): exec_query[e.get("executionId")]=desc
                if e.get("sparkPlanInfo"): walk(e["sparkPlanInfo"])
            elif 'SQLAdaptiveExecutionUpdate' in line:
                e=json.loads(line)
                if e.get("sparkPlanInfo"): walk(e["sparkPlanInfo"])
            elif '"Event":"SparkListenerJobStart"' in line:
                e=json.loads(line); eid=e.get("Properties",{}).get("spark.sql.execution.id")
                if eid is not None:
                    eid=int(eid)
                    for sid in e.get("Stage IDs",[]): stage_exec[sid]=eid
            elif 'SparkListenerStageSubmitted' in line and 'Scan' in line:
                e=json.loads(line); si=e.get("Stage Info",{}); ops=set()
                for r in si.get("RDD Info",[]):
                    sc=r.get("Scope"); nm2=None
                    if sc:
                        try: nm2=json.loads(sc).get("name")
                        except: nm2=None
                    nm2=nm2 or r.get("Name")
                    if nm2: ops.add(nm2)
                if any("Scan" in o for o in ops): scan_stage_ids.add(si.get("Stage ID"))
            elif '"Accumulables"' in line and 'SparkListenerTaskEnd' in line:
                e=json.loads(line); sid=e.get("Stage ID"); eid=stage_exec.get(sid)
                if eid is None: continue
                in_scan=sid in scan_stage_ids
                tmax_b=0; tmax_by=0                            # per-task max batches/bytes (scan-stage)
                for a in e.get("Task Info",{}).get("Accumulables",[]):
                    nm=a.get("Name"); aid=a.get("ID")
                    if nm=="gpuTime":
                        v=parse_val(a.get("Update"))
                        if v is not None:
                            exec_gpu[eid]+=v
                            if in_scan: exec_ssgpu[eid]+=v
                    elif nm in SCAN_NAME_METRICS:            # scan-only names: attribute by name
                        v=parse_val(a.get("Update"))
                        if v is not None: exec_scan[eid][nm]+=v
                    elif nm=="output columnar batches" and in_scan:
                        tmax_b=max(tmax_b, parse_val(a.get("Update")) or 0)
                    elif nm=="sum of output GPU batch bytes" and in_scan:
                        tmax_by=max(tmax_by, parse_val(a.get("Update")) or 0)
                    if aid in scanacc:                        # batches/bytes: by scan-node accId
                        v=parse_val(a.get("Update"))
                        if v is not None: exec_scan[eid][scanacc[aid]]+=v
                if in_scan:
                    exec_scanmax[eid][0]+=tmax_b; exec_scanmax[eid][1]+=tmax_by
    return exec_query, exec_gpu, exec_ssgpu, exec_scan, exec_scanmax

def main():
    d=sys.argv[1]; cfg=os.path.basename(d).replace("mpb-","").replace("-results","")
    times=load_csv(d)
    exec_query,exec_gpu,exec_ssgpu,exec_scan,exec_scanmax=parse(os.path.join(d,"eventlog-test-1"))
    byq=defaultdict(list)
    for eid,q in exec_query.items(): byq[q].append(eid)
    rows=[]
    for q,eids in byq.items():
        eids=sorted(eids); cold_e=eids[0]; warm_e=eids[1:]
        ct=times.get(q,[]);
        cold_ms = ct[0] if ct else 0
        warm_ms = sum(ct[1:])/len(ct[1:]) if len(ct)>1 else 0
        def wmean(fn): return sum(fn(e) for e in warm_e)/len(warm_e) if warm_e else 0
        scan_s   = wmean(lambda e: exec_scan[e].get("scan time",0))/1e9
        decode_s = wmean(lambda e: exec_scan[e].get("GPU decode time",0))/1e9
        ss_gpu   = wmean(lambda e: exec_ssgpu.get(e,0))/1e9
        wq_gpu   = wmean(lambda e: exec_gpu.get(e,0))/1e9
        nb_acc = wmean(lambda e: exec_scan[e].get("output columnar batches",0))
        ob_acc = wmean(lambda e: exec_scan[e].get("sum of output GPU batch bytes",0))
        nb_max = wmean(lambda e: exec_scanmax[e][0])
        ob_max = wmean(lambda e: exec_scanmax[e][1])
        # Batch count: rigorous accId is correct when it captured the scan (incl. Expand/broadcast
        # queries like query28). When accId undercounts (subquery scans -> nb<=2 despite real scan),
        # fall back to per-task-max over scan stages (scan is the biggest emitter when no exploding op).
        if nb_acc > 2:
            nb, ob = nb_acc, ob_acc
        elif scan_s > 1 and nb_max > 2:
            nb, ob = nb_max, ob_max
        else:
            nb, ob = nb_acc, ob_acc
        avg_mib = (ob/nb/2**20) if nb else 0
        pct = (ob/nb/TARGET*100) if nb else 0
        # explosion guard: if the FALLBACK (accId undercounted) yields an implausibly tiny avg batch,
        # a downstream operator exploded the count -> mark n/a rather than assert a wrong number.
        if nb_acc <= 2 and nb > 2 and avg_mib < 8:
            nb, avg_mib, pct = -1, -1, -1
        rows.append([q,round(cold_ms),round(warm_ms),round(scan_s,1),round(decode_s,1),
                     round(ss_gpu,1),round(wq_gpu,1),int(nb),round(avg_mib,1),round(pct,1)])
    rows.sort(key=lambda r:-r[2])
    out=f"{d}/../mpb-perquery-{cfg}.csv"
    with open(out,"w",newline="") as f:
        w=csv.writer(f)
        w.writerow(["query","cold_ms","warm_ms","scan_s","decode_s","scanstage_gpu_s",
                    "wq_gpu_s","scan_batches","avg_batch_mib","pct_target"])
        w.writerows(rows)
    # validation totals (compare to aggregate parser)
    print(f"##### {cfg} : {len(rows)} queries -> {out}")
    print(f"  WARM total {sum(r[2] for r in rows)/1000:.1f}s  scan {sum(r[3] for r in rows):.0f}s  "
          f"decode {sum(r[4] for r in rows):.0f}s  wq_gpu {sum(r[6] for r in rows):.0f}s (per-iter means)")

if __name__=="__main__": main()
