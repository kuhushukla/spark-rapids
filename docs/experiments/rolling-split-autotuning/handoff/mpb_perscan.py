#!/usr/bin/env python3
# Per-(query, table-scan) table for 2g / 4g / listed: effective split, batch fullness, decode time,
# plus the query's warm runtime. Reads event logs only (local). Attribution: SQLExecutionStart /
# AQE plan gives execId->query and each scan node's table (from the parquet path) + its metric accIds;
# task accumulables are summed by accId; scanMaxSplitBytes comes from DriverAccumUpdates. Values are
# the mean over the query's warm executions (iterations 2-5).
import json, os, re, sys, csv
from collections import defaultdict
RUNS={"2g":"data/mpb-2g-results","4g":"data/mpb-4g-results","listed":"data/mpb-listedfull-results"}
BASE="/home/kuhu/Reps/spark-rapids"
NDS=["store_sales","store_returns","catalog_sales","catalog_returns","web_sales","web_returns",
     "inventory","catalog_page","customer_address","customer_demographics","customer","date_dim",
     "time_dim","item","store","call_center","web_site","web_page","warehouse","ship_mode","reason",
     "income_band","household_demographics","promotion"]
def norm(t):
    if t in NDS: return t
    c=[x for x in NDS if x.startswith(t)]
    return c[0] if len(c)==1 else t
SCANM={"GPU decode time","op time","output columnar batches","sum of output GPU batch bytes"}
def parse_val(v):
    s=str(v)
    if ":" in s:
        try: h,m,r=s.split(":"); return int((int(h)*3600+int(m)*60+float(r))*1e9)
        except: return 0
    try: return int(v)
    except: return 0

def parse(path):
    exec_query={}
    node_acc=defaultdict(dict)    # (query,execId,table) -> {metric: accId}
    split_acc={}                  # accId -> (query,execId,table)
    accval=defaultdict(int)       # accId -> summed task value
    driver={}                     # accId -> value
    def walk(n, q, eid):
        if "Scan" in n.get("nodeName",""):
            m=re.search(r'parquet_sf3k[^/\]]*/([a-z_0-9]+)', n.get("simpleString",""))
            if m:
                tbl=norm(m.group(1))
                for me in n.get("metrics",[]):
                    nm=me.get("name"); aid=me.get("accumulatorId")
                    if nm in SCANM: node_acc[(q,eid,tbl)][nm]=aid
                    elif nm=="scan max split bytes (effective)": split_acc[aid]=(q,eid,tbl)
        for c in n.get("children",[]): walk(c,q,eid)
    with open(path) as f:
        for line in f:
            if 'SparkListenerSQLExecutionStart' in line or 'SQLAdaptiveExecutionUpdate' in line:
                e=json.loads(line); eid=e.get("executionId"); d=e.get("description","")
                if 'ExecutionStart' in line:
                    if re.fullmatch(r'query\d+(_part\d+)?',d): exec_query[eid]=d
                q=exec_query.get(eid)
                if q and e.get("sparkPlanInfo"): walk(e["sparkPlanInfo"], q, eid)
            elif '"Accumulables"' in line and 'SparkListenerTaskEnd' in line:
                e=json.loads(line)
                for a in e.get("Task Info",{}).get("Accumulables",[]):
                    aid=a.get("ID")
                    if aid is not None: accval[aid]+=parse_val(a.get("Update"))
            elif '"accumUpdates"' in line and 'DriverAccumUpdates' in line:
                e=json.loads(line)
                for p in e.get("accumUpdates",[]):
                    if isinstance(p,list) and len(p)==2: driver[p[0]]=max(driver.get(p[0],0),p[1])
    # group by (query,table): mean over warm execIds
    byq=defaultdict(list)
    for eid,q in exec_query.items(): byq[q].append(eid)
    out={}  # (query,table) -> dict of means
    keys=set((q,t) for (q,e,t) in node_acc)
    for (q,t) in keys:
        warm=sorted(byq[q])[1:]
        def wmean(getter):
            vals=[getter(e) for e in warm if (q,e,t) in node_acc or True]
            vals=[v for v in vals if v is not None]
            return sum(vals)/len(vals) if vals else 0
        def metric(e,nm):
            aid=node_acc.get((q,e,t),{}).get(nm)
            return accval.get(aid,0) if aid is not None else 0
        def splitv(e):
            for aid,(qq,ee,tt) in split_acc.items():
                if qq==q and ee==e and tt==t and aid in driver: return driver[aid]
            return 0
        dec=wmean(lambda e: metric(e,"GPU decode time"))/1e9
        op=wmean(lambda e: metric(e,"op time"))/1e9
        nb=wmean(lambda e: metric(e,"output columnar batches"))
        ob=wmean(lambda e: metric(e,"sum of output GPU batch bytes"))
        sp=wmean(splitv)
        out[(q,t)]=dict(split=sp, decode_s=dec, op_s=op, batches=nb,
                        avg_mib=(ob/nb/2**20 if nb else 0), pct=(ob/nb/2**30*100 if nb else 0))
    return out

data={}
for r,d in RUNS.items():
    data[r]=parse(f"{BASE}/{d}/eventlog-test-1"); print("parsed",r,file=sys.stderr)

# per-query warm runtime
def warm_csv(run):
    import glob
    p=glob.glob(f"{BASE}/{RUNS[run]}/*-test-1.csv")[0]
    t=defaultdict(list)
    for row in csv.reader(open(p)):
        if len(row)<3 or not row[1].strip().startswith("query"): continue
        try: t[row[1].strip()].append(float(row[2]))
        except: pass
    return {q:sum(v[1:])/len(v[1:]) for q,v in t.items() if len(v)>1}
warm={r:warm_csv(r) for r in RUNS}

# write full CSV
allkeys=sorted(set().union(*[set(data[r]) for r in RUNS]))
outcsv=f"{BASE}/data/mpb-perscan-2g-4g-listed.csv"
with open(outcsv,"w",newline="") as fh:
    w=csv.writer(fh)
    w.writerow(["query","table","warm2g_ms","warm_listed_ms","qtime_x",
        "split2g_MiB","split4g_MiB","splitL_MiB","full2g%","full4g%","fullL%",
        "decode2g_s","decode4g_s","decodeL_s","scanOp2g_s","scanOp4g_s","scanOpL_s"])
    for (q,t) in allkeys:
        def g(r,k): return data[r].get((q,t),{}).get(k,0)
        w2=warm["2g"].get(q,0); wl=warm["listed"].get(q,0)
        w.writerow([q,t,round(w2),round(wl),round(w2/wl,2) if wl else 0,
            round(g("2g","split")/2**20),round(g("4g","split")/2**20),round(g("listed","split")/2**20),
            round(g("2g","pct")),round(g("4g","pct")),round(g("listed","pct")),
            round(g("2g","decode_s"),1),round(g("4g","decode_s"),1),round(g("listed","decode_s"),1),
            round(g("2g","op_s"),1),round(g("4g","op_s"),1),round(g("listed","op_s"),1)])
print("wrote",outcsv,f"({len(allkeys)} query-table scans)")
