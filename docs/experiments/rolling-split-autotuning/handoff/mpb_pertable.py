#!/usr/bin/env python3
# Per-TABLE scan cost vs split across the maxPartitionBytes configs (local; reads event logs only).
# For each config, attribute scan-node metrics to their table (from the scan simpleString path) by
# accumulator id, summed over the whole run. Also read the effective maxSplitBytes per table from the
# scanMaxSplitBytes driver accumulator. Emits, per big table, the split + scan time + GPU decode +
# batch fullness at each config, so we can read off which split minimizes each table's scan cost.
import json, os, re, sys
from collections import defaultdict

CFGS=["128m","256m","512m","1g","2g","4g"]
BASE="/home/kuhu/Reps/spark-rapids/data"
BIG=["store_sales","catalog_sales","web_sales","store_returns","catalog_returns","web_returns",
     "inventory","catalog_page","customer","date_dim"]
SCANM={"scan time","GPU decode time","output columnar batches","sum of output GPU batch bytes"}
NDS=["store_sales","store_returns","catalog_sales","catalog_returns","web_sales","web_returns",
     "inventory","catalog_page","customer_address","customer_demographics","customer","date_dim",
     "time_dim","item","store","call_center","web_site","web_page","warehouse","ship_mode","reason",
     "income_band","household_demographics","promotion"]
def norm(t):
    # plan strings truncate, so t may be a prefix ("store_sa") or full name. Resolve to the unique
    # NDS table; keep raw if ambiguous (e.g. "catalog_" -> sales/returns/page).
    if t in NDS: return t
    c=[x for x in NDS if x.startswith(t)]
    return c[0] if len(c)==1 else t

def parse(path):
    acc_table={}   # accId -> table
    acc_metric={}  # accId -> metric name
    split_acc={}   # scanMaxSplitBytes accId -> table
    driver={}      # accId -> value
    sums=defaultdict(lambda: defaultdict(int))  # table -> metric -> sum
    def walk(n):
        if "Scan" in n.get("nodeName",""):
            m=re.search(r'parquet_sf3k[^/\]]*/([a-z_0-9]+)', n.get("simpleString",""))
            tbl=m.group(1) if m else None
            if tbl:
                tbl=norm(tbl)
                for me in n.get("metrics",[]):
                    nm=me.get("name")
                    if nm in SCANM:
                        acc_table[me.get("accumulatorId")]=tbl; acc_metric[me.get("accumulatorId")]=nm
                    elif nm=="scan max split bytes (effective)":
                        split_acc[me.get("accumulatorId")]=tbl
        for c in n.get("children",[]): walk(c)
    with open(path) as f:
        for line in f:
            if 'SparkListenerSQLExecutionStart' in line or 'SQLAdaptiveExecutionUpdate' in line:
                e=json.loads(line)
                if e.get("sparkPlanInfo"): walk(e["sparkPlanInfo"])
            elif '"Accumulables"' in line and 'SparkListenerTaskEnd' in line:
                e=json.loads(line)
                for a in e.get("Task Info",{}).get("Accumulables",[]):
                    aid=a.get("ID")
                    if aid in acc_table:
                        v=a.get("Update")
                        if v is None: continue
                        s=str(v)
                        if ":" in s:
                            try: h,mn,r=s.split(":"); val=int((int(h)*3600+int(mn)*60+float(r))*1e9)
                            except: continue
                        else:
                            try: val=int(v)
                            except: continue
                        sums[acc_table[aid]][acc_metric[aid]]+=val
            elif '"accumUpdates"' in line and 'DriverAccumUpdates' in line:
                e=json.loads(line)
                for p in e.get("accumUpdates",[]):
                    if isinstance(p,list) and len(p)==2: driver[p[0]]=max(driver.get(p[0],0),p[1])
    split={}
    for aid,tbl in split_acc.items():
        if aid in driver: split[tbl]=max(split.get(tbl,0), int(driver[aid]))
    return sums, split

allcfg={}
for c in CFGS:
    allcfg[c]=parse(f"{BASE}/mpb-{c}-results/eventlog-test-1")
    print(f"parsed {c}", file=sys.stderr)

for tbl in BIG:
    print(f"\n### {tbl}")
    print(f"{'cfg':6s}{'split':>9s}{'scan_s':>9s}{'decode_s':>9s}{'batches':>10s}{'avgMiB':>8s}{'%full':>7s}")
    for c in CFGS:
        sums,split=allcfg[c]
        s=sums.get(tbl,{}); sp=split.get(tbl,0)
        st=s.get("scan time",0)/1e9; de=s.get("GPU decode time",0)/1e9
        nb=s.get("output columnar batches",0); ob=s.get("sum of output GPU batch bytes",0)
        avg=ob/nb/2**20 if nb else 0; pf=ob/nb/2**30*100 if nb else 0
        print(f"{c:6s}{sp/2**20:8.0f}M{st:9.0f}{de:9.0f}{nb:10.0f}{avg:8.0f}{pf:7.1f}")
