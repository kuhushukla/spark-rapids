#!/usr/bin/env python3
# Parse one maxPartitionBytes config's ab output (event log + per-query CSV) into a metrics summary.
# Efficient: string-prefilters each event-log line before json.loads (the logs are ~50-100 MB).
#
# Emits, per config:
#   - per-query runtime: cold (iter1) and warm (mean of iters 2-5), from the per-query CSV
#   - config totals from the event log task accumulables (summed across all tasks/iters):
#       scan time, gpuTime (semaphore-holding = GPU-active), gpuSemaphoreWait, op time,
#       op time (excl. SemWait), numOutputBatches, outputBatchBytes
#   - per-table effective maxSplitBytes: SparkPlanInfo scan node (table from simpleString) joined to
#       the scanMaxSplitBytes driver-accum value (SparkListenerDriverAccumUpdates)
# Usage: mpb_parse.py <results_dir>   e.g. data/mpb-128m-results
import json, sys, os, csv, re

TARGET = 1024**3  # 1 GiB GPU target batch

NDS_TABLES = ["store_sales","store_returns","catalog_sales","catalog_returns","web_sales",
    "web_returns","inventory","customer_address","customer_demographics","customer","date_dim",
    "time_dim","item","store","call_center","catalog_page","web_site","web_page","warehouse",
    "ship_mode","reason","income_band","household_demographics","promotion","dbgen_version"]

def norm_table(t):
    # Spark truncates plan strings, so t may be a prefix (e.g. "store_sa"). Resolve to the unique
    # NDS table it prefixes; if ambiguous (e.g. "catalog_") or unknown, keep as-is with a '?'.
    if t in NDS_TABLES: return t
    matches = [x for x in NDS_TABLES if x.startswith(t)]
    if len(matches) == 1: return matches[0]
    if len(matches) > 1: return t + "?(ambig)"
    return t

def parse_val(v):
    # accumulable Update is either a numeric (nanos/bytes) or a formatted time "HH:MM:SS.mmm" -> nanos
    if v is None: return None
    if isinstance(v, (int, float)): return int(v)
    s = str(v)
    if ":" in s:
        try:
            hh, mm, rest = s.split(":")
            return int((int(hh)*3600 + int(mm)*60 + float(rest)) * 1e9)
        except Exception:
            return None
    try: return int(s)
    except Exception: return None

def load_csv_times(d):
    # per-query CSV: rows (app_id, query, time_ms) in iteration order (5 per query)
    path = None
    for fn in os.listdir(d):
        if fn.endswith("-test-1.csv"):
            path = os.path.join(d, fn)
    times = {}  # query -> [t1..t5]
    if not path: return times
    with open(path) as f:
        for row in csv.reader(f):
            if len(row) < 3: continue
            q = row[1].strip()
            try: t = float(row[2])
            except: continue
            if not q.startswith("query"): continue
            times.setdefault(q, []).append(t)
    return times

def parse_eventlog(path):
    name_sum = {}          # task/all-op metric name -> summed Update (gpuTime, op time, ...)
    plan_split = {}        # table -> maxSplitBytes (from plan+driver accum)
    acc_to_table = {}      # scanMaxSplitBytes accId -> table
    scan_acc = {}          # accId -> metric name, ONLY for metrics on scan nodes (batch/scan-time)
    scan_sum = {}          # scan metric name -> summed Update (scan-node only)
    driver_acc = {}        # accId -> value (driver updates)
    scan_stage_ids = set() # stage IDs whose RDD scopes contain a *Scan* operator
    scan_stage_gpu = [0]   # summed gpuTime over tasks in scan-containing stages (mutable box)
    # task/all-operator totals (summed by metric NAME across the whole plan):
    WANT = {"gpuTime","gpuSemaphoreWait","op time","op time (excl. SemWait)"}
    # scan-node-specific metrics (summed by accId, scan nodes only):
    SCAN_METRICS = {"scan time","output columnar batches","sum of output GPU batch bytes",
                    "GPU decode time","op time","buffer time"}
    def walk(n):
        nm = n.get("nodeName","")
        if "Scan" in nm:
            simple = n.get("simpleString","")
            loc = re.search(r'parquet_sf3k[^/\]]*/([a-z_0-9]+)', simple)
            tbl = loc.group(1) if loc else None
            for me in n.get("metrics",[]):
                mn = me.get("name"); aid = me.get("accumulatorId")
                if mn == "scan max split bytes (effective)" and tbl:
                    acc_to_table[aid] = tbl
                elif mn in SCAN_METRICS:
                    scan_acc[aid] = mn
        for c in n.get("children",[]): walk(c)
    with open(path) as f:
        for line in f:
            if '"Event":"SparkListenerStageSubmitted"' in line and 'Scan' in line:
                e = json.loads(line)
                si = e.get("Stage Info",{})
                ops = set()
                for r in si.get("RDD Info",[]):
                    sc = r.get("Scope"); nm2 = None
                    if sc:
                        try: nm2 = json.loads(sc).get("name")
                        except: nm2 = None
                    nm2 = nm2 or r.get("Name")
                    if nm2: ops.add(nm2)
                if any("Scan" in o for o in ops):
                    scan_stage_ids.add(si.get("Stage ID"))
            elif '"Accumulables"' in line and '"SparkListenerTaskEnd"' in line:
                e = json.loads(line)
                sid = e.get("Stage ID")
                in_scan_stage = sid in scan_stage_ids
                for a in e.get("Task Info",{}).get("Accumulables",[]):
                    nm = a.get("Name"); aid = a.get("ID")
                    if nm in WANT:
                        v = parse_val(a.get("Update"))
                        if v is not None: name_sum[nm] = name_sum.get(nm,0) + v
                        if nm == "gpuTime" and in_scan_stage and v is not None:
                            scan_stage_gpu[0] += v
                    if aid in scan_acc:
                        v = parse_val(a.get("Update"))
                        if v is not None:
                            k = scan_acc[aid]; scan_sum[k] = scan_sum.get(k,0) + v
            elif '"sparkPlanInfo"' in line:
                e = json.loads(line)
                pi = e.get("sparkPlanInfo")
                if pi: walk(pi)
            elif '"accumUpdates"' in line and 'DriverAccumUpdates' in line:
                e = json.loads(line)
                for pair in e.get("accumUpdates", []):
                    if isinstance(pair, list) and len(pair)==2:
                        driver_acc[pair[0]] = max(driver_acc.get(pair[0],0), pair[1])
    for accId, tbl in acc_to_table.items():
        if accId in driver_acc:
            t = norm_table(tbl)
            plan_split[t] = max(plan_split.get(t,0), int(driver_acc[accId]))
    return name_sum, scan_sum, plan_split, scan_stage_gpu[0]

def main():
    d = sys.argv[1]
    cfg = os.path.basename(d).replace("mpb-","").replace("-results","")
    times = load_csv_times(d)
    ns, scan, split, scan_stage_gpu = parse_eventlog(os.path.join(d,"eventlog-test-1"))
    cold = sum(v[0] for v in times.values() if v)
    warm = sum(sum(v[1:])/len(v[1:]) for v in times.values() if len(v)>1)
    print(f"##### {cfg} #####")
    print(f"queries: {len(times)}  | COLD total {cold/1000:.1f}s  | WARM total {warm/1000:.1f}s")
    def s(n): return ns.get(n,0)/1e9
    def sc(n): return scan.get(n,0)/1e9
    print("-- SCAN-NODE only (attributed by accId) --")
    print(f"  scan time         : {sc('scan time'):9.1f} s")
    print(f"  GPU decode time   : {sc('GPU decode time'):9.1f} s")
    print(f"  buffer time       : {sc('buffer time'):9.1f} s")
    print(f"  op time (scan node): {sc('op time'):9.1f} s")
    print("-- SCAN-STAGE (tasks in scan-containing stages; incl. filter/project/join in-stage) --")
    print(f"  gpuTime (scan-stage): {scan_stage_gpu/1e9:9.1f} s")
    print("-- WHOLE-QUERY (all tasks / all ops) --")
    print(f"  gpuTime (GPU-active): {s('gpuTime'):9.1f} s")
    print(f"  gpuSemaphoreWait    : {s('gpuSemaphoreWait'):9.1f} s")
    print(f"  op time (all ops)   : {s('op time'):9.1f} s")
    print(f"  op time (excl. Sem) : {s('op time (excl. SemWait)'):9.1f} s")
    nb = scan.get("output columnar batches",0); ob = scan.get("sum of output GPU batch bytes",0)
    if nb:
        print(f"SCAN out batches: {nb}   avg {ob/nb/2**20:.0f} MiB  ({ob/nb/TARGET*100:.0f}% of 1GiB target)")
    print("per-table effective maxSplitBytes:")
    for tbl,v in sorted(split.items(), key=lambda x:-x[1])[:14]:
        print(f"   {tbl:26s} {v/2**20:8.1f} MiB")

if __name__ == "__main__":
    main()
