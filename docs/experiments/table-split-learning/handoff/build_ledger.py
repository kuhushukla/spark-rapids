#!/usr/bin/env python3
"""Build the table-split-learning ledger: one row per (arm, query, iteration, table).

`learnt_from` names whichever query last WROTE the shared history file before this execution -- it is
reconstructed from run order, not observed. The plugin keys history on (table, columns, filters), so
the previous writer is not necessarily the record this scan read: two queries projecting different
columns get different keys and do not share a split. Read the label as "previous writer", and use
the split value itself as the evidence of what was actually applied.

Metric scoping follows the established schema:
  - gpuTime / semaphore wait / task time are per-TASK accumulators, SCAN TASKS ONLY
    (scan task = Input Metrics Bytes Read > 0). gpuTime is semaphoreHoldingTime, i.e. occupancy.
  - batch bytes and batch counts are node-scoped to GpuScan parquet, both from the same node.
  - split comes from the scan node's 'scan max split bytes' driver metric (matched by prefix, so
    logs written under the older '... (effective)' name still parse), which with AQE on is
    registered in the SQLAdaptiveExecutionUpdate plan, not the initial one. It is DEBUG-level, so
    every arm must run with spark.rapids.sql.metrics.level=DEBUG; an arm whose scans carry no
    split metric is an error, not a blank cell.

Cold (iteration 1) and warm (iterations 2..N) are kept in separate fields and never combined.
"""
import json, re, csv, glob, os, sys, collections, statistics as st

ROOT = sys.argv[1] if len(sys.argv) > 1 else "/data/table-split-learning-20260820a"
# Second arg = output path. Defaults to a per-run name so building one run's ledger never overwrites
# another's; results/ledger.tsv stays whatever was last built by hand.
RESULTS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "results")
OUT = (sys.argv[2] if len(sys.argv) > 2 else
       os.path.join(RESULTS, "ledger-%s.tsv" % os.path.basename(ROOT.rstrip("/")).replace(
           "table-split-learning-", "")))
# NDS query names come from the nds_power description; the local kit's bench scala files set no
# description, so the query falls back to the one the manifest records for the arm.
QRE = re.compile(r"(query\d+(?:_part\d+)?)")
TBL = re.compile(r"parquet_sf3k_decimal/([a-z_]+)")
# Path segments that name a format or a mount rather than a table, so they are never a table label.
GENERIC_SEG = {"parquet", "orc", "data", "warehouse", "hdfs", "user", "tmp", ""}
# GPU scan nodes carry Location only inside simpleString, where it is truncated at 8 characters
# ("store_sa", and "catalog_" is ambiguous). NDS column prefixes are unique per table, so resolve
# from the projected column list instead when the path is short.
PREFIX = {"ss_": "store_sales", "sr_": "store_returns", "cs_": "catalog_sales",
          "cr_": "catalog_returns", "ws_": "web_sales", "wr_": "web_returns",
          "inv_": "inventory", "ca_": "customer_address", "cd_": "customer_demographics",
          "hd_": "household_demographics", "cc_": "call_center", "cp_": "catalog_page",
          "web_": "web_site", "wp_": "web_page", "ib_": "income_band", "sm_": "ship_mode",
          "c_": "customer", "d_": "date_dim", "t_": "time_dim", "i_": "item", "s_": "store",
          "p_": "promotion", "r_": "reason", "w_": "warehouse"}
COLS = re.compile(r"\[([a-z_]+[0-9]*#)")


def table_of(node, default=None):
    """Table label for a scan node, dataset-agnostic.

    NDS keeps the table in the path (parquet_sf3k_decimal/<table>). Other datasets do not, and GPU
    scan nodes truncate Location at 8 characters inside simpleString, so three sources are tried in
    order of reliability: the full path, the NDS column prefix, then the last meaningful path
    segment. `default` (the dataset name) is the last resort for a single-table dataset like
    clickstream, whose path ends in .../parquet.
    """
    loc = str((node.get("metadata") or {}).get("Location", ""))
    m = TBL.search(loc)
    if m and len(m.group(1)) > 8:          # full path from metadata
        return m.group(1)
    c = COLS.search(node.get("simpleString") or "")
    if c:
        col = c.group(1)
        for pre in sorted(PREFIX, key=len, reverse=True):
            if col.startswith(pre):
                return PREFIX[pre]
    if m:
        return m.group(1)
    for seg in reversed(re.sub(r"[\[\]]", " ", loc).split("/")):
        seg = seg.strip().rstrip(",").split(" ")[0]
        if seg and seg.lower() not in GENERIC_SEG and not seg.startswith("file:"):
            return seg
    return default
M = 2 ** 20

# Metric names come from the kit so a plugin rename is a one-line change, not a hunt.
import importlib.util as _ilu
_emp = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                    "..", "..", "rolling-split-autotuning", "handoff", "eventlog_metrics.py")
_sp = _ilu.spec_from_file_location("eventlog_metrics", _emp)
EM = _ilu.module_from_spec(_sp); _sp.loader.exec_module(EM)
BATCH_BYTES = EM.BATCH_BYTES
BATCH_BYTES_ALIASES = EM.BATCH_BYTES_ALIASES

FATAL = ("pool allocation", "Exception in the executor plugin",
         "Could not find or load main class", "does not support Spark build")


def arm_health(arm_dir, expected):
    """Is this arm a FULLY successful run? Returns (ok, reason).

    An arm can produce numbers and still be untrustworthy: GPU contention killed the executor on
    sweep-csH3@W1-477m, iterations 1-2 never recorded, and the event log's first execution was a
    WARM run that the ledger then labelled cold. Every reported value must come from an arm that
    ran all its iterations with no fatal error.
    """
    log = os.path.join(arm_dir, "run.log")
    if not os.path.exists(log):
        return False, "no-run-log"
    body = open(log, errors="ignore").read()
    for f in FATAL:
        if f in body:
            return False, "fatal:" + f.split()[0]
    its = [int(m.group(1)) for m in re.finditer(r"ITER \S+ (\d+) \d+", body)]
    # An arm whose log was touched seconds ago is RUNNING, not broken. Reporting it as a failure
    # raises a red banner for an arm that is working fine.
    import time
    fresh = (time.time() - os.path.getmtime(log)) < 180
    if not its:
        return False, "in-progress" if fresh else "no-iterations"
    if len(its) != expected:
        return False, (f"in-progress ({len(its)}/{expected} iters)" if fresh
                       else f"iters={len(its)}/{expected}")
    if sorted(its) != list(range(1, expected + 1)):
        return False, f"non-contiguous:{sorted(its)}"
    return True, "ok"


def dur(x):
    if isinstance(x, str) and ":" in x:
        h, m, s = x.split(":")
        return int(h) * 3600 + int(m) * 60 + float(s)
    try:
        return float(x)
    except (TypeError, ValueError):
        return 0.0


def num(v):
    if isinstance(v, (int, float)):
        return float(v)
    m = re.search(r"\(\s*([0-9]+)\s*bytes\s*\)", str(v))
    if m:
        return float(m.group(1))
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def parse_arm(arm_dir, autotuner_on=True, writers=None, default_query=None, default_table=None,
              window="-"):
    el = os.path.join(arm_dir, "eventlog-test-1")
    if not os.path.exists(el):
        return []
    # The NDS driver puts the query name in every SQLExecutionStart description; the local kit's
    # bench scala files do not. Only fall back to the manifest's query when NOTHING in this log
    # carries a query name - otherwise the fallback would also label the setup executions (view
    # creation, schema reads) that carry no query name on purpose.
    named = any(QRE.search(json.loads(ln).get("description", "") or "")
                for ln in open(el, errors="ignore") if "SQLExecutionStart" in ln)
    qof, order = {}, []
    split_acc, scan_acc = {}, {}
    saw_gpu_scan = False
    for ln in open(el, errors="ignore"):
        spi = eid = None
        if "SQLExecutionStart" in ln:
            e = json.loads(ln); eid = e["executionId"]
            m = QRE.search(e.get("description", "") or "")
            q = m.group(1) if m else (None if named else default_query)
            if not q:
                continue
            qof[eid] = q
            order.append((e["time"], eid, q))
            spi = e.get("sparkPlanInfo")
        elif "SQLAdaptiveExecutionUpdate" in ln:
            e = json.loads(ln); eid = e.get("executionId"); spi = e.get("sparkPlanInfo")
        if not spi or eid not in qof:
            continue
        stk = [spi]
        while stk:
            x = stk.pop()
            nm = (x.get("nodeName") or "").strip()
            if "Scan" in nm:
                if nm.startswith("GpuScan"):
                    saw_gpu_scan = True
                tbl = table_of(x, default=default_table)
                for me in (x.get("metrics") or []):
                    n = str(me.get("name", ""))
                    if EM.is_split_metric(n):
                        split_acc[me["accumulatorId"]] = (eid, tbl)
                    elif nm == "GpuScan parquet" and (n in BATCH_BYTES_ALIASES or n in (
                            "output columnar batches",
                            "GPU decode time (excl. SemWait)", "scan time (excl. SemWait)",
                            "GPU decode time", "scan time")):
                        # prefer the excl. SemWait variants: they subtract semaphore queueing so a
                        # change reflects real work rather than waiting behind other tasks
                        scan_acc[me["accumulatorId"]] = (
                            eid, BATCH_BYTES if n in BATCH_BYTES_ALIASES else n)
            stk.extend(x.get("children") or [])

    # Fail on the first bad arm rather than emitting a ledger whose split column is silently "-".
    if saw_gpu_scan and not split_acc:
        raise SystemExit(
            f"{el}: GpuScan nodes carry no '{EM.SCAN_SPLIT_BYTES}' metric.\n"
            f"  Either the plugin jar predates the metric, or this arm did not run with\n"
            f"  --conf spark.rapids.sql.metrics.level=DEBUG (the metric is DEBUG-level only).\n"
            f"  The ledger's split column comes from it; refusing to build a ledger without it.")

    splits = collections.defaultdict(set)
    batch = collections.defaultdict(float)
    for ln in open(el, errors="ignore"):
        if "DriverAccumUpdates" in ln:
            for aid, v in (json.loads(ln).get("accumUpdates") or []):
                if aid in split_acc:
                    splits[split_acc[aid]].add(int(num(v)))
            continue
        if '"SparkListenerTaskEnd"' not in ln:
            continue
        e = json.loads(ln)
        for a in (e.get("Task Info", {}) or {}).get("Accumulables", []) or []:
            k = scan_acc.get(a.get("ID"))
            if k:
                batch[k] += num(a.get("Update", 0))

    # stage -> execution, then per-execution scan-task metrics
    s2e = {}
    for ln in open(el, errors="ignore"):
        if '"SparkListenerJobStart"' not in ln:
            continue
        e = json.loads(ln)
        try:
            j = int((e.get("Properties", {}) or {}).get("spark.sql.execution.id"))
        except (TypeError, ValueError):
            continue
        for si in e.get("Stage Infos", []):
            s2e[si["Stage ID"]] = j
    per = collections.defaultdict(lambda: collections.defaultdict(float))
    for ln in open(el, errors="ignore"):
        if '"SparkListenerTaskEnd"' not in ln:
            continue
        e = json.loads(ln); eid = s2e.get(e.get("Stage ID"))
        if eid is None or eid not in qof:
            continue
        tm = e.get("Task Metrics", {}) or {}
        if ((tm.get("Input Metrics", {}) or {}).get("Bytes Read", 0) or 0) <= 0:
            continue
        p = per[eid]
        p["scan_tasks"] += 1
        p["input_bytes"] += (tm.get("Input Metrics", {}) or {}).get("Bytes Read", 0) or 0
        p["task_time_s"] += (tm.get("Executor Run Time", 0) or 0) / 1000.0
        for a in (e.get("Task Info", {}) or {}).get("Accumulables", []) or []:
            n = a.get("Name", "")
            if n == "gpuTime":
                p["occupancy_s"] += dur(a.get("Update", 0))
            elif "emaphore" in n and "ait" in n.lower():
                p["sem_wait_s"] += dur(a.get("Update", 0))

    # wall per execution
    start, wall = {}, {}
    for ln in open(el, errors="ignore"):
        if "SQLExecutionStart" in ln:
            e = json.loads(ln); start[e["executionId"]] = e["time"]
        elif "SQLExecutionEnd" in ln:
            e = json.loads(ln); i = e["executionId"]
            if i in start:
                wall[i] = (e["time"] - start[i]) / 1000.0

    order.sort()
    seq = collections.Counter()
    # who last wrote a record for each table, in execution order -> learnt_from
    last_writer = writers if writers is not None else {}
    rows = []
    for t, eid, q in order:
        # Executions that touched no scan are not query iterations (setup, schema reads, writes with
        # no input). Skipping them BEFORE numbering keeps iteration 1 the real cold execution.
        if not per.get(eid, {}).get("scan_tasks") and not any(e2 == eid for (e2, _tb) in splits):
            continue
        seq[q] += 1
        it = seq[q]
        tables = sorted({tb for (e2, tb) in splits if e2 == eid and tb})
        for tb in tables or [None]:
            sp = sorted(splits.get((eid, tb), []))
            prev = last_writer.get(tb)
            if not autotuner_on:
                learnt = "n/a-autotuner-off"
            else:
                # Not Spark's out-of-box 128 MB: FilePartition.maxSplitBytes =
                # min(spark.sql.files.maxPartitionBytes, max(openCostInBytes, bytesPerCore)),
                # and these runs set maxPartitionBytes=2gb. So the fallback is 2048M on the big
                # tables (capped) and 4M on tiny ones (openCost floor).
                # identity is (query, window): a record written by the SAME query on a DIFFERENT
                # window is NOT "own" - that conflation is the whole thing this experiment measures.
                me = (q, window)
                if prev is None:
                    learnt = "spark-maxSplitBytes(mpb=2g)"
                elif prev == me:
                    learnt = "own"
                elif prev[0] == q:
                    learnt = f"prev-writer:own-query@{prev[1]}"   # same query, other window
                elif prev[1] == window:
                    # always name the window too: a bare "cs04" reads as if the window were unknown
                    learnt = f"prev-writer:{prev[0]}@{prev[1]}"   # other query, same window
                else:
                    learnt = f"prev-writer:{prev[0]}@{prev[1]}"  # other query AND other window
            b = batch.get((eid, BATCH_BYTES), 0.0)
            nb = batch.get((eid, "output columnar batches"), 0.0)
            ns = lambda k: batch.get((eid, k), 0.0) / 1e9      # nsTiming -> seconds
            dec = ns("GPU decode time (excl. SemWait)")
            scn = ns("scan time (excl. SemWait)")
            dec = (batch.get((eid, "GPU decode time (excl. SemWait)"))
                   or batch.get((eid, "GPU decode time"), 0.0)) / 1e9
            scn = (batch.get((eid, "scan time (excl. SemWait)"))
                   or batch.get((eid, "scan time"), 0.0)) / 1e9
            p = per.get(eid, {})
            rows.append(dict(
                query=q, iteration=it, phase="cold" if it == 1 else "warm", table=tb or "-",
                split_bytes=";".join(str(x) for x in sp) or "-",
                split_mb=";".join(f"{x/M:.0f}" for x in sp) or "-",
                learnt_from=learnt,
                wall_s=round(wall.get(eid, 0.0), 3),
                occupancy_s=round(p.get("occupancy_s", 0.0), 2),
                sem_wait_s=round(p.get("sem_wait_s", 0.0), 2),
                task_time_s=round(p.get("task_time_s", 0.0), 2),
                scan_tasks=int(p.get("scan_tasks", 0)),
                input_gib=round(p.get("input_bytes", 0.0) / 2**30, 2),
                decode_s=round(dec, 2), scan_time_s=round(scn, 2),
                batches=int(nb), avg_batch_mb=round(b / nb / M, 1) if nb else 0.0,
                fullness_pct=round(b / nb / 2**30 * 100, 1) if nb else 0.0,
                exec_id=eid))
        if tables:
            for tb in tables:
                last_writer[tb] = (q, window)
    return rows


# Arms that share a history FILE also share learning across applications. Group them so the
# "who wrote this table's record last" map survives from one application to the next, in run order.
# Without this an arm run as its own application always reports sparkDefault, even when the split
# value plainly came from another query.
# Arms that share a history FILE also share learning across applications. Group them so the
# "who wrote this table's record last" map survives from one application to the next, in run order.
# Without this an arm run as its own application always reports sparkDefault, even when the split
# value plainly came from another query.
#
# run_learning_bench.sh writes manifest.json with the arm list, its run order and each arm's history
# file, so nothing here is tied to NDS or to a fixed set of queries. Runs made before the manifest
# existed (20260820a/b/c) fall back to the table below.
LEGACY = {
    "off-query9":            ("none-autotuner-off", "none",     None,              "query9"),
    "off-query28":           ("none-autotuner-off", "none",     None,              "query28"),
    "core1-shared-query9":   ("core1", "shared",   "core1-shared",      "query9"),
    "core1-shared-query28":  ("core1", "shared",   "core1-shared",      "query28"),
    "none-shared-query9":    ("none",  "shared",   "none-shared",       "query9"),
    "none-shared-query28":   ("none",  "shared",   "none-shared",       "query28"),
    "core1-iso-query9":      ("core1", "isolated", "core1-iso-query9",  "query9"),
    "core1-iso-query28":     ("core1", "isolated", "core1-iso-query28", "query28"),
    "none-iso-query9":       ("none",  "isolated", "none-iso-query9",   "query9"),
    "none-iso-query28":      ("none",  "isolated", "none-iso-query28",  "query28"),
}
LEGACY_ORDER = ["off-query9", "off-query28",
                "core1-shared-query9", "core1-shared-query28",
                "none-shared-query9", "none-shared-query28",
                "core1-iso-query9", "core1-iso-query28",
                "none-iso-query9", "none-iso-query28"]

ITERS_EXPECTED = 5
mf = os.path.join(ROOT, "manifest.json")
DATASET = "nds"
if os.path.exists(mf):
    man = json.load(open(mf))
    DATASET = man.get("dataset", "nds")
    ITERS_EXPECTED = int(man.get("iters", 5))
    # `window` names the data window this arm read (run_window_bench.sh). Without it every window
    # collapses to the same `table` value, since the whole point is that they share one table label.
    _seen = {}
    for _a in man["arms"]:
        _seen[_a["arm"]] = _a          # last wins; guards against duplicate manifest lines
    man["arms"] = list(_seen.values())
    PLAN = [(a["arm"], a["ceiling"] if a["ceiling"] != "off" else "none-autotuner-off",
             a["history_mode"], a["history_file"] or None, a["query"], a.get("window", "-"))
            for a in man["arms"]]
    print(f"manifest: dataset={DATASET} arms={len(PLAN)}", file=sys.stderr)
else:
    PLAN = [(a,) + LEGACY[a] + ("-",) for a in LEGACY_ORDER]
    print("no manifest.json - using the legacy NDS arm table", file=sys.stderr)

WRITERS = collections.defaultdict(dict)      # history file -> {table: last query to write it}

all_rows = []
for arm, ceiling, hist, hfile, q, win in PLAN:
    d = os.path.join(ROOT, arm)
    if not os.path.isdir(d):
        print(f"  {arm}: missing", file=sys.stderr); continue
    # Off-ness comes from the manifest's ceiling, NOT the arm name: window arms are named
    # "p1-time-off-1", which does not start with "off", so a name test silently marked them
    # autotuner-on and invented learnt_from values for runs that have no history at all.
    ok, why = arm_health(d, ITERS_EXPECTED)
    if not ok:
        print(f"  EXCLUDED {arm}: {why}", file=sys.stderr)
    for r in parse_arm(d, autotuner_on=(ceiling != "none-autotuner-off"),
                       writers=WRITERS[hfile], default_query=q, default_table=DATASET,
                       window=win):
        r.update(arm=arm, ceiling=ceiling, history_mode=hist, window=win, run_ok=why)
        all_rows.append(r)

cols = ["arm", "ceiling", "history_mode", "query", "window", "run_ok", "iteration", "phase", "table",
        "split_mb", "learnt_from", "wall_s", "occupancy_s", "sem_wait_s", "task_time_s",
        "scan_tasks", "input_gib", "decode_s", "scan_time_s", "batches", "avg_batch_mb", "fullness_pct",
        "split_bytes", "exec_id"]
os.makedirs(os.path.dirname(OUT), exist_ok=True)
with open(OUT, "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=cols, delimiter="\t", extrasaction="ignore")
    w.writeheader()
    for r in all_rows:
        w.writerow(r)
print(f"wrote {os.path.normpath(OUT)}: {len(all_rows)} rows, "
      f"{len({r['arm'] for r in all_rows})} arms, {len({r['query'] for r in all_rows})} queries")
