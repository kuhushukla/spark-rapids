#!/usr/bin/env python3
# Partition-RULE report — sibling of gen_partition_report.py, which stays as the record of the
# 2026-08-10/11 study. This one reports the FULL rule (partition_rule_full.py): shrink AND expand,
# with/without the ratio scan-split autotuner, plus the shuffle read/write time and spill columns.
#
# 100% from the event log. Metric = avg-of-warm (iter1 dropped, inside gen_ratio_report.parse()).
# No driver stdout. No time-based metric feeds any heuristic — the shuffle times here are OUTCOME
# columns only; the rule itself stays on bytes.
#
# ARM PAIRING (this is the dWall rule — do not change it):
#   autotuner OFF:  <q>-pbase       (200 parts)  <->  <q>-pparts      (rule's parts)
#   autotuner ON :  <q>-ftt-ratio   (200 parts)  <->  <q>-ratio-parts (rule's parts)
# Each -parts arm is diffed ONLY against the arm it derived from, at the SAME split, so the delta
# isolates the partition change. Never diff across splits (that mixes in the split effect); the
# same-split guard below raises instead.
#
# Usage: gen_partition_rule_report.py [--run-dir DIR] [--queries "cs01 cs03 ..."]
#          [--out ../results/partition-rule-report.html] [--force]
import argparse, collections, glob, importlib.util, json, math, os, re, statistics, subprocess, sys

HERE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location("gr", os.path.join(HERE, "gen_ratio_report.py"))
gr = importlib.util.module_from_spec(spec); spec.loader.exec_module(gr)
RULE = os.path.join(HERE, "partition_rule_full.py")
G = 2**30
PAREN = re.compile(r'\(\s*([0-9]+)\s*bytes\s*\)')

# query -> (bucket, what it is meant to exercise). Buckets chosen from the 537-arm rule sweep.
BUCKET = {
    "hs3":   ("ratio",  "heaviest single scan (67 GiB segment); shuffle &lt;1 GiB so the rule floors at 16"),
    "rw6":   ("ratio",  "5-theme union + explode &rarr; per-TABLE split sizing; shuffle &lt;1 GiB"),
    "cs03":  ("shrink", "38&ndash;48 GiB shuffle &rarr; rule 200&rarr;48; also the proven split lever"),
    "pv06":  ("shrink", "114&ndash;115 GiB shuffle &rarr; rule 200&rarr;120/128"),
    "pv03g": ("shrink", "137&ndash;151 GiB shuffle &rarr; rule 200&rarr;160; only measured reduce-side spill case"),
    "cs01":  ("expand", "312 GiB shuffle &rarr; rule 200&rarr;320 (ColumnarExchange term)"),
    "pv07g": ("expand", "pv03g rollup + access_method in the key; built to exceed 200 GiB"),
}


def _install_strict_eventlog():
    """Point gen_ratio_report at the strict picker for THIS report only.

    gr.parse() calls eventlog() internally, so overriding our own call sites is not enough — the
    truncated log would still reach split/wall/gpuTime through parse(). Rebinding the attribute on
    the loaded module object affects only this process; gen_ratio_report.py on disk is unchanged and
    the existing reports that depend on its current behaviour are untouched.
    """
    gr.eventlog = eventlog


def eventlog(d):
    """STRICT event-log pick: a COMPLETE log or nothing.

    gen_ratio_report.eventlog() filters '.inprogress' but then falls back to
    `(glob(el/*)+[None])[0]` when the filtered list is empty — so an arm that only ever produced a
    killed/partial log returns that .inprogress file and gets reported as if it were a finished
    5-iteration arm. Hit for real on 2026-08-12: a killed pv03g run left a 13 MB .inprogress log.
    Here we never return one. If several complete logs exist (an arm re-run in place), take the
    NEWEST, since the older one is the superseded attempt.
    """
    done = [e for e in glob.glob(f"{d}/el/*") if "inprogress" not in e]
    if not done:
        return None
    return max(done, key=os.path.getmtime)


def accum_val(x):
    """Event-log accumulable values: plain number | '1.80GB (1932411008 bytes)' | '00:00:02.245'."""
    if isinstance(x, (int, float)):
        return float(x)
    if not isinstance(x, str):
        return 0.0
    m = PAREN.search(x)
    if m:
        return float(m.group(1))
    if ':' in x:
        try:
            h, mi, rest = x.split(':')
            return int(h) * 3600 + int(mi) * 60 + float(rest)
        except Exception:
            return 0.0
    try:
        return float(x)
    except Exception:
        return 0.0


def arm_metrics(d, iters):
    """Per-iteration shuffle/spill/skew metrics for one arm, straight from its event log.

    Shuffle WRITE time  = task 'Shuffle Write Time'      (ns -> s)
    Shuffle READ  time  = 'Fetch Wait Time' + 'Remote Requests Duration'  (ms -> s)
      NOTE local[16]: every fetch is local, so read time is normally ~0. It is reported anyway so a
      change is visible rather than assumed absent.
    Spill               = gpuSpillToHostBytes + gpuSpillToDiskBytes, split scan/map vs reduce
      (GpuTaskMetrics.scala:566; Spark's Memory/Disk Bytes Spilled carry the same values -
       TrampolineUtil.scala:117-121 increments both in one call).
    Skew                = max/median of per-task shuffle-READ bytes over a reduce stage.
    """
    el = eventlog(d)
    out = dict(w_time=0.0, r_time=0.0, w_bytes=0.0, r_bytes=0.0, w_recs=0.0,
               spill_red=0.0, spill_scan=0.0, reducers=0, skew=0.0, taskfail=0,
               scan_in_bytes=0.0, scan_tasks=0)
    if not el or not os.path.exists(el):
        return out
    stage_kind, stage_reads = {}, collections.defaultdict(list)
    stage_spill = collections.defaultdict(float)
    red_tasks = set()
    for ln in open(el, errors='ignore'):
        if '"SparkListenerTaskEnd"' not in ln:
            continue
        try:
            e = json.loads(ln)
        except Exception:
            continue
        sid = e.get("Stage ID")
        ti = e.get("Task Info", {}) or {}
        tm = e.get("Task Metrics", {}) or {}
        if (e.get("Task End Reason", {}) or {}).get("Reason", "") not in ("Success", ""):
            out["taskfail"] += 1
        inp = (tm.get("Input Metrics", {}) or {}).get("Bytes Read", 0) or 0
        sw = tm.get("Shuffle Write Metrics", {}) or {}
        sr = tm.get("Shuffle Read Metrics", {}) or {}
        rd = (sr.get("Local Bytes Read", 0) or 0) + (sr.get("Remote Bytes Read", 0) or 0)
        out["w_time"] += (sw.get("Shuffle Write Time", 0) or 0) / 1e9
        out["w_bytes"] += sw.get("Shuffle Bytes Written", 0) or 0
        out["w_recs"] += sw.get("Shuffle Records Written", 0) or 0
        out["r_time"] += ((sr.get("Fetch Wait Time", 0) or 0)
                          + (sr.get("Remote Requests Duration", 0) or 0)) / 1000.0
        out["r_bytes"] += rd
        kind = "scan/map" if inp > 0 else ("reduce" if rd > 0 else "other")
        if sid not in stage_kind or stage_kind[sid] == "other":
            stage_kind[sid] = kind
        if kind == "scan/map":
            # bytes actually READ from storage by the scan stage (Spark Input Metrics). Reported
            # next to the decoded bytes so the read->decode expansion is visible per arm: the
            # autotuner sizes the split from exactly that ratio (decoded/listed).
            out["scan_in_bytes"] += inp
            out["scan_tasks"] += 1
        if kind == "reduce":
            red_tasks.add((sid, ti.get("Task ID")))
            stage_reads[sid].append(rd)
        for ac in ti.get("Accumulables", []) or []:
            if (ac.get("Name") or "") in ("gpuSpillToHostBytes", "gpuSpillToDiskBytes"):
                stage_spill[sid] += accum_val(ac.get("Update", 0))
    for s, v in stage_spill.items():
        if stage_kind.get(s) == "reduce":
            out["spill_red"] += v
        elif stage_kind.get(s) == "scan/map":
            out["spill_scan"] += v
    for s, v in stage_reads.items():
        v = sorted(v)
        if len(v) >= 2:
            med = statistics.median(v)
            if med > 0:
                out["skew"] = max(out["skew"], max(v) / med)
    it = max(1, iters)
    for k in ("w_time", "r_time", "w_bytes", "r_bytes", "w_recs", "spill_red", "spill_scan",
              "scan_in_bytes"):
        out[k] /= it
    out["reducers"] = len(red_tasks) // it
    return out


def scan_metrics(d, iters):
    """Scan-side metrics per iteration, from PER-TASK accumulables of the GpuScan / GpuShuffleCoalesce
    plan nodes. Metric names verified present in these event logs (GpuScan parquet exposes 'GPU decode
    time', 'scan time', 'buffer time', 'sum of output GPU batch bytes', 'output columnar batches';
    GpuShuffleCoalesce exposes 'concat batch time' and 'output columnar batches').

    BATCH FULLNESS is the one that mattered on cs03 (645m->4g collapsed read_chunk 7x): it is
      avg emitted scan batch bytes = sum(output GPU batch bytes) / sum(output columnar batches),
    reported against the 1 GiB target. Underfull batches decode inefficiently, so this explains
    gpuTime/decode moves that byte counts alone do not.

    'excl. SemWait' variants are preferred for the TIME metrics: they subtract GPU-semaphore waiting,
    so a change reflects real work rather than queueing behind other tasks.
    """
    el = eventlog(d)
    out = dict(scan_bpt=None, shuf_bpt=None, avg_batch=None, fullness=None,
               decode_s=0.0, scan_s=0.0, buffer_s=0.0, concat_s=0.0, batch_bytes=0.0, batches=0.0)
    if not el:
        return out
    # The decoded-bytes metric was renamed twice ("output batch bytes" in revans2 PR#1,
    # "decoded batch bytes" in cudf-spark #15584); accept every spelling so old runs keep parsing.
    WANT_SCAN = {"GPU decode time (excl. SemWait)": "decode_s", "scan time (excl. SemWait)": "scan_s",
                 "buffer time (excl. SemWait)": "buffer_s", "output columnar batches": "scan_batches",
                 "sum of output GPU batch bytes": "batch_bytes",
                 "output batch bytes": "batch_bytes", "decoded batch bytes": "batch_bytes"}
    WANT_SHUF = {"concat batch time (excl. SemWait)": "concat_s", "output columnar batches": "shuf_batches"}
    acc = {}                                    # accId -> field name
    def walk(n):
        nm = (n.get("nodeName") or "").strip()
        table = WANT_SCAN if nm == "GpuScan parquet" else (WANT_SHUF if nm == "GpuShuffleCoalesce" else None)
        if table:
            for m in (n.get("metrics") or []):
                if m.get("name") in table and m.get("accumulatorId") is not None:
                    acc[m["accumulatorId"]] = table[m["name"]]
        for c in (n.get("children") or []):
            walk(c)
    for ln in open(el, errors='ignore'):
        if '"sparkPlanInfo"' not in ln:
            continue
        try:
            spi = json.loads(ln).get("sparkPlanInfo")
        except Exception:
            continue
        if spi:
            walk(spi)
    tot = collections.Counter()
    scan_tasks, shuf_tasks = set(), set()
    SCAN_FIELDS = {"decode_s", "scan_s", "buffer_s", "batch_bytes", "scan_batches"}
    for ln in open(el, errors='ignore'):
        if '"SparkListenerTaskEnd"' not in ln:
            continue
        try:
            e = json.loads(ln)
        except Exception:
            continue
        ti = e.get("Task Info", {}) or {}
        tm = e.get("Task Metrics", {}) or {}
        tid = (e.get("Stage ID"), ti.get("Task ID"))
        # Scope each metric to the stage kind it belongs to, explicitly: scan/decode/fullness from
        # SCAN-side tasks (they read input bytes), shuffle-coalesce from REDUCE-side tasks (they read
        # shuffle and no input). Without this a task that both reads input and reads shuffle would
        # contribute to the wrong side.
        inp = (tm.get("Input Metrics", {}) or {}).get("Bytes Read", 0) or 0
        srm = tm.get("Shuffle Read Metrics", {}) or {}
        rd = (srm.get("Local Bytes Read", 0) or 0) + (srm.get("Remote Bytes Read", 0) or 0)
        is_scan, is_reduce = inp > 0, (rd > 0 and inp == 0)
        for ac in ti.get("Accumulables", []) or []:
            f = acc.get(ac.get("ID"))
            if not f:
                continue
            if f in SCAN_FIELDS and not is_scan:
                continue
            if f not in SCAN_FIELDS and not is_reduce:
                continue
            v = accum_val(ac.get("Update", 0))
            tot[f] += v
            if f == "scan_batches":
                scan_tasks.add(tid)
            elif f == "shuf_batches":
                shuf_tasks.add(tid)
    it = max(1, iters)
    # the time metrics are nanosecond accumulators -> seconds, per iteration
    out["decode_s"] = tot["decode_s"] / 1e9 / it
    out["scan_s"] = tot["scan_s"] / 1e9 / it
    out["buffer_s"] = tot["buffer_s"] / 1e9 / it
    out["concat_s"] = tot["concat_s"] / 1e9 / it
    out["batch_bytes"] = tot["batch_bytes"] / it
    out["batches"] = tot["scan_batches"] / it
    if scan_tasks:
        out["scan_bpt"] = tot["scan_batches"] / len(scan_tasks)
    if shuf_tasks:
        out["shuf_bpt"] = tot["shuf_batches"] / len(shuf_tasks)
    if tot["scan_batches"]:
        out["avg_batch"] = tot["batch_bytes"] / tot["scan_batches"]
        out["fullness"] = out["avg_batch"] / G            # vs the 1 GiB target batch
    return out


def datasize_and_ratio(d):
    """Max exchange dataSize (uncompressed) over warm runs, and uncompressed/compressed ratio.
    AQE coalesces on the COMPRESSED size while the rule sizes on the uncompressed one, so this
    ratio is the proposal's 'not optional' confound (SHUFFLE-PARTITIONS-TEST-PROPOSAL:270-272)."""
    try:
        o = json.loads(subprocess.run(["python3", RULE, d, "--json"],
                                      capture_output=True, text=True, timeout=1800).stdout)
    except Exception:
        return None
    return o


def rule_cell(o, cur=200):
    """The shrink/growth cell: what the rule decided from THIS arm's own runs."""
    if not o:
        return "&ndash;", ""
    act = o.get("action", "?")
    new = o.get("new_shuffle", "?")
    cls = {"SHRINK": "ok", "EXPAND": "warn", "KEEP": "mut"}.get(act, "")
    return f"{cur}&rarr;{new}<br><span class=mut>{act}</span>", cls


def fmt_bytes(b):
    """Scale the unit so a genuinely tiny value reads as tiny rather than as a missing '0.0G'.
    csH3/hs3 shuffle only ~3-6 MiB across 5 iterations (they GROUP BY a ~5-value key), which a
    fixed GiB format printed as 0.0G and made look like a broken extraction."""
    if not b:
        return "0"
    if b >= G:
        return f"{b/G:.1f}G"
    if b >= 2**20:
        return f"{b/2**20:.1f}M"
    if b >= 2**10:
        return f"{b/2**10:.0f}K"
    return f"{b:.0f}B"


def fmt_split(b):
    if not b:
        return "?"
    return f"{b/2**20:.0f}m" if b < G else f"{b/G:.0f}g"


def warm_walls(d):
    el = eventlog(d)
    w, _ = gr.el_walls_split(el)
    return [x / 1000.0 for x in (w[1:] if len(w) > 1 else w)]


def sweep_table(run_dir, queries, sweep=("256m", "512m", "1g", "2g", "4g")):
    """The OFF split sweep per query, with the optimum marked. This is the baseline every Δ in the
    2x2 is measured from, so it belongs in the report rather than in a runner log."""
    head = "".join(f"<th>{m}</th>" for m in sweep)
    body = []
    for q in queries:
        cells, best, bestm = [], None, None
        for m in sweep:
            d = f"{run_dir}/{q}-off-{m}"
            if not eventlog(d):
                cells.append((m, None)); continue
            try:
                w = gr.parse(d, q)["wall"] / 1000.0
            except Exception:
                w = None
            cells.append((m, w))
            if w is not None and (best is None or w < best):
                best, bestm = w, m
        if all(w is None for _, w in cells):
            continue
        tds = "".join(
            "<td>&ndash;</td>" if w is None else
            (f"<td class=ok><b>{w:.1f}s</b></td>" if m == bestm else f"<td>{w:.1f}s</td>")
            for m, w in cells)
        body.append(f"<tr><td class=q>{q}</td>{tds}<td><b>{bestm}</b></td></tr>")
    if not body:
        return ""
    return (f"<h2>OFF split sweep &mdash; where each baseline comes from</h2>"
            f"<p class=mut>Autotuner OFF, default 200 partitions, avg-of-warm. The winner (green) is the "
            f"split the <code>parts</code> arms run at. A hand-picked baseline would flatter the "
            f"autotuner, so the runner refuses to run the stage without a sweep.</p>"
            f"<table><thead><tr><th>query</th>{head}<th>optimum</th></tr></thead><tbody>"
            f"{chr(10).join(body)}</tbody></table>")


def summary_2x2(run_dir, queries):
    """One row per 2x2 cell, every Δ against the SAME query's autotuner-OFF baseline at its swept
    optimum — i.e. 'what does each heuristic cost vs doing nothing', which is the comparison that
    answers the experiment. (The per-arm table below instead uses same-split pairing, which isolates
    the partition knob but cannot show the split's own cost.)"""
    body = []
    for q in queries:
        cells = [("baseline OFF", f"{run_dir}/{q}-pbase"),
                 ("partition rule only", f"{run_dir}/{q}-pparts"),
                 ("split sizing only", f"{run_dir}/{q}-ftt-ratio"),
                 ("both", f"{run_dir}/{q}-ratio-parts")]
        got = [(lbl, d) for lbl, d in cells if eventlog(d)]
        if not got:
            continue
        base = None
        first = True
        for lbl, d in got:
            try:
                p = gr.parse(d, q)
            except Exception:
                continue
            w = p["wall"] / 1000.0
            ws = warm_walls(d)
            sd = statistics.pstdev(ws) if len(ws) > 1 else 0.0
            if base is None and lbl == "baseline OFF":
                base = w
            if base is None or lbl == "baseline OFF":
                dcell, dcls = "&mdash;", ""
            else:
                gap = w - base
                bs = warm_walls(f"{run_dir}/{q}-pbase")
                pooled = ((statistics.pstdev(bs) + sd) / 2) if len(bs) > 1 else sd
                sig = abs(gap) / pooled if pooled else 0
                dcell = f"{gap:+.2f}s ({gap/base*100:+.0f}%)<br><span class=mut>{sig:.1f}&sigma;</span>"
                dcls = " class=mut" if sig <= 1.0 else (" class=warn" if gap > 0 else " class=ok")
            qcell = f"<td class=q rowspan={len(got)}>{q}</td>" if first else ""
            body.append(f"<tr>{qcell}<td>{lbl}</td><td>{fmt_split(p.get('split'))}</td>"
                        f"<td>{w:.2f}s</td><td{dcls}>{dcell}</td><td>{sd:.2f}</td>"
                        f"<td class=mut>{', '.join(f'{x:.1f}' for x in ws)}</td></tr>")
            first = False
    if not body:
        return ""
    return ("<h2>The 2&times;2 &mdash; every cell vs doing nothing</h2>"
            "<p class=mut>Baseline = autotuner OFF at the swept optimum, 200 partitions. "
            "&sigma; is the gap over the two arms' pooled per-iteration stdev; grey = within 1&sigma; "
            "(noise). Warm iterations are listed so you can see stability, not just the mean.</p>"
            "<table><thead><tr><th>query</th><th>cell</th><th>split</th><th>wall</th>"
            "<th>&Delta; vs baseline</th><th>sd</th><th>warm iterations</th></tr></thead><tbody>"
            + chr(10).join(body) + "</tbody></table>")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", default="/data/scan-split-partrule-20260812")
    ap.add_argument("--queries", default="cs01 cs03 pv03g pv06 pv07g hs3 rw6")
    ap.add_argument("--iters", type=int, default=5)
    ap.add_argument("--out", default=os.path.join(HERE, "..", "results", "partition-rule-report.html"))
    ap.add_argument("--force", action="store_true", help="overwrite an existing report")
    a = ap.parse_args()
    _install_strict_eventlog()

    outp = os.path.abspath(a.out)
    if os.path.exists(outp) and not a.force:
        sys.exit(f"{outp} exists; pass --force to overwrite")

    rows, missing = [], []
    for q in a.queries.split():
        bucket, why = BUCKET.get(q, ("?", ""))
        # (label, arm dir, its same-split baseline dir or None if it IS the baseline)
        pairs = [("OFF / 200",        f"{a.run_dir}/{q}-pbase",      None),
                 ("OFF / rule",       f"{a.run_dir}/{q}-pparts",     f"{a.run_dir}/{q}-pbase"),
                 ("ratio / 200",      f"{a.run_dir}/{q}-ftt-ratio",  None),
                 ("ratio / rule",     f"{a.run_dir}/{q}-ratio-parts", f"{a.run_dir}/{q}-ftt-ratio")]
        present = [(lbl, d, b) for lbl, d, b in pairs if eventlog(d)]
        for lbl, d, b in pairs:
            if not eventlog(d):
                missing.append(f"{q} {lbl} ({d})")
        if not present:
            continue
        for i, (lbl, d, basedir) in enumerate(present):
            p = gr.parse(d, q)
            m = arm_metrics(d, a.iters)
            o = datasize_and_ratio(d)
            wall = p["wall"] / 1000.0
            if basedir is None or not eventlog(basedir):
                dwall, dcls = "&mdash;", ""
            else:
                bs, as_ = gr.parse(basedir, q).get("split"), p.get("split")
                if bs and as_ and bs != as_:
                    raise SystemExit(f"same-split guard: {q} {lbl} split={as_} vs base {bs} — "
                                     f"dWall would mix the split effect into the partition delta")
                bw = gr.parse(basedir, q)["wall"] / 1000.0
                gap = wall - bw
                pooled = ((statistics.pstdev(warm_walls(basedir)) + statistics.pstdev(warm_walls(d))) / 2
                          if warm_walls(basedir) and warm_walls(d) else 0.0)
                dwall = f"{gap:+.1f}s ({gap/bw*100:+.0f}%)"
                dcls = " class=mut" if abs(gap) <= pooled else (" class=warn" if gap > 0 else " class=ok")
            rcell, rcls = rule_cell(o)
            unc = (o or {}).get("legacy_max_e_gib", 0)
            ratio_uc = (unc * G / m["w_bytes"]) if m["w_bytes"] else float('nan')
            s = scan_metrics(d, a.iters)
            fz = (f"{s['avg_batch']/2**20:.0f}m<br><span class=mut>{s['fullness']*100:.0f}% of 1g</span>"
                  if s['avg_batch'] else "&ndash;")
            bpt = (f"{s['scan_bpt']:.2f}" if s['scan_bpt'] is not None else "&ndash;") + \
                  " / " + (f"{s['shuf_bpt']:.2f}" if s['shuf_bpt'] is not None else "&ndash;")
            cells = (f"<td>{lbl}</td>"
                     f"<td class={rcls}>{rcell}</td>"
                     f"<td>{m['reducers']}</td>"
                     f"<td>{fmt_split(p.get('split'))}</td>"
                     f"<td>{wall:.1f}s</td><td{dcls}>{dwall}</td>"
                     f"<td>{p.get('gpu_s')}</td>"
                     f"<td>{s['decode_s']:.1f}s</td>"
                     f"<td>{s['scan_s']:.1f}s</td>"
                     f"<td>{fz}</td>"
                     f"<td>{bpt}</td>"
                     f"<td>{s['concat_s']:.1f}s</td>"
                     f"<td>{m['w_time']:.1f}s</td><td>{m['r_time']:.1f}s</td>"
                     f"<td>{fmt_bytes(m['scan_in_bytes'])}<br><span class=mut>"
                     f"{fmt_bytes(s['batch_bytes'])} dec &middot; "
                     f"{(s['batch_bytes']/m['scan_in_bytes']) if m['scan_in_bytes'] else 0:.2f}x</span></td>"
                     f"<td>{fmt_bytes(m['w_bytes'])}/{fmt_bytes(m['r_bytes'])}</td>"
                     f"<td>{fmt_bytes(unc*G)}<br><span class=mut>{ratio_uc:.2f}x</span></td>"
                     f"<td>{m['spill_red']/G:.1f}/{m['spill_scan']/G:.1f}G</td>"
                     f"<td>{m['skew']:.2f}</td>"
                     f"<td class={'ok' if m['taskfail']==0 else 'bad'}>{m['taskfail']}</td>")
            if i == 0:
                qcell = (f"<td class=q rowspan={len(present)}>{q}<br><span class=mut>{bucket}</span>"
                         f"<br><span class=mut>{why}</span></td>")
                rows.append(f"<tr>{qcell}{cells}</tr>")
            else:
                rows.append(f"<tr>{cells}</tr>")

    qlist = a.queries.split()
    sweep_block = sweep_table(a.run_dir, qlist)
    summary_block = summary_2x2(a.run_dir, qlist)
    miss_block = ("<p class=mut><b>Arms not present (not silently dropped):</b><br>"
                  + "<br>".join(missing) + "</p>") if missing else ""

    H = f"""<!doctype html><meta charset=utf-8><title>Partition-count RULE &mdash; shrink &amp; expand</title>
<style>
body{{font:14px/1.5 -apple-system,Segoe UI,Roboto,sans-serif;color:#1a1a1a;max-width:1400px;margin:2rem auto;padding:0 1rem}}
h1{{font-size:20px}}h2{{font-size:15px;margin-top:1.3rem}}
table{{border-collapse:collapse;font-size:12px;width:100%}}th,td{{border:1px solid #e3e3e3;padding:4px 7px;text-align:right}}
th:first-child,td:first-child,td.q,td:nth-child(2){{text-align:left}}thead{{background:#f6f8fa}}
td.q{{font-weight:600;vertical-align:top}}.mut{{color:#777;font-weight:400;font-size:10.5px}}
.ok{{color:#137333;font-weight:600}}.warn{{color:#b06000;font-weight:600}}.bad{{color:#c5221f;font-weight:600}}
.leg{{background:#f6f8fa;border:1px solid #e3e3e3;border-radius:8px;padding:.6rem 1rem;margin:1rem 0;font-size:12px}}
code{{background:#f2f2f2;padding:0 3px;border-radius:3px}}
</style>
<h1>Partition-count rule &mdash; shrink &amp; expand, with and without the ratio split autotuner</h1>
<p class=mut>A5000, Spark 3.5.3 + RAPIDS (clean jar, no chunking overrides), <code>local[16]</code>, target batch 1 GiB,
AQE on. Rule = <code>partition_rule_full.py</code> (full spec, per query run): per-stage SUM &rarr;
<code>ceil(input/T)</code> &rarr; wave-rounded, downward-only with 3 gates; expansion =
max(spill_2x if spill&gt;50 GiB and no skew, ColumnarExchange/T). Metric = <b>avg-of-warm</b>, iter1 dropped.
All numbers from the event log.</p>
<div class=leg>
<b>rule</b> = what the rule decided from this arm's own runs (shrink / growth) &middot;
<b>reducers</b> = actual post-AQE reduce tasks per iter (differs from the set value when AQE coalesces) &middot;
<b>dWall</b> = vs the SAME-SPLIT 200-partition arm it derived from &middot;
<b>decode</b> / <b>scan time</b> = GpuScan <code>GPU decode time</code> / <code>scan time</code> (excl. SemWait) per iter, SCAN-side tasks only.
<b>These are ELAPSED-TIME timers on a shared GPU, not work.</b> Measured on cs01 (2026-08-12): changing ONLY the
partition count 200&rarr;336 left the scan byte-identical (batches 3079, rows 35.92e9, read buffer 727.2 GB,
output batch bytes 2274.1 GB &mdash; all +0.00%) yet decode fell 53% and scan time rose 39%. <code>scan time</code> wraps the
blocking <code>batches.hasNext</code> pull (GpuFileSourceScanExec.scala:485-495), so it absorbs downstream back-pressure;
<code>GPU decode time</code> wraps only the cuDF decode call, which runs faster when the GPU is less contended.
Read the BYTE columns (fullness, batches/task, bytes) for work; treat these two and <b>gpuTime</b> as contention-sensitive &middot;
<b>batch fullness</b> = sum(output GPU batch bytes) &divide; output columnar batches, vs the 1 GiB target &mdash;
underfull batches decode inefficiently (cs03 645m&rarr;4g collapsed decode 7&times;) &middot;
<b>batches/task</b> = scan batches per scan task / shuffle-coalesce batches per reduce task &middot;
<b>concat</b> = GpuShuffleCoalesce <code>concat batch time</code>, REDUCE-side tasks only &middot;
<b>scan bytes read</b> = bytes read from storage by SCAN-stage tasks per iter (Spark Input Metrics), with decoded bytes and the read&rarr;decode expansion beneath &mdash; the autotuner sizes the split from exactly that expansion &middot;
<b>shufW/R time</b> = task <code>Shuffle Write Time</code> / <code>Fetch Wait Time + Remote Requests Duration</code> per iter &middot;
<b>dataSize</b> = uncompressed exchange bytes (the rule's input) and uncompressed/compressed ratio &middot;
<b>spill</b> = GPU spill, reduce-side / scan-side &middot; <b>skew</b> = max/median reduce-task read bytes.<br>
<b>dWall colour:</b> grey = within the two arms' pooled per-iter stdev; <span class=ok>green</span> faster,
<span class=warn>orange</span> slower beyond it.<br>
<b>local[16] caveat:</b> all shuffle fetches are local, so read time is expected near zero &mdash; reported so
a change is visible rather than assumed.
</div>
{sweep_block}
{summary_block}
<h2>Per-arm detail &mdash; same-split pairing</h2>
<p class=mut>Here <b>dWall</b> pairs each rule arm with the 200-partition arm at the SAME split, so it
isolates the partition knob alone (it cannot show the split's own cost &mdash; that is the table above).</p>
<table><thead><tr><th>query</th><th>arm</th><th>rule</th><th>reducers</th><th>split</th><th>wall</th><th>dWall</th>
<th>gpuTime</th><th>decode</th><th>scan time</th><th>batch fullness</th><th>batches/task<br>scan / shuf</th>
<th>concat</th><th>scan bytes read<br>decoded &middot; exp</th><th>shufW time</th><th>shufR time</th><th>shufBytes W/R</th><th>dataSize unc<br>unc/comp</th>
<th>spill red/scan</th><th>skew</th><th>taskFail</th></tr></thead>
<tbody>
{chr(10).join(rows)}
</tbody></table>
{miss_block}
"""
    with open(outp, "w") as f:
        f.write(H)
    print(f"wrote {outp}  ({len(rows)} arm rows, {len(missing)} missing)")


if __name__ == "__main__":
    main()
