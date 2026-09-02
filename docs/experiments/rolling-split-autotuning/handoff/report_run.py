#!/usr/bin/env python3
"""Per-execution split report for any run that produced a Spark event log.

The other generators compare arms against a swept baseline and need the arm-dir layout
run_scan_bench.sh produces. This one needs only a log, so it also reads output from ab and
customer_power.py, where there is one application and no sweep to compare against.

What it answers: what split did each execution plan with, and did the learnt value converge.

    report_run.py /data/run/cs02-ftt-ratio          # an arm dir
    report_run.py /data/smoke-ab/el                 # a directory of logs
    report_run.py /data/smoke-ab/el/local-17883...  # one log
    report_run.py <path> --html out.html

Exits non-zero when a log has GpuScan nodes but no split metric, which means the jar predates it
or the run was not at spark.rapids.sql.metrics.level=DEBUG.
"""

import argparse
import collections
import html
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import gen_ratio_report as gr

M = 2 ** 20


def scan(path):
    """(event log, [(exec id, query, split bytes, wall ms, n scan nodes)]) in run order.

    Query names come from the SQLExecutionStart description, which customer_power.py sets to the
    benchmark name. Runs that leave it unset show '-'; the split is unaffected either way.
    """
    el = gr.eventlog(path)
    if not el:
        raise SystemExit(f"{path}: no event log found")
    walls, splits, ids = gr._el_scan(el)

    # Scan nodes are counted from BOTH plans: the SQLExecutionStart plan is pre-conversion and
    # names them "Scan parquet", so GpuScan only appears once AQE re-reports the plan. A query with
    # no exchange never gets that update and has its GpuScans in the initial plan instead.
    desc, nodes = {}, collections.defaultdict(int)
    for ln in open(el, errors="ignore"):
        if "SparkListener" not in ln:
            continue
        start = "SQLExecutionStart" in ln
        if not start and "SQLAdaptiveExecutionUpdate" not in ln:
            continue
        try:
            e = json.loads(ln)
        except ValueError:
            continue
        eid = e.get("executionId")
        if start:
            desc[eid] = (e.get("description") or "").strip()

        seen = [0]

        def walk(n):
            if (n.get("nodeName") or "").startswith("GpuScan"):
                seen[0] += 1
            for c in n.get("children") or []:
                walk(c)
        walk(e.get("sparkPlanInfo") or {})
        nodes[eid] = max(nodes[eid], seen[0])

    rows = [(i, desc.get(i) or "-", sp, w, nodes.get(i, 0))
            for i, sp, w in zip(ids, splits, walls)]
    return el, rows


G = 2 ** 30


def metrics_by_query(el, rows):
    """Scan-stage work metrics per query, over that query's warm executions.

    gen_ratio_report.parse aggregates one arm over all its warm executions, which mixes queries when
    several share an application. Same accumulators, grouped by query and normalised per iteration.
    """
    warm, first = collections.defaultdict(set), {}
    for i, q, _, _, _ in rows:
        if q not in first:
            first[q] = i          # each query's own cold execution
        else:
            warm[q].add(i)
    exec_q = {i: q for i, q, _, _, _ in rows}

    sids = gr.scan_batch_accids(el)
    batch_ids = sids.get("output columnar batches", set())
    bytes_ids = sids.get(gr.BATCH_BYTES, set())
    tgt = gr.batch_target_bytes(el)

    acc = collections.defaultdict(lambda: dict(
        gpu=0.0, sem=0.0, taskT=0.0, scan=0.0, shuf=0.0, batches=0.0, bbytes=0.0, tasks=set()))
    per_exec_gpu = collections.defaultdict(float)   # (query, exec) -> gpuTime seconds
    stage2exec = {}
    for ln in open(el, errors="ignore"):
        if '"Event":"SparkListenerJobStart"' in ln:
            try:
                e = json.loads(ln)
                j = int((e.get("Properties", {}) or {}).get("spark.sql.execution.id"))
            except (ValueError, TypeError):
                continue
            for si in e.get("Stage Infos", []) or []:
                stage2exec[si.get("Stage ID")] = j
            continue
        if '"Event":"SparkListenerTaskEnd"' not in ln:
            continue
        try:
            e = json.loads(ln)
        except ValueError:
            continue
        eid = stage2exec.get(e.get("Stage ID"))
        q = exec_q.get(eid)
        if q is None or eid not in warm[q]:
            continue
        a = acc[q]
        ti, tm = e.get("Task Info", {}) or {}, e.get("Task Metrics", {}) or {}
        read = (tm.get("Input Metrics", {}) or {}).get("Bytes Read", 0) or 0
        a["scan"] += read
        a["shuf"] += (tm.get("Shuffle Write Metrics", {}) or {}).get("Shuffle Bytes Written", 0) or 0
        is_scan = read > 0
        for ac in ti.get("Accumulables", []) or []:
            aid, nm = ac.get("ID"), ac.get("Name", "")
            try:
                v = float(ac.get("Update", 0) or 0)
            except (TypeError, ValueError):
                v = 0.0
            if aid in batch_ids:
                a["batches"] += v
                a["tasks"].add((e.get("Stage ID"), ti.get("Task ID")))
            elif aid in bytes_ids:
                a["bbytes"] += v
            if is_scan:
                if nm == "gpuTime":
                    v_s = gr._dur_s(ac.get("Update", 0))
                    a["gpu"] += v_s
                    per_exec_gpu[(q, eid)] += v_s
                elif nm == "gpuSemaphoreWait":
                    a["sem"] += gr._dur_s(ac.get("Update", 0))
        if is_scan:
            a["taskT"] += tm.get("Executor Run Time", 0) or 0

    out = {}
    for q, a in acc.items():
        it = len(warm[q]) or 1
        nt = len(a["tasks"]) or 1
        out[q] = dict(
            gpu_s=round(a["gpu"] / it),
            sem_s=round(a["sem"] / it, 1),
            effgpu_s=round((a["gpu"] + a["sem"]) / it, 1),
            taskT_s=round(a["taskT"] / 1e3 / it, 1),
            scanGiB=round(a["scan"] / G / it, 2),
            shufGiB=round(a["shuf"] / G / it, 2),
            bptask=round(a["batches"] / nt, 2),
            fullpct=round(100 * a["bbytes"] / a["batches"] / tgt) if a["batches"] else 0,
            # per-warm-execution gpuTime, kept unaggregated so a significance test is possible
            gpu_samples=[per_exec_gpu[(q, e)] for e in sorted(warm[q])],
        )
    return out, tgt


CALLOUT_PCT = 15.0


def compare(base_path, test_path, tol=CALLOUT_PCT):
    """Per-query test-vs-baseline on split and gpuTime.

    Wall time is deliberately not compared here: ab's regression_check.py already does it from the
    time_log CSVs, with a t-test so a difference is only called out when it is significant. Split
    and gpuTime are only in the event log, which that check never reads.

    Percentages are test relative to baseline; positive means test is larger.
    """
    b_el, b_rows = scan(base_path)
    t_el, t_rows = scan(test_path)
    b_met, _ = metrics_by_query(b_el, b_rows)
    t_met, tgt = metrics_by_query(t_el, t_rows)

    def warm_split(rows):
        by_q = collections.OrderedDict()
        for _, q, sp, _, _ in rows:
            by_q.setdefault(q, []).append(sp)
        return {q: max(v[1:] or v) for q, v in by_q.items()}

    b_split, t_split = warm_split(b_rows), warm_split(t_rows)
    pct = lambda t, b: None if not b else (t - b) / b * 100.0

    out = []
    for q in t_split:
        if q not in b_split:
            continue
        bg, tg = b_met.get(q, {}).get("gpu_s"), t_met.get(q, {}).get("gpu_s")
        dsp, dg = pct(t_split[q], b_split[q]), pct(tg, bg)
        callout = ", ".join(f"{n} {d:+.0f}%" for n, d in (("split", dsp), ("gpuTime", dg))
                            if d is not None and abs(d) > tol)
        out.append(dict(query=q, base_split=b_split[q], test_split=t_split[q], dsplit=dsp,
                        base_gpu=bg, test_gpu=tg, dgpu=dg, callout=callout))
    return b_el, t_el, out, tgt


CONVERGE_TOL = 0.01


def verdict(rows, tol=CONVERGE_TOL):
    """Per query: did the split settle after the first execution?

    Within tol rather than exactly: the ratio is recomputed from each run's own decoded bytes, so
    successive splits land within a few bytes of each other rather than on one integer.
    """
    by_q = collections.OrderedDict()
    for _, q, sp, _, _ in rows:
        by_q.setdefault(q, []).append(sp)
    out = []
    for q, sps in by_q.items():
        later = sps[1:]
        if not later:
            out.append((q, sps[0], "single execution"))
            continue
        lo, hi = min(later), max(later)
        spread = (hi - lo) / hi if hi else 0.0
        if spread > tol:
            out.append((q, sps[0], f"NOT converged: {lo/M:.1f}M..{hi/M:.1f}M"))
        elif abs(hi - sps[0]) / max(hi, sps[0]) <= tol:
            out.append((q, sps[0], "no change"))
        else:
            out.append((q, sps[0], f"converged to {hi/M:.1f}M"))
    return out


def _mtable(mets, tgt, verdicts):
    if not mets:
        return ""
    cols = [("gpu_s", "gpuTime s"), ("sem_s", "semWait s"), ("effgpu_s", "effGPU s"),
            ("taskT_s", "taskT s"), ("scanGiB", "scan GiB"), ("shufGiB", "shuf GiB"),
            ("bptask", "batches/task"), ("fullpct", "Full%")]
    head = "".join(f"<th>{h}</th>" for _, h in cols)
    body = ""
    for q, _, _ in verdicts:
        m = mets.get(q)
        if not m:
            continue
        body += (f"<tr><td>{html.escape(q)}</td>"
                 + "".join(f"<td>{m[k]}</td>" for k, _ in cols) + "</tr>")
    return (f"<h2>Scan-stage work</h2><p>Warm iterations only, per iteration. Full% is against this "
            f"run's batchSizeBytes ({tgt/M:.0f}M).</p>"
            f"<table><tr><th>query</th>{head}</tr>{body}</table>")


def render(el, rows, verdicts, mets=None, tgt=None):
    body = "\n".join(
        f"<tr><td>{i}</td><td>{html.escape(q)}</td><td>{sp/M:.1f}M</td>"
        f"<td>{w:.0f}</td><td>{n}</td></tr>"
        for i, q, sp, w, n in rows)
    vs = "\n".join(f"<tr><td>{html.escape(q)}</td><td>{first/M:.1f}M</td>"
                   f"<td>{html.escape(v)}</td></tr>" for q, first, v in verdicts)
    return (f"<!doctype html><meta charset=utf-8><title>scan split per execution</title>"
            f"<style>body{{font:14px system-ui;margin:2rem}}td,th{{padding:.3rem .8rem;"
            f"border-bottom:1px solid #ddd;text-align:right}}td:first-child,th:first-child,"
            f"td:nth-child(2),th:nth-child(2){{text-align:left}}h2{{margin-top:2rem}}</style>"
            f"<h1>Scan split per execution</h1><p>{html.escape(el)}</p>"
            f"<table><tr><th>exec</th><th>query</th><th>split</th><th>wall ms</th>"
            f"<th>scans</th></tr>{body}</table>"
            f"<h2>Per query</h2><table><tr><th>query</th><th>first split</th><th>then</th></tr>"
            f"{vs}</table>"
            + _mtable(mets, tgt, verdicts)
            + "<p>A query's first execution plans at Spark's maxSplitBytes because history is empty; "
              "later ones use the learnt value. 'scans' is GpuScan nodes in the plan &mdash; the "
              "split shown is the largest across them.</p>")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("path", help="arm dir, directory of event logs, or one event log")
    ap.add_argument("--html", help="also write an HTML table here")
    ap.add_argument("--baseline", help="a second run to compare against; reports split and gpuTime "
                                       "deltas per query. Wall time is left to ab's regression_check.py.")
    ap.add_argument("--gpu-csv",
                    help="write per-iteration gpuTime as ms in ab's time_log schema "
                         "(application_id,query,time/milliseconds), so ab's regression_check.py "
                         "can t-test it: regression_check.py --baselineTimes A --testTimes B")
    ap.add_argument("--callout-pct", type=float, default=CALLOUT_PCT,
                    help=f"flag a query when |delta| exceeds this percent (default {CALLOUT_PCT:g})")
    a = ap.parse_args()

    if a.baseline:
        try:
            b_el, t_el, cmps, _ = compare(a.baseline, a.path, a.callout_pct)
        except gr.MissingSplitMetric as ex:
            raise SystemExit(f"!! {ex}")
        print(f"baseline: {b_el}\ntest:     {t_el}\n")
        print(f"{'query':<34} {'split b->t':>22} {'d%':>7} {'gpuTime b->t':>16} {'d%':>7}  callout")
        bad = 0
        for c in cmps:
            sp = f"{c['base_split']/M:.0f}M -> {c['test_split']/M:.0f}M"
            gp = f"{c['base_gpu']} -> {c['test_gpu']}"
            ds = f"{c['dsplit']:+.0f}" if c['dsplit'] is not None else "-"
            dg = f"{c['dgpu']:+.0f}" if c['dgpu'] is not None else "-"
            bad += bool(c['callout'])
            print(f"{c['query']:<34} {sp:>22} {ds:>7} {gp:>16} {dg:>7}  {c['callout']}")
        print(f"\n{bad} of {len(cmps)} queries flagged at >{a.callout_pct:g}%")
        raise SystemExit(1 if bad else 0)

    try:
        el, rows = scan(a.path)
    except gr.MissingSplitMetric as ex:
        raise SystemExit(f"!! {ex}")
    if not rows:
        raise SystemExit(f"{el}: no query executions found")

    print(f"event log: {el}")
    print(f"{'exec':>5} {'query':<34} {'split':>9} {'wall_ms':>8} {'scans':>6}")
    for i, q, sp, w, n in rows:
        print(f"{i:>5} {q:<34} {sp/M:>8.1f}M {w:>8.0f} {n:>6}")

    vs = verdict(rows)
    print()
    for q, first, v in vs:
        print(f"{q:<34} first={first/M:.1f}M  {v}")

    mets, tgt = metrics_by_query(el, rows)
    if mets:
        print(f"\nwarm iterations only, per iteration. Full% is vs this run's "
              f"batchSizeBytes ({tgt/M:.0f}M).")
        hdr = ("query", "gpuTime", "semWait", "effGPU", "taskT", "scanGiB", "shufGiB",
               "B/task", "Full%")
        print(f"{hdr[0]:<34} " + " ".join(f"{h:>8}" for h in hdr[1:]))
        for q, _, _ in vs:
            m = mets.get(q)
            if not m:
                continue
            print(f"{q:<34} {m['gpu_s']:>8} {m['sem_s']:>8} {m['effgpu_s']:>8} "
                  f"{m['taskT_s']:>8} {m['scanGiB']:>8} {m['shufGiB']:>8} "
                  f"{m['bptask']:>8} {m['fullpct']:>8}")

    if a.gpu_csv:
        import csv as _csv
        app = os.path.basename(el)
        with open(a.gpu_csv, "w", newline="") as f:
            w = _csv.writer(f)
            w.writerow(["application_id", "query", "time/milliseconds"])
            for q, m in mets.items():
                for v in m["gpu_samples"]:
                    w.writerow([app, q, round(v * 1000)])
        print(f"wrote {a.gpu_csv}")

    if a.html:
        with open(a.html, "w") as f:
            f.write(render(el, rows, vs, mets, tgt))
        print(f"\nwrote {a.html}")


if __name__ == "__main__":
    main()
