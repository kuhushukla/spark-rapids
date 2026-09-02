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


def verdict(rows):
    """Per query: did the split settle after the first execution?"""
    by_q = collections.OrderedDict()
    for _, q, sp, _, _ in rows:
        by_q.setdefault(q, []).append(sp)
    out = []
    for q, sps in by_q.items():
        later = set(sps[1:])
        if not later:
            out.append((q, sps[0], "single execution"))
        elif len(later) == 1:
            out.append((q, sps[0], "converged" if sps[0] != sps[1] else "no change"))
        else:
            out.append((q, sps[0], "NOT converged: " + ", ".join(f"{s/M:.1f}M" for s in sorted(later))))
    return out


def render(el, rows, verdicts):
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
            f"<p>A query's first execution plans at Spark's maxSplitBytes because history is empty; "
            f"later ones use the learnt value. 'scans' is GpuScan nodes in the plan &mdash; the split "
            f"shown is the largest across them.</p>")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("path", help="arm dir, directory of event logs, or one event log")
    ap.add_argument("--html", help="also write an HTML table here")
    a = ap.parse_args()

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

    if a.html:
        with open(a.html, "w") as f:
            f.write(render(el, rows, vs))
        print(f"\nwrote {a.html}")


if __name__ == "__main__":
    main()
