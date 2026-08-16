#!/usr/bin/env python3
# Clean 2x2 report for the 2026-08-14 rerun.
#
# Reports MEDIAN wall, not mean: the intermittent-spike failure mode seen in the earlier batch
# (pv03g warm walls 153, 209, 192, 161) breaks means while medians survive it. Every wall column
# also lists the individual warm iterations so stability is visible rather than assumed.
#
# Columns are the ones from the original ratio report, all scan-stage-scoped and per iteration:
#   gpu time      scan-stage gpuTime (GPU semaphore-holding time = real GPU work)
#   task time     scan-stage Executor Run Time (whole task wall, summed over tasks)
#   sem wait      scan-stage gpuSemaphoreWait (blocked waiting for a GPU permit)
#   eff gpu       gpu time + sem wait
#   batches/task  scan output columnar batches divided by scan tasks
#   fullness      average emitted batch bytes as a percent of the run's own target batch size
# plus split, partitions, reducers, scan GiB, shuffle GiB, the rule's decision, spill and failures.
#
# Plain words only in headers and notes: no arrows, sigma, multiplication signs or other symbols.
#
# Usage: gen_clean_2x2_report.py [--run-dir DIR] [--queries "hs3 pvH cs03 pv06 cs01 pv03g"]
#          [--out ../results/clean-2x2-report.html] [--force]
import argparse, glob, importlib.util, json, os, re, statistics, subprocess, sys, collections

HERE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location("gr", os.path.join(HERE, "gen_ratio_report.py"))
gr = importlib.util.module_from_spec(spec); spec.loader.exec_module(gr)
# Reuse the sibling report's extractors so no column is lost and both reports agree by construction:
#   pr.scan_metrics -> decode, scan time, concat, batch fullness, scan and shuffle batches per task
#   pr.arm_metrics  -> shuffle write/read time and bytes, GPU spill by stage kind, skew, failures
_ps = importlib.util.spec_from_file_location("pr", os.path.join(HERE, "gen_partition_rule_report.py"))
pr = importlib.util.module_from_spec(_ps); _ps.loader.exec_module(pr)
RULE = os.path.join(HERE, "partition_rule_full.py")
G = 2**30
PAREN = re.compile(r'\(\s*([0-9]+)\s*bytes\s*\)')

CATEGORY = {
    "hs3":   "scan heavy",
    "pvH":   "scan heavy",
    "cs03":  "shuffle shrink",
    "pv06":  "shuffle shrink",
    "cs01":  "shuffle expand",
    "pv03g": "shuffle expand (reports keep, see notes)",
}
SWEEP = ("256m", "512m", "1g", "2g", "4g")


def eventlog(d):
    """Complete logs only, newest first. A killed run leaves a .inprogress file that would otherwise
    be reported as if it were a finished arm."""
    done = [e for e in glob.glob(f"{d}/el/*") if "inprogress" not in e]
    return max(done, key=os.path.getmtime) if done else None


def warm_walls(d):
    el = eventlog(d)
    if not el:
        return []
    w, _ = gr.el_walls_split(el)
    return [x / 1000.0 for x in (w[1:] if len(w) > 1 else w)]


def med(xs):
    return statistics.median(xs) if xs else 0.0


def accum(x):
    if isinstance(x, (int, float)):
        return float(x)
    if not isinstance(x, str):
        return 0.0
    m = PAREN.search(x)
    if m:
        return float(m.group(1))
    try:
        return float(x)
    except Exception:
        return 0.0


def extras(d, iters):
    """Reducer count, GPU spill split by stage kind, and task failures. Not in parse()."""
    el = eventlog(d)
    out = dict(reducers=0, spill_reduce=0.0, spill_scan=0.0, fails=0)
    if not el:
        return out
    kind, red = {}, set()
    spill = collections.Counter()
    for ln in open(el, errors="ignore"):
        if '"SparkListenerTaskEnd"' not in ln:
            continue
        try:
            e = json.loads(ln)
        except Exception:
            continue
        sid = e.get("Stage ID"); ti = e.get("Task Info", {}) or {}; tm = e.get("Task Metrics", {}) or {}
        if (e.get("Task End Reason", {}) or {}).get("Reason", "") not in ("Success", ""):
            out["fails"] += 1
        inp = (tm.get("Input Metrics", {}) or {}).get("Bytes Read", 0) or 0
        srm = tm.get("Shuffle Read Metrics", {}) or {}
        rd = (srm.get("Local Bytes Read", 0) or 0) + (srm.get("Remote Bytes Read", 0) or 0)
        k = "scan" if inp > 0 else ("reduce" if rd > 0 else "other")
        if sid not in kind or kind[sid] == "other":
            kind[sid] = k
        if k == "reduce":
            red.add((sid, ti.get("Task ID")))
        for ac in ti.get("Accumulables", []) or []:
            if (ac.get("Name") or "") in ("gpuSpillToHostBytes", "gpuSpillToDiskBytes"):
                spill[kind.get(sid, "other")] += accum(ac.get("Update", 0))
    it = max(1, iters)
    out["reducers"] = len(red) // it
    out["spill_reduce"] = spill["reduce"] / it / G
    out["spill_scan"] = spill["scan"] / it / G
    return out


def by_stage_kind(d, iters):
    """gpuTime, semWait and task time split by SCAN/MAP versus REDUCE tasks.

    gpuTime is a TASK-level accumulator (GpuTaskMetrics.scala:383, incremented once per task at
    GpuSemaphore.scala:499 as lastReleased-lastAcquired), so it has no node attribution. The original
    ratio report scoped it to scan tasks only, via Input Metrics Bytes Read > 0. That silently drops
    the reduce side, which is exactly where the partition-count rule acts: measured on pv06, reduce
    tasks use 258 s of GPU time against 84 s on the map side, so a scan-only column understates the
    query by a factor of three. Both kinds are reported here.
    """
    el = eventlog(d)
    out = {k: dict(tasks=0, gpu=0.0, sem=0.0, taskT=0.0) for k in ("scan", "reduce")}
    if not el:
        return out
    def dur(x):
        if isinstance(x, str) and ":" in x:
            try:
                h, m, s = x.split(":"); return int(h) * 3600 + int(m) * 60 + float(s)
            except Exception:
                return 0.0
        try:
            return float(x)
        except Exception:
            return 0.0
    n = 0
    for ln in open(el, errors="ignore"):
        if "SQLExecutionStart" in ln:
            n += 1
        if '"SparkListenerTaskEnd"' not in ln:
            continue
        try:
            e = json.loads(ln)
        except Exception:
            continue
        ti = e.get("Task Info", {}) or {}; tm = e.get("Task Metrics", {}) or {}
        inp = (tm.get("Input Metrics", {}) or {}).get("Bytes Read", 0) or 0
        srm = tm.get("Shuffle Read Metrics", {}) or {}
        rd = (srm.get("Local Bytes Read", 0) or 0) + (srm.get("Remote Bytes Read", 0) or 0)
        k = "scan" if inp > 0 else ("reduce" if rd > 0 else None)
        if not k:
            continue
        r = out[k]
        r["tasks"] += 1
        r["taskT"] += (tm.get("Executor Run Time", 0) or 0) / 1000.0
        for ac in ti.get("Accumulables", []) or []:
            nm = ac.get("Name", "")
            if nm == "gpuTime":
                r["gpu"] += dur(ac.get("Update", 0))
            elif nm == "gpuSemaphoreWait":
                r["sem"] += dur(ac.get("Update", 0))
    n = max(n, 1)
    for k in out:
        out[k]["tasks"] //= n
        for f in ("gpu", "sem", "taskT"):
            out[k][f] /= n
    return out


def rule_for(d):
    try:
        return json.loads(subprocess.run(["python3", RULE, d, "--json"],
                                         capture_output=True, text=True, timeout=1800).stdout)
    except Exception:
        return None


def fmt_split(b):
    if not b:
        return "none"
    return f"{b/2**20:.0f}m" if b < G else f"{b/G:.0f}g"


def sweep_rows(run_dir, q):
    out = []
    best = bestm = None
    for m in SWEEP:
        d = f"{run_dir}/{q}-off-{m}"
        ws = warm_walls(d)
        if not ws:
            out.append((m, None, []))
            continue
        v = med(ws)
        out.append((m, v, ws))
        if best is None or v < best:
            best, bestm = v, m
    return out, bestm


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", default="/data/clean-run-20260814")
    ap.add_argument("--queries", default="hs3 pvH cs03 pv06 cs01 pv03g")
    ap.add_argument("--iters", type=int, default=5)
    ap.add_argument("--out", default=os.path.join(HERE, "..", "results", "clean-2x2-report.html"))
    ap.add_argument("--force", action="store_true")
    a = ap.parse_args()
    outp = os.path.abspath(a.out)
    if os.path.exists(outp) and not a.force:
        sys.exit(f"{outp} exists, pass --force to overwrite")

    sweep_html, body, notes = [], [], []
    for q in a.queries.split():
        rows, bestm = sweep_rows(a.run_dir, q)
        if any(v is not None for _, v, _ in rows):
            tds = "".join("<td>not run</td>" if v is None else
                          (f"<td class=ok><b>{v:.1f}</b></td>" if m == bestm else f"<td>{v:.1f}</td>")
                          for m, v, _ in rows)
            sweep_html.append(f"<tr><td class=q>{q}</td>{tds}<td><b>{bestm or 'none'}</b></td></tr>")

        cells = [("autotuner off, default partitions", f"{a.run_dir}/{q}-pbase"),
                 ("autotuner off, rule partitions", f"{a.run_dir}/{q}-pparts"),
                 ("split sizing on, default partitions", f"{a.run_dir}/{q}-ftt-ratio"),
                 ("split sizing on, rule partitions", f"{a.run_dir}/{q}-ratio-parts")]
        got = [(lbl, d) for lbl, d in cells if eventlog(d)]
        if not got:
            notes.append(f"{q}: no arms present yet")
            continue
        base_med = None
        for i, (lbl, d) in enumerate(got):
            p = gr.parse(d, q)
            if not p or p.get("err"):
                continue
            e = extras(d, a.iters)
            r = rule_for(d)
            ws = warm_walls(d)
            wmed = med(ws)
            if lbl.startswith("autotuner off, default"):
                base_med = wmed
            if base_med is None or i == 0:
                dcell, dcls = "baseline", ""
            else:
                gap = wmed - base_med
                bws = warm_walls(f"{a.run_dir}/{q}-pbase")
                pooled = ((statistics.pstdev(bws) + statistics.pstdev(ws)) / 2) if len(bws) > 1 and len(ws) > 1 else 0
                sig = abs(gap) / pooled if pooled else 0
                dcell = (f"{gap:+.1f} s ({gap/base_med*100:+.0f} pct)<br>"
                         f"<span class=mut>{sig:.1f} times noise</span>")
                dcls = " class=mut" if sig <= 1 else (" class=warn" if gap > 0 else " class=ok")
            act = (r or {}).get("action", "none")
            newp = (r or {}).get("new_shuffle", "")
            qcell = (f"<td class=q rowspan={len(got)}>{q}<br><span class=mut>{CATEGORY.get(q,'')}</span></td>"
                     if i == 0 else "")
            k = by_stage_kind(d, a.iters)
            s = pr.scan_metrics(d, a.iters)      # decode, scan time, concat, batches per task
            m2 = pr.arm_metrics(d, a.iters)      # shuffle times and bytes, skew
            unc = (r or {}).get("legacy_max_e_gib", 0)
            ratio_uc = (unc * G / m2["w_bytes"]) if m2["w_bytes"] else 0
            dec = p.get("scanGiB", 0) or 0
            decoded = (s["batch_bytes"] / G) if s.get("batch_bytes") else 0
            exp = (decoded / dec) if dec else 0
            sd = statistics.pstdev(ws) if len(ws) > 1 else 0.0
            shbpt = f"{s['shuf_bpt']:.2f}" if s.get("shuf_bpt") is not None else "none"
            body.append(
                f"<tr>{qcell}<td>{lbl}</td>"
                f"<td>{fmt_split(p.get('split'))}</td><td>{e['reducers']}</td>"
                f"<td><b>{wmed:.1f}</b><br><span class=mut>{', '.join(f'{x:.1f}' for x in ws)}</span></td>"
                f"<td>{sd:.2f}</td>"
                f"<td{dcls}>{dcell}</td>"
                f"<td>{k['scan']['tasks']} / {k['reduce']['tasks']}</td>"
                f"<td>{k['scan']['gpu']:.1f} / {k['reduce']['gpu']:.1f}</td>"
                f"<td>{k['scan']['sem']:.1f} / {k['reduce']['sem']:.1f}</td>"
                f"<td>{k['scan']['taskT']:.1f} / {k['reduce']['taskT']:.1f}</td>"
                f"<td>{p.get('effgpu_s')}</td>"
                f"<td>{s['decode_s']:.1f}</td><td>{s['scan_s']:.1f}</td><td>{s['concat_s']:.1f}</td>"
                f"<td>{p.get('bptask')} / {shbpt}</td><td>{p.get('fullpct')}</td>"
                f"<td>{dec:.1f} / {decoded:.1f}<br><span class=mut>{exp:.2f} times</span></td>"
                f"<td>{m2['w_bytes']/G:.1f} / {m2['r_bytes']/G:.1f}</td>"
                f"<td>{m2['w_time']:.1f} / {m2['r_time']:.1f}</td>"
                f"<td>{unc:.1f}<br><span class=mut>{ratio_uc:.2f} times</span></td>"
                f"<td>{m2['skew']:.2f}</td>"
                f"<td>{act} {newp}</td>"
                f"<td>{e['spill_reduce']:.1f} / {e['spill_scan']:.1f}</td>"
                f"<td class={'ok' if e['fails']==0 else 'bad'}>{e['fails']}</td></tr>")

    swhead = "".join(f"<th>{m}</th>" for m in SWEEP)
    H = f"""<!doctype html><meta charset=utf-8><title>Clean 2x2 rerun, split sizing and partition count</title>
<style>
body{{font:14px/1.55 -apple-system,Segoe UI,Roboto,sans-serif;color:#1a1a1a;max-width:1500px;margin:2rem auto;padding:0 1rem}}
h1{{font-size:20px}}h2{{font-size:15px;margin-top:1.4rem}}
table{{border-collapse:collapse;font-size:12px;width:100%}}th,td{{border:1px solid #e3e3e3;padding:4px 7px;text-align:right}}
th:first-child,td:first-child,td.q,td:nth-child(2){{text-align:left}}thead{{background:#f6f8fa}}
td.q{{font-weight:600;vertical-align:top}}.mut{{color:#666;font-weight:400;font-size:10.5px}}
.ok{{color:#137333;font-weight:600}}.warn{{color:#b06000;font-weight:600}}.bad{{color:#c5221f;font-weight:600}}
.leg{{background:#f6f8fa;border:1px solid #e3e3e3;border-radius:8px;padding:.6rem 1rem;margin:1rem 0;font-size:12px}}
code{{background:#f2f2f2;padding:0 3px;border-radius:3px}}
</style>
<h1>Clean rerun: does split sizing or the partition count rule help</h1>
<p class=mut>A5000, Spark 3.5.3 with RAPIDS clean jar, local 16 cores, target batch 1 GiB, AQE on,
5 iterations per arm with the first dropped as cold. Wall is the MEDIAN of the warm iterations, with
every warm iteration listed beside it. All numbers come from the event log.</p>
<div class=leg>
Every time column is scan stage only and per iteration.
<b>gpu time</b>, <b>sem wait</b> and <b>task time</b> are given as scan tasks then reduce tasks. gpuTime is a per TASK accumulator with no node attribution, so it covers everything that task does on the GPU. Reporting scan tasks alone understates queries whose work is reduce side: pv06 reduce tasks use 258 s of GPU time against 84 s on the map side.
<b>task time</b> is Executor Run Time summed over scan tasks.
<b>sem wait</b> is time blocked waiting for a GPU permit.
<b>eff gpu</b> is gpu time plus sem wait.
<b>batches per task</b> is scan output batches divided by scan tasks.
<b>fullness</b> is the average emitted batch size as a percent of the target batch size.
<b>spill</b> is GPU spill in GiB, reduce side first then scan side.
<b>change vs baseline</b> compares medians and states the gap as a multiple of the two arms' pooled
per iteration spread, so anything at or below one times noise should be read as no change.
</div>
<h2>Split sweep with the autotuner off, median wall in seconds</h2>
<p class=mut>The winner in green is the split each baseline arm runs at. The baseline is never a
hand picked value.</p>
<table><thead><tr><th>query</th>{swhead}<th>best</th></tr></thead><tbody>
{chr(10).join(sweep_html) or '<tr><td>no sweeps present</td></tr>'}
</tbody></table>
<h2>The two by two</h2>
<table><thead><tr><th>query</th><th>arm</th><th>split</th><th>reducers</th>
<th>wall median s<br><span class=mut>warm iterations</span></th><th>sd</th><th>change vs baseline</th>
<th>tasks<br><span class=mut>scan / reduce</span></th><th>gpu time s<br><span class=mut>scan / reduce</span></th><th>sem wait s<br><span class=mut>scan / reduce</span></th><th>task time s<br><span class=mut>scan / reduce</span></th><th>eff gpu s<br><span class=mut>scan only</span></th>
<th>decode s</th><th>scan time s</th><th>concat s</th>
<th>batches per task<br><span class=mut>scan / shuffle</span></th><th>fullness pct</th>
<th>scan GiB<br><span class=mut>read / decoded</span></th>
<th>shuffle GiB<br><span class=mut>written / read</span></th>
<th>shuffle time s<br><span class=mut>write / read</span></th>
<th>data size GiB<br><span class=mut>uncompressed</span></th><th>skew</th>
<th>rule says</th><th>spill GiB<br><span class=mut>reduce / scan</span></th><th>failures</th></tr></thead>
<tbody>
{chr(10).join(body) or '<tr><td>no arms present</td></tr>'}
</tbody></table>
{'<p class=mut>' + '<br>'.join(notes) + '</p>' if notes else ''}
"""
    with open(outp, "w") as f:
        f.write(H)
    print(f"wrote {outp} with {len(body)} arm rows")


if __name__ == "__main__":
    main()
