#!/usr/bin/env python3
"""Window split-learning report.

Question: when a split is learnt on one slice of a table (or by another query), how far from THIS
job's own best does it land?

Reference is each job's own swept optimum (lowest warm-median wall), never a configured constant.
Every number is a warm median of iterations 2..N at a split that was actually run, so the inherited
split is measured, not inferred from one cold iteration.
"""
import argparse, csv, os, collections, statistics as st

ap = argparse.ArgumentParser()
ap.add_argument("--ledger", required=True)
ap.add_argument("--outroot", required=True)
ap.add_argument("--out", default=None)
A = ap.parse_args()
# Written straight into the directory the http server serves from, so there is ONE copy and it can
# never go stale. A symlink in this experiment's own results/ points at it.
SERVED = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                      "..", "..", "rolling-split-autotuning", "results")
BASE = A.out or os.path.join(SERVED, "window-report-" + os.path.basename(A.outroot.rstrip("/")))

all_rows = list(csv.DictReader(open(A.ledger), delimiter="\t"))
# HARD GATE: only fully successful arms may contribute a number. An arm that ran 3 of 5 iterations
# after an executor died still yields plausible-looking values; those must never reach a table.
rows = [r for r in all_rows if r.get("run_ok", "ok") == "ok"]
_ex = sorted({(r["arm"], r.get("run_ok", "?")) for r in all_rows if r.get("run_ok", "ok") != "ok"})
EXCLUDED = [(a, w) for a, w in _ex if not w.startswith("in-progress")]
RUNNING = [(a, w) for a, w in _ex if w.startswith("in-progress")]
for r in rows:
    r["iteration"] = int(r["iteration"])
    for k in ("wall_s", "occupancy_s", "decode_s", "task_time_s", "scan_time_s", "sem_wait_s"):
        r[k] = float(r[k] or 0)

MET = [("wall_s", "wall"), ("task_time_s", "task time"), ("occupancy_s", "gpuTime"),
       ("decode_s", "decode")]

# ---- warm medians per (query, window, split) from the SWEEP arms (autotuner off, fixed split)
# ONE arm per (query, window, split), so every median rests on exactly the same number of samples.
# Two arms can share a split (e.g. grid "2g" and refine "2048m" are both 2048 MiB); pooling them
# would silently give some rows twice the samples of others. The most recent arm wins.
by_arm = collections.defaultdict(list)
for r in rows:
    if not r["arm"].startswith("sweep-") or r["iteration"] == 1 or r["split_mb"] in ("-", ""):
        continue
    by_arm[(r["query"], r["window"], int(r["split_mb"]), r["arm"])].append(r)

def arm_time(arm):
    """When this arm actually ran. Ties between arms at the same split go to the newest measurement,
    so a re-measured optimum supersedes the stale one."""
    try:
        return os.path.getmtime(os.path.join(A.outroot, arm, "run.log"))
    except OSError:
        return 0.0

chosen, dropped = {}, []
for (q, w, sp, arm), recs in by_arm.items():
    prev = chosen.get((q, w, sp))
    if prev is None or len(recs) > len(prev[1]) or (
            len(recs) == len(prev[1]) and arm_time(arm) > arm_time(prev[0])):
        if prev is not None:
            dropped.append((q, w, sp, prev[0]))
        chosen[(q, w, sp)] = (arm, recs)
    else:
        dropped.append((q, w, sp, arm))

perf = {}
for key, (arm, recs) in chosen.items():
    perf[key] = {m: st.median([x[m] for x in recs]) for m, _ in MET}
    perf[key]["n"] = len(recs)
    perf[key]["arm"] = arm

ODD_N = sorted({(f"{q}@{w}", sp, p_["n"]) for (q, w, sp), p_ in perf.items() if p_["n"] != 4})

# ---- what each job learnt / inherited, from the LEARN arms
roles = {}          # (query, window, split) -> role string
for r in rows:
    if r["history_mode"] not in ("shared", "isolated") or r["split_mb"] in ("-", ""):
        continue
    k = (r["query"], r["window"], int(r["split_mb"]))
    lf = r["learnt_from"]
    role = ("spark-fallback" if lf.startswith("spark-maxSplitBytes")
            else "own-learnt" if lf == "own" else f"inherited from {lf}")
    if k not in roles or roles[k] == "spark-fallback":
        roles[k] = role





def job_chart(q, w, perf, roles):
    """Wall (blue, left axis) and gpuTime (orange, right axis) vs split, log2 x.
    Same style as gen_ratio_report.make_plot_b64 so the two report families look alike.
    Optimum = hollow ring; self-learnt = green star; inherited = red diamond."""
    os.environ.setdefault("MPLCONFIGDIR", os.path.join(os.environ.get("TMPDIR", "/tmp"), "mpl"))
    import io, base64
    import matplotlib; matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    sp = sorted(x for (qq, ww, x) in perf if (qq, ww) == (q, w))
    if len(sp) < 3:
        return None
    wl = [perf[(q, w, x)]["wall_s"] for x in sp]
    gp = [perf[(q, w, x)]["occupancy_s"] for x in sp]
    lbl = lambda m: (f"{m//1024}g" if m >= 1024 and m % 1024 == 0 else f"{m}m")
    fig, ax = plt.subplots(figsize=(6.4, 4.3)); ax2 = ax.twinx()
    ax.plot(sp, wl, "o-", color="#1a5fa8", lw=2, zorder=3, label="wall")
    ax2.plot(sp, gp, "s--", color="#c0651b", lw=1.4, zorder=2, label="gpuTime")
    ax.set_xscale("log", base=2); ax.set_xticks(sp)
    ax.set_xticklabels([lbl(x) for x in sp], fontsize=7, rotation=45)
    ax.set_xlabel("split"); ax.set_ylabel("wall (s)", color="#1a5fa8")
    ax2.set_ylabel("gpuTime (s)", color="#c0651b")
    ax.tick_params(axis="y", labelcolor="#1a5fa8"); ax2.tick_params(axis="y", labelcolor="#c0651b")
    opt = min(sp, key=lambda x: perf[(q, w, x)]["wall_s"])
    i = sp.index(opt)
    # vertical guide at each split that matters, so the three can be compared at a glance
    # Values go in the legend, not inline: the marked splits are often within a few percent of each
    # other (256m/296m/308m), so inline annotations overlap and become unreadable.
    ax.axvline(opt, color="#1a5fa8", ls=":", lw=1.4, zorder=1)
    ax.plot(opt, wl[i], "o", ms=13, mfc="none", mec="#1a5fa8", mew=1.8, zorder=5,
            label=f"optimum {lbl(opt)} - {wl[i]:.1f}s")
    for v in sp:
        r = roles.get((q, w, v))
        if v == opt or not r:
            continue
        j = sp.index(v)
        if r == "own-learnt":
            ax.axvline(v, color="#1baf7a", ls=":", lw=1.4, zorder=1)
            ax.plot(v, wl[j], "*", color="#1baf7a", ms=15, zorder=6,
                    label=f"self-learnt {lbl(v)} - {wl[j]:.1f}s")
        elif r.startswith("inherited"):
            src = r.replace("inherited from ", "")
            ax.axvline(v, color="#e0483d", ls=":", lw=1.4, zorder=1)
            ax.plot(v, wl[j], "D", ms=9, mfc="#e0483d", mec="#7a1414", mew=1.2, zorder=6,
                    label=f"inherited {lbl(v)} from {src} - {wl[j]:.1f}s")
    ax.set_title(f"{q} @ {w}   (blue = wall, orange dashed = gpuTime)", fontsize=9)
    h, l = ax.get_legend_handles_labels()
    if h:
        ax.legend(h, l, fontsize=6.6, loc="upper center", bbox_to_anchor=(0.5, -0.28),
                  ncol=1, frameon=False, handletextpad=0.5)
    fig.tight_layout()
    buf = io.BytesIO(); fig.savefig(buf, format="png", dpi=110); plt.close(fig)
    return base64.b64encode(buf.getvalue()).decode()


def seq_svg(name, q1, w1, q2, w2, tests, rows, perf, cold):
    """Inline SVG: app1 learns -> history file -> app2 (separate JVM) reads it.

    The point the text timeline failed to make is that the two steps are SEPARATE applications and
    the arrow through the history file is the only thing connecting them.
    """
    def prog(arm):
        rs = sorted([r for r in rows if r["arm"] == arm], key=lambda r: r["iteration"])
        if not rs:
            return None, None, None
        first = rs[0]
        conv = [r for r in rs if r["learnt_from"] == "own"]
        return (first["split_mb"], first["learnt_from"],
                conv[-1]["split_mb"] if conv else first["split_mb"])
    a1_cold, a1_src, a1_conv = prog(f"{name}-shared-1")
    a2_cold, a2_src, a2_conv = prog(f"{name}-shared-2")
    if a1_cold is None or a2_cold is None:
        return ""
    inherited = a2_src and not a2_src.startswith("spark-maxSplitBytes")
    sp = sorted(x for (qq, ww, x) in perf if (qq, ww) == (q2, w2))
    opt = min(sp, key=lambda x: perf[(q2, w2, x)]["wall_s"]) if sp else None
    B, G, O, R = "#2b6cb0", "#718096", "#c05621", "#c53030"
    def box(x, y, w, h, stroke, fill):
        return (f"<rect x={x} y={y} width={w} height={h} rx=6 fill='{fill}' stroke='{stroke}' "
                f"stroke-width='1.5'/>")
    t = lambda x, y, s_, sz=12, c="#1a1a1a", b=0, anc="start": (
        f"<text x={x} y={y} font-size={sz} fill='{c}' text-anchor='{anc}' "
        f"font-family='-apple-system,Segoe UI,Roboto,sans-serif'"
        f"{' font-weight=600' if b else ''}>{s_}</text>")
    p_ = []
    p_.append("<svg width='100%' viewBox='0 0 900 258' style='max-width:900px'>")
    p_.append("<defs><marker id='ah' markerWidth='9' markerHeight='9' refX='8' refY='3' orient='auto'>"
              f"<path d='M0,0 L0,6 L8,3 z' fill='{G}'/></marker>"
              "<marker id='ahr' markerWidth='9' markerHeight='9' refX='8' refY='3' orient='auto'>"
              f"<path d='M0,0 L0,6 L8,3 z' fill='{R}'/></marker></defs>")
    # app 1
    p_.append(box(8, 30, 250, 120, B, "#f7fbff"))
    p_.append(t(20, 22, "APPLICATION 1 &#183; JVM starts", 10.5, G))
    p_.append(t(20, 52, f"{q1} @ {w1}", 14, B, 1))
    p_.append(t(20, 76, f"iter 1: {a1_cold}m", 12))
    p_.append(t(120, 76, "&#8592; Spark fallback (no history)", 10.5, G))
    p_.append(t(20, 98, f"iter 2-5: {a1_conv}m", 12, B, 1))
    p_.append(t(140, 98, "&#8592; measured its own data", 10.5, G))
    p_.append(t(20, 126, "JVM exits", 10.5, G))
    # history
    p_.append(f"<line x1=262 y1=90 x2=340 y2=90 stroke='{G}' stroke-width='1.5' marker-end='url(#ah)'/>")
    p_.append(t(268, 82, "writes", 10.5, G))
    p_.append(box(345, 55, 175, 70, G, "#f7f7f7"))
    p_.append(t(432, 76, "history file", 11, G, 0, "middle"))
    p_.append(t(432, 100, f"{a1_conv}m", 16, "#1a1a1a", 1, "middle"))
    p_.append(t(432, 143, f"key = table only", 10, G, 0, "middle"))
    p_.append(t(432, 156, "(no query, no window)", 10, G, 0, "middle"))
    # arrow to app2
    col = R if inherited else G
    mk = "url(#ahr)" if inherited else "url(#ah)"
    p_.append(f"<line x1=524 y1=90 x2=612 y2=90 stroke='{col}' stroke-width='2' marker-end='{mk}'/>")
    p_.append(t(530, 82, "reads" if inherited else "empty", 10.5, col, 1))
    # app 2
    p_.append(box(616, 30, 276, 120, O, "#fffaf5"))
    p_.append(t(628, 22, "APPLICATION 2 &#183; fresh JVM", 10.5, G))
    p_.append(t(628, 52, f"{q2} @ {w2}", 14, O, 1))
    if inherited:
        p_.append(t(628, 76, f"iter 1: {a2_cold}m", 12, R, 1))
        p_.append(t(716, 76, f"&#8592; INHERITED from {a2_src}", 10.5, R, 1))
    else:
        p_.append(t(628, 76, f"iter 1: {a2_cold}m", 12))
        p_.append(t(716, 76, "&#8592; Spark fallback", 10.5, G))
    p_.append(t(628, 98, f"iter 2-5: {a2_conv}m", 12, O, 1))
    p_.append(t(740, 98, "&#8592; overwrites with its own", 10.5, G))
    if opt:
        p_.append(t(628, 126, f"this job's optimum: {opt}m", 11, "#2f855a", 1))

    # What the inherited split actually cost, against a run at this job's own optimum. Both are warm
    # medians at a pinned split, so they are directly comparable.
    inh = int(a2_cold) if str(a2_cold).isdigit() else None
    pi = perf.get((q2, w2, inh)) if inh else None
    po = perf.get((q2, w2, opt)) if opt else None
    if pi and po:
        y = 176
        p_.append(f"<line x1=8 y1={y-12} x2=892 y2={y-12} stroke='#e3e3e3'/>")
        p_.append(t(8, y, f"{q2}@{w2} measured at each split (warm median):", 10.5, G))
        cols = [(150, "split"), (250, "wall s"), (370, "gpuTime s"), (500, "decode s")]
        for x, lab in cols:
            p_.append(t(x, y, lab, 10, G, 0, "middle"))
        rows_ = [("inherited", inh, pi, R if inherited else "#1a1a1a"),
                 ("this job's optimum", opt, po, "#2f855a")]
        for i, (lab, sp_, pv, c) in enumerate(rows_):
            yy = y + 20 + i * 18
            p_.append(t(8, yy, lab, 11, c, 1))
            p_.append(t(150, yy, f"{sp_}m", 11, c, 0, "middle"))
            for x, k in ((250, "wall_s"), (370, "occupancy_s"), (500, "decode_s")):
                p_.append(t(x, yy, f"{pv[k]:.2f}", 11, c, 0, "middle"))
        yy = y + 20 + 2 * 18
        d = lambda k: (pi[k] - po[k]) / po[k] * 100
        ds = (inh - opt) / opt * 100
        p_.append(t(8, yy, "difference", 11, G, 1))
        p_.append(t(150, yy, f"{ds:+.0f}%", 11, G, 0, "middle"))
        for x, k in ((250, "wall_s"), (370, "occupancy_s"), (500, "decode_s")):
            v = d(k)
            p_.append(t(x, yy, f"{v:+.1f}%", 11,
                        "#c53030" if v > 9 else ("#2f855a" if v < -1 else G), 1, "middle"))
    p_.append("</svg>")
    return "".join(p_)


# Same order as the sequence sections: by scan intensity of the query (bytes read / bytes listed),
# heaviest first, then window. Alphabetical would put csH3 last while it leads everywhere else.
QRANK_JOBS = {"csH3": 0, "cs04": 1, "cs02": 2}
# Median-based candidates. Today's rule is last-writer-wins (ScanSplitAutotuner.scala:130-131);
# these are order-independent alternatives, scored offline against the same sweeps.
_own = collections.defaultdict(dict)
for r in rows:
    if r["learnt_from"] == "own" and r["split_mb"] not in ("-", ""):
        _own[r["query"]][r["window"]] = int(r["split_mb"])
# Cross-query inheritance uses the SOURCE QUERY'S MEDIAN across its windows, not whichever window
# happened to run last. That makes the inherited value a property of the source query rather than an
# artifact of run order.
QMED = {q: int(st.median(sorted(v.values()))) for q, v in _own.items()}

jobs = sorted({(q, w) for (q, w, _) in perf}, key=lambda j: (QRANK_JOBS.get(j[0], 9), j[1]))
H, M = [], []
H.append("""<!doctype html><meta charset=utf-8><title>Window split learning</title><style>
body{font:14px/1.55 -apple-system,Segoe UI,Roboto,sans-serif;color:#1a1a1a;max-width:1200px;margin:2rem auto;padding:0 1rem}
h1{font-size:20px}h2{font-size:15px;margin-top:1.6rem}
table{border-collapse:collapse;font-size:12px;width:100%}th,td{border:1px solid #e3e3e3;padding:4px 7px;text-align:right}
th:first-child,td:first-child{text-align:left}thead{background:#f6f8fa}
.opt{background:#eef7ee;font-weight:600}.ok{background:#f4faf4}.bad{background:#fdf1f0}.warn{background:#fffaf0}.mut{color:#666;font-size:11px}
</style><h1>Window split learning</h1>""")
M.append("# Window split learning\n")
intro = ("Reference is each job's own swept optimum (lowest warm-median wall), not a configured "
         "constant. All values are warm medians of iterations 2..N at a split that was actually run. "
         "128m is Spark's stock default; the runs otherwise set maxPartitionBytes=2g.")
H.append(f"<p class=mut>{intro}</p>"); M.append(intro + "\n")
if ODD_N:
    H.append("<div style='border:1px solid #f0b0a0;background:#fdf1f0;padding:.5rem;margin:.5rem 0;"
             "font-size:12px'><b>Rows not based on 4 warm iterations</b> (expected 4 = iters 2-5 of one "
             "arm):<ul>" + "".join(f"<li>{j} {sp}m &mdash; n={n}</li>" for j, sp, n in ODD_N) + "</ul></div>")
    M.append("\n> **Rows not based on 4 warm iterations:** "
             + "; ".join(f"{j} {sp}m n={n}" for j, sp, n in ODD_N) + "\n")

if RUNNING:
    H.append("<div style='border:1px solid #cfe0f0;background:#f4f9fd;padding:.5rem;margin:.5rem 0;"
             "font-size:12px'><b>Still running</b> &mdash; these arms have not finished their "
             "iterations yet, so their cells read <i>pending</i>. Not a failure:<ul>"
             + "".join(f"<li><code>{a}</code> &mdash; {w}</li>" for a, w in RUNNING) + "</ul></div>")
if EXCLUDED:
    warn = ("<div style='border:1px solid #f0b0a0;background:#fdf1f0;padding:.6rem;margin:.6rem 0;"
            "font-size:12px'><b>Excluded from every table below</b> &mdash; these arms did not "
            "complete all iterations or hit a fatal error, so none of their numbers are used:<ul>"
            + "".join(f"<li><code>{a}</code> &mdash; {w}</li>" for a, w in EXCLUDED) + "</ul></div>")
    H.append(warn)
    M.append("\n> **Excluded from every table below** - these arms did not complete all iterations "
             "or hit a fatal error, so none of their numbers are used:\n")
    for a, w in EXCLUDED:
        M.append(f"> - `{a}` - {w}")
    M.append("")

for q, w in jobs:
    splits = sorted(x for (qq, ww, x) in perf if (qq, ww) == (q, w))
    if not splits:
        continue
    opt = min(splits, key=lambda x: perf[(q, w, x)]["wall_s"])
    d128 = perf.get((q, w, 128))
    H.append(f"<h2>{q} @ {w}</h2>")
    _b64 = job_chart(q, w, perf, roles)
    if _b64:
        H.append(f"<img src='data:image/png;base64,{_b64}' style='max-width:640px;display:block'/>")
    M.append(f"\n## {q} @ {w}\n")
    # only the splits that mean something; the full grid is the curve above
    keep = [(opt, "optimum")]
    for v in splits:
        r = roles.get((q, w, v))
        if v == opt:
            continue
        if r == "own-learnt":
            keep.append((v, "self-learnt"))
        # run-order inherited rows are dropped: which window's value got inherited was an artifact of
        # the sequence order, not a policy. The cross-query rows below use the source query's median.
    # median-rule candidates: shown at the nearest measured split when not measured exactly
    def _near(v):
        return v if (q, w, v) in perf else min(splits, key=lambda x: abs(x - v))
    for oq in sorted(QMED):
        if oq == q:
            continue
        v = _near(QMED[oq])
        tag = f"{QMED[oq]}m" + ("" if v == QMED[oq] else f" (at {v}m)")
        keep.append((v, f"cross-query &larr; {oq} median {tag}"))
    if d128 and all(v != 128 for v, _ in keep):
        keep.append((128, "Spark 128m default"))
    seen_v = set(); dedup = []
    for v, role in keep:
        if v in seen_v:
            continue
        seen_v.add(v); dedup.append((v, role))
    keep = dedup
    H.append("<table><thead><tr><th>split</th><th>role</th><th>n</th>"
             + "".join(f"<th>{lab} s</th><th>vs opt</th>" for _, lab in MET)
             + "</tr></thead><tbody>")
    M.append("| split | role | n | " + " | ".join(f"{lab} s | vs opt" for _, lab in MET) + " |")
    M.append("|---" * (3 + 2 * len(MET)) + "|")
    for v, role in keep:
        pv = perf[(q, w, v)]
        cells = [f"{v}m", role, str(pv["n"])]
        for m, _ in MET:
            base = perf[(q, w, opt)][m]
            cells += [f"{pv[m]:.2f}", "-" if v == opt else f"{(pv[m]-base)/base*100:+.1f}%"]
        cls = ("opt" if v == opt else "bad" if role.startswith("inherited")
               else "warn" if "median" in role else "mut")
        H.append(f"<tr class={cls}>" + "".join(f"<td>{c}</td>" for c in cells) + "</tr>")
        M.append("| " + " | ".join(cells) + " |")
    H.append("</tbody></table>")

# =====================================================================================
# Section 2: the cold run that actually happened. In production a query runs ONCE with
# whatever split it inherited, so this is the real scenario, not a throwaway. Compared
# cold-to-cold: shared-2 iter1 (inherited split) vs iso-2 iter1 (empty history -> Spark
# fallback) vs off-2 iter1 (autotuner disabled). Same query, same window, same position.
# =====================================================================================
import subprocess
SEQ = [l.split() for l in subprocess.run(
    ["python3", os.path.join(os.path.dirname(os.path.abspath(__file__)), "wincfg.py"), "sequences"],
    capture_output=True, text=True).stdout.strip().splitlines() if l.strip()]
# Ordered by WHICH QUESTION the sequence answers, then chronologically by window. Q1 (same query,
# different window) first because it is the simpler case and the baseline for reading Q2.
Q1 = ("adjacent-window-drift", "distant-window-drift", "window-drift-scan-heavy",
      "filtered-query-window-drift")
Q2 = ("cross-query-same-window", "cross-query-similar-ratio-control",
      "cross-query-reverse-lo-to-hi", "filter-vs-projection-same-window",
      "cross-query-AND-window-confounded")
ORDER = {t: i for i, t in enumerate(Q1 + Q2)}
# Within a question, order by SCAN INTENSITY of the donor (bytes read / bytes listed, measured):
# csH3 ~302% (reads all 4 cols 3x via UNION ALL), cs04 ~101% (all 4 cols), cs02 ~39-74% (drops one).
# The split lever matters most where the scan dominates, so the heaviest query reads first.
QRANK = {"csH3": 0, "cs04": 1, "cs02": 2}
def seq_group(t):
    return "Q1 - same query, different data window" if t in Q1 else "Q2 - different query"
SEQ.sort(key=lambda r: (0 if r[5] in Q1 else 1, QRANK.get(r[1], 9), r[2], r[4],
                        ORDER.get(r[5], 99)))

# Iteration 1 of a SWEEP arm is a cold run at a known split - the correct cold reference, unlike the
# 2g fallback which only reflects our own maxPartitionBytes setting.
sweep_cold = {}
for r in rows:
    if r["arm"].startswith("sweep-") and r["iteration"] == 1 and r["split_mb"] not in ("-", ""):
        sweep_cold[(r["query"], r["window"], int(r["split_mb"]))] = r

cold = {}
for r in rows:
    if r["iteration"] != 1:
        continue
    for suf in ("-shared-2", "-iso-2", "-off-2", "-shared-1", "-iso-1", "-off-1"):
        if r["arm"].endswith(suf):
            cold[(r["arm"][:-len(suf)], suf.replace("-", ""))] = r

if SEQ:
    # One row per sequence, head-to-head. Single cold measurements, so a delta smaller than the
    # measured p90 run-to-run CoV (~5% wall, ~5% gpuTime) is reported as "within noise", not a win.
    NOISE = 5.0
    H.append("<h2>Cold start &mdash; the run that actually inherited</h2><p class=mut>"
             "RUN-ORDER VALUES (measured): the inherited split is whichever window the source query "
             "ran in this sequence, not its median. "
             "In production a query runs ONCE with whatever split it inherited, so this is the real "
             "scenario. Compared against a <b>cold run at this job's own optimum split</b> (iteration 1 "
             "of the corresponding sweep arm) &mdash; not against the 2g fallback, which only reflects "
             "our own maxPartitionBytes setting. Both rows are iteration 1, same query, same window. "
             f"<b>One measurement each</b>, so a gap under {NOISE:.0f}% is inside the measured spread.</p>"
             "<table><thead><tr><th>sequence</th><th>inherited</th><th>vs cold @optimum</th>"
             "<th>wall</th><th>gpuTime</th><th>decode</th><th>verdict</th></tr></thead><tbody>")
    M.append("\n## Cold start - the run that actually inherited\n")
    M.append("In production a query runs ONCE with whatever split it inherited. Compared against a cold "
             "run at this job's own optimum split (iteration 1 of the matching sweep arm), NOT against "
             "the 2g fallback. Both rows are iteration 1, same query, same window. One measurement "
             f"each, so a gap under {NOISE:.0f}% is inside the measured spread.\n")
    M.append("| sequence | inherited | vs cold @optimum | wall | gpuTime | decode | verdict |")
    M.append("|---|---|---|---|---|---|---|")
    for name, q1, w1, q2, w2, tests in SEQ:
        sh = cold.get((name, "shared2"))
        if not sh or sh["split_mb"] in ("-", "") or sh["learnt_from"].startswith("spark-maxSplitBytes"):
            continue
        sp = sorted(x for (qq, ww, x) in perf if (qq, ww) == (q2, w2))
        if not sp:
            continue
        o = min(sp, key=lambda x: perf[(q2, w2, x)]["wall_s"])
        ref = sweep_cold.get((q2, w2, o))
        if not ref:
            continue
        d = {}
        for m, lab in (("wall_s", "wall"), ("occupancy_s", "gpuTime"), ("decode_s", "decode")):
            d[lab] = (sh[m] - (ref[m] or 1e-9)) / (ref[m] or 1e-9) * 100
        worse = [k for k, v in d.items() if v >= NOISE]
        better = [k for k, v in d.items() if v <= -NOISE]
        if not worse:
            verd, cls = "as good as optimum", "ok"
        elif worse and better:
            verd, cls = f"worse {'/'.join(worse)}, better {'/'.join(better)}", "warn"
        else:
            verd, cls = f"worse on {'/'.join(worse)}", "bad"
        cells = [name, f"{sh['split_mb']}m &larr; {sh['learnt_from']}", f"{o}m",
                 f"{sh['wall_s']:.2f} vs {ref['wall_s']:.2f} ({d['wall']:+.1f}%)",
                 f"{sh['occupancy_s']:.1f} vs {ref['occupancy_s']:.1f} ({d['gpuTime']:+.1f}%)",
                 f"{sh['decode_s']:.1f} vs {ref['decode_s']:.1f} ({d['decode']:+.1f}%)", verd]
        H.append(f"<tr class={cls}>" + "".join(f"<td>{c}</td>" for c in cells) + "</tr>")
        M.append("| " + " | ".join(str(c).replace("&larr;", "<-") for c in cells) + " |")
    H.append("</tbody></table>")

# =====================================================================================
# Section 3: the two questions, answered per sequence.
# =====================================================================================
if SEQ:
    H.append("<h2>Answers</h2><p class=mut>One block per sequence. Every candidate was measured as a "
             "pinned sweep arm (warm median of iters 2-5), so all rows are comparable. PASS = within "
             "9% of this job's own optimum on BOTH wall and gpuTime; a saving always passes.</p>")
    M.append("\n## Answers\n")
    M.append("One block per sequence. Every candidate measured as a pinned sweep arm (warm median of "
             "iters 2-5). PASS = within 9% of this job's own optimum on BOTH wall and gpuTime; a "
             "saving always passes.\n")
    for name, q1, w1, q2, w2, tests in SEQ:
        sp = sorted(x for (qq, ww, x) in perf if (qq, ww) == (q2, w2))
        if not sp:
            continue
        o = min(sp, key=lambda x: perf[(q2, w2, x)]["wall_s"])
        ow, og = perf[(q2, w2, o)]["wall_s"], perf[(q2, w2, o)]["occupancy_s"]
        sh = cold.get((name, "shared2"))
        isow = [r for r in rows if r["arm"] == f"{name}-iso-2" and r["iteration"] > 1
                and r["split_mb"] not in ("-", "")]
        cands = [("this job's optimum", o)]
        if q1 != q2 and q1 in QMED:
            # CROSS-QUERY: use the source query's median across its windows. Which window's value got
            # inherited in the run was an artifact of sequence order, not a property of the source.
            v = QMED[q1] if (q2, w2, QMED[q1]) in perf else min(sp, key=lambda x: abs(x - QMED[q1]))
            tag = f"{QMED[q1]}m" if v == QMED[q1] else f"{QMED[q1]}m (at {v}m)"
            cands.append((f"cross-query &larr; {q1} median {tag}", v))
        elif sh and sh["split_mb"] not in ("-", ""):
            cands.append((f"inherited &larr; {sh['learnt_from']}", int(sh["split_mb"])))
        if isow:
            cands.append(("own-learnt (no inheritance)", int(isow[-1]["split_mb"])))
        cands.append(("Spark 128m default", 128))
        _sn = set(); cands = [c for c in cands if not (c[1] in _sn or _sn.add(c[1]))]

        grp = seq_group(tests)
        if grp != globals().get("_last_grp_ans"):
            H.append(f"<h3 style='font-size:14px;margin:1.8rem 0 .2rem;border-bottom:2px solid #ddd;"
                     f"padding-bottom:.2rem'>{grp}</h3>")
            M.append(f"\n### {grp}\n")
            globals()["_last_grp_ans"] = grp
        hdr = f"{q1}@{w1} &rarr; {q2}@{w2}"
        sub = f"{tests} &middot; target <b>{q2}@{w2}</b> &middot; optimum {o}m at {ow:.2f}s"
        H.append(f"<h4 style='font-size:13px;margin:1.2rem 0 .3rem'>{hdr}</h4>"
                 f"<p class=mut style='margin:.1rem 0 .4rem'>{sub}</p>"
                 "<table><thead><tr><th>candidate</th><th>split</th><th>wall s</th>"
                 "<th>gpuTime s</th><th>verdict</th></tr></thead><tbody>")
        M.append(f"\n#### {q1}@{w1} -> {q2}@{w2}\n")
        M.append(f"*{tests}* &middot; target **{q2}@{w2}** &middot; optimum {o}m at {ow:.2f}s\n")
        M.append("| candidate | split | wall s | gpuTime s | verdict |")
        M.append("|---|---|---|---|---|")
        for lab, v in cands:
            p_ = perf.get((q2, w2, v))
            if not p_:
                row = [lab, f"{v}m", "not measured", "-", "-"]; cls = "mut"
            else:
                dw = (p_["wall_s"] - ow) / ow * 100
                dg = (p_["occupancy_s"] - og) / og * 100
                if v == o:
                    wtxt, gtxt, verd, cls = f"{p_['wall_s']:.2f}", f"{p_['occupancy_s']:.2f}", "reference", "opt"
                else:
                    wtxt = f"{p_['wall_s']:.2f} ({dw:+.1f}%)"
                    gtxt = f"{p_['occupancy_s']:.2f} ({dg:+.1f}%)"
                    ok = dw <= 9 and dg <= 9
                    verd = "PASS" if ok else ("FAIL wall" if dw > 9 and dg <= 9
                            else "FAIL gpuTime" if dg > 9 and dw <= 9 else "FAIL both")
                    cls = "ok" if ok else "bad"
                row = [lab, f"{v}m", wtxt, gtxt, verd]
            H.append(f"<tr class={cls}>" + "".join(f"<td>{c}</td>" for c in row) + "</tr>")
            M.append("| " + " | ".join(str(c).replace("&larr;", "<-").replace("&rarr;", "->") for c in row) + " |")
        H.append("</tbody></table>")


# =====================================================================================
# Timeline: the actual order of executions per shared history file, and which record each
# one read. This is where inheritance is visible as an event rather than a summary.
# =====================================================================================
if SEQ:
    H.append("<h2>Timeline &mdash; who inherited what</h2><p class=mut>One block per shared history "
             "file. Each line is one execution in run order. Step 2 runs in a SEPARATE application, so "
             "a record it reads survived a JVM restart. &#9656; marks the inheritance event.</p>")
    M.append("\n## Timeline - who inherited what\n")
    M.append("One block per shared history file. Each line is one execution in run order. Step 2 runs "
             "in a separate application, so a record it reads survived a JVM restart. `>>` marks the "
             "inheritance event.\n")
    for name, q1, w1, q2, w2, tests in SEQ:
        lines = []
        for step, (qq, ww) in ((1, (q1, w1)), (2, (q2, w2))):
            rs = sorted([r for r in rows if r["arm"] == f"{name}-shared-{step}"],
                        key=lambda r: r["iteration"])
            for r in rs:
                src = r["learnt_from"]
                mark = ">>" if (step == 2 and r["iteration"] == 1 and
                                not src.startswith("spark-maxSplitBytes")) else "  "
                lines.append((mark, f"app{step}", f"{qq}@{ww}", f"it{r['iteration']}",
                              f"{r['split_mb']}m", src, f"{r['wall_s']:.2f}s"))
        if not lines:
            continue
        grp = seq_group(tests)
        if grp != globals().get("_last_grp_tl"):
            H.append(f"<h3 style='font-size:14px;margin:1.8rem 0 .2rem;border-bottom:2px solid #ddd;"
                     f"padding-bottom:.2rem'>{grp}</h3>")
            M.append(f"\n### {grp}\n")
            globals()["_last_grp_tl"] = grp
        H.append(f"<h4 style='font-size:13px;margin:1.4rem 0 .3rem'>{name} &mdash; <span class=mut>{tests}</span></h4>")
        H.append(seq_svg(name, q1, w1, q2, w2, tests, rows, perf, cold))
        H.append("<details><summary class=mut style='font-size:11px;cursor:pointer'>per-iteration detail</summary>"
                 "<pre style='font-size:11.5px;background:#fafafa;border:1px solid #eee;padding:.5rem;overflow-x:auto'>")
        M.append(f"\n#### {name}\n\n```")
        for mark, app, qw, it, sp, src, w in lines:
            ln = f"{mark} {app:5s} {qw:12s} {it:4s} split={sp:>7s}  from={src:<28s} wall={w}"
            H.append(ln); M.append(ln)
        H.append("</pre></details>"); M.append("```")

H.append("</body>")
# atomic: write to .tmp then rename, so the http server never serves a half-written file
for ext, body in ((".html", "\n".join(H)), (".md", "\n".join(M))):
    tmp = BASE + ext + ".tmp"
    with open(tmp, "w") as fh:
        fh.write(body + "\n")
    os.replace(tmp, BASE + ext)
print(f"wrote {os.path.normpath(BASE)}.{{html,md}}  jobs={len(jobs)}")
