#!/usr/bin/env python3
"""Three-section answer report: Q1, Q2, then correlations.

Q1  same query, different data window   - does a split learnt on window A work on window B?
Q2  different query, median source      - does query A's median split work for query B?
Q3  correlations and conclusions

Every cell is a warm median (iters 2..N) at a PINNED split from a sweep arm, so all rows are
comparable. A split that was never measured on that job is scored at the nearest measured split and
marked with '~'; those cells are estimates, not measurements.
"""
import argparse, csv, os, collections, statistics as st

ap = argparse.ArgumentParser()
ap.add_argument("--ledger", required=True)
ap.add_argument("--outroot", required=True)
ap.add_argument("--out", default=None)
ap.add_argument("--chart", default=None, metavar="QUERY@WINDOW:PATH",
                help="render one sweep chart to PATH and exit WITHOUT writing the report. Use this "
                     "to preview a chart; importing this module instead re-runs the writer and "
                     "silently drops any section whose --q2-ledger was not passed.")
ap.add_argument("--explain", default=None, metavar="QUERY",
                help="drill into one query's sweep from the RAW event logs under --outroot: per "
                     "window and split, the scan-stage task count, span, gpuTime, semaphore-holder "
                     "concurrency and wait; plus each window's input-file and row-group statistics. "
                     "Prints and exits WITHOUT writing the report.")
ap.add_argument("--q2-ledger", default=None,
                help="ledger from the all-data Q2 run; when given, Q2 is reported from EXECUTED "
                     "transfers on the whole table instead of window-scoped ones")
A = ap.parse_args()
SERVED = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                      "..", "..", "rolling-split-autotuning", "results")
BASE = A.out or os.path.join(SERVED, "answers-" + os.path.basename(A.outroot.rstrip("/")))

rows = [r for r in csv.DictReader(open(A.ledger), delimiter="\t") if r.get("run_ok", "ok") == "ok"]

sweep = collections.defaultdict(list)
for r in rows:
    if r["arm"].startswith("sweep-") and int(r["iteration"]) > 1 and r["split_mb"] not in ("-", ""):
        sweep[(r["query"], r["window"], int(r["split_mb"]))].append(r)
perf = {k: {m: st.median([float(x[m]) for x in v]) for m in ("wall_s", "occupancy_s", "decode_s")}
        for k, v in sweep.items()}

own = collections.defaultdict(dict)          # query -> window -> split it learnt there
for r in rows:
    if r["learnt_from"] == "own" and r["split_mb"] not in ("-", ""):
        own[r["query"]][r["window"]] = int(r["split_mb"])
QMED = {q: int(st.median(sorted(v.values()))) for q, v in own.items()}
QUERIES = ["csH3", "cs04", "cs02"]
WINDOWS = ["W1", "W2", "W3"]
# one shape+colour per SOURCE query, stable across every chart, so a mark is readable without
# tracing back to the legend. Two sources previously shared the diamond.
SRC_MARK = {"csH3": ("D", "#c22", 15), "cs04": ("^", "#7b3fb8", 19), "cs02": ("p", "#0b8a8a", 23)}
TARGET = 1024.0                              # batchSizeBytes in MiB; ratio = TARGET / split

def splits_for(q, w):
    return sorted(x for (qq, ww, x) in perf if (qq, ww) == (q, w))

def noise_sd(q, w):
    """Pooled within-split run-to-run stdev of wall for this job, in seconds.

    Each split was measured over several warm iterations, so the spread WITHIN one split is pure
    run-to-run noise. Pooling those gives the floor below which two splits cannot be separated.
    """
    sds = [st.stdev([float(x["wall_s"]) for x in sweep[(q, w, s)]])
           for s in splits_for(q, w) if len(sweep[(q, w, s)]) > 1]
    return st.mean(sds) if sds else 0.0


def opt_band(q, w):
    """Every split whose median wall is within one pooled noise sd of the best.

    A bare argmin over median wall is not an optimum when the curve is flat: on cs04@W3 the argmin
    (2048m, 16.04s) beats 367m (16.16s) by 0.12s against a 2.18s noise floor. Scoring "cost vs the
    optimum" against such a pick reports the pick, not the split. Callers that need a single number
    still get one from opt_of, but they should say the band alongside it.
    """
    sp = splits_for(q, w)
    if not sp:
        return []
    best = min(perf[(q, w, x)]["wall_s"] for x in sp)
    sd = noise_sd(q, w)
    return [x for x in sp if perf[(q, w, x)]["wall_s"] - best <= sd]


def band_centre(band):
    """The band member nearest the band's geometric centre.

    Splits live on a multiplicative scale, so the centre is exp(mean(log)). Taking band[len//2]
    instead would pick the UPPER middle of an even-length band and bias every pick high.
    """
    import math
    if not band:
        return None
    c = math.exp(st.mean([math.log(x) for x in band]))
    return min(band, key=lambda x: abs(math.log(x) - math.log(c)))


def opt_of(q, w):
    """This job's optimum: the CENTRE of the set of splits its data cannot separate.

    Not the argmin of median wall. Eight of the nine jobs have several splits within one pooled
    noise sd of the best, so an argmin reports which tie member happened to win a coin flip -
    on cs04@W3 it picked 2048m over 367m by 0.12s against a 2.18s noise floor, and every "cost
    vs optimum" in this report was then measured against that pick.
    """
    return band_centre(opt_band(q, w))

def score(q, w, want):
    """Cost of using `want` on job (q,w), vs that job's own optimum. Returns (used, dwall, dgpu, exact)."""
    sp = splits_for(q, w)
    o = opt_of(q, w)
    if not sp or o is None:
        return None
    used = want if (q, w, want) in perf else min(sp, key=lambda x: abs(x - want))
    p, b = perf[(q, w, used)], perf[(q, w, o)]
    return (used, (p["wall_s"] - b["wall_s"]) / b["wall_s"] * 100,
            (p["occupancy_s"] - b["occupancy_s"]) / b["occupancy_s"] * 100, used == want)

def sweep_chart(q, w):
    """Wall (blue, left) and gpuTime (orange, right) across every measured split for one job.
    Marks: optimum, this job's own learnt value, and each source query's median."""
    os.environ.setdefault("MPLCONFIGDIR", os.path.join(os.environ.get("TMPDIR", "/tmp"), "mpl"))
    import io, base64
    import matplotlib; matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    sp = splits_for(q, w)
    if len(sp) < 3:
        return None
    wl = [perf[(q, w, x)]["wall_s"] for x in sp]
    gp = [perf[(q, w, x)]["occupancy_s"] for x in sp]
    lbl = lambda m: (f"{m//1024}g" if m >= 1024 and m % 1024 == 0 else f"{m}m")
    fig, ax = plt.subplots(figsize=(7.2, 4.8)); ax2 = ax.twinx()
    # evenly spaced by index, not by value: a log axis crowds 296/308/367/384 into each other and
    # the tick labels collide, which reads as missing points
    xs = list(range(len(sp)))
    xi = {v: i for i, v in enumerate(sp)}
    ax.plot(xs, wl, "o-", color="#1a5fa8", lw=2, zorder=3)
    ax2.plot(xs, gp, "s--", color="#c0651b", lw=1.4, zorder=2)
    ax.set_xticks(xs)
    ax.set_xticklabels([lbl(x) for x in sp], fontsize=8.5, rotation=45)
    ax.set_xlim(-0.4, len(sp) - 0.6)
    ax.set_xlabel("split"); ax.set_ylabel("wall (s)", color="#1a5fa8")
    ax2.set_ylabel("gpuTime (s)", color="#c0651b")
    ax.tick_params(axis="y", labelcolor="#1a5fa8"); ax2.tick_params(axis="y", labelcolor="#c0651b")
    # every overlay is HOLLOW and sized apart, so the blue data point underneath stays visible even
    # when the optimum, the own value and a source median all land on the same split
    o = opt_of(q, w); i = xi[o]
    ax.axvline(i, color="#1a5fa8", ls=":", lw=1.3)
    ax.plot(i, wl[i], "o", ms=13, mfc="none", mec="#1a5fa8", mew=1.8, zorder=5,
            label=f"optimum {lbl(o)} - {wl[i]:.1f}s")
    v = own.get(q, {}).get(w)
    if v and (q, w, v) in perf:
        j = xi[v]
        ax.axvline(j, color="#1baf7a", ls=":", lw=1.3)
        ax.plot(j, wl[j], "*", mfc="none", mec="#1baf7a", mew=1.6, ms=22, zorder=6,
                label=f"own value {lbl(v)} - {wl[j]:.1f}s")
    for src in (s_ for s_ in QUERIES if s_ != q and s_ in QMED):
        want = QMED[src]
        u = want if (q, w, want) in perf else min(sp, key=lambda x: abs(x - want))
        j = xi[u]
        mk, col, msz = SRC_MARK[src]
        ax.plot(j, wl[j], mk, ms=msz, mfc="none", mec=col, mew=1.4, zorder=6,
                label=f"from {src} median {lbl(want)}" + ("" if u == want else f" (~{lbl(u)})")
                      + f" - {wl[j]:.1f}s")
    ax.set_title(f"{q} @ {w}", fontsize=12)
    h, l = ax.get_legend_handles_labels()
    seen = set(); hl = [(a, b) for a, b in zip(h, l) if not (b in seen or seen.add(b))]
    if hl:
        ax.legend([a for a, _ in hl], [b for _, b in hl], fontsize=8, loc="upper center",
                  bbox_to_anchor=(0.5, -0.28), frameon=False, markerscale=0.55,
                  handletextpad=1.0, labelspacing=0.7)
    fig.tight_layout()
    buf = io.BytesIO(); fig.savefig(buf, format="png", dpi=130); plt.close(fig)
    return base64.b64encode(buf.getvalue()).decode()


def _dur(x):
    """Accumulable durations are serialised as 'HH:MM:SS.mmm' by NanoSecondAccumulator."""
    if isinstance(x, str) and ":" in x:
        h, m, s = x.split(":")
        return int(h) * 3600 + int(m) * 60 + float(s)
    try:
        return float(x)
    except (TypeError, ValueError):
        return 0.0


def scan_stage_from_eventlog(path):
    """Per warm SQL execution, the scan stage measured straight from the event log.

    The scan stage is the first stage of the execution's job. Concurrency of semaphore HOLDERS is
    gpuTime/span: gpuTime sums each task's release-minus-acquire interval (GpuSemaphore.scala:499),
    so it is a concurrency-weighted sum, not work. Waiting is excluded - it lands in
    gpuSemaphoreWait - which is why a split that serialises tasks shows LESS gpuTime for the
    same bytes.
    """
    import json
    stage_job, tasks = {}, []
    for line in open(path):
        try:
            e = json.loads(line)
        except ValueError:
            continue
        t = e.get("Event", "")
        if t == "SparkListenerJobStart":
            eid = (e.get("Properties") or {}).get("spark.sql.execution.id")
            for si in e["Stage Infos"]:
                stage_job[si["Stage ID"]] = eid
        elif t == "SparkListenerTaskEnd":
            ti, tm = e["Task Info"], (e.get("Task Metrics") or {})
            acc = {x["Name"]: x.get("Update", 0) for x in ti.get("Accumulables", []) if "Name" in x}
            tasks.append(dict(
                stage=e["Stage ID"], eid=stage_job.get(e["Stage ID"]),
                launch=ti["Launch Time"], finish=ti["Finish Time"],
                gpu=_dur(acc.get("gpuTime", 0)), wait=_dur(acc.get("gpuSemaphoreWait", 0)),
                dec=_dur(acc.get("GPU decode time", 0)) / 1e9,
                run=tm.get("Executor Run Time", 0) / 1000.0,
                cpu=tm.get("Executor CPU Time", 0) / 1e9,
                gc=tm.get("JVM GC Time", 0) / 1000.0,
                dur=(ti["Finish Time"] - ti["Launch Time"]) / 1000.0,
                inb=(tm.get("Input Metrics") or {}).get("Bytes Read", 0)))
    out = []
    for eid in {x["eid"] for x in tasks if x["eid"]}:
        if int(eid) < 2:                      # iteration 1 is cold; never mixed with warm
            continue
        stg = min({x["stage"] for x in tasks if x["eid"] == eid})
        g = [x for x in tasks if x["eid"] == eid and x["stage"] == stg]
        if len(g) < 5:
            continue
        span = (max(x["finish"] for x in g) - min(x["launch"] for x in g)) / 1000.0
        sg = sum(x["gpu"] for x in g)
        d = sorted(x["dur"] for x in g)
        out.append(dict(eid=int(eid), tasks=len(g), span=span, gpu=sg,
                        holders=sg / span if span else 0,
                        wait=sum(x["wait"] for x in g), run=sum(x["run"] for x in g),
                        cpu=sum(x["cpu"] for x in g), dec=sum(x["dec"] for x in g),
                        gc=sum(x["gc"] for x in g),
                        t50=d[len(d) // 2], t90=d[int(0.9 * (len(d) - 1))], tmax=d[-1],
                        gib=sum(x["inb"] for x in g) / 2 ** 30))
    return out


def window_data_stats(win):
    """Input-file and row-group statistics for one declared window, read from the parquet footers."""
    import glob, re
    import pyarrow.parquet as pq
    import wincfg
    w = wincfg.CFG["windows"][win]
    lo, hi = w.get("from"), w.get("to")
    files = []
    for f in glob.glob(os.path.join(wincfg.CFG["dataset"], "*", "*", "*.parquet")):
        m = re.search(r"/ym=([^/]+)/", f)
        if not m:
            continue
        ym = m.group(1)
        if (lo and ym < lo) or (hi and ym >= hi):
            continue
        files.append(f)
    sizes = [os.path.getsize(f) for f in files]
    rg, rows = [], 0
    for f in files:
        md = pq.ParquetFile(f).metadata
        rows += md.num_rows
        for i in range(md.num_row_groups):
            rg.append(md.row_group(i).total_byte_size)
    return dict(files=len(files), bytes=sum(sizes), sizes=sorted(sizes), rgs=sorted(rg), rows=rows)


if A.explain:
    import glob
    q, _, only_w = A.explain.partition("@")
    pct = lambda v, p: v[min(len(v) - 1, int(p / 100.0 * len(v)))] if v else 0
    MB = 2 ** 20
    print(f"\n### input data per window  (dataset partitioned by wiki, ym)")
    print(f"{'win':>4} {'files':>7} {'GiB':>8} {'rows':>14} {'file MB p50':>12} {'p90':>8} "
          f"{'max':>8} {'rowgrps':>8} {'rg MB p50':>10} {'p90':>7}")
    for w_ in WINDOWS:
        try:
            d = window_data_stats(w_)
        except Exception as ex:                       # noqa: BLE001 - report, do not mask
            print(f"{w_:>4}  data stats unavailable: {ex}")
            continue
        print(f"{w_:>4} {d['files']:>7} {d['bytes']/2**30:>8.2f} {d['rows']:>14,} "
              f"{pct(d['sizes'],50)/MB:>12.1f} {pct(d['sizes'],90)/MB:>8.1f} "
              f"{max(d['sizes'])/MB:>8.1f} {len(d['rgs']):>8} "
              f"{pct(d['rgs'],50)/MB:>10.1f} {pct(d['rgs'],90)/MB:>7.1f}")
    print(f"\n### is the swept optimum separable from noise?")
    print(f"{'job':>10} {'chosen':>8} {'wall':>7} {'noise sd':>9} {'splits within 1 sd of best'}")
    for qq in QUERIES:
        for w_ in WINDOWS:
            if not splits_for(qq, w_):
                continue
            o, sd, band = opt_of(qq, w_), noise_sd(qq, w_), opt_band(qq, w_)
            print(f"{qq+'@'+w_:>10} {str(o)+'m':>8} {perf[(qq,w_,o)]['wall_s']:>7.2f} {sd:>9.2f} "
                  + ("SEPARABLE" if len(band) == 1 else
                     " ".join(f"{x}m({perf[(qq,w_,x)]['wall_s']:.2f})" for x in band)))

    # Picking the argmin inside a tie band is a coin flip, and on cs04@W3 it lands on the tail.
    # Compare it with two deterministic rules over the same band: its MEDIAN split, and the member
    # with the lowest gpuTime.
    print(f"\n### three ways to choose inside the tie band")
    print(f"{'job':>10} | {'lowest':>7} {'wall':>6} {'gpu':>6} | {'band centre':>11} {'wall':>6} "
          f"{'gpu':>6} | {'best gpu in band':>16} {'wall':>6} {'gpu':>6}")
    for qq in QUERIES:
        for w_ in WINDOWS:
            band = opt_band(qq, w_)
            if not band:
                continue
            o = opt_of(qq, w_)
            import math
            gc_ = math.exp(st.mean([math.log(x) for x in band]))
            bm = min(band, key=lambda x: abs(math.log(x) - math.log(gc_)))
            bg = min(band, key=lambda x: perf[(qq, w_, x)]["occupancy_s"])
            f = lambda x: (f"{perf[(qq,w_,x)]['wall_s']:>6.2f} "
                           f"{perf[(qq,w_,x)]['occupancy_s']:>6.1f}")
            print(f"{qq+'@'+w_:>10} | {str(o)+'m':>7} {f(o)} | {str(bm)+'m':>11} {f(bm)} | "
                  f"{str(bg)+'m':>16} {f(bg)}")
    if only_w:
        # per-execution detail for one job: is the spread a few slow executions or uniform?
        for d in sorted(glob.glob(os.path.join(A.outroot, f"sweep-{q}@{only_w}-*"))):
            els = glob.glob(os.path.join(d, "el", "*"))
            if not els:
                continue
            v = sorted(scan_stage_from_eventlog(els[0]), key=lambda x: x["span"])
            if not v:
                continue
            arm = os.path.basename(d)
            wl = {(r["arm"], int(r["iteration"])): float(r["wall_s"]) for r in rows}
            print(f"\n### {arm}  per warm execution")
            print(f"{'exec':>5} {'wall':>6} {'span':>6} {'tsk s p50':>10} {'p90':>7} {'max':>7} "
                  f"{'max/p50':>8} {'gpuTime':>8} {'holders':>8} {'semwait':>8} {'gc':>7}")
            for x in v:
                print(f"{x['eid']:>5} {wl.get((arm, x['eid']), 0):>6.2f} {x['span']:>6.2f} "
                      f"{x['t50']:>10.2f} {x['t90']:>7.2f} "
                      f"{x['tmax']:>7.2f} {x['tmax']/x['t50']:>8.1f} {x['gpu']:>8.2f} "
                      f"{x['holders']:>8.2f} {x['wait']:>8.2f} {x['gc']:>7.2f}")
        raise SystemExit(0)
    for w_ in WINDOWS:
        print(f"\n### {q}@{w_}  scan stage, warm executions, from the raw event logs")
        print(f"{'split':>6} {'wall':>6} {'#tsk':>6} {'MB/tsk':>7} {'span':>6} {'gpuTime':>8} "
              f"{'holders':>8} {'semwait':>8} {'run':>7} {'cpu':>7} {'decode':>7} {'GiB':>6}")
        for d in sorted(glob.glob(os.path.join(A.outroot, f"sweep-{q}@{w_}-*"))):
            tag = os.path.basename(d).rsplit("-", 1)[1]
            if not tag or tag[0].isalpha():
                continue
            mb = int(tag[:-1]) * 1024 if tag.endswith("g") else int(tag[:-1])
            els = glob.glob(os.path.join(d, "el", "*"))
            if not els:
                continue
            v = scan_stage_from_eventlog(els[0])
            if not v:
                continue
            m = lambda k: st.median([x[k] for x in v])
            wl = perf.get((q, w_, mb), {}).get("wall_s", 0)
            print(f"{mb:>6} {wl:>6.2f} {m('tasks'):>6.0f} {m('gib')*1024/m('tasks'):>7.0f} "
                  f"{m('span'):>6.2f} {m('gpu'):>8.2f} {m('holders'):>8.2f} {m('wait'):>8.2f} "
                  f"{m('run'):>7.2f} {m('cpu'):>7.2f} {m('dec'):>7.2f} {m('gib'):>6.2f}")
    raise SystemExit(0)

if A.chart:
    import base64
    spec, path = A.chart.rsplit(":", 1)
    cq, cw = spec.split("@")
    b = sweep_chart(cq, cw)
    if not b:
        raise SystemExit(f"no chart for {spec} (fewer than 3 measured splits)")
    with open(path, "wb") as f:
        f.write(base64.b64decode(b))
    print(f"wrote {path}")
    raise SystemExit(0)

H, M = [], []
H.append("""<!doctype html><meta charset=utf-8><title>Split learning - answers</title><style>
body{font:14px/1.6 -apple-system,Segoe UI,Roboto,sans-serif;color:#1a1a1a;max-width:1120px;margin:2rem auto;padding:0 1rem}
h1{font-size:21px}h2{font-size:16px;margin-top:2rem;border-bottom:2px solid #ddd;padding-bottom:.3rem}
h3{font-size:13.5px;margin-top:1.3rem}
table{border-collapse:collapse;font-size:12px;width:100%;margin:.5rem 0}
th,td{border:1px solid #e3e3e3;padding:5px 8px;text-align:right}
th:first-child,td:first-child{text-align:left}thead{background:#f6f8fa}
.mut{color:#666;font-size:11.5px}.ok{background:#f2faf2}.bad{background:#fdf2f1}.ref{background:#eef4fb;font-weight:600}
code{background:#f4f4f4;padding:0 3px}
</style><h1>Split learning &mdash; answers</h1>""")
M.append("# Split learning — answers\n")

pre = ("Every cell is a warm median (iterations 2..N) at a <b>pinned split</b>, so all rows are "
       "directly comparable. Cost is measured against <b>that job's own optimum</b>, defined as "
       "the <b>centre of the splits its own data cannot separate</b>: every split whose median "
       "wall is within one pooled run-to-run stdev of the fastest. A plain argmin is not used "
       "because it names whichever tie member won a coin flip. "
       "A split never measured on a job is scored at the nearest measured split and marked "
       "<code>~</code> &mdash; those are estimates. PASS = within 9% on both wall and gpuTime.")
H.append(f"<p class=mut>{pre}</p>")
M.append("Every cell is a warm median (iters 2..N) at a pinned split. Cost is vs that job's own "
         "optimum = the centre of the splits its data cannot separate (within one pooled "
         "run-to-run stdev of the fastest median wall), not a plain argmin. "
         "`~` marks a split scored at the nearest measured value (an estimate). "
         "PASS = within 9% on both wall and gpuTime.\n")

def cell(sc):
    if not sc:
        return "-", "mut"
    used, dw, dg, exact = sc
    tag = "" if exact else f" ~{used}m"
    cls = "ok" if (dw <= 9 and dg <= 9) else "bad"
    return f"{dw:+.1f}%w {dg:+.1f}%g{tag}", cls

# ---------------------------------------------------------------- sweep curves
H.append("<h2>Sweep curves</h2><p class=mut>Every measured split for each job. Blue solid = wall "
         "(left axis), orange dashed = gpuTime (right axis). Ring = optimum (centre of the "
         "statistically tied splits), star = the value "
         "this job learns for itself; each source query&#39;s median has its own outlined shape "
         "(red diamond = csH3, purple triangle = cs04, teal pentagon = cs02). All marks are hollow so "
         "the data point underneath stays visible when several land on the same split.</p>")
M.append("\n## Sweep curves\n\n(charts in the HTML version)\n")
H.append("<div style='display:flex;flex-wrap:wrap;gap:12px'>")
for _q in QUERIES:
    for _w in WINDOWS:
        b64 = sweep_chart(_q, _w)
        if b64:
            H.append(f"<img src='data:image/png;base64,{b64}' style='width:510px'/>")
H.append("</div>")

# ---------------------------------------------------------------- Q1
H.append("<h2>Q1 &mdash; same query, different data window</h2>"
         "<p class=mut>Each query learns a split per window. Rows are the window being run; columns "
         "are which window's value is used. The diagonal is the query using its own value for that "
         "window; off-diagonal is a value carried over from another window.</p>")
M.append("\n## Q1 — same query, different data window\n")
M.append("Rows = window being run. Columns = which window's learnt value is used. Diagonal = own value.\n")
q1_res = []
for q in QUERIES:
    if q not in own:
        continue
    H.append(f"<h3>{q}</h3><table><thead><tr><th>running on</th><th>its optimum</th>"
             + "".join(f"<th>use {w}'s value ({own[q].get(w,'-')}m)</th>" for w in WINDOWS)
             + "</tr></thead><tbody>")
    M.append(f"\n### {q}\n")
    M.append("| running on | its optimum | " + " | ".join(
        f"use {w}'s value ({own[q].get(w,'-')}m)" for w in WINDOWS) + " |")
    M.append("|---|---|" + "---|" * len(WINDOWS))
    for w in WINDOWS:
        if not splits_for(q, w):
            continue
        o = opt_of(q, w)
        cells = [f"{q}@{w}", f"{o}m"]
        clss = ["", "ref"]
        for src in WINDOWS:
            v = own[q].get(src)
            if v is None:
                cells.append("-"); clss.append("mut"); continue
            sc = score(q, w, v)
            t, c = cell(sc)
            if src == w:
                c = "ref"
            else:
                q1_res.append((q, src, w, sc))
            cells.append(t); clss.append(c)
        H.append("<tr>" + "".join(f"<td class={clss[i]}>{c}</td>" for i, c in enumerate(cells)) + "</tr>")
        M.append("| " + " | ".join(cells) + " |")
    H.append("</tbody></table>")

ok1 = [r for r in q1_res if r[3] and r[3][1] <= 9 and r[3][2] <= 9]
s1 = (f"<b>Q1 result:</b> {len(ok1)} of {len(q1_res)} cross-window transfers stay within 9% on both "
      f"metrics.")
if q1_res:
    ws = [r[3][1] for r in q1_res if r[3]]
    s1 += f" Wall cost ranges {min(ws):+.1f}% to {max(ws):+.1f}%, median {st.median(ws):+.1f}%."
H.append(f"<p>{s1}</p>"); M.append("\n" + s1.replace("<b>", "**").replace("</b>", "**") + "\n")

# ---------------------------------------------------------------- Q2
# the whole-table Q2 is spliced in here: it answers the same question with the window held
# constant, so it belongs before the window-scoped version rather than after it
H.append("__WHOLE_Q2_HTML__"); M.append("__WHOLE_Q2_MD__")
H.append("<h2>Q2 &mdash; different query, using the source query's median</h2>"
         "<p class=mut>The value a query hands over is its <b>median across its own windows</b>, so it "
         "is a property of that query rather than of run order. Rows are the job being run; columns "
         "are which query the value came from.</p>")
M.append("\n## Q2 — different query, using the source query's median\n")
M.append("The handed-over value is the source query's median across its windows. "
         "Rows = job being run. Columns = source query.\n")
H.append("<table><thead><tr><th>running</th><th>its optimum</th><th>its own value</th>"
         + "".join(f"<th>from {q} (median {QMED.get(q,'-')}m)</th>" for q in QUERIES)
         + "</tr></thead><tbody>")
M.append("| running | its optimum | its own value | " + " | ".join(
    f"from {q} (median {QMED.get(q,'-')}m)" for q in QUERIES) + " |")
M.append("|---|---|---|" + "---|" * len(QUERIES))
q2_res = []
for q in QUERIES:
    for w in WINDOWS:
        if not splits_for(q, w):
            continue
        o = opt_of(q, w)
        selfv = own[q].get(w)
        sc_self = score(q, w, selfv) if selfv else None
        t_self, c_self = cell(sc_self)
        cells = [f"{q}@{w}", f"{o}m", f"{selfv}m: {t_self}" if selfv else "-"]
        clss = ["", "ref", c_self]
        for src in QUERIES:
            if src == q or src not in QMED:
                cells.append("(self)" if src == q else "-"); clss.append("mut"); continue
            sc = score(q, w, QMED[src])
            t, c = cell(sc)
            q2_res.append((src, q, w, sc))
            cells.append(t); clss.append(c)
        H.append("<tr>" + "".join(f"<td class={clss[i]}>{c}</td>" for i, c in enumerate(cells)) + "</tr>")
        M.append("| " + " | ".join(cells) + " |")
H.append("</tbody></table>")

ok2 = [r for r in q2_res if r[3] and r[3][1] <= 9 and r[3][2] <= 9]
s2 = f"<b>Q2 result:</b> {len(ok2)} of {len(q2_res)} cross-query transfers stay within 9% on both metrics."
if q2_res:
    ws = [r[3][1] for r in q2_res if r[3]]
    s2 += f" Wall cost ranges {min(ws):+.1f}% to {max(ws):+.1f}%, median {st.median(ws):+.1f}%."
H.append(f"<p>{s2}</p>"); M.append("\n" + s2.replace("<b>", "**").replace("</b>", "**") + "\n")

# ---------------------------------------------------------------- correlation
H.append("<h2>Correlation &mdash; what predicts the cost</h2>"
         "<p class=mut>Split gap = how far the handed-over value is from the split that job learns "
         "for itself (1.00x = identical). Split by whether the value came from the SAME query on "
         "another window (Q1) or a DIFFERENT query (Q2).</p>")
M.append("\n## Correlation — what predicts the cost\n")
M.append("Split gap = how far the handed-over value is from what that job learns for itself "
         "(1.00x = identical). Split by Q1 (same query) vs Q2 (different query).\n")

q1_pairs, q2_pairs = [], []
for src, tgt, w, sc in q2_res:
    if not sc:
        continue
    tv = own[tgt].get(w)
    if not tv:
        continue
    q2_pairs.append((max(QMED[src], tv) / min(QMED[src], tv), sc[1], sc[2], f"{src}&rarr;{tgt}@{w}"))
for q, src, w, sc in q1_res:
    if not sc:
        continue
    tv = own[q].get(w); sv = own[q].get(src)
    if not tv or not sv:
        continue
    q1_pairs.append((max(sv, tv) / min(sv, tv), sc[1], sc[2], f"{q}@{src}&rarr;{w}"))
q1_pairs.sort(); q2_pairs.sort()

def _corr(pairs, title, note, HH, MM):
    if not pairs:
        return
    HH.append(f"<h3>{title}</h3><p class=mut>{note}</p><table><thead><tr><th>transfer</th>"
             "<th>split gap</th><th>wall cost</th><th>gpuTime cost</th></tr></thead><tbody>")
    MM.append(f"\n### {title}\n\n{note}\n")
    MM.append("| transfer | split gap | wall cost | gpuTime cost |")
    MM.append("|---|---|---|---|")
    for gap, dw, dg, lab in pairs:
        cls = "ok" if (dw <= 9 and dg <= 9) else "bad"
        HH.append(f"<tr class={cls}><td>{lab}</td><td>{gap:.2f}x</td><td>{dw:+.1f}%</td>"
                  f"<td>{dg:+.1f}%</td></tr>")
        MM.append(f"| {lab.replace('&rarr;','->')} | {gap:.2f}x | {dw:+.1f}% | {dg:+.1f}% |")
    HH.append("</tbody></table>")
    if len(pairs) > 2:
        gs = [p[0] for p in pairs]; ws = [p[1] for p in pairs]
        mg, mw = st.mean(gs), st.mean(ws)
        cov = sum((g - mg) * (w_ - mw) for g, w_, _, _ in pairs)
        den = (sum((g - mg) ** 2 for g in gs) * sum((w_ - mw) ** 2 for w_ in ws)) ** 0.5
        rho = cov / den if den else 0
        t = (f"<b>r = {rho:.2f}</b> between split gap and wall cost, n={len(pairs)}. "
             f"Median wall cost <b>{st.median(ws):+.1f}%</b>, worst <b>{max(ws):+.1f}%</b>. "
             f"{sum(1 for p in pairs if p[1] <= 9 and p[2] <= 9)} of {len(pairs)} within 9% on both.")
        HH.append(f"<p>{t}</p>"); MM.append("\n" + t.replace("<b>", "**").replace("</b>", "**") + "\n")


def corr_table(p_, t_, n_):
    _corr(p_, t_, n_, H, M)


def corr_table_w(p_, t_, n_):
    _corr(p_, t_, n_, WH, WM)


corr_table(q1_pairs, "Q1 &mdash; same query, value from another window",
           "The split came from the same query running on a different data window.")
corr_table(q2_pairs, "Q2 &mdash; different query",
           "The split came from a different query (its median across windows).")

WH, WM = [], []
# ---------------------------------------------------------------- Q2 on the whole table
if A.q2_ledger and os.path.exists(A.q2_ledger):
    qrows = [r for r in csv.DictReader(open(A.q2_ledger), delimiter="\t")
             if r.get("run_ok", "ok") == "ok"]
    # One arm per split, not pooled. Repeat arms at the same split can disagree far beyond the noise
    # floor (cs04@328m: 36.42s vs 53.22s, the later arm degrading within itself). Pooling moved
    # cs04's optimum from 328m to 529m. Use the earliest arm and surface the disagreement.
    qba = collections.defaultdict(lambda: collections.defaultdict(list))
    for r in qrows:
        if r["arm"].startswith("sweep-") and int(r["iteration"]) > 1 and r["split_mb"] not in ("-", ""):
            qba[(r["query"], int(r["split_mb"]))][r["arm"]].append(r)
    qperf = {}
    QDIS = []
    for k, arms in qba.items():
        meds = {a: st.median([float(x["wall_s"]) for x in v]) for a, v in arms.items()}
        lo, hi = min(meds.values()), max(meds.values())
        if len(meds) > 1 and (hi - lo) / lo > 0.10:
            QDIS.append((k, dict(sorted(meds.items()))))
        recs = arms[sorted(arms)[0]]
        qperf[k] = {m: st.median([float(x[m]) for x in recs])
                    for m in ("wall_s", "occupancy_s", "decode_s")}
        # every arm's wall median, so a cell can show the range when repeats disagree rather than
        # hiding it in a separate panel
        qperf[k]["wall_all"] = sorted(meds.values())
        qperf[k]["gpu_all"] = sorted(st.median([float(x["occupancy_s"]) for x in v])
                                     for v in arms.values())
    def qopt(q):
        """Same rule as opt_of: the CENTRE of the splits this query's data cannot separate.

        Noise is the pooled within-split stdev of the scoring arm's warm walls - the same arm the
        median comes from, so the floor and the value are measured on identical runs.
        """
        sp = sorted(x for (qq, x) in qperf if qq == q)
        if not sp:
            return None
        sds = [st.stdev([float(x["wall_s"]) for x in qba[(q, s)][sorted(qba[(q, s)])[0]]])
               for s in sp if len(qba[(q, s)][sorted(qba[(q, s)])[0]]) > 1]
        sd = st.mean(sds) if sds else 0.0
        best = min(qperf[(q, x)]["wall_s"] for x in sp)
        return band_centre([x for x in sp if qperf[(q, x)]["wall_s"] - best <= sd])

    QQ = sorted({qq for (qq, _) in qperf})
    qown = {}
    for r in qrows:
        if r["learnt_from"] == "own" and r["split_mb"] not in ("-", ""):
            qown[r["query"]] = int(r["split_mb"])
    # Bytes each query actually reads per execution. The autotuner never sees this - it records
    # per SCAN NODE, so csH3's three UNION ALL branches look like one table read to it.
    qscan = {}
    for r in qrows:
        try:
            g = float(r["input_gib"])
        except (KeyError, ValueError):
            continue
        if g > qscan.get(r["query"], 0):
            qscan[r["query"]] = g

    import glob as _glob, wincfg as _wc
    tbl_gib = sum(os.path.getsize(f) for f in
                  _glob.glob(os.path.join(_wc.CFG["dataset"], "*", "*", "*.parquet"))) / 2 ** 30
    ntx = len({r["arm"].split("-shared-")[0] for r in qrows if "-shared-" in r["arm"]})
    WH.append("<h2>Q2 (whole table) &mdash; different query, same data</h2><p class=mut>"
             f"Every query runs on the full {tbl_gib:.1f} GiB, so the QUERY is the only variable. "
             f"All {ntx} transfers were <b>executed</b>; the inherited split is what the target "
             "actually received and its cost comes from a pinned sweep arm at that split. "
             "No proxies.</p>")
    WM.append("\n## Q2 (whole table) — different query, same data\n")
    WM.append(f"Every query runs on the full {tbl_gib:.1f} GiB, so the query is the only variable. "
              f"All {ntx} transfers were executed; no proxies.\n")

    WH.append("<h3>Each query on the full table</h3><table><thead><tr><th>query</th>"
             "<th>its optimum</th><th>wall s</th><th>what it learns</th><th>wall s</th>"
             "<th>vs opt</th></tr></thead><tbody>")
    WM.append("\n### Each query on the full table\n")
    WM.append("| query | its optimum | wall s | what it learns | wall s | vs opt |")
    WM.append("|---|---|---|---|---|---|")
    for q in QQ:
        sp = sorted(x for (qq, x) in qperf if qq == q)
        if not sp:
            continue
        o = qopt(q)
        ov = qown.get(q)
        if ov and (q, ov) in qperf:
            d = (qperf[(q, ov)]["wall_s"] - qperf[(q, o)]["wall_s"]) / qperf[(q, o)]["wall_s"] * 100
            ow, dt = f"{qperf[(q, ov)]['wall_s']:.2f}", f"{d:+.1f}%"
        else:
            ow, dt = "-", "-"
        cells = [q, f"{o}m", f"{qperf[(q,o)]['wall_s']:.2f}", f"{ov}m" if ov else "-", ow, dt]
        WH.append("<tr>" + "".join(f"<td>{c}</td>" for c in cells) + "</tr>")
        WM.append("| " + " | ".join(cells) + " |")
    WH.append("</tbody></table>")

    WH.append("<h3>The executed transfers</h3><table><thead><tr><th>transfer</th>"
             "<th>inherited split</th><th>target's optimum</th><th>split change</th>"
             "<th>scan GiB<br><span class=mut>source &rarr; target</span></th>"
             "<th>wall s<br><span class=mut>inherited &rarr; optimum</span></th>"
             "<th>gpuTime s<br><span class=mut>inherited &rarr; optimum</span></th>"
             "<th>verdict</th></tr></thead><tbody>")
    WM.append("\n### The executed transfers\n")
    WM.append("| transfer | inherited split | target's optimum | split change | scan GiB (src -> tgt) | "
             "wall s (inh -> opt) | gpuTime s (inh -> opt) | verdict |")
    WM.append("|---|---|---|---|---|---|---|---|")
    q2all, seen_t, qrows_t = [], set(), []
    for r in qrows:
        a_ = r["arm"]
        if a_.endswith("-shared-2") and int(r["iteration"]) == 1 \
                and not r["learnt_from"].startswith("spark-"):
            qrows_t.append(r)
    # source then target, so the table reads as a matrix rather than run order
    qrows_t.sort(key=lambda r: (r["arm"].split("_to_")[0].split("@")[0],
                                r["arm"].split("_to_")[1].split("@")[0]))
    for r in qrows_t:
        a_ = r["arm"]
        if not (a_.endswith("-shared-2") and int(r["iteration"]) == 1):
            continue
        if r["learnt_from"].startswith("spark-"):
            continue
        src, tgt = a_[:-len("-shared-2")].split("_to_")
        if (src, tgt) in seen_t:
            continue
        seen_t.add((src, tgt))
        q = tgt.split("@")[0]; v = int(r["split_mb"])
        sp = sorted(x for (qq, x) in qperf if qq == q)
        if not sp or (q, v) not in qperf:
            continue
        o = qopt(q)
        dw = (qperf[(q, v)]["wall_s"] - qperf[(q, o)]["wall_s"]) / qperf[(q, o)]["wall_s"] * 100
        dg = (qperf[(q, v)]["occupancy_s"] - qperf[(q, o)]["occupancy_s"]) / qperf[(q, o)]["occupancy_s"] * 100
        ok = dw <= 9 and dg <= 9
        ds = (v - o) / o * 100
        sq = src.split("@")[0]
        sb, tb = qscan.get(sq), qscan.get(q)
        scan = (f"{sb:.0f} &rarr; {tb:.0f} ({(tb-sb)/sb*100:+.0f}%)" if sb and tb else "-")
        def rng(key, allkey, fmt, iv):
            """inherited -> optimum. When the optimum split has repeat arms that disagree, show the
            full range and the drift range it implies, so the uncertainty sits in the cell."""
            vals = qperf[(q, o)].get(allkey) or [qperf[(q, o)][key]]
            lo, hi = vals[0], vals[-1]
            if (hi - lo) / lo <= 0.10:
                d = (iv - qperf[(q, o)][key]) / qperf[(q, o)][key] * 100
                return f"{fmt.format(iv)} &rarr; {fmt.format(qperf[(q,o)][key])} ({d:+.1f}%)"
            dlo, dhi = (iv - hi) / hi * 100, (iv - lo) / lo * 100
            return (f"{fmt.format(iv)} &rarr; {fmt.format(lo)}-{fmt.format(hi)} "
                    f"({dlo:+.1f}% to {dhi:+.1f}%)")
        wcell = rng("wall_s", "wall_all", "{:.2f}", qperf[(q, v)]["wall_s"])
        gcell = rng("occupancy_s", "gpu_all", "{:.1f}", qperf[(q, v)]["occupancy_s"])
        # When the target's optimum has repeat arms that disagree, the verdict depends on which one
        # is used as the reference. Mark those INDETERMINATE rather than resolving it in favour of
        # whichever bound happens to pass.
        wall_arms = qperf[(q, o)].get("wall_all") or [qperf[(q, o)]["wall_s"]]
        spread = len(wall_arms) > 1 and (wall_arms[-1] - wall_arms[0]) / wall_arms[0] > 0.10
        if spread:
            dl = (qperf[(q, v)]["wall_s"] - wall_arms[-1]) / wall_arms[-1] * 100
            dh = (qperf[(q, v)]["wall_s"] - wall_arms[0]) / wall_arms[0] * 100
            passes_all = dh <= 9 and dg <= 9
            fails_all = dl > 9
            verdict = "PASS" if passes_all else ("FAIL" if fails_all else "indeterminate")
        else:
            verdict = "PASS" if ok else "FAIL"
        cells = [f"{sq} &rarr; {q}", f"{v}m", f"{o}m", f"{ds:+.0f}%", scan, wcell, gcell, verdict]
        cls = "ok" if verdict == "PASS" else ("warn" if verdict == "indeterminate" else "bad")
        WH.append(f"<tr class={cls}>" + "".join(f"<td>{c}</td>" for c in cells) + "</tr>")
        WM.append("| " + " | ".join(c.replace("&rarr;", "->") for c in cells) + " |")
        tv = qown.get(q)
        if tv:
            q2all.append((max(v, tv) / min(v, tv), dw, dg, f"{src.split('@')[0]}&rarr;{q}"))
    WH.append("</tbody></table>")
    if q2all:
        n_ok = sum(1 for p in q2all if p[1] <= 9 and p[2] <= 9)
        ws = [p[1] for p in q2all]
        # Every number in this paragraph is computed from the same qperf/qopt the table above uses.
        # It previously carried hardcoded percentages that silently went stale when the optimum
        # rule changed.
        def _band_of(q):
            sp = sorted(x for (qq, x) in qperf if qq == q)
            sds = [st.stdev([float(x["wall_s"]) for x in qba[(q, s)][sorted(qba[(q, s)])[0]]])
                   for s in sp if len(qba[(q, s)][sorted(qba[(q, s)])[0]]) > 1]
            sd = st.mean(sds) if sds else 0.0
            best = min(qperf[(q, x)]["wall_s"] for x in sp)
            return sp, [x for x in sp if qperf[(q, x)]["wall_s"] - best <= sd]

        flat, outside = [], []
        for q in QQ:
            sp, bd = _band_of(q)
            o = qopt(q)
            flat.append(f"{q} {min(bd)}m" if min(bd) == max(bd)
                        else f"{q} {min(bd)}-{max(bd)}m")
            for x in sp:
                if x not in bd:
                    outside.append((qperf[(q, x)]["wall_s"] - qperf[(q, o)]["wall_s"])
                                   / qperf[(q, o)]["wall_s"] * 100)
        vac = [p for p in q2all if p[0] == 1.0]      # split gap of exactly 1x
        inform = [p for p in q2all if p[0] != 1.0]
        t = (f"<b>{n_ok} of {len(q2all)} pass.</b> Wall cost {min(ws):+.1f}% to {max(ws):+.1f}%, "
             f"median {st.median(ws):+.1f}%."
             "<br><br><b>Read these rows with two caveats &mdash; they are weaker evidence "
             "than the pass count suggests.</b><br>"
             "<b>1. The cost curve is flat where every candidate lives.</b> Each query's optimum "
             "here is the centre of the splits its own data cannot separate at the measured noise "
             f"floor: {'; '.join(flat)}. Any split inside those ranges would have passed. Outside "
             f"them the penalty is real ({min(outside):+.1f}% to {max(outside):+.1f}% wall). The "
             "transfers were not tested against a sensitive region.<br>"
             + (f"<b>2. {len(vac)} of the transfers are vacuous.</b> "
                + ", ".join(sorted(p[3] for p in vac))
                + " hand over a split the target's own autotuner already computed "
                  "(gap exactly 1.00x), so they test nothing.<br><br>" if vac else "")
             + "The informative transfers are: "
             + ", ".join(f"{p[3]} ({p[1]:+.1f}% wall)" for p in sorted(inform, key=lambda x: x[3]))
             + ".")
        WH.append(f"<p>{t}</p>"); WM.append("\n" + t.replace("<b>", "**").replace("</b>", "**") + "\n")
        corr_table_w(sorted(q2all), "Q2 (whole table) &mdash; correlation",
                   "Same columns as the correlation tables above, for the executed whole-table transfers.")

H.append("</body>")
H = [x for e in H for x in (WH if e == "__WHOLE_Q2_HTML__" else [e])]
M = [x for e in M for x in (WM if e == "__WHOLE_Q2_MD__" else [e])]
for ext, body in ((".html", "\n".join(H)), (".md", "\n".join(M))):
    tmp = BASE + ext + ".tmp"
    with open(tmp, "w") as f:
        f.write(body + "\n")
    os.replace(tmp, BASE + ext)
print(f"wrote {os.path.normpath(BASE)}.{{html,md}}")
