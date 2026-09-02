#!/usr/bin/env python3
"""Offline evaluation of split-inheritance POLICIES against already-measured sweep data.

No new runs. For each job (query, window) we know the wall/gpuTime at many pinned splits, so any
policy that outputs a split can be scored by looking up what that split measured.

Policies compared:
  own-window     the split this query learnt on THIS window (what the autotuner does today)
  own-median     per-query median across that query's windows      <- keyed (table, query)
  table-median   median across every record on the table           <- keyed (table) only
  cross-median   another query's own-median, applied to this job   <- the Q2 case, made deterministic

Splits not measured exactly are scored at the nearest measured split and flagged, since a policy is
only as good as the evidence behind it.
"""
import csv, sys, collections, statistics as st

LEDGER = sys.argv[1] if len(sys.argv) > 1 else "../results/ledger-window-learning-20260824.tsv"
rows = list(csv.DictReader(open(LEDGER), delimiter="\t"))

sweep = collections.defaultdict(list)
for r in rows:
    if r["arm"].startswith("sweep-") and int(r["iteration"]) > 1 and r["split_mb"] not in ("-", ""):
        sweep[(r["query"], r["window"], int(r["split_mb"]))].append(r)
perf = {k: {m: st.median([float(x[m]) for x in v]) for m in ("wall_s", "occupancy_s")}
        for k, v in sweep.items()}

own = collections.defaultdict(dict)
for r in rows:
    if r["learnt_from"] == "own" and r["split_mb"] not in ("-", ""):
        own[r["query"]][r["window"]] = int(r["split_mb"])

qmed = {q: int(st.median(sorted(v.values()))) for q, v in own.items()}
tmed = int(st.median(sorted(s for v in own.values() for s in v.values())))

print("records per query (split learnt per window):")
for q, v in sorted(own.items()):
    print(f"   {q:6s} {sorted(v.values())}  -> own-median {qmed[q]}m")
print(f"   table-median across all records: {tmed}m\n")

def score(q, w, want):
    sp = sorted(x for (qq, ww, x) in perf if (qq, ww) == (q, w))
    o = min(sp, key=lambda x: perf[(q, w, x)]["wall_s"])
    b = perf[(q, w, o)]
    use = want if (q, w, want) in perf else min(sp, key=lambda x: abs(x - want))
    p = perf[(q, w, use)]
    return (o, use, (p["wall_s"] - b["wall_s"]) / b["wall_s"] * 100,
            (p["occupancy_s"] - b["occupancy_s"]) / b["occupancy_s"] * 100)

hdr = f"{'job':10s} {'opt':>6s} | {'own-window':>22s} | {'own-median':>22s} | {'table-median':>22s}"
print(hdr); print("-" * len(hdr))
agg = collections.defaultdict(list)
for q in ("csH3", "cs04", "cs02"):
    for w in ("W1", "W2", "W3"):
        if not any((qq, ww) == (q, w) for (qq, ww, _) in perf):
            continue
        cells = []
        for name, want in (("own-window", own[q].get(w)), ("own-median", qmed[q]),
                           ("table-median", tmed)):
            if want is None:
                cells.append(f"{'-':>22s}"); continue
            o, use, dw, dg = score(q, w, want)
            agg[name].append((dw, dg))
            tag = f"{want}m" if use == want else f"{want}m~{use}m"
            cells.append(f"{tag:>9s} {dw:+6.1f}%w {dg:+6.1f}%g")
        o = min((x for (qq, ww, x) in perf if (qq, ww) == (q, w)),
                key=lambda x: perf[(q, w, x)]["wall_s"])
        print(f"{q+'@'+w:10s} {str(o)+'m':>6s} | " + " | ".join(cells))

print()
print(f"{'policy':14s} {'median wall':>12s} {'worst wall':>11s} {'median gpu':>11s} {'worst gpu':>10s}")
for k, v in agg.items():
    w_ = [x[0] for x in v]; g_ = [x[1] for x in v]
    print(f"{k:14s} {st.median(w_):+11.1f}% {max(w_):+10.1f}% {st.median(g_):+10.1f}% {max(g_):+9.1f}%")

print("\ncross-query (each query's own-median applied to another query's job):")
print(f"{'target job':10s} {'opt':>6s} " + " ".join(f"{'<-'+q:>20s}" for q in sorted(qmed)))
for q in ("csH3", "cs04", "cs02"):
    for w in ("W1", "W2", "W3"):
        if not any((qq, ww) == (q, w) for (qq, ww, _) in perf):
            continue
        out = []
        for src in sorted(qmed):
            if src == q:
                out.append(f"{'(self)':>20s}"); continue
            o, use, dw, dg = score(q, w, qmed[src])
            tag = f"{qmed[src]}m" if use == qmed[src] else f"{qmed[src]}~{use}"
            out.append(f"{tag:>7s} {dw:+5.1f}%w {dg:+5.1f}%g")
        o = min((x for (qq, ww, x) in perf if (qq, ww) == (q, w)),
                key=lambda x: perf[(q, w, x)]["wall_s"])
        print(f"{q+'@'+w:10s} {str(o)+'m':>6s} " + " ".join(out))
print("\n'~' marks a split that was not measured exactly; scored at the nearest measured split.")
