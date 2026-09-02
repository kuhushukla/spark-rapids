#!/usr/bin/env python3
"""Per-arm breakdown behind each (query, split) median.

show_q2.py pools every sweep arm at the same split. If a split was measured twice (grid + refine, or
a re-run), pooling mixes them and can move which split looks optimal. This prints the arms so that
can be checked rather than assumed.
"""
import csv, sys, collections, statistics as st

led = sys.argv[1]
rows = [r for r in csv.DictReader(open(led), delimiter="\t") if r.get("run_ok", "ok") == "ok"]
g = collections.defaultdict(lambda: collections.defaultdict(list))
for r in rows:
    if r["arm"].startswith("sweep-") and int(r["iteration"]) > 1 and r["split_mb"] not in ("-", ""):
        g[(r["query"], int(r["split_mb"]))][r["arm"]].append(float(r["wall_s"]))

for q in sorted({qq for (qq, _) in g}):
    print(f"== {q}")
    for sp in sorted(x for (qq, x) in g if qq == q):
        arms = g[(q, sp)]
        allv = [v for a in arms for v in arms[a]]
        det = "  ".join(f"{a}:{st.median(v):.2f}(n={len(v)})" for a, v in sorted(arms.items()))
        flag = "  <-- MULTIPLE ARMS" if len(arms) > 1 else ""
        print(f"   {sp:>5}m pooled={st.median(allv):7.2f} n={len(allv)}{flag}")
        if len(arms) > 1:
            print(f"         {det}")
