#!/usr/bin/env python3
"""Q2 on the full table: the six cross-query transfers, all executed.

Each row is a transfer that ran. The inherited split is what the target actually received, and its
cost comes from a pinned sweep arm measured at that split - no proxies, no offline scoring.
"""
import csv, sys, collections, statistics as st

led = sys.argv[1] if len(sys.argv) > 1 else "../results/ledger-q2-alldata-20260825.tsv"
rows = [r for r in csv.DictReader(open(led), delimiter="\t") if r.get("run_ok", "ok") == "ok"]

# Per ARM first. Two arms at the same split can disagree far beyond the noise floor - cs04@328m
# measured 36.42s and 53.22s (46% apart), the second degrading within itself (40.6 -> 60.3 -> 54.6).
# Pooling them silently moved cs04's optimum from 328m to 529m. So: keep arms separate, use the
# EARLIEST (measured during the clean sweep phase), and flag any split whose arms disagree.
by_arm = collections.defaultdict(lambda: collections.defaultdict(list))
for r in rows:
    if r["arm"].startswith("sweep-") and int(r["iteration"]) > 1 and r["split_mb"] not in ("-", ""):
        by_arm[(r["query"], int(r["split_mb"]))][r["arm"]].append(r)

DISAGREE = []
perf = {}
for k, arms in by_arm.items():
    meds = {a: st.median([float(x["wall_s"]) for x in v]) for a, v in arms.items()}
    lo, hi = min(meds.values()), max(meds.values())
    if len(meds) > 1 and (hi - lo) / lo > 0.10:
        DISAGREE.append((k, dict(sorted(meds.items()))))
    base = sorted(arms)[0]                      # earliest arm name == clean sweep phase
    recs = arms[base]
    perf[k] = {m: st.median([float(x[m]) for x in recs]) for m in ("wall_s", "occupancy_s")}
own = {}
for r in rows:
    if r["learnt_from"] == "own" and r["split_mb"] not in ("-", ""):
        own[r["query"]] = int(r["split_mb"])

if DISAGREE:
    print("!! splits whose repeat arms disagree by >10% - scored from the FIRST arm only:")
    for (q, sp), meds in sorted(DISAGREE):
        det = "  ".join(f"{a.split('-')[-1]}={v:.2f}" for a, v in meds.items())
        print(f"   {q}@{sp}m  {det}")
    print()
print("== Q2: cross-query transfers, EXECUTED on the full 135.7 GiB table")
print(f"{'transfer':24s} {'inherited':>10s} {'tgt opt':>8s} {'wall vs opt':>12s} {'gpu vs opt':>11s}  verdict")
seen = set()
for r in rows:
    a = r["arm"]
    if not (a.endswith("-shared-2") and int(r["iteration"]) == 1):
        continue
    if r["learnt_from"].startswith("spark-"):
        continue
    src, tgt = a[:-len("-shared-2")].split("_to_")
    if (src, tgt) in seen:
        continue
    seen.add((src, tgt))
    q = tgt.split("@")[0]
    v = int(r["split_mb"])
    sp = sorted(x for (qq, x) in perf if qq == q)
    if not sp:
        continue
    o = min(sp, key=lambda x: perf[(q, x)]["wall_s"])
    lab = f"{src.split('@')[0]} -> {q}"
    if (q, v) not in perf:
        print(f"{lab:24s} {str(v)+'m':>10s}  NOT MEASURED on target")
        continue
    dw = (perf[(q, v)]["wall_s"] - perf[(q, o)]["wall_s"]) / perf[(q, o)]["wall_s"] * 100
    dg = (perf[(q, v)]["occupancy_s"] - perf[(q, o)]["occupancy_s"]) / perf[(q, o)]["occupancy_s"] * 100
    ok = dw <= 9 and dg <= 9
    print(f"{lab:24s} {str(v)+'m':>10s} {str(o)+'m':>8s} {dw:+11.1f}% {dg:+10.1f}%  "
          f"{'PASS' if ok else 'FAIL'}")
