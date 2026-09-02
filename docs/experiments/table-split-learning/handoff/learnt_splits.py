#!/usr/bin/env python3
"""Print the split values the learning arms actually used, so they can be swept as real points.

The inherited split otherwise exists only in a single cold iteration, which is not comparable to a
warm median. Emits: "<query> <window> <split_mb> <role>" per line.
"""
import csv, sys, os

led = sys.argv[1]
if not os.path.exists(led):
    sys.exit(0)
rows = list(csv.DictReader(open(led), delimiter="\t"))
out = {}
for r in rows:
    if r["history_mode"] not in ("shared", "isolated"):
        continue                                  # sweep/off arms have nothing to learn from
    if r["split_mb"] in ("-", ""):
        continue
    sp = int(r["split_mb"])
    key = (r["query"], r["window"], sp)
    lf = r["learnt_from"]
    if lf.startswith("spark-maxSplitBytes"):
        role = "spark-fallback"
    elif lf == "own":
        role = "own-learnt"
    else:
        role = f"inherited:{lf}"
    # a split can appear as both; prefer the informative label
    if key not in out or out[key] == "spark-fallback":
        out[key] = role
# Also emit each job's CURRENT optimum, so refine re-measures it in the same batch as the
# candidates. Otherwise the optimum was measured hours earlier and machine drift between the two
# batches lands in the comparison (measured: same 2048m split differed 8.7% across 40 minutes).
import statistics as st, collections
warm = collections.defaultdict(list)
for r in rows:
    if r["arm"].startswith("sweep-") and int(r["iteration"]) > 1 and r["split_mb"] not in ("-", ""):
        warm[(r["query"], r["window"], int(r["split_mb"]))].append(float(r["wall_s"]))
jobs = {(q, w) for (q, w, _) in out}
for (q, w) in sorted(jobs):
    sp = [s_ for (qq, ww, s_) in warm if (qq, ww) == (q, w)]
    if sp:
        opt = min(sp, key=lambda x: st.median(warm[(q, w, x)]))
        out[(q, w, opt)] = "optimum-remeasure"

for (q, w, sp), role in sorted(out.items()):
    print(f"{q} {w} {sp} {role}")
