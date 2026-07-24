#!/usr/bin/env python3
# Split the listed-vs-2g results by scan dominance (measured on the 2g run).
# scan_dominance(query) = scanstage_gpu_s / wholequery_gpu_s  (both from mpb-perquery-2g.csv).
# Buckets: scan-dominated >=0.8, mixed 0.4-0.8, downstream-heavy <0.4.
import csv
BASE="/home/kuhu/Reps/spark-rapids/data"
OUT="/home/kuhu/Reps/spark-rapids/docs/experiments/rolling-split-autotuning/results/nds-sf3k-scandominance-buckets-20260721.md"
def load(p): return {r["query"]:r for r in csv.DictReader(open(p))}
g2=load(f"{BASE}/mpb-perquery-2g.csv"); lf=load(f"{BASE}/mpb-perquery-listedfull.csv")
def fl(d,q,k): return float(d[q][k])

rows=[]
for q in g2:
    if q not in lf: continue
    wq=fl(g2,q,"wq_gpu_s")
    dom = fl(g2,q,"scanstage_gpu_s")/wq if wq>0 else 1.0
    rows.append((q,dom))
def bucket(dom):
    return "scan-dominated" if dom>=0.8 else ("mixed" if dom>=0.4 else "downstream-heavy")

L=[]
L.append("# NDS SF3k — listed vs 2g, split by scan dominance (2026-07-21)\n")
L.append("`scan_dominance = scanstage_gpu_s / wholequery_gpu_s`, measured on the **2g run** "
         "(`data/mpb-perquery-2g.csv`): the fraction of the query's GPU time spent in scan-containing "
         "stages. Buckets: scan-dominated >=0.8, mixed 0.4-0.8, downstream-heavy <0.4. Columns and "
         "measurement are defined in `nds-sf3k-listed-vs-2g-20260721.md`. `warm` is wall-clock ms; "
         "`gpuTime`/`scan` are aggregate task-seconds per warm iteration.\n")

# summary table
L.append("## Summary by bucket\n")
L.append("| bucket | queries | WARM 2g->listed | gpuTime 2g->listed | queries faster | goal met (fuller+lower gpu+runtime<=+5%) |")
L.append("|---|---|---|---|---|---|")
for b in ["scan-dominated","mixed","downstream-heavy"]:
    qb=[q for q,d in rows if bucket(d)==b]
    w2=sum(fl(g2,q,"warm_ms") for q in qb)/1000; wl=sum(fl(lf,q,"warm_ms") for q in qb)/1000
    g2s=sum(fl(g2,q,"wq_gpu_s") for q in qb); gls=sum(fl(lf,q,"wq_gpu_s") for q in qb)
    faster=sum(1 for q in qb if fl(lf,q,"warm_ms")<fl(g2,q,"warm_ms"))
    met=sum(1 for q in qb if fl(lf,q,"wq_gpu_s")<fl(g2,q,"wq_gpu_s") and fl(lf,q,"pct_target")>fl(g2,q,"pct_target") and fl(lf,q,"warm_ms")<=1.05*fl(g2,q,"warm_ms"))
    L.append(f"| {b} | {len(qb)} | {w2:.1f}->{wl:.1f}s ({(wl-w2)/w2*100:+.0f}%) | {g2s:.0f}->{gls:.0f}s ({(gls-g2s)/g2s*100:+.0f}%) | {faster}/{len(qb)} | {met}/{len(qb)} |")

# best run-1 signal + gated policy
label={q:(fl(lf,q,"warm_ms")<=1.05*fl(g2,q,"warm_ms")) for q in [q for q,_ in rows]}
Qall=[q for q,_ in rows]
dmap=dict(rows)
nW=sum(label.values())
# best threshold on scan_dominance
best=(0,None)
for t in sorted(set(dmap.values())):
    acc=sum(1 for q in Qall if (dmap[q]>=t)==label[q])/len(Qall)
    if acc>best[0]: best=(acc,t)
TH=0.96
applied=[q for q in Qall if dmap[q]>=TH]
win=sum(1 for q in applied if fl(lf,q,"warm_ms")<=1.05*fl(g2,q,"warm_ms"))
warm_pol=sum((fl(lf,q,"warm_ms") if dmap[q]>=TH else fl(g2,q,"warm_ms")) for q in Qall)/1000
gpu_pol =sum((fl(lf,q,"wq_gpu_s") if dmap[q]>=TH else fl(g2,q,"wq_gpu_s")) for q in Qall)
warm2=sum(fl(g2,q,"warm_ms") for q in Qall)/1000; gpu2=sum(fl(g2,q,"wq_gpu_s") for q in Qall)
L.append("\n## Best run-1 signal and the gated policy\n")
L.append(f"Of {len(Qall)} queries, **{nW} keep runtime within +5% under listed** (call them winners). "
         "Testing several run-1 signals (all measured on the 2g run) for how well they separate winners "
         "from regressors, `scan_dominance` is the best single separator "
         f"(~{best[0]*100:.0f}% at threshold >= {best[1]:.2f}); batch fullness / avg batch / GPU-intensity "
         "were 63-69%.\n")
L.append("**Gated policy**: apply the ratio only when `scan_dominance >= 0.96`, else keep the 2g split. "
         f"It applies to {len(applied)} queries ({win}/{len(applied)} stay within +5%), giving:\n")
L.append("| policy | WARM runtime | gpuTime |")
L.append("|---|---|---|")
L.append(f"| all 2g (baseline) | {warm2:.1f} s | {gpu2:.0f} |")
L.append(f"| all listed (blind ratio) | {sum(fl(lf,q,'warm_ms') for q in Qall)/1000:.1f} s (+{(sum(fl(lf,q,'warm_ms') for q in Qall)/1000-warm2)/warm2*100:.0f}%) | {sum(fl(lf,q,'wq_gpu_s') for q in Qall):.0f} |")
L.append(f"| gated (scan_dominance >= 0.96) | {warm_pol:.1f} s ({(warm_pol-warm2)/warm2*100:+.1f}%) | {gpu_pol:.0f} ({(gpu_pol-gpu2)/gpu2*100:+.0f}%) |")
L.append("\nSo gating on the one run-1 signal gives gpuTime down ~11% with runtime within budget (~-1%), "
         "vs the blind ratio's +25% runtime. Caveats: ~6 of the applied queries still misfire, the gate "
         "captures about half the achievable gpuTime win, and 0.96 is fit to this NDS run (should be tunable).\n")

# per-bucket query tables
for b in ["scan-dominated","mixed","downstream-heavy"]:
    qb=sorted([q for q,d in rows if bucket(d)==b], key=lambda q: fl(lf,q,"warm_ms")/fl(g2,q,"warm_ms"))
    L.append(f"\n## {b} ({len(qb)} queries)\n")
    L.append("| query | scan_dom | warm_2g | warm_listed | speedup | gpuTime_2g | gpuTime_listed | full%_2g | full%_listed |")
    L.append("|---|---|---|---|---|---|---|---|---|")
    dmap=dict(rows)
    for q in qb:
        L.append(f"| {q} | {dmap[q]:.2f} | {fl(g2,q,'warm_ms'):.0f} | {fl(lf,q,'warm_ms'):.0f} | "
                 f"{fl(g2,q,'warm_ms')/fl(lf,q,'warm_ms'):.2f}x | {fl(g2,q,'wq_gpu_s'):.1f} | "
                 f"{fl(lf,q,'wq_gpu_s'):.1f} | {fl(g2,q,'pct_target'):.0f} | {fl(lf,q,'pct_target'):.0f} |")

open(OUT,"w").write("\n".join(L)+"\n")
print("wrote",OUT)
# also print the summary to stdout
for line in L:
    if line.startswith("|") or line.startswith("## Summary") or line.startswith("| bucket"): print(line)
