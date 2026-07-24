#!/usr/bin/env python3
# Fullness-vs-gpuTime chart for the POC doc. One dot per query: x = batch fullness under fill-to-target (% of
# 1 GiB target), y = gpuTime reduction under fill-to-target (%), color = scan-dominance bucket, size ~ 2g gpuTime
# (absolute GPU weight). Reads the bucket doc table (already-published numbers). Light surface, validated
# categorical palette (blue/orange/aqua), legend + selective direct labels (identity never color-alone).
import re, matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

DOC="docs/experiments/rolling-split-autotuning/results/nds-sf3k-scandominance-buckets-20260721.md"
BUCKET_COLOR={"scan":"#2a78d6","mixed":"#eb6834","down":"#1baf7a"}
BUCKET_LABEL={"scan":"scan-dominated (≥0.8)","mixed":"mixed (0.4–0.8)","down":"downstream-heavy (<0.4)"}
SURF="#fcfcfb"; INK="#0b0b0b"; MUTE="#52514e"

rows=[]; bucket=None
for line in open(DOC):
    if line.startswith("## scan-dominated"): bucket="scan"
    elif line.startswith("## mixed"): bucket="mixed"
    elif line.startswith("## downstream"): bucket="down"
    m=re.match(r"\|\s*(query\w+)\s*\|\s*([\d.]+)\s*\|\s*(\d+)\s*\|\s*(\d+)\s*\|\s*([\d.]+)x\s*\|\s*([\d.]+)\s*\|\s*([\d.]+)\s*\|\s*(\d+)\s*\|\s*(\d+)\s*\|", line)
    if m and bucket:
        q,sd,w2,wl,sp,g2,gl,f2,fl=m.groups()
        g2=float(g2); gl=float(gl)
        red=(g2-gl)/g2*100 if g2>0 else 0
        rows.append((bucket,q,float(fl),red,g2))

fig,ax=plt.subplots(figsize=(8.6,5.4),dpi=150)
fig.patch.set_facecolor(SURF); ax.set_facecolor(SURF)
for b in ("down","mixed","scan"):  # draw scan last (on top)
    rs=[r for r in rows if r[0]==b]
    ax.scatter([r[2] for r in rs],[r[3] for r in rs],
               s=[max(30,r[4]*1.1) for r in rs], c=BUCKET_COLOR[b],
               edgecolors=SURF, linewidths=1.3, alpha=0.9, label=BUCKET_LABEL[b], zorder=3)
# selective direct labels: a few high-weight / illustrative queries
LAB={"query28":(4,6),"query88":(4,-12),"query66":(4,4),"query71":(6,-2),
     "query93":(-6,8),"query67":(4,4),"query50":(4,-12),"query78":(-30,6)}
byq={r[1]:r for r in rows}
for q,(dx,dy) in LAB.items():
    if q in byq:
        r=byq[q]
        ax.annotate(q, (r[2],r[3]), textcoords="offset points", xytext=(dx,dy),
                    fontsize=7.5, color=MUTE)
ax.axhline(0, color="#d8d7d2", lw=1, zorder=1)
ax.set_xlabel("batch fullness under fill-to-target  (% of 1 GiB target)", color=INK, fontsize=10)
ax.set_ylabel("gpuTime reduction, 2g → fill-to-target  (%)", color=INK, fontsize=10)
ax.set_title("Fuller scan batches cut gpuTime — but the win concentrates in scan-dominated queries",
             color=INK, fontsize=10.5, pad=10)
ax.grid(True, color="#ececea", lw=0.8, zorder=0)
for s in ("top","right"): ax.spines[s].set_visible(False)
for s in ("left","bottom"): ax.spines[s].set_color("#c9c8c3")
ax.tick_params(colors=MUTE, labelsize=8.5)
leg=ax.legend(loc="lower left", frameon=True, framealpha=0.9, edgecolor="#d8d7d2", fontsize=8.5,
              title="bucket (marker size ∝ 2g gpuTime)", title_fontsize=8.5)
leg.get_frame().set_facecolor(SURF)
leg.get_title().set_color(MUTE)
for t in leg.get_texts(): t.set_color(INK)
fig.tight_layout()
OUT="docs/experiments/rolling-split-autotuning/results/nds-sf3k-fullness-vs-gputime-20260722.png"
fig.savefig(OUT, facecolor=SURF)
print("wrote", OUT, "with", len(rows), "queries")
