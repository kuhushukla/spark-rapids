#!/usr/bin/env python3
# U-curve plot: warm runtime vs maxPartitionBytes for BOTH Overture queries, with the fill-to-target
# converged split marked (near-optimal for profiling, overshoot for real-world). Validated palette.
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
SURF="#fcfcfb"; INK="#0b0b0b"; MUTE="#52514e"; BLUE="#2a78d6"; ORANGE="#eb6834"; AQUA="#1baf7a"
# (maxPartitionBytes MB, warm ms)
# RW numbers are the drift-cancelled interleaved probe (10 warm rounds, r2-10 mean); 128m/4g from the sweep.
PROF=[(128,9165),(256,7888),(512,6844),(1024,6558),(2048,7405),(4096,8552)]
RW  =[(128,7380),(256,5407),(512,4621),(1024,4914),(2048,4601),(4096,5459)]
FTT_PROF=(741,6667); FTT_RW=(1250,4840)

fig,ax=plt.subplots(figsize=(8.4,5.0),dpi=150)
fig.patch.set_facecolor(SURF); ax.set_facecolor(SURF)
def line(data,color,label,optima=None):
    xs=[m for m,_ in data]; ys=[v/1000 for _,v in data]
    ax.plot(xs,ys,"-o",color=color,lw=2,ms=6,label=label,zorder=3)
    # ring the optimum/optima (twin minima for the real-world query)
    marks=optima if optima else [min(data,key=lambda d:d[1])[0]]
    for mx in marks:
        my=dict(data)[mx]; ax.scatter([mx],[my/1000],s=140,facecolors="none",edgecolors=color,lw=2,zorder=4)
line(PROF,BLUE,"profiling query (optimum 1g)")
line(RW,ORANGE,"real-world query (twin optima 512m ≈ 2g; 1g = skew bump)",optima=[512,2048])
# call out the skew bump at 1g
ax.annotate("1g skew bump\n(byte skew 2.18×)",(1024,RW[3][1]/1000),textcoords="offset points",xytext=(6,10),fontsize=8,color=ORANGE)
# ftt converged points
ax.scatter([FTT_PROF[0]],[FTT_PROF[1]/1000],s=130,marker="D",color=AQUA,edgecolors=SURF,lw=1.3,zorder=5)
ax.annotate("ftt 741 MB\n(≈ optimum)",(FTT_PROF[0],FTT_PROF[1]/1000),textcoords="offset points",xytext=(8,6),fontsize=8,color=AQUA)
ax.scatter([FTT_RW[0]],[FTT_RW[1]/1000],s=130,marker="D",color=AQUA,edgecolors=SURF,lw=1.3,zorder=5)
ax.annotate("ftt 1.22 GiB\n(~5% over optimum)",(FTT_RW[0],FTT_RW[1]/1000),textcoords="offset points",xytext=(8,-20),fontsize=8,color=AQUA)
ax.set_xscale("log",base=2)
ax.set_xticks([128,256,512,1024,2048,4096]); ax.set_xticklabels(["128m","256m","512m","1g","2g","4g"])
ax.set_xlabel("maxPartitionBytes (log)",color=INK); ax.set_ylabel("warm runtime (s)",color=INK)
ax.set_title("maxPartitionBytes U-curve — the optimum is query-dependent; fill-to-target self-tunes near it",
             color=INK,fontsize=10.5,pad=10)
ax.grid(True,color="#ececea",lw=0.8,zorder=0)
for s in ("top","right"): ax.spines[s].set_visible(False)
for s in ("left","bottom"): ax.spines[s].set_color("#c9c8c3")
ax.tick_params(colors=MUTE,labelsize=9)
leg=ax.legend(loc="upper center",frameon=False,fontsize=9)
for t in leg.get_texts(): t.set_color(INK)
fig.tight_layout()
OUT="docs/experiments/rolling-split-autotuning/results/nds-overture-ucurves-20260723.png"
fig.savefig(OUT,facecolor=SURF); print("wrote",OUT)
