#!/usr/bin/env python3
# Generate a succinct self-contained HTML report for the scan-dominance POC, styled like the prior
# team-summary reports (CSS vars, light/dark, tiles, tables) with an inline-SVG fullness-vs-gpuTime
# scatter (validated categorical palette blue/orange/aqua, hover <title>, legend + direct labels).
# Numbers come from the published docs/CSVs (grounded).
import re, csv, os
R="docs/experiments/rolling-split-autotuning/results"
OUT=f"{R}/nds-sf3k-scandominance-report-20260722.html"

# --- scatter points + winner rows from the bucket doc ---
BC={"scan":"var(--blue)","mixed":"var(--orange)","down":"var(--aqua)"}
BL={"scan":"scan-dominated (≥0.8)","mixed":"mixed (0.4–0.8)","down":"downstream-heavy (<0.4)"}
pts=[]; bucket=None
for line in open(f"{R}/nds-sf3k-scandominance-buckets-20260721.md"):
    if line.startswith("## scan-dominated"): bucket="scan"
    elif line.startswith("## mixed"): bucket="mixed"
    elif line.startswith("## downstream"): bucket="down"
    m=re.match(r"\|\s*(query\w+)\s*\|\s*([\d.]+)\s*\|\s*(\d+)\s*\|\s*(\d+)\s*\|\s*([\d.]+)x\s*\|\s*([\d.]+)\s*\|\s*([\d.]+)\s*\|\s*(\d+)\s*\|\s*(\d+)\s*\|", line)
    if m and bucket:
        q,sd,w2,wl,sp,g2,gl,f2,fl=m.groups(); g2=float(g2); gl=float(gl)
        red=(g2-gl)/g2*100 if g2>0 else 0
        pts.append((bucket,q,float(fl),red,g2))

# --- top-20 winners with decode/gpuTime from per-query CSVs ---
def load(p): return {r["query"]:r for r in csv.DictReader(open(p))}
g2c=load("data/mpb-perquery-2g.csv"); lc=load("data/mpb-perquery-listedfull.csv")
def fnum(x):
    try: return float(x)
    except: return 0.0
WIN=["query66","query47","query71","query28","query88","query99","query9","query59","query70","query2",
 "query57","query96","query89","query13","query31","query61","query27","query48","query32","query63",
 "query26","query77","query90","query43","query86","query36","query53","query98","query16","query37",
 "query3","query92","query55","query83","query58","query52","query42","query12","query20"]  # all 39 scan-dominated winners
wrows=[]
for q in WIN:
    a=g2c[q]; b=lc[q]
    w2=fnum(a["warm_ms"]); wl=fnum(b["warm_ms"])
    d2=fnum(a["decode_s"]); dl=fnum(b["decode_s"])
    s2=fnum(a["scanstage_gpu_s"]); sl=fnum(b["scanstage_gpu_s"])   # SCAN-STAGE gpuTime (not whole-query)
    dwarm=(wl-w2)/w2*100 if w2 else 0; saved=s2-sl; dgpu=saved/s2*100 if s2 else 0
    wrows.append((q,w2,wl,dwarm,d2,dl,s2,sl,saved,dgpu,fnum(a["pct_target"]),fnum(b["pct_target"])))
wrows.sort(key=lambda r:-r[8])   # by absolute scan-stage gpuTime seconds saved

# --- SVG scatter ---
W,H=760,430; PL,PR,PT,PB=52,18,20,44
def sx(v): return PL+(v/100)*(W-PL-PR)
def sy(v): return H-PB-((v+5)/105)*(H-PT-PB)   # y range ~ -5..100
svg=[f'<svg viewBox="0 0 {W} {H}" width="100%" role="img" aria-label="batch fullness vs gpuTime reduction by bucket">']
svg.append(f'<rect x="0" y="0" width="{W}" height="{H}" fill="var(--surf)"/>')
for gy in (0,25,50,75,100):
    svg.append(f'<line x1="{PL}" y1="{sy(gy):.0f}" x2="{W-PR}" y2="{sy(gy):.0f}" stroke="var(--line)" stroke-width="1"/>')
    svg.append(f'<text x="{PL-6}" y="{sy(gy)+3:.0f}" text-anchor="end" font-size="10" fill="var(--mut)">{gy}%</text>')
for gx in (0,25,50,75,100):
    svg.append(f'<text x="{sx(gx):.0f}" y="{H-PB+16}" text-anchor="middle" font-size="10" fill="var(--mut)">{gx}%</text>')
svg.append(f'<text x="{(PL+W-PR)/2:.0f}" y="{H-6}" text-anchor="middle" font-size="11" fill="var(--ink)">batch fullness under fill-to-target (% of 1 GiB target)</text>')
svg.append(f'<text x="14" y="{(PT+H-PB)/2:.0f}" text-anchor="middle" font-size="11" fill="var(--ink)" transform="rotate(-90 14 {(PT+H-PB)/2:.0f})">gpuTime reduction 2g→fill-to-target (%)</text>')
for b in ("down","mixed","scan"):
    for r in [p for p in pts if p[0]==b]:
        rad=max(4,(r[4]**0.5)*1.15)
        sgn = "−" if r[3] >= 0 else "+"
        svg.append(f'<circle cx="{sx(r[2]):.1f}" cy="{sy(r[3]):.1f}" r="{rad:.1f}" fill="{BC[b]}" stroke="var(--surf)" stroke-width="1.4" opacity="0.9"><title>{r[1]}: fullness {r[2]:.0f}%, gpuTime {sgn}{abs(r[3]):.0f}%, 2g gpuTime {r[4]:.0f}s</title></circle>')
for q,dx,dy in [("query28",6,-4),("query88",6,10),("query66",6,0),("query71",8,4),("query16",6,-4)]:
    r=next((p for p in pts if p[1]==q),None)
    if r: svg.append(f'<text x="{sx(r[2])+dx:.0f}" y="{sy(r[3])+dy:.0f}" font-size="9.5" fill="var(--mut)">{q}</text>')
lx=PL+8
for b in ("scan","mixed","down"):
    svg.append(f'<circle cx="{lx}" cy="{PT+8}" r="5" fill="{BC[b]}"/>')
    svg.append(f'<text x="{lx+10}" y="{PT+11}" font-size="10.5" fill="var(--ink)">{BL[b]}</text>')
    lx += 20 + len(BL[b])*6.4
svg.append('</svg>')
SVG="\n".join(svg)

def tbl(head, rows, right_from=1):
    h="".join(f"<th{' style=text-align:left' if i<right_from else ''}>{c}</th>" for i,c in enumerate(head))
    body=""
    for row in rows:
        body+="<tr>"+"".join(f"<td{' style=text-align:left' if i<right_from else ''}>{c}</td>" for i,c in enumerate(row))+"</tr>"
    return f"<table><thead><tr>{h}</tr></thead><tbody>{body}</tbody></table>"

winhead=["query","warm ms 2g→ftt","Δwarm","decode_s 2g→ftt","scan-stage gpuTime_s 2g→ftt","saved_s","gpu↓%","full% 2g→ftt"]
winrows=[[r[0],f"{r[1]:.0f}→{r[2]:.0f}",f"{r[3]:+.0f}%",f"{r[4]:.1f}→{r[5]:.1f}",f"{r[6]:.1f}→{r[7]:.1f}",f"{r[8]:.1f}",f"{r[9]:.0f}%",f"{r[10]:.0f}→{r[11]:.0f}"] for r in wrows]
winrows.append([f"<b>total ({len(wrows)})</b>","—","—",f"<b>{sum(r[4] for r in wrows):.0f}→{sum(r[5] for r in wrows):.0f}</b>",f"<b>{sum(r[6] for r in wrows):.0f}→{sum(r[7] for r in wrows):.0f}</b>",f"<b>{sum(r[8] for r in wrows):.0f}</b>","−46%","—"])

ss=[["query93","0.98×","2048 → 2286","79 → 88"],["query67","0.87×","583 → 2321","20 → 90"],
    ["query78","0.84×","580 → 1656","29 → 92"],["query14_part1","0.87×","94 → 3888","41 → 94"],
    ["query71","1.20×","98 → 2906","4 → 85"],["query80","0.62×","65 → 1659","6 → 82"],
    ["query28","1.03×","2048 → 2858","66 → 87"],["query88","1.02×","2048 → 3810","49 → 91"]]

HTML=f"""<!doctype html><html lang=en><head><meta charset=utf-8>
<meta name=viewport content="width=device-width,initial-scale=1">
<title>Scan-split sizing POC — scan dominance</title><style>
:root{{--bg:#fcfcfb;--surf:#fff;--ink:#0b0b0b;--mut:#52514e;--line:#e6e5e2;--blue:#2a78d6;--orange:#eb6834;--aqua:#1baf7a;--code:#f4f3f0}}
@media(prefers-color-scheme:dark){{:root{{--bg:#151513;--surf:#1f1f1d;--ink:#fff;--mut:#c3c2b7;--line:#33322f;--blue:#3987e5;--orange:#d95926;--aqua:#199e70;--code:#26261f}}}}
*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--ink);font:15px/1.6 -apple-system,Segoe UI,Roboto,sans-serif}}
.wrap{{max-width:1000px;margin:0 auto;padding:32px 20px 64px}}
h1{{font-size:25px;margin:0 0 4px}}.sub{{color:var(--mut);margin:0 0 22px}}
h2{{font-size:17px;margin:34px 0 10px;border-bottom:1px solid var(--line);padding-bottom:6px}}
p{{margin:9px 0}}code{{background:var(--code);padding:1px 5px;border-radius:4px;font:13px ui-monospace,Menlo,monospace}}
.tiles{{display:flex;gap:12px;flex-wrap:wrap;margin:14px 0}}
.tile{{background:var(--surf);border:1px solid var(--line);border-radius:10px;padding:14px 18px;min-width:150px;flex:1}}
.tile .lab{{color:var(--mut);font-size:12.5px}}.tile .big{{font-size:23px;font-weight:600;margin-top:3px}}
.blue{{color:var(--blue)}}.orange{{color:var(--orange)}}.aqua{{color:var(--aqua)}}
pre{{background:var(--code);border:1px solid var(--line);border-radius:10px;padding:13px 15px;overflow-x:auto;font:12.5px/1.6 ui-monospace,Menlo,monospace}}
figure{{margin:14px 0;background:var(--surf);border:1px solid var(--line);border-radius:10px;padding:12px}}
figcaption{{color:var(--mut);font-size:12.5px;padding:8px 2px 0}}
.tblwrap{{overflow:auto;border:1px solid var(--line);border-radius:10px;margin:10px 0}}.scroll{{max-height:420px}}
table{{border-collapse:collapse;width:100%;font-size:13px}}
th,td{{padding:6px 11px;text-align:right;border-bottom:1px solid var(--line);white-space:nowrap}}
th{{position:sticky;top:0;background:var(--surf);color:var(--mut);font-weight:600}}
a{{color:var(--blue)}}ul{{margin:8px 0}}
</style></head><body><div class=wrap>
<h1>Scan-split sizing to cut GPU work within a runtime budget</h1>
<p class=sub>NDS SF3k · sparkh (8×16=128 cores) · autotuner off baseline vs projection-aware <code>fill-to-target</code> split · 2026-07-22</p>

<p><b>Question.</b> Can we change the scan split so <b>gpuTime and scan time drop</b> while <b>runtime stays within +5% of the best</b> (the <code>2g</code> config, 172.9&nbsp;s)? <b>Answer:</b> yes, selectively — gate the fuller-batch split on scan-dominance.</p>
<p><code>fill-to-target</code> (<b>ftt</b>) sizes each scan task to decode to ~one full 1&nbsp;GiB GPU batch: <code>maxSplitBytes = batchSize / (decodedBytes/listedBytes)</code> (config <code>ratioBasis=listed</code>). <code>2g</code> is the baseline — Spark's own <code>maxSplitBytes</code> at <code>maxPartitionBytes=2gb</code>.</p>

<div class=tiles>
<div class=tile><div class=lab>batch fullness (mean)</div><div class="big blue">15% → 63%</div><div class=lab>of 1&nbsp;GiB target (×4.3)</div></div>
<div class=tile><div class=lab>gpuTime, gated policy</div><div class="big aqua">−11%</div><div class=lab>4849 → 4311 task-s</div></div>
<div class=tile><div class=lab>runtime, gated policy</div><div class="big">−1.1%</div><div class=lab>172.9 → 171.0 s (in budget)</div></div>
<div class=tile><div class=lab>goal-met queries</div><div class="big orange">42 / 103</div><div class=lab>39 scan-dominated</div></div>
</div>

<h2>Formulas</h2>
<pre>fill-to-target split  : maxSplitBytes = clamp( batchSize / (decodedBytes/listedBytes), floor, 8 GiB )   batchSize = 1 GiB
2g split      : min(maxPartitionBytes=2gb, max(openCost, bytesPerCore))          (autotuner off)
scan_dominance: scanstage_gpuTime / wholequery_gpuTime            (run-1 signal, measured on 2g)
goal met      : full%_ftt > full%_2g  AND  gpuTime_ftt < gpuTime_2g  AND  warm_ftt ≤ 1.05·warm_2g
gate          : apply fill-to-target iff scan_dominance ≥ 0.96, else keep 2g</pre>

<h2>How fuller batches help (aggregate, 2g → fill-to-target)</h2>
{tbl(["metric","2g","fill-to-target","change"],[["scan time (task-s)","3,247","1,879","−42%"],["GPU decode (task-s)","1,248","635","−49%"],["gpuTime (task-s)","4,849","3,687","−24%"],["mean batch fullness","15%","63%","×4.3"],["WARM runtime (s)","172.9","215.9","+25% (blind)"]])}
<p>Fuller, fewer batches → less per-batch decode/op overhead. But blind application costs +25% runtime; the GPU-work win only converts to a runtime win when the scan is the bottleneck.</p>

<figure>{SVG}<figcaption>One dot per query; marker size ∝ 2g gpuTime. Fuller batches (right) cut gpuTime (up), but only scan-dominated (blue) queries land high on both — downstream-heavy (aqua) get fuller batches yet ~0 gpuTime gain. Hover for values.</figcaption></figure>

<h2>By scan-dominance bucket</h2>
{tbl(["bucket","queries","fullness 2g→ftt","gpuTime 2g→ftt","WARM 2g→ftt","goal met"],[["scan-dominated","61","16%→63%","1819→1023 (−44%)","68.2→74.8 (+10%)","39/61"],["mixed","30","10%→64%","1396→1037 (−26%)","60.7→89.6 (+48%)","2/30"],["downstream-heavy","12","18%→58%","1634→1627 (−0%)","44.0→51.5 (+17%)","1/12"]])}

<h2>Gated policy (apply fill-to-target iff scan_dominance ≥ 0.96)</h2>
{tbl(["policy","WARM runtime","gpuTime"],[["all 2g (best runtime)","172.9 s","4849"],["all fill-to-target (blind ratio)","215.9 s (+25%)","3687 (−24%)"],["gated (scan_dom ≥ 0.96)","171.0 s (−1.1%)","4311 (−11%)"]])}

<h2>All 39 scan-dominated winners — scan-stage gpuTime + runtime (2g → fill-to-target)</h2>
<p>gpuTime here is <b>scan-stage</b> (tasks in scan-containing stages), not whole-query — for these
scan-dominated queries the two nearly coincide (1081 vs 1089 task-s). Sorted by <b>seconds saved</b>;
<code>Δwarm</code> is the overall runtime change (negative = faster; all within +5% by construction).</p>
<div class="tblwrap scroll">{tbl(winhead,winrows)}</div>

<h2>maxSplitBytes used — store_sales scans (2g → fill-to-target)</h2>
<div class=tblwrap>{tbl(["query","qtime","split MiB","full%"],ss)}</div>
<p>fill-to-target drives the split up several-fold to fill batches to ~82–94%; per-scan value varies with dynamic partition pruning.</p>

<h2>Full reports</h2>
<ul>
<li><a href="nds-sf3k-scandominance-poc-final-20260722.md">POC final doc</a> — formulas, all tables, resource index</li>
<li><a href="nds-mpb-sweep-perquery-20260720.md">maxPartitionBytes baseline sweep</a> (128m→4g)</li>
<li><a href="nds-sf3k-listed-vs-2g-20260721.md">fill-to-target vs 2g</a> · <a href="nds-sf3k-scandominance-buckets-20260721.md">scan-dominance buckets</a> · <a href="nds-sf3k-perscan-2g-4g-listed-20260721.md">per-scan split/fullness</a></li>
</ul>
</div></body></html>"""

open(OUT,"w").write(HTML)
print("wrote", OUT, f"({len(HTML)} bytes, {len(pts)} scatter pts)")
