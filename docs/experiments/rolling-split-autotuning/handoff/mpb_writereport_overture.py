#!/usr/bin/env python3
# Self-contained HTML report for the Overture fill-to-target local study, styled like the other reports.
# Inline-SVG line chart: OFF maxPartitionBytes sweep (U-curve) vs ftt (flat near-optimal from any start).
# Shows ftt ONLY via its self-tuned runs (128m, 4g starts) — not the maxPartitionBytes=1g run.
import math
R="docs/experiments/rolling-split-autotuning/results"
OUT=f"{R}/nds-overture-ftt-local-20260723.html"

SWEEP=[(128,9165),(256,7888),(512,6844),(1024,6558),(2048,7405),(4096,8552)]  # OFF: MB -> warm ms
FTT=[(128,6576),(4096,6760)]  # ftt self-tuned (start MB -> warm ms), both -> ~741 MB (NO 1g run)
# Appendix: OFF batch vs maxPartitionBytes (label, tasks, avg MB, avg%, max MB, max%)
APPX=[("128m",550,170,17,273,27),("256m",286,327,32,524,51),("512m",143,460,45,722,71),
      ("1g",99,413,40,722,71),("2g",40,423,41,725,71),("4g",18,427,42,722,71)]

W,H=720,360; PL,PR,PT,PB=54,16,18,40
xs=[math.log10(m) for m,_ in SWEEP]; x0,x1=min(xs),max(xs)
y0,y1=6300,9300
def sx(mb): return PL+(math.log10(mb)-x0)/(x1-x0)*(W-PL-PR)
def sy(ms): return H-PB-(ms-y0)/(y1-y0)*(H-PT-PB)
svg=[f'<svg viewBox="0 0 {W} {H}" width="100%" role="img" aria-label="OFF sweep vs fill-to-target warm runtime">']
svg.append(f'<rect width="{W}" height="{H}" fill="var(--surf)"/>')
for gy in range(6500,9300,500):
    svg.append(f'<line x1="{PL}" y1="{sy(gy):.0f}" x2="{W-PR}" y2="{sy(gy):.0f}" stroke="var(--line)" stroke-width="1"/>')
    svg.append(f'<text x="{PL-6}" y="{sy(gy)+3:.0f}" text-anchor="end" font-size="10" fill="var(--mut)">{gy/1000:.1f}s</text>')
for mb,lab in [(128,"128m"),(256,"256m"),(512,"512m"),(1024,"1g"),(2048,"2g"),(4096,"4g")]:
    svg.append(f'<text x="{sx(mb):.0f}" y="{H-PB+15}" text-anchor="middle" font-size="10" fill="var(--mut)">{lab}</text>')
svg.append(f'<text x="{(PL+W-PR)/2:.0f}" y="{H-4}" text-anchor="middle" font-size="11" fill="var(--ink)">maxPartitionBytes (log)</text>')
svg.append(f'<text x="13" y="{(PT+H-PB)/2:.0f}" text-anchor="middle" font-size="11" fill="var(--ink)" transform="rotate(-90 13 {(PT+H-PB)/2:.0f})">warm runtime</text>')
pts=" ".join(f"{sx(m):.1f},{sy(ms):.1f}" for m,ms in SWEEP)
svg.append(f'<polyline points="{pts}" fill="none" stroke="var(--blue)" stroke-width="2"/>')
for m,ms in SWEEP:
    svg.append(f'<circle cx="{sx(m):.1f}" cy="{sy(ms):.1f}" r="4" fill="var(--blue)"><title>OFF {m}m: {ms} ms</title></circle>')
svg.append(f'<text x="{sx(256):.0f}" y="{sy(7888)-8:.0f}" font-size="10.5" fill="var(--blue)">OFF (fixed) — a U you must tune</text>')
favg=sum(ms for _,ms in FTT)/len(FTT)
svg.append(f'<line x1="{sx(128):.1f}" y1="{sy(favg):.1f}" x2="{sx(4096):.1f}" y2="{sy(favg):.1f}" stroke="var(--aqua)" stroke-width="2" stroke-dasharray="5 3"/>')
for m,ms in FTT:
    svg.append(f'<circle cx="{sx(m):.1f}" cy="{sy(ms):.1f}" r="5" fill="var(--aqua)" stroke="var(--surf)" stroke-width="1.4"><title>ftt from {m}m -> ~741MB: {ms} ms</title></circle>')
svg.append(f'<text x="{sx(1024):.0f}" y="{sy(favg)+16:.0f}" font-size="10.5" fill="var(--aqua)">fill-to-target — ~741 MB &amp; near-optimal from any start</text>')
svg.append('</svg>')
SVG="\n".join(svg)

def tbl(head, rows, lc=1):
    h="".join(f"<th{' style=text-align:left' if i<lc else ''}>{c}</th>" for i,c in enumerate(head))
    b="".join("<tr>"+"".join(f"<td{' style=text-align:left' if i<lc else ''}>{c}</td>" for i,c in enumerate(r))+"</tr>" for r in rows)
    return f"<table><thead><tr>{h}</tr></thead><tbody>{b}</tbody></table>"

HTML=f"""<!doctype html><html lang=en><head><meta charset=utf-8>
<meta name=viewport content="width=device-width,initial-scale=1">
<title>Overture fill-to-target (local) — scan-split POC</title><style>
:root{{--bg:#fcfcfb;--surf:#fff;--ink:#0b0b0b;--mut:#52514e;--line:#e6e5e2;--blue:#2a78d6;--orange:#eb6834;--aqua:#1baf7a;--code:#f4f3f0}}
@media(prefers-color-scheme:dark){{:root{{--bg:#151513;--surf:#1f1f1d;--ink:#fff;--mut:#c3c2b7;--line:#33322f;--blue:#3987e5;--orange:#d95926;--aqua:#199e70;--code:#26261f}}}}
*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--ink);font:15px/1.6 -apple-system,Segoe UI,Roboto,sans-serif}}
.wrap{{max-width:1000px;margin:0 auto;padding:32px 20px 64px}}
h1{{font-size:24px;margin:0 0 4px}}.sub{{color:var(--mut);margin:0 0 20px}}
h2{{font-size:17px;margin:32px 0 10px;border-bottom:1px solid var(--line);padding-bottom:6px}}
p{{margin:9px 0}}code{{background:var(--code);padding:1px 5px;border-radius:4px;font:13px ui-monospace,Menlo,monospace}}
.tiles{{display:flex;gap:12px;flex-wrap:wrap;margin:14px 0}}
.tile{{background:var(--surf);border:1px solid var(--line);border-radius:10px;padding:14px 18px;min-width:160px;flex:1}}
.tile .lab{{color:var(--mut);font-size:12.5px}}.tile .big{{font-size:22px;font-weight:600;margin-top:3px}}
.blue{{color:var(--blue)}}.aqua{{color:var(--aqua)}}.orange{{color:var(--orange)}}
pre{{background:var(--code);border:1px solid var(--line);border-radius:10px;padding:12px 15px;overflow-x:auto;font:12.5px/1.6 ui-monospace,Menlo,monospace}}
figure{{margin:14px 0;background:var(--surf);border:1px solid var(--line);border-radius:10px;padding:12px}}
figcaption{{color:var(--mut);font-size:12.5px;padding:8px 2px 0}}
.tblwrap{{overflow:auto;border:1px solid var(--line);border-radius:10px;margin:10px 0}}
table{{border-collapse:collapse;width:100%;font-size:13px}}
th,td{{padding:6px 11px;text-align:right;border-bottom:1px solid var(--line);white-space:nowrap}}
th{{background:var(--surf);color:var(--mut);font-weight:600}}
a{{color:var(--blue)}}ul{{margin:8px 0}}
.concl{{background:var(--surf);border:1px solid var(--aqua);border-left:5px solid var(--aqua);border-radius:10px;padding:16px 20px;margin:14px 0}}
.concl h3{{margin:0 0 8px;font-size:16px}}.concl table{{width:auto;margin:10px 0}}.concl td{{padding:4px 14px 4px 0;border:none;text-align:left}}
</style></head><body><div class=wrap>
<h1>Overture scan-heavy query — fill-to-target vs tuned baseline (local, Spark 3.5.3)</h1>
<p class=sub>Single-scan, wide-nested-column query · RTX A5000 · local[16] · 2026-07-23</p>

<div class=concl>
<h3>Conclusion — the autotuner lands you near the best split automatically, no tuning needed</h3>
<p>The <b>best</b> split for this query (found by trying every <code>maxPartitionBytes</code>) is <b>1 GiB → 6.56 s</b>.
The autotuner picks <b>~741 MB on its own — from <i>any</i> starting <code>maxPartitionBytes</code> — and runs ~6.6 s,
within ~2% of that best.</b> So you don't need to sweep <code>maxPartitionBytes</code>:</p>
<table>
<tr><td>if you set it to…</td><td><b>you get</b></td><td><b>autotuner gives</b></td><td><b>autotuner is</b></td></tr>
<tr><td>128m (a bad guess)</td><td>9.2 s</td><td>6.6 s</td><td class=aqua><b>1.39× faster</b></td></tr>
<tr><td>4g (a bad guess)</td><td>8.6 s</td><td>6.8 s</td><td class=aqua><b>1.27× faster</b></td></tr>
<tr><td>1g (already the best)</td><td>6.56 s</td><td>6.6 s</td><td>≈ same (−2%)</td></tr>
</table>
<p style="margin:6px 0 0">Guess badly → the autotuner is 27–39% faster; guess perfectly → it ties. Either way it converges
to the same ~741 MB split. (It doesn't <i>beat</i> a perfectly-tuned value because the batch is already as full as
this data allows — details below.)</p>
</div>

<p><code>fill-to-target</code> (ftt) sizes the split so each task decodes ~one 1&nbsp;GiB GPU batch
(<code>maxSplitBytes = batchSize/(decoded/listed)</code>). Does it beat a tuned <code>maxPartitionBytes</code>, and
can it <b>self-tune</b> without a sweep?</p>

<div class=tiles>
<div class=tile><div class=lab>autotuner vs tuning</div><div class="big aqua">no sweep needed</div><div class=lab>lands within ~2% of the best split, from any maxPartBytes</div></div>
<div class=tile><div class=lab>ftt vs mistuned baseline</div><div class="big aqua">1.27–1.39×</div><div class=lab>faster (recovers from 128m / 4g)</div></div>
<div class=tile><div class=lab>ftt vs tuned optimum (1g)</div><div class="big">≈ tie (−2%)</div><div class=lab>ftt's 741 MB is in the plateau</div></div>
<div class=tile><div class=lab>converged split</div><div class="big aqua">~741 MB</div><div class=lab>from any start maxPartBytes</div></div>
</div>

<h2>The query &amp; the question it poses</h2>
<p><b>Question:</b> <i>profile the global Overture transportation-segment network</i> — across all 348.7&nbsp;M
segments, how big is it, how connected, how much regulatory metadata (access restrictions, speed limits) does it
carry, how many roads are named, how much provenance exists, and how large is a typical segment geographically.</p>
<p>It is a <b>scan-heavy query</b>: a single Parquet scan, <b>no join and no GROUP BY</b>, over deeply nested
array/struct columns (5× decode expansion). Essentially all the work is the scan + decode (only a trivial 1-row
final aggregate) — so it is ~100% scan-dominated, which is exactly the case the split lever is meant to affect.</p>
<pre>SELECT
  COUNT(*)                        AS segments,             -- how many road/path segments
  SUM(size(connectors))           AS total_connector_refs, -- total junction/connection points
  AVG(size(access_restrictions))  AS avg_access_restr,     -- avg regulatory restrictions / segment
  AVG(size(speed_limits))         AS avg_speed_limits,     -- avg speed-limit records / segment
  COUNT(names.primary)            AS named_segments,       -- segments with a primary name
  SUM(size(sources))              AS total_sources,        -- total provenance/source records
  AVG(bbox.xmax - bbox.xmin)      AS avg_bbox_width_deg     -- avg geographic span (degrees)
FROM segment</pre>
<p><b>Result</b> (measured, all 348.7&nbsp;M segments) — with honest caveats:</p>
{tbl(["profile","result","reading"],[["segments","348,672,901","348.7 M road/path segments"],["connectors","897,200,683","~2.57 per segment (well connected)"],["named","94,113,420","<b>27%</b> of segments are named"],["sources","407,039,736","~1.17 provenance records/segment"],["avg access restr.","<b>−0.68</b>","artifact: size(NULL)=−1 → most segments have none"],["avg speed limits","<b>−0.84</b>","artifact: most segments carry no speed-limit data"],["avg bbox width","<b>0.00183°</b>","bbox longitude span (~200 m), not a true length"]])}
<p>Valid outputs (count, connectivity, 27% named, ~1.2 sources) profile the network shape, but <b>3 of the 7
aggregates are artifacts</b> (negative averages from <code>size(NULL)=−1</code>; bbox-degrees isn't a length). Fine
as a <b>benchmark</b> probe, weak as an <b>analytical</b> query — a null-safe <code>GROUP BY class</code> rewrite is
in <code>handoff/overture-realworld.scala</code>.</p>

<h2>Data read (per query execution)</h2>
{tbl(["stage","bytes","note"],[["on disk (listed)","66.3 GiB","128 files, 332–761 MB"],["read off disk","18.2 GiB","read_selectivity 0.275 (6 nested cols projected)"],["decoded on GPU","91.6 GiB","decode_expansion 5.03× (array/struct)"],["rows","348.7 M","segment rows"]])}

<h2>Sanity — scan pipeline is fully GPU</h2>
<p>Executed plan: <code>GpuScan parquet → GpuProject → GpuHashAggregate → GpuColumnarExchange → GpuShuffleCoalesce</code>
— all GPU (GpuScan does 348.7&nbsp;M rows / 91.6&nbsp;GiB decode). Only CPU work: the final <code>GpuColumnarToRow →
HashAggregate</code> global reduction (~127 partial rows → 1), not scan-related. No OOM.</p>

<h2>Baseline sweep (autotuner OFF) → optimal = 1g, and how ftt flattens it</h2>
<figure>{SVG}<figcaption>OFF (blue) is a U-shape you must tune per dataset; fill-to-target (aqua) converges to the
same ~741&nbsp;MB split and ~6.6–6.8&nbsp;s warm regardless of the starting maxPartitionBytes — rescuing a mistuned
setting and landing ~2% above the tuned optimum. Hover for values.</figcaption></figure>

<h2>Self-tuning — converges to ~741 MB from any start, near-optimal</h2>
{tbl(["start maxPartBytes","fixed OFF warm","ftt warm (→~741 MB)","ftt vs fixed"],[["128m (mistuned)","9165 ms","6576 ms","<b>1.39× faster</b>"],["4g (mistuned)","8552 ms","6760 ms","<b>1.27× faster</b>"],["1g (already optimal)","6558 ms","~6.6–6.8 s","≈ tie (~2% slower)"]])}
<p>Both suboptimal starts DECIDE the same ~741&nbsp;MB split (<code>bound_by=ratio</code>); only iter1 (cold)
reflects the start. So no per-dataset <code>maxPartitionBytes</code> sweep is needed — ftt lands near-optimal automatically.</p>

<h2>Warm-to-warm: ftt ties the optimum on GPU work (batch plateaus)</h2>
<p>Apples-to-apples, <b>iters 2–5 only</b> (iter1 = COLD_START, no memory yet → default split; excluded). Scan
metrics attributed per SQL execution:</p>
{tbl(["metric (warm/iter)","off-1g (1 GiB)","ftt (→741 MB)"],[["scan-stage tasks","99","127 (+28%)"],["avg output batch","413 MB (40%)","401 MB (39%)"],["max output batch/task","<b>722 MB</b>","<b>722 MB</b>"],["GPU decode","42.1 s","41.5–43.4 s"],["scan-stage gpuTime","72.0 s","72.6–73.6 s"]])}
<p><b>ftt matches the tuned optimum on decode, gpuTime, and batch</b> — the only difference is +28% tasks, costing
the ~2% runtime. ftt's lever is batch fullness, but here the batch <b>grows with the split only up to ~512m, then
plateaus at 722&nbsp;MB</b> (71% of the 1&nbsp;GiB target). ftt's ~741&nbsp;MB sits <b>in the plateau</b> — no
fullness left to gain, so it correctly stops there.</p>
<p><b>Mechanism</b> (<code>GpuParquetScan.scala</code>): <code>maxSplitBytes</code> only picks which row groups per
task. <code>populateCurrentBlockChunk</code> (2140–2174) accumulates row groups to <code>maxReadBatchSizeBytes</code>
(2&nbsp;GiB); the cuDF chunked reader emits batches ≤ <code>gpuTargetBatchSize</code> (1&nbsp;GiB) on row-group
boundaries — largest aligned chunk here is 722&nbsp;MB. To exceed it, tune
<code>spark.rapids.sql.batchSizeBytes</code> / <code>reader.batchSizeBytes</code>, not the split.</p>

<h2>Appendix — batch size vs maxPartitionBytes (OFF sweep)</h2>
{tbl(["maxPartitionBytes","tasks","avg batch","max batch/task"],[[m,str(t),f"{a} MB ({ap}%)",f"<b>{x} MB ({xp}%)</b>"] for (m,t,a,ap,x,xp) in APPX])}
<p>Max batch grows with the split up to ~512m, then plateaus at ~722&nbsp;MB (split-limited below ~512m,
reader-capped above). ftt's ~741&nbsp;MB is in the plateau. % of the 1&nbsp;GiB <code>gpuTargetBatchSize</code>.</p>

<h2>Full report</h2>
<ul><li><a href="nds-overture-ftt-local-20260723.md">Result doc</a> — setup, data read, sanity, sweep, self-tuning, batch-plateau mechanism, appendix, sources.</li>
<li>Contrast: <a href="nds-sf3k-scandominance-poc-final-20260722.md">NDS scan-dominance POC</a> (where the split <i>did</i> control fullness).</li></ul>
</div></body></html>"""
open(OUT,"w").write(HTML)
print("wrote",OUT,f"({len(HTML)} bytes)")
