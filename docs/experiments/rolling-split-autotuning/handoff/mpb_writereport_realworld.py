#!/usr/bin/env python3
# HTML report for the Overture REAL-WORLD query study (baseline sweep + fill-to-target). Styled like the others.
# Inline-SVG U-curve (OFF sweep) with the ftt converged point marked ABOVE the optimum (honest overshoot).
import math
R="docs/experiments/rolling-split-autotuning/results"
OUT=f"{R}/nds-overture-realworld-20260723.html"
# drift-cancelled interleaved probe (10 warm rounds, r2-10 mean); 128m/4g from the sweep
SWEEP=[(128,7380),(256,5407),(512,4621),(1024,4914),(2048,4601),(4096,5459)]
FTT_MB=1250; FTT_MS=4840   # converged ~1.22 GiB, warm ~4840
OPT=[(512,4621),(2048,4601)]  # twin optima
BUMP=(1024,4914)             # 1g skew bump

W,H=720,340; PL,PR,PT,PB=54,16,18,40
xs=[math.log10(m) for m,_ in SWEEP]; x0,x1=min(xs),max(xs); y0,y1=4300,7600
def sx(mb): return PL+(math.log10(mb)-x0)/(x1-x0)*(W-PL-PR)
def sy(ms): return H-PB-(ms-y0)/(y1-y0)*(H-PT-PB)
svg=[f'<svg viewBox="0 0 {W} {H}" width="100%" role="img" aria-label="real-world query sweep vs fill-to-target">']
svg.append(f'<rect width="{W}" height="{H}" fill="var(--surf)"/>')
for gy in range(4500,7600,500):
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
# twin-optimum rings + labels
for om,oms in OPT:
    svg.append(f'<circle cx="{sx(om):.1f}" cy="{sy(oms):.1f}" r="7" fill="none" stroke="var(--blue)" stroke-width="2"/>')
svg.append(f'<text x="{sx(OPT[0][0]):.0f}" y="{sy(OPT[0][1])+18:.0f}" text-anchor="middle" font-size="10" fill="var(--blue)">optimum</text>')
svg.append(f'<text x="{sx(OPT[1][0]):.0f}" y="{sy(OPT[1][1])+18:.0f}" text-anchor="middle" font-size="10" fill="var(--blue)">optimum</text>')
# 1g skew bump callout
svg.append(f'<text x="{sx(BUMP[0]):.0f}" y="{sy(BUMP[1])-9:.0f}" text-anchor="middle" font-size="10" fill="var(--orange)">1g skew bump (2.18×)</text>')
# ftt point
svg.append(f'<circle cx="{sx(FTT_MB):.1f}" cy="{sy(FTT_MS):.1f}" r="6" fill="var(--aqua)" stroke="var(--surf)" stroke-width="1.4"><title>ftt -> 1.22 GiB: {FTT_MS} ms</title></circle>')
svg.append(f'<text x="{sx(FTT_MB)+8:.0f}" y="{sy(FTT_MS)+14:.0f}" font-size="10.5" fill="var(--aqua)">ftt (1.22 GiB) — ~5% above optimum</text>')
svg.append('</svg>')
SVG="\n".join(svg)

def tbl(head, rows, lc=1):
    h="".join(f"<th{' style=text-align:left' if i<lc else ''}>{c}</th>" for i,c in enumerate(head))
    b="".join("<tr>"+"".join(f"<td{' style=text-align:left' if i<lc else ''}>{c}</td>" for i,c in enumerate(r))+"</tr>" for r in rows)
    return f"<table><thead><tr>{h}</tr></thead><tbody>{b}</tbody></table>"

HTML=f"""<!doctype html><html lang=en><head><meta charset=utf-8>
<meta name=viewport content="width=device-width,initial-scale=1">
<title>Overture real-world query — sweep + fill-to-target</title><style>
:root{{--bg:#fcfcfb;--surf:#fff;--ink:#0b0b0b;--mut:#52514e;--line:#e6e5e2;--blue:#2a78d6;--orange:#eb6834;--aqua:#1baf7a;--code:#f4f3f0}}
@media(prefers-color-scheme:dark){{:root{{--bg:#151513;--surf:#1f1f1d;--ink:#fff;--mut:#c3c2b7;--line:#33322f;--blue:#3987e5;--orange:#d95926;--aqua:#199e70;--code:#26261f}}}}
*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--ink);font:15px/1.6 -apple-system,Segoe UI,Roboto,sans-serif}}
.wrap{{max-width:1000px;margin:0 auto;padding:32px 20px 64px}}
h1{{font-size:23px;margin:0 0 4px}}.sub{{color:var(--mut);margin:0 0 18px}}
h2{{font-size:17px;margin:30px 0 10px;border-bottom:1px solid var(--line);padding-bottom:6px}}
p{{margin:9px 0}}code{{background:var(--code);padding:1px 5px;border-radius:4px;font:13px ui-monospace,Menlo,monospace}}
.concl{{background:var(--surf);border:1px solid var(--orange);border-left:5px solid var(--orange);border-radius:10px;padding:16px 20px;margin:14px 0}}
.concl h3{{margin:0 0 8px;font-size:16px}}
.tiles{{display:flex;gap:12px;flex-wrap:wrap;margin:14px 0}}
.tile{{background:var(--surf);border:1px solid var(--line);border-radius:10px;padding:14px 18px;min-width:160px;flex:1}}
.tile .lab{{color:var(--mut);font-size:12.5px}}.tile .big{{font-size:22px;font-weight:600;margin-top:3px}}
.blue{{color:var(--blue)}}.aqua{{color:var(--aqua)}}.orange{{color:var(--orange)}}
pre{{background:var(--code);border:1px solid var(--line);border-radius:10px;padding:12px 15px;overflow-x:auto;font:12px/1.55 ui-monospace,Menlo,monospace}}
figure{{margin:14px 0;background:var(--surf);border:1px solid var(--line);border-radius:10px;padding:12px}}
figcaption{{color:var(--mut);font-size:12.5px;padding:8px 2px 0}}
table{{border-collapse:collapse;width:100%;font-size:13px;margin:6px 0}}
th,td{{padding:6px 11px;text-align:right;border-bottom:1px solid var(--line);white-space:nowrap}}
th{{background:var(--surf);color:var(--mut);font-weight:600}}
a{{color:var(--blue)}}
</style></head><body><div class=wrap>
<h1>Overture real-world query — maxPartitionBytes sweep + fill-to-target (local, Spark 3.5.3)</h1>
<p class=sub>Road-network coverage by class · scan-heavy · RTX A5000 · local[16] · 2026-07-23</p>

<div class=concl>
<h3>Conclusion — the autotuner rescues mistuned settings and sidesteps the 1g skew bump, landing ~5% above the twin optimum</h3>
<p>The <b>best</b> splits are a <b>twin optimum at 512m ≈ 2g (~4.61 s)</b> — and the curve is a <b>W, not a U</b>:
<b>1g is a reproducible +7% "skew bump"</b> between them (file-alignment skew, see below). The autotuner converges to
<b>1.22 GiB from any start</b> (~4.84 s): it <b>rescues a badly-set</b> <code>maxPartitionBytes</code> and even
<b>beats a hand-set 1g</b>, but lands ~5% above the twin optimum.</p>
<table>
<tr><td>if you set it to…</td><td><b>you get</b></td><td><b>autotuner gives</b></td><td><b>autotuner is</b></td></tr>
<tr><td>128m (bad)</td><td>7.4 s</td><td>4.8 s</td><td class=aqua><b>1.52× faster</b></td></tr>
<tr><td>4g (bad)</td><td>5.5 s</td><td>4.8 s</td><td class=aqua><b>1.13× faster</b></td></tr>
<tr><td>1g (skew bump)</td><td>4.91 s</td><td>4.84 s</td><td class=aqua><b>1.02× faster</b></td></tr>
<tr><td>512m / 2g (the best)</td><td>4.61 s</td><td>4.84 s</td><td class=orange><b>~5% slower</b></td></tr>
</table>
<p style="margin:6px 0 0">The optimal split is <b>query-dependent</b> (512m/2g here vs 1g for the profiling query) and
even <b>non-monotonic</b>. The autotuner's single "fill one 1&nbsp;GiB batch/task" target can't hit every optimum, but
ftt is <b>not</b> skew-aware — it feeds the same Spark bin-packer and never looks at file sizes; it only dodged the 1g
resonance because its computed 1.22&nbsp;GiB landed <i>off</i> the 888&nbsp;MB file boundary. Round powers-of-two
(512m/1g/2g) more often coincide with a file boundary; ftt's odd content-derived value incidentally less so.
Value: avoid a bad setting, not beat a tuned one.</p>
</div>

<div class=tiles>
<div class=tile><div class=lab>optimum (OFF sweep)</div><div class="big blue">512m ≈ 2g → 4.61 s</div><div class=lab>twin minima — a W, not a U</div></div>
<div class=tile><div class=lab>1g skew bump</div><div class="big orange">+7%</div><div class=lab>byte skew 2.18× at file-size alignment</div></div>
<div class=tile><div class=lab>autotuner converges to</div><div class="big aqua">1.22 GiB</div><div class=lab>from any start; ~5% above optimum</div></div>
<div class=tile><div class=lab>vs mistuned baseline</div><div class="big aqua">1.13–1.52×</div><div class=lab>faster</div></div>
</div>

<h2>The query &amp; the question it poses</h2>
<p><b>Real-world question:</b> <i>road-network coverage by class</i> — for each road subtype+class: how many segments,
% named, % with a speed limit, avg connectors. A genuine data-coverage analysis (null-safe). <b>Scan-heavy:</b> one
66&nbsp;GiB scan + a tiny <code>GROUP BY subtype,class</code> (~20 groups), so scan+decode dominate.</p>
<pre>SELECT subtype, class, COUNT(*) AS segments,
  ROUND(100.0*COUNT(names.primary)/COUNT(*),1)                                  AS pct_named,
  ROUND(100.0*SUM(CASE WHEN size(speed_limits)>0 THEN 1 ELSE 0 END)/COUNT(*),1) AS pct_speed_limit,
  ROUND(AVG(CASE WHEN size(connectors)>0 THEN size(connectors) ELSE 0 END),2)   AS avg_connectors
FROM segment GROUP BY subtype, class ORDER BY segments DESC</pre>
<p><b>Result</b> (real insight — major roads are far better annotated):</p>
{tbl(["subtype","class","segments","% named","% speed_limit","avg connectors"],[["road","residential","127.9 M","42.0","8.8","2.28"],["road","service","61.5 M","4.1","1.8","2.70"],["road","tertiary","20.8 M","55.1","20.6","2.66"],["road","secondary","11.4 M","68.9","33.3","2.70"],["road","primary","7.4 M","69.2","41.3","2.64"],["road","trunk","4.2 M","61.6","38.8","2.45"],["road","motorway","1.1 M","34.8","41.8","2.17"],["rail","standard_gauge","1.5 M","13.6","0.0","3.69"]],2)}

<h2>Data read (per query execution)</h2>
{tbl(["stage","bytes","note"],[["on disk (listed)","66.3 GiB","128 files"],["read off disk","12.2 GiB","read_selectivity 0.184 (fewer/smaller cols than profiling query)"],["decoded on GPU","54.3 GiB","decode_expansion 4.45×"],["rows","348.7 M","segments"]])}

<h2>Baseline sweep (autotuner OFF) → twin optima 512m ≈ 2g; 1g is a skew bump</h2>
<figure>{SVG}<figcaption>Drift-cancelled interleaved probe (10 warm rounds, all configs back-to-back each round).
OFF (blue) has <b>twin minima at 512m and 2g</b> with a <b>+7% bump at 1g</b> (file-alignment skew). Fill-to-target
(aqua) converges to 1.22&nbsp;GiB — ~5% above the optimum but past the 1g bump. Hover for values.</figcaption></figure>
{tbl(["maxPartitionBytes","tasks","max batch","warm mean (ms)","vs optimum"],[["128m","550","—","7380","+60%"],["<b>512m</b>","<b>143</b>","510 MB","<b>4621</b>","≈ opt"],["1g","99","766 MB","4914","<b>+7% (skew)</b>"],["<b>2g</b>","<b>40</b>","766 MB","<b>4601</b>","≈ opt"],["4g","18","766 MB","5459","+19%"]])}

<h2>Why 1g is a skew bump — the W-curve explained (all measured)</h2>
<p>The 1g inversion shows up in <b>gpuTime and scan time too</b>, so it is real GPU work, not scheduling noise.
<b>Byte skew</b> = <code>max(bytes/task) ÷ median(bytes/task)</code> for the scan stage — how far the fattest task's
input stretches above the typical one (1.0 = balanced; a stage ends on its slowest task, so high skew → straggler).</p>
{tbl(["config","tasks","byte skew","avg batch","scan time","GPU decode","gpuTime","wall ms"],[["256m","286","1.73×","194 M","60.3 s","41.3 s","61.8 s","5407"],["<b>512m</b>","143","1.59×","<b>389 M</b>","51.2 s","33.1 s","49.0 s","<b>4621</b>"],["<b>1g</b>","99","<b>2.18×</b>","<b>354 M ↓</b>","<b>56.1 s ↑</b>","36.6 s ↑","<b>56.6 s ↑</b>","<b>4914 ↑</b>"],["<b>2g</b>","40","1.37×","381 M","52.8 s","24.1 s","38.1 s","<b>4601</b>"],["4g","18","1.22×","409 M","59.2 s","22.9 s","34.5 s","5459"]],1)}
<p><b>The measured chain at 1g:</b> byte skew <b>peaks at 2.18×</b> → avg batch <b>drops to 354 M</b>, below 512m's
389 M <i>even though splits are bigger</i> → decode <b>less efficient (36.6 s > 33.1 s)</b> → gpuTime <b>rises (56.6 s)</b>
→ scan time <b>rises (56.1 s)</b> → wall clock <b>rises (4914 ms)</b>.</p>
<p><b>Why skew peaks at 1g — file alignment.</b> The 128 files are <b>611–1125 MB, median 888 MB</b> (row groups are
small &amp; uniform: 2.6–23 MB, <i>not</i> fat). Spark bin-packs files into splits capped at <code>maxPartitionBytes</code>:
<b>512m &lt; file</b> → each file halves into ~2 even tasks (skew 1.59×); <b>1g ≈ file (888 MB)</b> → splits align to
file boundaries → a 1125 MB file → one ~1g task <b>+ a small remainder</b> → fat right tail + remainder shoulder
(skew 2.18×), and the remainders decode as small batches; <b>2g &gt; file</b> → 2–3 files combine per split, variance
averages out (skew 1.37×). So 1g is the <b>file-size resonance point</b>: worst packing → emptiest average batch → the bump.
<span style="color:var(--mut)">(Skew-peaks-at-1g and runtime-peaks-at-1g are directly measured; the file-alignment cause is inferred from the
file-size histogram + Spark's bin-packing.)</span></p>

<h2>ftt vs fixed settings — wall, gpuTime &amp; scan-time diffs (iters 2–5)</h2>
<p>ftt converges to 1.22&nbsp;GiB: <b>scan 54.2 s, gpuTime 46.9 s, wall 4840 ms</b>. Δ = ftt − baseline.</p>
{tbl(["baseline","wall","Δ wall","scan s","Δ scan","gpuTime s","Δ gpuTime"],[["128m (mistuned)","7380","<b>−34%</b>","60.3","−10%","61.8","<b>−24%</b>"],["4g (mistuned)","5459","<b>−11%</b>","59.2","−8%","34.5","+36%"],["<b>512m (optimum)</b>","<b>4621</b>","+5%","51.2","+6%","49.0","−4%"],["<b>2g (optimum)</b>","<b>4601</b>","+5%","52.8","+3%","38.1","+23%"],["1g (skew bump)","4914","<b>−2%</b>","56.1","−3%","56.6","<b>−17%</b>"]],1)}
<p>ftt <b>cuts gpuTime &amp; scan time vs the small-split configs</b> (128m, 512m, 1g — where skew / small batches bloat
GPU work) but runs <b>higher gpuTime than 2g/4g</b> (few large tasks decode very efficiently). Vs the 1g skew bump it
wins on wall (−2%) and gpuTime (−17%); vs the twin optimum it's ~5% slower on wall despite comparable GPU work — the
optima win on <b>parallelism</b>, not GPU efficiency.</p>

<h2>Warm-to-warm: fuller batches, fewer tasks — but slower (iters 2–5)</h2>
{tbl(["metric (warm/iter)","512m (optimum)","ftt (→1.22 GiB)"],[["scan-stage tasks","143","73"],["avg output batch","389 MB (38%)","323 MB (32%)"],["max output batch","510 MB","766 MB"],["GPU decode","33.1 s","29.9–32.1 s"],["scan-stage gpuTime","49.0 s","45.6–48.1 s"]])}
<p>ftt makes <b>fuller batches</b> (766 vs 510 MB) with <b>fewer tasks</b> (73 vs 143) and marginally lower GPU work,
yet is <b>~5% slower</b> — this query is bottlenecked on <b>parallelism</b>, not GPU work. Fuller ≠ faster.</p>

<h2>Full report</h2>
<ul><li><a href="nds-overture-realworld-20260723.md">Result doc</a> — query, result, sweep, ftt, warm-to-warm, sources.</li>
<li>Companion (profiling query, ftt near-optimal): <a href="nds-overture-ftt-local-20260723.md">nds-overture-ftt-local-20260723</a>.</li></ul>
</div></body></html>"""
open(OUT,"w").write(HTML)
print("wrote",OUT,f"({len(HTML)} bytes)")
