#!/usr/bin/env python3
# Index page for the RW6-RW9 real-world scan-split study — cross-query summary + links. MD + HTML.
R="docs/experiments/rolling-split-autotuning/results"
# query, scanned(off disk, measured Spark input), optimum, ftt split, ftt vs opt, skew opt->ftt, gpu opt->ftt, verdict, slug
ROWS=[
 ("rw6 provenance (5-theme union)","10.8 GiB","1g","per-table 0.4–2.1 GB","≈ / beats","7.65× → 1.85×","106 → 89 s","WIN (per-table)","nds-overture-rw6-provenance-20260723"),
 ("rw7 road freshness (explode)","2.1 GiB","2g","4.99 GB","+6% wall","1.48× → 1.25×","22.8 → 12.6 s (−45%)","gpu-lean, +6% wall","nds-overture-rw7-roadfreshness-20260723"),
 ("rw8 multilingual roads","1.7 GiB","2g","7.9 GB","+11% wall","1.54× → 1.16×","5.5 → 2.8 s (−49%)","gpu-lean, +11% wall","nds-overture-rw8-multilingual-20260723"),
 ("rw9 POI addressing","1.2 GiB","flat ≥1g","1.92 GB","≈ tie","1.27× → 1.36×","11.6 → 2.6 s","tie (flat)","nds-overture-rw9-poiaddressing-20260723"),
 ("gf2 geometry types (5-theme)","48.1 GiB","512m","per-theme 1.3–6.0 GB","+6% wall","2.00× → 1.50×","126 → 94 s (−26%)","gpu-lean, +6% wall","nds-overture-gf2-geometrytypes-20260724"),
 ("road coverage (earlier)","13.6 GiB","512m ≈ 2g","1.22 GB","+5% wall","—","—","gpu-lean, +5% wall","nds-overture-realworld-20260723"),
]
# goal analysis: query, scanned, ftt split, gpuTime saved, decode saved, wall Δ vs optimum, within 5%
GOAL=[
 ("rw6 provenance","10.8 GiB","per-table 0.4–2.1 GB","−16%","−10%","−0.8% (faster)","YES"),
 ("rw7 freshness","2.1 GiB","4.99 GB","−44%","−43%","+6.6%","no"),
 ("rw8 multilingual","1.7 GiB","7.9 GB","−49%","−41%","+11.4%","no"),
 ("rw9 POI","1.2 GiB","1.92 GB","−78%","−73%","+0.6% (≈tie)","YES"),
 ("gf2 geometry","48.1 GiB","per-theme 1.3–6.0 GB","−26%","−23%","+5.7%","no"),
 ("road coverage","13.6 GiB","1.22 GB","−4%","−6%","+4.7%","YES (marginal)"),
]
LESSONS=[
 "<b>The optimum is genuinely query-dependent</b> — across six real queries it ranges 512m → 1g → 2g → flat. No single fixed <code>maxPartitionBytes</code> wins them all.",
 "<b>On a big geometry scan (gf2, 48 GiB), per-theme sizing is a safety feature</b>: cutting gpuTime by going to a big split is a trap — a global 4g split explodes byte skew to 13.3× (one 2.4 GB / 18.9 s straggler) and wall to +49%. ftt's per-theme splits keep skew at 1.5× and harvest −26% gpuTime at +5.7% wall.",
 "<b>Per-table sizing is ftt's real edge</b> (rw6): a multi-table union forces one global split onto 5 wildly different tables → byte skew 7.65×. ftt sizes each table from its own ratio → skew 1.85×, gpuTime 106→89 s, wall matches the best fixed split. A global knob <i>cannot</i> do this.",
 "<b>On single-table queries ftt trades wall for GPU efficiency</b> (rw7/rw8): highly-compressible columns → low decoded/listed ratio → a 5–8 GB split → fewer, fuller tasks that <b>cut gpuTime 45–49% and decode ~40%</b> below the optimum, at the cost of <b>+6–11% wall</b> (these are parallelism-bound, so fewer tasks slightly slows wall even as GPU work drops).",
 "<b>ftt always cuts gpuTime and decode</b> (fewer, fuller batches = less GPU work) — a real efficiency win; it only costs wall when the query is parallelism-bound rather than GPU-bound.",
 "<b>Value of the autotuner:</b> avoid a bad setting, balance multi-table scans, and land start-independently in the right neighbourhood — not beat a hand-tuned single-table optimum.",
]
def md():
    L=["# Overture real-world scan-split study (RW6–RW9) — index — 2026-07-23\n",
       "Four genuinely real-world, scan-heavy Overture queries (`overture-realworld-2.scala`), each swept over "
       "`maxPartitionBytes` {256m…4g} with the fill-to-target autotuner, warm iters 2–5, one query per session "
       "(`BENCHMARK-METHOD.md`), RTX A5000. All fully on GPU.\n",
       "## Cross-query summary\n",
       "| query | scanned | OFF optimum | ftt split | ftt vs opt | byte skew opt→ftt | gpuTime opt→ftt | verdict |",
       "|---|---|---|---|---|---|---|---|"]
    for q,sz,opt,fs,vs,sk,gp,vd,slug in ROWS:
        L.append(f"| [{q}]({slug}.md) | {sz} | {opt} | {fs} | {vs} | {sk} | {gp} | {vd} |")
    L.append("\n## What we learned\n")
    for i,x in enumerate(LESSONS,1):
        L.append(f"{i}. "+x.replace('<b>','**').replace('</b>','**').replace('<code>','`').replace('</code>','`').replace('<i>','*').replace('</i>','*'))
    L.append("\n## Goal: save gpuTime + decode, keep runtime within 5% (ON=ftt vs OFF=hand-tuned optimum)\n")
    L.append("| query | scanned | ftt split | gpuTime saved | decode saved | wall Δ vs optimum | within 5% |")
    L.append("|---|---|---|---|---|---|---|")
    for q,sc,sp,g,d,w,ok in GOAL:
        L.append(f"| {q} | {sc} | {sp} | {g} | {d} | {w} | {ok} |")
    L.append("\n**GPU-efficiency half — achieved everywhere:** every query cuts gpuTime (−4% to −78%) and decode "
             "(−6% to −73%). **≤5% runtime half — vs the hand-tuned optimum:** met for rw6, rw9 (faster) and road "
             "coverage; **rw7 (+6.6%), rw8 (+11.4%), gf2 (+5.7%)** run fewer tasks than the 16 cores can absorb, so "
             "they save GPU work but lose 5.7–11% wall. Against an *untuned default* `maxPartitionBytes`, ftt is "
             "within 5% or faster everywhere.\n")
    L.append("## Reports\n")
    for q,sz,opt,fs,vs,sk,gp,vd,slug in ROWS:
        L.append(f"- **{q}** — [md]({slug}.md) · [html]({slug}.html)")
    open(f"{R}/nds-overture-rw2-index-20260723.md","w").write("\n".join(L))
    return f"{R}/nds-overture-rw2-index-20260723.md"

CSS=open("docs/experiments/rolling-split-autotuning/handoff/_rw2css.txt").read() if False else None
def html():
    from importlib.machinery import SourceFileLoader
    css=SourceFileLoader("g","docs/experiments/rolling-split-autotuning/handoff/rw2_writereport.py").load_module().CSS
    def row(cells,tag="td",lc=1):
        return "<tr>"+"".join(f"<{tag}{' style=text-align:left' if i<lc else ''}>{c}</{tag}>" for i,c in enumerate(cells))+"</tr>"
    def tbl(hdr,rows,lc=1):
        return f'<div style="overflow-x:auto"><table><thead>{row(hdr,tag="th",lc=lc)}</thead><tbody>{"".join(row(r,lc=lc) for r in rows)}</tbody></table></div>'
    trs="".join(row([f'<a href="{slug}.html">{q}</a>',sz,opt,fs,vs,sk,gp,f'<b>{vd}</b>'],lc=1)
                for q,sz,opt,fs,vs,sk,gp,vd,slug in ROWS)
    lessons="".join(f"<li>{x}</li>" for x in LESSONS)
    cards="".join(f'<a class=card href="{slug}.html"><div class=q>{q}</div><div class=meta>{sz} · optimum {opt} · ftt {fs}</div><div class=vd>{vd}</div></a>' for q,sz,opt,fs,vs,sk,gp,vd,slug in ROWS)
    H=f"""<!doctype html><html lang=en><head><meta charset=utf-8><meta name=viewport content="width=device-width,initial-scale=1">
<title>Overture real-world scan-split study (RW6–RW9)</title><style>{css}
.card{{display:block;text-decoration:none;background:var(--surf);border:1px solid var(--line);border-radius:10px;padding:13px 16px;margin:8px 0;color:var(--ink)}}
.card .q{{font-weight:600;color:var(--blue)}}.card .meta{{color:var(--mut);font-size:12.5px;margin-top:2px}}.card .vd{{font-size:12.5px;margin-top:3px}}
li{{margin:6px 0}}</style></head><body><div class=wrap>
<h1>Overture real-world scan-split study — RW6–RW9</h1>
<p class=sub>Six scan-heavy Overture queries (RW6–9, geometry-full GF2, road coverage) × maxPartitionBytes sweep + fill-to-target · warm 2–5 · one query/session · RTX A5000 · 2026-07</p>
<div class=concl><h3>The through-line</h3><p>Across six real queries the best split ranges <b>512m → 1g → 2g → flat</b> — no single fixed
<code>maxPartitionBytes</code> wins. fill-to-target's standout is <b>per-table sizing</b> (rw6: byte skew 7.65×→1.85×, a global knob can't do this). On single-table queries it <b>cuts gpuTime 45–49% and decode ~40%</b> (fewer, fuller tasks = less GPU work), trading <b>+6–11% wall</b> where the query is parallelism-bound (rw7/rw8). A GPU-efficiency win, not an "overshoot".</p></div>
<h2>Cross-query summary</h2><div style="overflow-x:auto"><table><thead>{row(["query","scanned","OFF optimum","ftt split","ftt vs opt","byte skew opt→ftt","gpuTime opt→ftt","verdict"],tag="th",lc=1)}</thead><tbody>{trs}</tbody></table></div>
<h2>Goal: save gpuTime + decode, runtime within 5% (ON=ftt vs OFF=hand-tuned optimum)</h2>
{tbl(["query","scanned","ftt split","gpuTime saved","decode saved","wall Δ vs optimum","within 5%"],[[q,sc,sp,g,d,w,f'<b>{ok}</b>'] for q,sc,sp,g,d,w,ok in GOAL],1)}
<p><b>GPU-efficiency half — achieved everywhere</b> (gpuTime −4% to −78%, decode −6% to −73%). <b>≤5% runtime half</b>
vs the hand-tuned optimum: met for rw6, rw9 (faster) and road coverage; <b>rw7 (+6.6%), rw8 (+11.4%), gf2 (+5.7%)</b>
run fewer tasks than the 16 cores absorb, so they save GPU work but lose 5.7–11% wall. Against an <i>untuned default</i>
<code>maxPartitionBytes</code>, ftt is within 5% or faster everywhere.</p>
<h2>What we learned</h2><ol>{lessons}</ol>
<h2>Reports</h2>{cards}
<p class=sub style="margin-top:18px">Method: <code>handoff/BENCHMARK-METHOD.md</code> · queries: <code>docs/experiments/overture-analytics/overture-realworld-2.scala</code></p>
</div></body></html>"""
    open(f"{R}/nds-overture-rw2-index-20260723.html","w").write(H)
    return f"{R}/nds-overture-rw2-index-20260723.html"
if __name__=="__main__":
    print("wrote",md()); print("wrote",html())
