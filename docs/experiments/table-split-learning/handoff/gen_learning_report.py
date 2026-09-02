#!/usr/bin/env python3
"""Report for the table-split-learning run, from results/ledger.tsv.

Both queries, every arm. Cold and warm are kept apart and never combined:
  cold  = iteration 1, ONE measurement, reported raw and tagged no-noise-estimate
  warm  = median of iterations 2..N, with every warm value listed beside it

A query scanning several tables has one ledger row per table per iteration; the per-execution
metrics (wall, occupancy, sem wait, tasks) are properties of the EXECUTION, so they are read from
the first row of each iteration rather than summed across tables. Split and fullness are per table.
"""
import argparse, csv, os, sys, collections, statistics as st

HERE = os.path.dirname(os.path.abspath(__file__))
ap = argparse.ArgumentParser()
ap.add_argument("--ledger", default=os.path.join(HERE, "..", "results", "ledger.tsv"))
ap.add_argument("--title", default="NDS SF3k", help="dataset label for the report heading")
ap.add_argument("--out", default=None, help="output basename; defaults to results/learning-report")
ARGS = ap.parse_args()
LEDGER = ARGS.ledger
BASE = ARGS.out or os.path.join(HERE, "..", "results", "learning-report")
OUT_HTML, OUT_MD = BASE + ".html", BASE + ".md"
# Arm and query order come from the ledger, which build_ledger.py writes in run order. Nothing here
# is tied to a fixed set of arms or queries, so a clickstream run reports the same way an NDS run does.
SPARK_FALLBACK = "spark-maxSplitBytes"


def load():
    rows = list(csv.DictReader(open(LEDGER), delimiter="\t"))
    for r in rows:
        r["iteration"] = int(r["iteration"])
        for k in ("wall_s", "occupancy_s", "sem_wait_s", "input_gib", "fullness_pct",
                  "decode_s", "scan_time_s", "task_time_s"):
            r[k] = float(r[k])
        r["scan_tasks"] = int(r["scan_tasks"])
    return rows


def summarise(rows):
    """(arm, query) -> per-execution series + per-table splits."""
    by = collections.defaultdict(lambda: collections.defaultdict(list))
    for r in rows:
        by[(r["arm"], r["query"])][r["iteration"]].append(r)
    out = {}
    for key, iters in by.items():
        recs = []
        for it in sorted(iters):
            g = iters[it]
            first = g[0]                      # execution-level metrics, identical across its rows
            recs.append(dict(
                iteration=it, phase=first["phase"], wall=first["wall_s"],
                occ=first["occupancy_s"], sem=first["sem_wait_s"], tasks=first["scan_tasks"],
                input_gib=first["input_gib"],
                splits={x["table"]: x["split_mb"] for x in g},
                learnt={x["table"]: x["learnt_from"] for x in g},
                fullness=first["fullness_pct"]))
        out[key] = recs
    return out


def fmt_splits(d):
    return ", ".join(f"{t}={v}M" for t, v in sorted(d.items()))


def fmt_learnt(d):
    vals = sorted(set(d.values()))
    return vals[0] if len(vals) == 1 else ", ".join(f"{t}:{v}" for t, v in sorted(d.items()))


def build(summary):
    lines_md, lines_html = [], []
    A = lines_html.append
    M = lines_md.append
    A("""<!doctype html><meta charset=utf-8><title>Table split learning</title>
<style>
body{font:14px/1.55 -apple-system,Segoe UI,Roboto,sans-serif;color:#1a1a1a;max-width:1500px;margin:2rem auto;padding:0 1rem}
h1{font-size:20px}h2{font-size:15px;margin-top:1.5rem}
table{border-collapse:collapse;font-size:12px;width:100%}th,td{border:1px solid #e3e3e3;padding:4px 7px;text-align:right}
th:first-child,td:first-child,td.q{text-align:left}thead{background:#f6f8fa}
td.q{font-weight:600}.mut{color:#666;font-size:10.5px}
.tag{background:#f2f2f2;border-radius:3px;padding:0 4px;font-size:10.5px;color:#555}
</style>
<h1>Table split learning on TITLE</h1>""".replace("TITLE", ARGS.title))
    M(f"# Table split learning on {ARGS.title}\n")

    intro = ("File cache disabled in every arm, verified: zero filecache accumulators registered and "
             "input bytes identical across all iterations and arms. History is keyed on "
             "(table, columns, filters), so two queries projecting different columns get different "
             "keys and do not share a split. 'shared' arms let both queries write one history file; "
             "'isolated' arms give each query its own. 'learnt from' names the previous WRITER of "
             "that file, which is not evidence this scan read it -- compare the cold split against "
             "the fallback to see what was actually applied.")
    A(f"<p class=mut>{intro}</p>")
    M(f"{intro}\n")

    # ---- 1. does inheriting the previous query's split get us closer to our own answer? ---------
    # Timing-free metric: compare the cold split against the split this query converges to on its
    # own. Distance is a property of the split values, so it carries none of the noise or the
    # execution-position confound that the cold WALL numbers do.
    conv = {}
    for r in RAW:
        if r["learnt_from"] == "own":
            conv[(r["query"], r["table"], r["arm"].split("-")[0])] = int(r["split_mb"])

    def cold_split(query, table, ceiling, mode):
        for r in RAW:
            if (r["iteration"] == 1 and r["query"] == query and r["table"] == table
                    and r["arm"].startswith(f"{ceiling}-{mode}")):
                return r
        return None

    A("<h2>1. Did inheriting another query's split help?</h2>"
      "<p class=mut>The cold split compared against the split this query eventually settles on for "
      "itself. Distance between two split values carries no timing noise and no execution-position "
      "confound, unlike comparing cold wall times. 'inherited' is the shared-history arm, where a "
      "previous query had already written a record for this table; 'fallback' is the isolated arm, "
      "where the history was empty and Spark's own maxSplitBytes applied.</p>"
      "<table><thead><tr><th>query</th><th>table</th><th>ceiling</th><th>converges to</th>"
      "<th>inherited split</th><th>off by</th><th>fallback split</th><th>off by</th>"
      "<th>closer split</th><th>outcome</th><th>wall s<br><span class=mut>inherited / fallback</span></th>"
      "<th>gpuTime s<br><span class=mut>inherited / fallback</span></th>"
      "<th>decode s<br><span class=mut>inherited / fallback</span></th>"
      "<th>scan time s<br><span class=mut>inherited / fallback</span></th>"
      "<th>sem wait s<br><span class=mut>inherited / fallback</span></th>"
      "</tr></thead><tbody>")
    M("\n## 1. Did inheriting another query's split help?\n")
    M("The cold split compared against the split this query eventually settles on for itself. "
      "Distance between two split values carries no timing noise and no execution-position confound, "
      "unlike comparing cold wall times.\n")
    M("| query | table | ceiling | converges to | inherited split | off by | fallback split | off by "
      "| closer split | outcome | wall s inh/fb | gpuTime s inh/fb | decode s inh/fb | scan time s inh/fb | sem wait s inh/fb |")
    M("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    seen = set()
    for r in RAW:
        if r["iteration"] != 1 or r["arm"] == "off":
            continue
        key = (r["query"], r["table"], r["arm"].split("-")[0])
        if key in seen or key not in conv:
            continue
        seen.add(key)
        q, tb, ceil = key
        c = conv[key]
        rs = cold_split(q, tb, ceil, "shared")
        ri = cold_split(q, tb, ceil, "iso")
        s = int(rs["split_mb"]) if rs else None
        s_src = rs["learnt_from"] if rs else None
        i = int(ri["split_mb"]) if ri else None
        e = lambda v: abs(v - c) / c * 100 if v else None
        inherited = s is not None and s_src is not None and not s_src.startswith(SPARK_FALLBACK)
        if not inherited:
            verdict = "nothing to inherit (first query)"
        elif e(s) < e(i):
            verdict = "INHERITED closer"
        elif e(i) < e(s):
            verdict = "FALLBACK closer"
        else:
            verdict = "tie"
        def pair(k, fmt="{:.2f}"):
            if not (rs and ri and inherited):
                return "-"
            return f"{fmt.format(rs[k])} / {fmt.format(ri[k])}"
        # The distance verdict says which split is nearer the one the query converges to. That is
        # NOT the same as which ran better: on clickstream the inherited split is nearer yet much
        # slower, because the converged split is itself worse than Spark's default there. So the
        # outcome is scored separately, from wall and decode, and only called when they agree.
        if not (rs and ri and inherited):
            outcome = "-"
        else:
            dw, dd = ri["wall_s"] - rs["wall_s"], ri["decode_s"] - rs["decode_s"]
            outcome = ("inherited faster" if dw > 0 and dd > 0 else
                       "inherited slower" if dw < 0 and dd < 0 else
                       "mixed (wall and decode disagree)")
        cells = [q, tb, ceil, f"{c}M",
                 f"{s}M" if inherited else "-", f"{e(s):.1f}%" if inherited else "-",
                 f"{i}M" if i else "-", f"{e(i):.1f}%" if i else "-", verdict, outcome,
                 pair("wall_s"), pair("occupancy_s", "{:.1f}"), pair("decode_s", "{:.1f}"),
                 pair("scan_time_s", "{:.1f}"), pair("sem_wait_s", "{:.1f}")]
        cls = "ok" if outcome == "inherited faster" else ("bad" if outcome == "inherited slower" else "mut")
        A("<tr>" + "".join(f"<td class=q>{c2}</td>" if j == 0 else
                           (f"<td class={cls}>{c2}</td>" if j == 9 else f"<td>{c2}</td>")
                           for j, c2 in enumerate(cells)) + "</tr>")
        M("| " + " | ".join(cells) + " |")
    A("</tbody></table>")

    # ---- 2. progression: what split each iteration ran with, and where it came from -------------
    A("<h2>2. Split progression per iteration</h2>"
      "<p class=mut>Each arrow is one iteration. The source in brackets is where that split came "
      "from: spark = Spark's own maxSplitBytes fallback, a query name = inherited from that query's "
      "record, own = recomputed from this query's own measurement.</p><pre>")
    M("\n## 2. Split progression per iteration\n")
    M("Each arrow is one iteration. Source in brackets: spark = Spark's maxSplitBytes fallback, "
      "a query name = inherited from that query, own = recomputed from this query's own measurement.\n")
    M("```")
    SRC = {"n/a-autotuner-off": "no-autotuner"}
    src = lambda v: "spark" if v.startswith(SPARK_FALLBACK) else SRC.get(v, v)
    for q in QUERIES:
        for tb in sorted({r["table"] for r in RAW if r["query"] == q}):
            line = f"{q} / {tb}"
            A(line); M(line)
            for arm in ARM_ORDER:
                seq = sorted([r for r in RAW if r["arm"] == arm and r["query"] == q
                              and r["table"] == tb], key=lambda r: r["iteration"])
                if not seq:
                    continue
                steps, prev = [], None
                for r in seq:
                    cur = (r["split_mb"], src(r["learnt_from"]))
                    if cur != prev:
                        steps.append(f"it{r['iteration']} {cur[0]}M [{cur[1]}]")
                        prev = cur
                tail = "  converged" if len(steps) <= 2 else f"  {len(steps)} changes"
                row = f"    {arm:20s} " + "  ->  ".join(steps) + tail
                A(row); M(row)
            A(""); M("")
    A("</pre>")
    M("```")

    for phase, title, note in (
            ("cold", "Appendix A. Cold: iteration 1, one measurement each",
             "One value per cell, not a median. A cold-vs-cold difference has no noise estimate "
             "behind it with a single run, so these are descriptive and tagged no-noise-estimate."),
            ("warm", f"Appendix B. Warm: median of iterations 2 to {MAXIT}",
             "Every warm value is listed beside the median so the median is auditable. Rows where the "
             "split changed mid-warm are tagged split-moved and get no median.")):
        A(f"<h2>{title}</h2><p class=mut>{note}</p>"
          "<table><thead><tr><th>query</th><th>arm</th><th>split MB</th><th>learnt from</th>"
          "<th>wall s</th><th>warm values</th><th>occupancy s</th><th>sem wait s</th>"
          "<th>scan tasks</th><th>fullness</th><th>input GiB</th><th>tag</th></tr></thead><tbody>")
        M(f"\n## {title}\n\n{note}\n")
        M("| query | arm | split MB | learnt from | wall s | warm values | occupancy s | sem wait s "
          "| scan tasks | fullness | input GiB | tag |")
        M("|---|---|---|---|---|---|---|---|---|---|---|---|")
        for q in QUERIES:
            for arm in ARM_ORDER:
                recs = summary.get((arm, q))
                if not recs:
                    continue
                sel = [r for r in recs if r["phase"] == phase]
                if not sel:
                    continue
                if phase == "cold":
                    r = sel[0]
                    row = [q, arm, fmt_splits(r["splits"]), fmt_learnt(r["learnt"]),
                           f"{r['wall']:.2f}", "-", f"{r['occ']:.1f}", f"{r['sem']:.1f}",
                           str(r["tasks"]), f"{r['fullness']:.1f}%", f"{r['input_gib']:.2f}",
                           "no-noise-estimate"]
                else:
                    sig = {fmt_splits(r["splits"]) for r in sel}
                    vals = [r["wall"] for r in sel]
                    moved = len(sig) > 1
                    row = [q, arm, " | ".join(sorted(sig)) if moved else sel[0] and fmt_splits(sel[0]["splits"]),
                           fmt_learnt(sel[0]["learnt"]),
                           "split-moved" if moved else f"{st.median(vals):.2f}",
                           ", ".join(f"{v:.2f}" for v in vals),
                           f"{st.median([r['occ'] for r in sel]):.1f}",
                           f"{st.median([r['sem'] for r in sel]):.1f}",
                           str(sel[0]["tasks"]), f"{sel[0]['fullness']:.1f}%",
                           f"{sel[0]['input_gib']:.2f}",
                           "split-moved" if moved else ""]
                A("<tr>" + "".join(
                    f"<td class=q>{c}</td>" if i == 0 else
                    (f"<td><span class=tag>{c}</span></td>" if i == len(row) - 1 and c else f"<td>{c}</td>")
                    for i, c in enumerate(row)) + "</tr>")
                M("| " + " | ".join(str(c) for c in row) + " |")
        A("</tbody></table>")
        M("")
    return "\n".join(lines_html) + "\n", "\n".join(lines_md) + "\n"


def first_seen(rows, key):
    """Distinct values in the order the ledger lists them, i.e. the order they were run."""
    out = []
    for r in rows:
        if r[key] not in out:
            out.append(r[key])
    return out


rows = load()
RAW = rows
ARM_ORDER = first_seen(rows, "arm")
QUERIES = first_seen(rows, "query")
MAXIT = max(r["iteration"] for r in rows)
summary = summarise(rows)
html, md = build(summary)
open(OUT_HTML, "w").write(html)
open(OUT_MD, "w").write(md)
print(f"wrote {os.path.normpath(OUT_HTML)}")
print(f"wrote {os.path.normpath(OUT_MD)}")
print(f"queries: {sorted({q for _, q in summary})}  arms: {len({a for a, _ in summary})}")
