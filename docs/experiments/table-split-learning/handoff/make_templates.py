#!/usr/bin/env python3
"""Generate the arm templates for the table-split-learning experiment.

One template per (ceiling, historyPath). A Spark application carries exactly ONE historyPath, so
'isolated' mode needs a separate template - and a separate ab invocation - per query. 'shared' mode
puts both queries in one invocation against one history file.

Base template is the existing gpu-autotuner-core1.template so every non-autotuner conf (executor
shape, shuffle manager, metrics level, maxPartitionBytes) stays byte-identical to previous runs.
"""
import argparse, os, re, sys

AB = "/home/kuhu/Reps/ab/templates/onprem-h"
BASE = os.path.join(AB, "gpu-autotuner-core1.template")

CEILING_LINE = re.compile(r'^\s*"--conf" "spark\.driver\.extraJavaOptions=.*$', re.M)
# The RAPIDS file cache serves repeat reads of the same table from cache, so Input Metrics Bytes Read
# becomes cache MISSES, and a query that runs after another query on the same table starts warm.
# That transfers across queries exactly like the split history does and confounds this experiment,
# so it is forced off in every arm.
FILECACHE_LINE = re.compile(r'^\s*"--conf" "spark\.rapids\.filecache\.enabled=.*$', re.M)
HISTORY_LINE = re.compile(r'^\s*"--conf" "spark\.rapids\.sql\.(scan\.splitAutotuner\.)?historyPath=.*$', re.M)
PAD = " " * 19


def build(ceiling, history_path, CORES=0):
    """ceiling=None means the autotuner is off: drop both of its confs entirely."""
    s = open(BASE).read()
    s = FILECACHE_LINE.sub(f'{PAD}"--conf" "spark.rapids.filecache.enabled=false"', s)
    if ceiling is None:
        s = CEILING_LINE.sub("", s)
        s = HISTORY_LINE.sub("", s)
        return re.sub(r"\n{2,}", "\n", s)
    # The heuristic has no system properties left; the only ceiling lever is minPartitionNum, and
    # the old ceiling=core<N> is exactly minPartitionNum = N x defaultParallelism. core1 IS the
    # default (conf unset), so it needs no line. N>1 would need the cluster's total core count,
    # which a static template cannot know -- pass it explicitly rather than guessing it here.
    m = re.fullmatch(r"core(\d+)", ceiling)
    if not m:
        raise SystemExit(f"ceiling={ceiling} not expressible against this heuristic (only core<N>)")
    n = int(m.group(1))
    if n == 1:
        s = CEILING_LINE.sub("", s)
    elif CORES:
        s = CEILING_LINE.sub(f'{PAD}"--conf" "spark.sql.files.minPartitionNum={n * CORES}"', s)
    else:
        raise SystemExit(f"ceiling={ceiling} needs --cores (cluster total, e.g. 8 exec x 16 = 128)")
    s = HISTORY_LINE.sub(f'{PAD}"--conf" "spark.rapids.sql.historyPath={history_path}"', s)
    return s


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tag", required=True, help="run tag, e.g. 20260820a; keeps history paths unique per run")
    ap.add_argument("--queries", default="query9,query28")
    ap.add_argument("--ceilings", default="core1",
                    help="space or comma separated; core1 only by default (see run_learning_bench.sh)")
    a = ap.parse_args()
    qs = [q.strip() for q in a.queries.split(",") if q.strip()]
    ceilings = [c for c in re.split(r"[,\s]+", a.ceilings) if c]

    made = []
    # baseline: autotuner off, both queries in one application
    name = f"tsl-{a.tag}-off.template"
    open(os.path.join(AB, name), "w").write(build(None, None))
    made.append((name, "off", "n/a", "both queries", "-"))

    for ceiling in ceilings:
        # shared: one history file, both queries in one application
        hp = f"/tmp/tsl-{a.tag}-{ceiling}-shared.tsv"
        name = f"tsl-{a.tag}-{ceiling}-shared.template"
        open(os.path.join(AB, name), "w").write(build(ceiling, hp))
        made.append((name, ceiling, "shared", "both queries", hp))
        # isolated: one history file per query, one application per query
        for q in qs:
            hp = f"/tmp/tsl-{a.tag}-{ceiling}-iso-{q}.tsv"
            name = f"tsl-{a.tag}-{ceiling}-iso-{q}.template"
            open(os.path.join(AB, name), "w").write(build(ceiling, hp))
            made.append((name, ceiling, "isolated", q, hp))

    w = max(len(m[0]) for m in made)
    print(f"{'template':{w}s} {'ceiling':8s} {'history':9s} {'queries':13s} historyPath")
    for n, c, h, q, p in made:
        print(f"{n:{w}s} {c:8s} {h:9s} {q:13s} {p}")
    print(f"\n{len(made)} templates written to {AB}")
    # fail loudly rather than silently emitting a template the runner would misread
    for n, c, h, q, p in made:
        body = open(os.path.join(AB, n)).read()
        assert "filecache.enabled=false" in body, f"{n}: file cache not disabled"
        if c == "off":
            assert "autotuner" not in body, f"{n}: autotuner conf leaked into the off arm"
        else:
            assert f"ceiling={c}" in body and p in body, f"{n}: ceiling or historyPath not substituted"
    print("all templates verified")


if __name__ == "__main__":
    main()
