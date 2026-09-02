#!/usr/bin/env python3
"""Across every clickstream split sweep we have, does gpuTime or task time fall as the split grows?

Answers two questions per query, per split point:
  scan-heaviness = scan-task time / all-task time   (scan task = Input Metrics Bytes Read > 0)
  benefit        = does scan-task gpuTime / task time fall as maxPartitionBytes grows?

gpuTime is semaphoreHoldingTime (GpuTaskMetrics.scala:383) - occupancy, and a SUM over tasks, so it
moves with task count. Task count is printed beside it for that reason.

Iteration 1 of each arm is COLD_START and is dropped, per the kit's rule; the rest are medianed.
"""
import json, os, glob, re, sys, collections, statistics as st

MPB_ORDER = {"256m": 256, "512m": 512, "1g": 1024, "2g": 2048, "4g": 4096, "6g": 6144}


def parse(el_dir):
    """-> (scan_gpu_s, scan_task_s, all_task_s, scan_tasks, wall_s per execution list)"""
    files = glob.glob(os.path.join(el_dir, "*"))
    if not files:
        return None
    el = max(files, key=os.path.getsize)
    s2e, start, end = {}, {}, {}
    bad = 0
    agg = collections.defaultdict(lambda: collections.defaultdict(float))
    for ln in open(el, errors="ignore"):
        # A killed JVM leaves a half-written last line. Count them - a log with many truncated lines
        # is not a complete arm and must not be medianed as if it were.
        if ln and not ln.rstrip().endswith("}"):
            bad += 1
            continue
        if '"SparkListenerJobStart"' in ln:
            e = json.loads(ln)
            try:
                j = int((e.get("Properties", {}) or {}).get("spark.sql.execution.id"))
            except (TypeError, ValueError):
                continue
            for si in e.get("Stage Infos", []):
                s2e[si["Stage ID"]] = j
        elif '"SparkListenerTaskEnd"' in ln:
            e = json.loads(ln)
            eid = s2e.get(e.get("Stage ID"))
            if eid is None:
                continue
            tm = e.get("Task Metrics", {}) or {}
            run = (tm.get("Executor Run Time", 0) or 0) / 1000.0
            a = agg[eid]
            a["all_task_s"] += run
            if ((tm.get("Input Metrics", {}) or {}).get("Bytes Read", 0) or 0) <= 0:
                continue
            a["scan_tasks"] += 1
            a["scan_task_s"] += run
            for ac in (e.get("Task Info", {}) or {}).get("Accumulables", []) or []:
                if ac.get("Name") == "gpuTime":
                    v = ac.get("Update", 0)
                    if isinstance(v, str) and ":" in v:
                        h, m, sec = v.split(":")
                        v = int(h) * 3600 + int(m) * 60 + float(sec)
                    a["scan_gpu_s"] += float(v or 0)
        elif "SQLExecutionStart" in ln:
            e = json.loads(ln); start[e["executionId"]] = e["time"]
        elif "SQLExecutionEnd" in ln:
            e = json.loads(ln); i = e["executionId"]
            if i in start:
                end[i] = (e["time"] - start[i]) / 1000.0
    # keep only executions that actually scanned; those are the query iterations
    execs = sorted(k for k in agg if agg[k]["scan_tasks"] > 0)
    if bad:
        print(f"   !! {el}: skipped {bad} truncated lines", file=sys.stderr)
    return [dict(agg[k], wall_s=end.get(k, 0.0)) for k in execs]


def main():
    roots = sys.argv[1:] or ["/data/scan-split-real", "/data/scan-split-2g",
                             "/data/scan-split-partrule-20260812", "/data/clean-run-20260814"]
    found = collections.defaultdict(dict)     # query -> mpb -> stats
    for root in roots:
        for d in sorted(glob.glob(os.path.join(root, "cs*-off-*"))):
            m = re.match(r"(cs[A-Za-z0-9]*)-off-(\w+)$", os.path.basename(d))
            if not m:
                continue
            q, mpb = m.groups()
            if mpb not in MPB_ORDER or mpb in found[q]:
                continue                      # first root wins; roots are in preference order
            r = parse(os.path.join(d, "el"))
            if not r or len(r) < 2:
                continue
            warm = r[1:]                      # drop COLD_START
            found[q][mpb] = dict(
                wall=st.median([x["wall_s"] for x in warm]),
                gpu=st.median([x["scan_gpu_s"] for x in warm]),
                task=st.median([x["scan_task_s"] for x in warm]),
                alltask=st.median([x["all_task_s"] for x in warm]),
                tasks=int(st.median([x["scan_tasks"] for x in warm])),
                n=len(warm), src=root)

    for q in sorted(found):
        pts = sorted(found[q].items(), key=lambda kv: MPB_ORDER[kv[0]])
        if not pts:
            continue
        share = st.median([p["task"] / p["alltask"] * 100 for _, p in pts if p["alltask"]])
        print(f"\n=== {q}   scan share of task time: {share:.0f}%   "
              f"({os.path.basename(pts[0][1]['src'])}, {pts[0][1]['n']} warm iters)")
        print(f'{"mpb":>6s} {"wall s":>8s} {"gpuTime s":>10s} {"task s":>9s} {"scan tasks":>11s}')
        base = None
        for mpb, p in pts:
            if base is None:
                base = p
            print(f'{mpb:>6s} {p["wall"]:8.1f} {p["gpu"]:10.1f} {p["task"]:9.1f} {p["tasks"]:11d}')
        first, last = pts[0][1], pts[-1][1]
        for lab, k in (("wall", "wall"), ("gpuTime", "gpu"), ("task time", "task")):
            d = (last[k] - first[k]) / first[k] * 100
            print(f'   {lab:10s} {pts[0][0]} -> {pts[-1][0]}: {d:+.1f}%'
                  f'{"   BENEFITS from a larger split" if d < -5 else ""}')


if __name__ == "__main__":
    main()
