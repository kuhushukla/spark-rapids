#!/usr/bin/env python3
# FULL-SPEC shuffle-partitions rule — sibling of partition_heuristic.py, which stays as the record of
# what produced results/SHUFFLE-PARTITIONS-RESULTS-20260810.md.
#
#   partition_heuristic.py  = the SIMPLIFIED rule that was actually tested on 2026-08-10/11:
#                             parts = max(cores, ceil(max_e dataSize / T)), max over INDIVIDUAL
#                             exchanges, no gates, no downward-only clause, aggregated over iterations.
#   partition_rule_full.py  = THIS FILE: the rule exactly as specified in
#                             SHUFFLE-PARTITIONS-TEST-PROPOSAL-20260807.md lines 24-96, plus the
#                             expand-on-spill extension, evaluated PER QUERY RUN.
#
# ---------------------------------------------------------------------------------------------
# PER-RUN, NOT AGGREGATED
#
# The proposal's `max` is over the STAGES of one run. A 5-iteration benchmark contains 5 separate
# executions of those stages, each with its own plan, its own stage IDs and its own accumulators —
# so the rule is evaluated independently for each iteration and applied ROLLING: iteration N runs on
# what iteration N-1 measured, the same way the scan-split autotuner learns from the previous run
# (the heuristic reads the most recent observation). Nothing is averaged or maxed across iterations, so one elevated
# iteration cannot silently set the answer. `--apply` selects which iteration's decision is reported
# as the one to use next (default: the last warm one = steady state).
#
# ---------------------------------------------------------------------------------------------
# THE RULE (shrink-only core, proposal lines 24-96 — implemented in full, unmodified), per run:
#
#   T      = spark.rapids.sql.batchSizeBytes            (1 GiB default, RapidsConf:675)
#   alpha  = 1.0 on GPU ("data size" is measured uncompressed; proposal 98-102, 152-170)
#   slots  = target_gpu_executors * cores_per_executor
#   current_effective = max(shuffle.partitions, adaptive.coalescePartitions.initialPartitionNum)
#
#   for each shuffle-consuming stage s of THIS run:
#       input[s]    = alpha * SUM(dataSize of every exchange feeding s)   <- SUM, so joins are handled
#       required[s] = ceil(input[s] / T)
#   size_requirement    = max_s required[s]
#   raw_suggestion      = max(slots, size_requirement)
#   downward_suggestion = ceil(raw_suggestion / slots) * slots            <- whole execution waves
#
#   KEEP (do nothing) if ANY of:
#       downward_suggestion >= current_effective        (the rule is downward-only)
#       required exchange/stage metrics are missing
#       the application had an OOM or a failed stage
#       any AFFECTED stage spilled  (affected = observed_task_count[s] > downward_suggestion,
#                                    i.e. only stages that would actually get fewer, larger tasks)
#   else apply: new_shuffle = min(current_shuffle, downward_suggestion)
#               new_initial = min(current_initial, downward_suggestion)
#
# ---------------------------------------------------------------------------------------------
# EXPANSION (extension — NOT in the proposal). Two terms, then the MAX of them.
#
#   1. SPILL      -> 2 x current_effective, but ONLY if reduce-side GPU spill exceeds spill_tol
#                    (default 50 GiB) AND there is no shuffle skew. Skew disqualifies it because
#                    doubling the partition count cannot split a single hot key — the skewed
#                    partition stays whole and keeps spilling.
#   2. ColumnarExchange -> ceil(max dataSize over GpuColumnarExchange nodes / batchSizeBytes).
#                    Node name verified against the event logs: plans expose 'GpuColumnarExchange'
#                    (GPU shuffle write) and plain 'Exchange'; this term uses the former only.
#
#   expansion = max(applicable terms), rounded up to whole waves; applied only when it exceeds
#   current_effective. Expansion is evaluated BEFORE the shrink path (memory/size safety first).
#
# A third term — shuffle-stage OOM -> 2x — is specified but NOT implemented here: it is a profiler
# signal. Verified absent from the event log: gpuRetryCount / gpuSplitAndRetryCount
# (GpuTaskMetrics.scala:385-386) do not appear in pv03g-shuf-16 (283 GiB spill), pv03g-off-4g or
# cs01-cov2, and neither does GpuSplitAndRetryOOM / OutOfMemoryError. It would be inert here.
#
# SKEW: max/median of per-task shuffle-READ bytes across a reduce stage, threshold --skew-factor
# (default 2.0). There was no pre-existing skew metric in the report generators to reuse.
#
# SCOPE NOTE (measured over all 99 on-disk arms, 2026-08-12): only REDUCE-side spill is actionable —
# the partition count does not size scan tasks. Two distinct spill populations exist:
#   * SCAN/map spill, driven by the SPLIT, not by partitions (pv03g OFF sweep: 0 / 0 / 6.4 / 25.3 /
#     33.1 GiB at 256m/512m/1g/2g/4g). Reported, never acted on here.
#   * REDUCE-side spill, driven by forcing partitions BELOW what the sizing block requires
#     (pv03g: 50 parts -> 32.0 GiB, 16 parts -> 283.0 GiB, both against a size_requirement of ~145).
#     This is what expand-on-spill exists for, and it is the same boundary the shrink path computes —
#     i.e. the rule's own suggestion sits just above where reduce tasks stop fitting a GPU batch.
# (An earlier note here claimed reduce-side spill was zero everywhere; that was based on a handful of
# arms and is wrong — it is zero in every run that respects the rule, and large in ones forced below it.)
#
# SPILL METRIC: gpuSpillToHostBytes + gpuSpillToDiskBytes (GpuTaskMetrics.scala:566, registered as
# named accumulators at :393-394). Spark's "Memory/Disk Bytes Spilled" carries the same numbers —
# TrampolineUtil.incTaskMetricsMemoryBytesSpilled increments both in one call (TrampolineUtil.scala:
# 117-121, disk :129-137) — so the two agree by construction.
#
# Usage: partition_rule_full.py <arm-dir-or-eventlog> [--cores N] [--slots N] [--batch 1g]
#          [--include-cold] [--current-shuffle N] [--current-initial N] [--apply last|first]
#          [--grow-factor 1.5] [--spill-tol-bytes 0] [--json]
import sys, json, glob, math, argparse, re, collections, statistics

G = 2**30
_BYTES_IN_PARENS = re.compile(r'\(\s*([0-9]+)\s*bytes\s*\)')


def tobytes(s, default=G):
    """Parse a size string like '1g' / '512m' / '1073741824'."""
    if not s:
        return default
    s = str(s).strip().lower()
    try:
        if s[-1] in 'kmgt':
            return int(float(s[:-1]) * {'k': 2**10, 'm': 2**20, 'g': G, 't': 2**40}[s[-1]])
        return int(s)
    except Exception:
        return default


def accum_val(x):
    """Event-log accumulable values come in three shapes:
         plain number                      -> SQLMetric (e.g. "data size")
         '1.80GB (1932411008 bytes)'       -> RAPIDS SizeInBytesAccumulator (gpuSpillToHostBytes)
         '00:00:02.245'                    -> RAPIDS NanoSecondAccumulator
       Returns a float; 0.0 when unparseable. A bare float() silently yields 0 on the second form."""
    if isinstance(x, (int, float)):
        return float(x)
    if not isinstance(x, str):
        return 0.0
    m = _BYTES_IN_PARENS.search(x)
    if m:
        return float(m.group(1))
    if ':' in x:
        try:
            h, mi, rest = x.split(':')
            return int(h) * 3600 + int(mi) * 60 + float(rest)
        except Exception:
            return 0.0
    try:
        return float(x)
    except Exception:
        return 0.0


def is_exchange(node):
    """A shuffle exchange = a plan node exposing the 'data size' metric
    (Exchange / GpuColumnarExchange / TakeOrderedAndProject)."""
    return any((m.get("name") == "data size") for m in (node.get("metrics", []) or []))


def find_regions(plan):
    """Split one physical plan into shuffle-consuming stages ('regions').

    A region is the slab of plan between one shuffle boundary and the next. The exchanges FEEDING a
    region are those reachable from it without crossing another exchange — so a join's two sides land
    in the SAME region and get summed, which is what the proposal's 'sum all exchanges entering a
    stage' clause requires (proposal 44-53).

    Returns: list of regions, each a list of exchange plan-nodes feeding it."""
    regions = []

    def walk(node, acc):
        for c in node.get("children", []) or []:
            if is_exchange(c):
                acc.append(c)          # c feeds the CURRENT region
                sub = []
                walk(c, sub)           # below c is a DIFFERENT region
                regions.append(sub)
            else:
                walk(c, acc)

    top = []
    walk(plan, top)
    regions.append(top)
    return [r for r in regions if r]


def ds_accids(node):
    """The 'data size' accumulatorIds on one exchange node."""
    return [m.get("accumulatorId") for m in (node.get("metrics", []) or [])
            if m.get("name") == "data size" and m.get("accumulatorId") is not None]


def ordict(d):
    return "none" if not d else ", ".join(f"stage {s}: {v/G:.2f} GiB" for s, v in sorted(d.items()))


def decide_one(inputs, stages, stage_tasks, stage_kind, stage_spill, stage_reads,
               ce_max, bsb, slots, cur_shuffle, cur_initial, unhealthy_note,
               spill_tol, skew_factor):
    """The rule, for ONE query run. `inputs` = list of (region#, summed dataSize, #exchanges);
    `ce_max` = max dataSize over this run's GpuColumnarExchange nodes."""
    cur_effective = max(cur_shuffle, cur_initial)
    missing = (not inputs) or all(t <= 0 for _, t, _ in inputs)
    size_requirement = max((math.ceil(t / bsb) if bsb else 0) for _, t, _ in inputs) if inputs else 0
    raw = max(slots, size_requirement)
    downward = math.ceil(raw / slots) * slots if slots else raw

    reduce_stages = [s for s in stages if stage_kind.get(s) == "reduce"]
    scan_stages = [s for s in stages if stage_kind.get(s) == "scan/map"]
    affected = [s for s in reduce_stages if stage_tasks[s] > downward]
    affected_spill = {s: stage_spill[s] for s in affected if stage_spill[s] > spill_tol}
    reduce_spill = {s: stage_spill[s] for s in reduce_stages if stage_spill[s] > spill_tol}
    scan_spill = {s: stage_spill[s] for s in scan_stages if stage_spill[s] > spill_tol}

    # --- shuffle skew: max/median of per-task shuffle-READ bytes, per reduce stage ---------------
    skew = 0.0
    for s in reduce_stages:
        v = sorted(stage_reads.get(s) or [])
        if len(v) >= 2:
            med = statistics.median(v)
            if med > 0:
                skew = max(skew, max(v) / med)
    skewed = skew > skew_factor

    # --- expansion terms, then the MAX of them ---------------------------------------------------
    terms = {}
    if reduce_spill and not skewed:
        terms["spill_2x"] = cur_effective * 2
    if ce_max > 0 and bsb:
        terms["columnar_exchange"] = math.ceil(ce_max / bsb)
    expand_raw = max(terms.values()) if terms else 0
    expanded = math.ceil(expand_raw / slots) * slots if (slots and expand_raw) else expand_raw

    if expanded > cur_effective:                     # expansion wins (memory/size safety first)
        action = "EXPAND"
        new_shuffle, new_initial = max(cur_shuffle, expanded), max(cur_initial, expanded)
        why = ", ".join(f"{k}={v}" for k, v in sorted(terms.items()))
        skewnote = f"; skew={skew:.2f} (>{skew_factor} would disable spill_2x)" if reduce_spill else ""
        reason = (f"max({why}) = {expand_raw} -> {expanded} > current_effective {cur_effective}"
                  f"{skewnote}")
    elif missing:
        action, new_shuffle, new_initial = "KEEP", cur_shuffle, cur_initial
        reason = "required exchange/stage metrics missing (no dataSize recorded)"
    elif unhealthy_note:
        action, new_shuffle, new_initial = "KEEP", cur_shuffle, cur_initial
        reason = f"application unhealthy: {unhealthy_note}"
    elif downward >= cur_effective:
        action, new_shuffle, new_initial = "KEEP", cur_shuffle, cur_initial
        reason = f"downward-only: {downward} >= current_effective {cur_effective}"
    elif affected_spill:
        action, new_shuffle, new_initial = "KEEP", cur_shuffle, cur_initial
        reason = (f"affected stage(s) {sorted(affected_spill)} already spill "
                  f"({max(affected_spill.values())/G:.2f} GiB) — do not enlarge their tasks")
    else:
        action = "SHRINK"
        new_shuffle, new_initial = min(cur_shuffle, downward), min(cur_initial, downward)
        reason = f"{downward} < current_effective {cur_effective}, all gates pass"

    return dict(size_requirement=size_requirement, raw_suggestion=raw, downward_suggestion=downward,
                action=action, reason=reason, new_shuffle=new_shuffle, new_initial=new_initial,
                expand_terms=terms, expand_target=expanded, skew=round(skew, 2),
                ce_max_gib=round(ce_max / G, 2),
                reduce_stages=sorted(reduce_stages), affected_stages=sorted(affected),
                reduce_spill_gib={int(s): round(v/G, 3) for s, v in reduce_spill.items()},
                scan_spill_gib={int(s): round(v/G, 3) for s, v in scan_spill.items()},
                reducers={int(s): stage_tasks[s] for s in sorted(reduce_stages)})


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("arm")
    ap.add_argument("--cores", type=int, default=None)
    ap.add_argument("--slots", type=int, default=None, help="executors*cores_per_executor (else=cores)")
    ap.add_argument("--batch", default=None, help="override batchSizeBytes (else from event-log env)")
    ap.add_argument("--include-cold", action="store_true",
                    help="include iter1 cold-start (default: warm only, iter1 dropped)")
    ap.add_argument("--current-shuffle", type=int, default=None)
    ap.add_argument("--current-initial", type=int, default=None)
    ap.add_argument("--apply", choices=("last", "first"), default="last",
                    help="which run's decision to report as the one to apply next (default: last warm)")
    ap.add_argument("--spill-tol-bytes", type=int, default=50 * G,
                    help="reduce-side GPU spill tolerated before the spill_2x term applies "
                         "(default 50 GiB)")
    ap.add_argument("--skew-factor", type=float, default=2.0,
                    help="max/median of per-task shuffle-READ bytes above which a reduce stage counts "
                         "as skewed, disabling spill_2x (doubling partitions cannot split a hot key). "
                         "Measured skew on the clickstream/pageviews arms is 1.00-1.01.")
    ap.add_argument("--json", action="store_true", help="machine-readable output (for the runner)")
    a = ap.parse_args()

    el = a.arm
    if '/el/' not in el:
        els = [e for e in glob.glob(f"{a.arm.rstrip('/')}/el/local-*") if 'inprogress' not in e]
        if not els:
            sys.exit(f"no event log under {a.arm}")
        el = els[0]

    bs = master = set_parts = set_initial = aqe_enabled = aqe_coalesce = None
    starts, plans = {}, {}
    accval = collections.defaultdict(float)
    stage_tasks = collections.Counter()
    stage_kind, stage_exec = {}, {}
    stage_spill = collections.defaultdict(float)
    stage_reads = collections.defaultdict(list)
    exec_failures = collections.Counter()
    exec_failed_stages = collections.defaultdict(list)
    exec_oom = collections.defaultdict(bool)

    for ln in open(el, errors='ignore'):
        if 'SparkListener' not in ln:
            continue
        try:
            e = json.loads(ln)
        except Exception:
            continue
        ev = e.get("Event", "")

        if ev.endswith("EnvironmentUpdate"):
            sp = e.get("Spark Properties", {})
            sp = sp if isinstance(sp, dict) else dict(sp)
            bs = sp.get("spark.rapids.sql.batchSizeBytes")
            master = sp.get("spark.master")
            set_parts = sp.get("spark.sql.shuffle.partitions")
            set_initial = sp.get("spark.sql.adaptive.coalescePartitions.initialPartitionNum")
            aqe_enabled = sp.get("spark.sql.adaptive.enabled")
            aqe_coalesce = sp.get("spark.sql.adaptive.coalescePartitions.enabled")

        elif ev.endswith("SQLExecutionStart"):
            starts[e.get("executionId")] = e.get("time")
            if e.get("sparkPlanInfo"):
                plans.setdefault(e.get("executionId"), e["sparkPlanInfo"])

        elif ev.endswith("SQLAdaptiveExecutionUpdate"):
            if e.get("sparkPlanInfo"):               # last update = final post-AQE plan
                plans[e.get("executionId")] = e["sparkPlanInfo"]

        elif ev == "SparkListenerJobStart":
            # stage -> SQL execution attribution, so every stage fact is scoped to ONE query run
            props = e.get("Properties", {}) or {}
            xid = props.get("spark.sql.execution.id")
            if xid is not None:
                try:
                    xid = int(xid)
                except Exception:
                    pass
                for sid in (e.get("Stage IDs") or []):
                    stage_exec[sid] = xid
                for si in (e.get("Stage Infos") or []):
                    if si.get("Stage ID") is not None:
                        stage_exec[si["Stage ID"]] = xid

        elif ev == "SparkListenerStageCompleted":
            si = e.get("Stage Info", {}) or {}
            if si.get("Failure Reason"):
                exec_failed_stages[stage_exec.get(si.get("Stage ID"))].append(si.get("Stage ID"))

        elif ev == "SparkListenerTaskEnd":
            sid = e.get("Stage ID")
            ti = e.get("Task Info", {}) or {}
            tm = e.get("Task Metrics", {}) or {}
            reason = (e.get("Task End Reason", {}) or {}).get("Reason", "")
            if reason not in ("Success", ""):
                exec_failures[stage_exec.get(sid)] += 1
                blob = json.dumps(e.get("Task End Reason", {}) or {})
                if "OutOfMemory" in blob or "OOM" in blob:
                    exec_oom[stage_exec.get(sid)] = True

            inp = (tm.get("Input Metrics", {}) or {}).get("Bytes Read", 0) or 0
            srm = tm.get("Shuffle Read Metrics", {}) or {}
            sread = (srm.get("Local Bytes Read", 0) or 0) + (srm.get("Remote Bytes Read", 0) or 0)
            stage_tasks[sid] += 1
            kind = "scan/map" if inp > 0 else ("reduce" if sread > 0 else "other")
            if sid not in stage_kind or stage_kind[sid] == "other":
                stage_kind[sid] = kind
            if kind == "reduce":
                stage_reads[sid].append(sread)       # for the skew test

            for ac in ti.get("Accumulables", []) or []:
                nm = ac.get("Name", "") or ""
                if nm in ("gpuSpillToHostBytes", "gpuSpillToDiskBytes"):
                    stage_spill[sid] += accum_val(ac.get("Update", 0))
                else:
                    aid = ac.get("ID")
                    if aid is not None:
                        accval[aid] += accum_val(ac.get("Update", 0))

    execs = sorted([i for i in plans if i in starts], key=lambda i: starts[i])
    warm = execs if a.include_cold else (execs[1:] if len(execs) > 1 else execs)
    if not warm:
        sys.exit(f"no SQL executions with a plan in {el}")

    bsb = tobytes(a.batch if a.batch else bs)
    cores = a.cores
    if cores is None:
        m = re.search(r'local\[(\d+)\]', master or '')
        cores = int(m.group(1)) if m else 16
    slots = a.slots or cores
    cur_shuffle = a.current_shuffle if a.current_shuffle is not None else int(set_parts or 200)
    cur_initial = (a.current_initial if a.current_initial is not None
                   else (int(set_initial) if set_initial else cur_shuffle))

    # ---- one decision per query run --------------------------------------------------------------
    per_run = []
    legacy_max_e = 0.0
    for i in warm:
        inputs = []
        for ri, region in enumerate(find_regions(plans[i])):
            total = 0.0
            for ex in region:
                for aid in ds_accids(ex):
                    v = accval.get(aid, 0.0)
                    total += v
                    legacy_max_e = max(legacy_max_e, v)
            inputs.append((ri, total, len(region)))     # alpha = 1.0 on GPU
        # ColumnarExchange term input: MAX dataSize over this run's GpuColumnarExchange nodes only
        # (node names verified in the logs: 'GpuColumnarExchange' = GPU shuffle write, vs plain
        # 'Exchange' = CPU). Not summed — the term is per-node by specification.
        ce_max = 0.0

        def ce_walk(n):
            nonlocal ce_max
            if (n.get("nodeName") or "").strip() == "GpuColumnarExchange":
                for aid in ds_accids(n):
                    ce_max = max(ce_max, accval.get(aid, 0.0))
            for c in n.get("children") or []:
                ce_walk(c)
        ce_walk(plans[i])

        stages = [s for s, x in stage_exec.items() if x == i]
        note = ""
        if exec_failed_stages.get(i) or exec_oom.get(i):
            note = (f"failed_stages={exec_failed_stages.get(i, [])} oom={exec_oom.get(i, False)} "
                    f"task_failures={exec_failures.get(i, 0)}")
        d = decide_one(inputs, stages, stage_tasks, stage_kind, stage_spill, stage_reads,
                       ce_max, bsb, slots, cur_shuffle, cur_initial, note,
                       a.spill_tol_bytes, a.skew_factor)
        d["exec_id"] = i
        d["inputs_gib"] = [round(t / G, 2) for _, t, _ in inputs]
        per_run.append(d)

    chosen = per_run[-1] if a.apply == "last" else per_run[0]

    out = dict(event_log=el, batch_size_bytes=bsb, cores=cores, slots=slots,
               current_shuffle=cur_shuffle, current_initial=cur_initial,
               warm_runs=len(warm), all_runs=len(execs), per_run=per_run, apply=a.apply,
               new_shuffle=chosen["new_shuffle"], new_initial=chosen["new_initial"],
               action=chosen["action"], reason=chosen["reason"],
               legacy_max_e_gib=round(legacy_max_e / G, 3),
               aqe_enabled=aqe_enabled, aqe_coalesce=aqe_coalesce)

    if a.json:
        print(json.dumps(out, indent=2))
        return

    print(f"  event log         : {el}")
    print(f"  batchSizeBytes(T) : {bsb/G:.2f} GiB   cores={cores}  slots={slots}  alpha=1.0")
    print(f"  AQE               : enabled={aqe_enabled or '(default true)'}  "
          f"coalesce={aqe_coalesce or '(default true)'}")
    print(f"  current           : shuffle.partitions={cur_shuffle}  initialPartitionNum="
          f"{set_initial or '(unset -> inherits)'}  -> current_effective="
          f"{max(cur_shuffle, cur_initial)}")
    print(f"  runs              : {len(execs)} total, {len(warm)} warm "
          f"({'incl cold' if a.include_cold else 'iter1 dropped'})")
    print("  --- PER-RUN decision (the rule is evaluated independently for each query run) ---")
    for d in per_run:
        print(f"    run exec={d['exec_id']}: stage inputs={d['inputs_gib']} GiB "
              f"-> size_req={d['size_requirement']} raw={d['raw_suggestion']} "
              f"downward={d['downward_suggestion']}")
        print(f"        reducers={d['reducers']}  affected={d['affected_stages']}  "
              f"reduce_spill={d['reduce_spill_gib'] or 'none'}  "
              f"scan_spill={d['scan_spill_gib'] or 'none'}")
        print(f"        expand terms={d['expand_terms'] or 'none'} -> {d['expand_target']}  "
              f"ce_max={d['ce_max_gib']} GiB  skew={d['skew']}")
        print(f"        {d['action']}: {d['reason']}  -> shuffle={d['new_shuffle']} "
              f"initial={d['new_initial']}")
    print(f"  APPLY NEXT ({a.apply} warm run): {chosen['action']} -> "
          f"spark.sql.shuffle.partitions={chosen['new_shuffle']}, "
          f"initialPartitionNum={chosen['new_initial']}")
    print(f"  [reconcile] legacy max_e dataSize = {legacy_max_e/G:.2f} GiB -> legacy parts = "
          f"{max(slots, math.ceil(legacy_max_e/bsb) if bsb else 0)}  "
          f"(the simplified rule in partition_heuristic.py)")


if __name__ == '__main__':
    main()
