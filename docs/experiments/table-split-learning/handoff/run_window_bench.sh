#!/usr/bin/env bash
# Data-window split learning. Everything is defined in windows.yaml; nothing is passed as a predicate
# on the command line, so the sweep and the learning runs always use identical data.
#
#   bash run_window_bench.sh check          precheck only
#   bash run_window_bench.sh sweep          per-job split sweep -> each job's own optimum
#   bash run_window_bench.sh learn          per-sequence off/shared/iso arms
#   bash run_window_bench.sh refine         sweep the splits the learning arms actually chose
#   bash run_window_bench.sh report         build ledger + report
#   bash run_window_bench.sh all            check, sweep, learn, refine, report
#
# Mechanism (GpuFileSourceScanExec.scala): label = rootPaths.headOption (585-587, unpruned) while
# listedBytes = dynamicallySelectedPartitions (588, post-pruning). All windows share one history key.
set -euo pipefail
export JAVA_HOME="${BENCH_JAVA_HOME:-${JAVA_HOME:-/usr/lib/jvm/java-1.17.0-openjdk-amd64}}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
W() { python3 "$HERE/wincfg.py" "$@"; }

MODE="${1:-all}"
TAG="${TAG:-$(date +%Y%m%d)}"
OUTROOT="${OUTROOT:-/data/window-learning-${TAG}}"
DATA="$(W dataset)"; ITERS="${ITERS:-$(W iters)}"; CEILING="$(W ceiling)"; MPB="$(W mpb)"
KIT="$(cd "$HERE/../../rolling-split-autotuning/handoff" && pwd)"
. "$KIT/bench_common.sh"
BENCH="$KIT/bench_clickstream.scala"
JAR="${RAPIDS_JAR:-/home/kuhu/Reps/spark-rapids/dist/rapids-353-local-keep.jar}"
SPARK_HOME="${TSL_SPARK_HOME:-/home/kuhu/Downloads/spark-3.5.3-bin-hadoop3}"; export SPARK_HOME
GPU_UUID="${GPU_UUID:-GPU-1aaa66fd-0c1e-935b-fe65-2c9ca7357504}"
LOCALDIR="${LOCALDIR:-/data/sparklocal-win-${TAG}}"
HIST="$OUTROOT/hist"; MANIFEST="$OUTROOT/manifest.json"

# One run at a time per GPU. precheck is a one-shot check, so two concurrent invocations both pass it
# and then collide on the same device; the loser dies with "pool allocation ... was less than
# allocation of". Set BENCH_NOLOCK=1 to opt out (e.g. runs pinned to different GPUs).
LOCK="${BENCH_LOCK:-/tmp/window-bench-${GPU_UUID##*-}.lock}"
if [ -z "${BENCH_NOLOCK:-}" ] && command -v flock >/dev/null 2>&1; then
  exec 9>"$LOCK"
  flock -n 9 || { echo "!! another run holds $LOCK - refusing to start (BENCH_NOLOCK=1 to override)"; exit 3; }
fi

mkdir -p "$OUTROOT" "$HIST" "$LOCALDIR"
[ -f "$MANIFEST.tmp" ] || : > "$MANIFEST.tmp"

manifest_add() {  # arm ceiling history_mode query history_file window
  printf '{"arm":"%s","ceiling":"%s","history_mode":"%s","query":"%s","history_file":"%s","window":"%s","dataset":"clickstream-win","backend":"local"}\n' \
    "$1" "$2" "$3" "$4" "$5" "$6" >> "$MANIFEST.tmp"
  python3 -c 'import json,sys
rows=[json.loads(l) for l in open(sys.argv[1]) if l.strip()]
# de-duplicate by arm, keeping the LAST entry: re-running an arm appends a second line, and the
# ledger would then parse that arm twice and pool the same iterations as if they were new samples.
seen={}
for r in rows: seen[r["arm"]]=r
rows=list(seen.values())
json.dump({"tag":sys.argv[2],"dataset":"clickstream-win","iters":int(sys.argv[3]),"arms":rows},
          open(sys.argv[4],"w"), indent=1)' "$MANIFEST.tmp" "$TAG" "$ITERS" "$MANIFEST"
}

# arm query window ceiling(off|core1) history_basename history_mode mpb
run_step() {
  local arm="$1" q="$2" win="$3" ceiling="$4" hf="$5" mode="$6" mpb="${7:-$MPB}"
  local out="$OUTROOT/$arm" pred; pred="$(W pred "$win")"
  # FORCE_ARMS="pat1 pat2" re-runs matching arms even if they look complete (e.g. an arm that
  # "succeeded" while the GPU was contended and therefore produced a meaningless result).
  local forced=0
  for pat in ${FORCE_ARMS:-}; do case "$arm" in *$pat*) forced=1;; esac; done
  [ $forced -eq 0 ] && [ -s "$out/eventlog-test-1" ] && { echo "  skip (done) $arm"; return 0; }
  local xc="" xj="" cc
  if [ "$ceiling" != off ]; then
    cc="$(ceiling_conf "$ceiling")" || return 1
    xc="$(history_conf "$HIST/${hf}.snapshot") $cc"
  fi
  echo "  $arm  q=$q win=$win mpb=$mpb"
  WHERE="$pred" LINK_EVENTLOG=yes spark_arm "$out" "$q" "$ITERS" "$mpb" "$xc" "$xj" || return 1
  manifest_add "$arm" "$ceiling" "$mode" "$q" "$hf" "$win"
}

do_sweep() {
  echo "== SWEEP (autotuner off; each job's own optimum)"
  while read -r q win; do
    for g in $(W grid); do run_step "sweep-$q@$win-$g" "$q" "$win" off "" none "$g"; done
  done < <(W sweep_jobs)
}

# ARMS=all (default) runs off/shared/iso. ARMS=shared runs only the two shared arms: shared-1 gives
# the source's own learnt value against an EMPTY history, which is what iso measures (verified equal
# on all 6 measurable jobs of the 20260824 run), and the 2g grid point is the config off measures.
do_learn() {
  echo "== LEARN (arms=${ARMS:-all})"
  while read -r name q1 w1 q2 w2 tests; do
    echo "-- $name  [$tests]"
    if [ "${ARMS:-all}" = all ]; then
      run_step "$name-off-1"  "$q1" "$w1" off ""              none
      run_step "$name-off-2"  "$q2" "$w2" off ""              none
    fi
    run_step "$name-shared-1" "$q1" "$w1" "$CEILING" "$name-shared" shared
    run_step "$name-shared-2" "$q2" "$w2" "$CEILING" "$name-shared" shared
    if [ "${ARMS:-all}" = all ]; then
      run_step "$name-iso-1"  "$q1" "$w1" "$CEILING" "$name-iso-1" isolated
      run_step "$name-iso-2"  "$q2" "$w2" "$CEILING" "$name-iso-2" isolated
    fi
  done < <(W sequences)
}

LEDGER="$HERE/../results/ledger-$(basename "$OUTROOT").tsv"

# The inherited split lives in ONE cold iteration, which cannot be compared to a warm median. So
# re-run each split the learning arms chose as a real sweep point and get warm medians at it.
do_refine() {
  echo "== REFINE (sweep the splits the learning arms chose)"
  # stderr is NOT suppressed: hiding it here once turned a SyntaxError into "no candidates",
  # which reads as a clean result. A missing ledger on the first pass is the only tolerated case.
  python3 "$HERE/build_ledger.py" "$OUTROOT" "$LEDGER" >/dev/null || {
    echo "  !! build_ledger failed; refine has nothing to sweep (see the error above)"; return 1; }
  # An optimum re-measure must be a NEW arm, or it is skipped as already-done and measures nothing.
  # Timestamping it also keeps the older measurement on disk; the report picks the newest by mtime.
  local stamp; stamp="r$(date +%H%M)"
  python3 "$HERE/learnt_splits.py" "$LEDGER" | while read -r q w sp role; do
    local nm="sweep-$q@$w-${sp}m"
    [ "$role" = "optimum-remeasure" ] && nm="sweep-$q@$w-${sp}m-$stamp"
    echo "  candidate $q@$w ${sp}m ($role) -> $nm"
    run_step "$nm" "$q" "$w" off "" none "${sp}m"
  done
}

do_report() {
  python3 "$HERE/build_ledger.py" "$OUTROOT" "$LEDGER"
  python3 "$HERE/gen_window_report.py" --ledger "$LEDGER" --outroot "$OUTROOT"
}

case "$MODE" in
  check)  bash "$HERE/precheck.sh" ;;
  sweep)  bash "$HERE/precheck.sh" && do_sweep ;;
  learn)  bash "$HERE/precheck.sh" && do_learn ;;
  refine) do_refine ;;
  report) do_report ;;
  all)    bash "$HERE/precheck.sh" && do_sweep && do_learn && do_refine && do_report ;;
  *) echo "usage: $0 {check|sweep|learn|refine|report|all}"; exit 2 ;;
esac
echo "DONE $(date +%H:%M:%S) -> $OUTROOT"
