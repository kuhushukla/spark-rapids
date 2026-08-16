#!/usr/bin/env bash
# Scan-split benchmark runner (any dataset: clickstream, pageviews, ...).
# Per query: GPU-fallback smoke check -> OFF split sweep -> split autotuner (strategy=ratio,
# ratioBasis=listed, ceiling=core1). 16 cores, event logs per arm. Spark scratch on --localdir.
#
# Usage: run_scan_bench.sh [--queries="csH3 csH cs03"] [--iters=3] [--data=PARQUET] [--jar=JAR]
#          [--bench=FILE] [--out=DIR] [--spark-home=PATH] [--gpu=UUID] [--sweep="256m 512m 1g 2g 4g"]
#          [--ceiling=core1] [--localdir=/data/_sparklocal-cs]
#   iters=3 => iter1 cold + 2 warm (parse warm = iters 2..N).
# Multi-dataset in ONE invocation: repeat --job='QUERIES|DATA|BENCH' (runs sequentially, shared --out). e.g.
#   run_scan_bench.sh --out=DIR \
#     --job='cs03 csH csH3|/data/wiki-clickstream/parquet|bench_clickstream.scala' \
#     --job='pv03g|/data/wiki-pageviews/parquet|bench_pageviews.scala' \
#     --job='gf1 hs3 rw7|/data/overture|bench_overture.scala'
# Without --job it falls back to the single --queries/--data/--bench.
#
# BUILD the --jar from repo root (JAVA_HOME=java-17, MAVEN_OPTS=-Xmx6g):
#   mvn -Dbuildver=353 -DskipTests -Dmaven.test.skip=true clean package -pl dist -am
#   -> dist/target/rapids-4-spark_2.12-*-cuda12.jar
set -uo pipefail
# Self-locating; no dependency on any source checkout. The plugin jar comes from --jar or $RAPIDS_JAR.
HANDOFF="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
QUERIES="csH3 csH cs03"
ITERS=3
DATA=/data/wiki-clickstream/parquet
JAR="${RAPIDS_JAR:-}"
BENCH="$HANDOFF/bench_clickstream.scala"
OUT=/data/scan-split-out
SPARK_HOME=/home/kuhu/Downloads/spark-3.5.3-bin-hadoop3
GPU=GPU-1aaa66fd-0c1e-935b-fe65-2c9ca7357504   # A5000 (never the T400)
SWEEP="256m 512m 1g 2g 4g"
CEILING=core1
LOCALDIR=/data/_sparklocal-cs
TARGET=""    # when set, adds --conf spark.rapids.sql.batchSizeBytes=$TARGET to every arm (also drives the autotuner target)
ONLY=all     # which stages to run: all | smoke | off | ratio | parts
XCONF=""     # extra --conf(s) appended to EVERY arm (e.g. "--conf spark.rapids.sql.reader.chunked=false")
AQE=on       # spark.sql.adaptive.enabled for every arm (on|off) -- the AQE-coalesce A/B
PLAN=no      # --plan: print the arm matrix + each arm's baseline provenance, then exit WITHOUT running
# --off-iters: minimum iterations an OFF-sweep arm must have to COUNT as a baseline candidate.
# Defaults to $ITERS. Lower it ONLY to reuse an older, shorter sweep; the optimum still comes from the
# sweep, this just relaxes the completeness bar. 3-iteration sweeps are exactly what produced the
# documented fake pv03g optimum (1g "121.7s", corrected to 151.1s by the 5-iter rerun), so when this
# is below $ITERS the stage prints a WARNING that must be carried into the report.
OFFITERS=""
# NOTE: there is deliberately NO knob for the `parts` stage baseline split. The doc's pipeline is
# smoke -> OFF split sweep -> ratio, and the OFF sweep's per-query OPTIMUM is the baseline every
# comparison uses (gen_ratio_report.py is "ratio-vs-OFF-optimum"). A hand-picked baseline (this
# script briefly had --pbase-mpb=128m) silently compares against an unswept point and flatters the
# autotuner. off_optimum() below derives it from the sweep, and the stage ERRORS if the sweep is absent.
NSYS=no      # when yes, wrap each arm's spark-shell in nsys (cuda,nvtx) -> $OUTD/prof.nsys-rep
JOBS=()   # each: "QUERIES|DATA|BENCH" (repeatable --job). Runs all datasets in one invocation.
for a in "$@"; do case "$a" in
  --job=*)        JOBS+=("${a#*=}") ;;
  --queries=*)    QUERIES="${a#*=}" ;;
  --iters=*)      ITERS="${a#*=}" ;;
  --data=*)       DATA="${a#*=}" ;;
  --jar=*)        JAR="${a#*=}" ;;
  --bench=*)      BENCH="${a#*=}" ;;
  --out=*)        OUT="${a#*=}" ;;
  --spark-home=*) SPARK_HOME="${a#*=}" ;;
  --gpu=*)        GPU="${a#*=}" ;;
  --sweep=*)      SWEEP="${a#*=}" ;;
  --ceiling=*)    CEILING="${a#*=}" ;;
  --localdir=*)   LOCALDIR="${a#*=}" ;;
  --target=*)     TARGET="${a#*=}" ;;
  --only=*)       ONLY="${a#*=}" ;;
  --xconf=*)      XCONF="${a#*=}" ;;
  --aqe=*)        AQE="${a#*=}" ;;
  --off-iters=*)  OFFITERS="${a#*=}" ;;
  --plan)         PLAN=yes ;;
  --nsys)         NSYS=yes ;;
  --nsys-cpu)     NSYS=cpu ;;
  -h|--help)      sed -n '2,14p' "$0"; exit 0 ;;
  *) echo "unknown arg: $a"; exit 2 ;;
esac; done
export JAVA_HOME=/usr/lib/jvm/java-1.17.0-openjdk-amd64
export CUDA_VISIBLE_DEVICES="$GPU"
export HANDOFF_DIR="$HANDOFF"   # off_optimum() loads gen_ratio_report.py from here
[ -z "$JAR" ] && { echo "!! no plugin jar: pass --jar=PATH or set RAPIDS_JAR"; exit 2; }
[ -f "$JAR" ] || { echo "!! jar not found: $JAR"; exit 2; }
mkdir -p "$LOCALDIR"
[ ${#JOBS[@]} -eq 0 ] && JOBS=("$QUERIES|$DATA|$BENCH")   # default: single dataset from --queries/--data/--bench
TARGETCONF=""; [ -n "$TARGET" ] && TARGETCONF="--conf spark.rapids.sql.batchSizeBytes=$TARGET"
AQECONF="--conf spark.sql.adaptive.enabled=$( [ "$AQE" = off ] && echo false || echo true )"
RULE="$HANDOFF/partition_rule_full.py"
# Partition-count rule applied to an arm's OWN prior run (rolling, exactly as the plugin's scan
# autotuner learns from the previous run). Echoes the partition count to set, or "" to leave alone.
# Only spark.sql.shuffle.partitions is set: the rule says leave initialPartitionNum unset so it
# inherits (SHUFFLE-PARTITIONS-TEST-PROPOSAL-20260807.md lines 92-95).
# NOTE: no backslashes inside the f-string/expression here -- python 3.10 rejects them
# ("f-string expression part cannot include a backslash"), and stderr is NOT suppressed, so a
# failure is visible instead of silently yielding "no decision" and skipping the arm.
# OFF-sweep optimum for one query: the swept maxPartitionBytes with the lowest avg-of-warm wall.
# Reads ONLY completed sweep arms ($OUT/$Q-off-<mpb> with >= $ITERS ITER lines) and reuses
# gen_ratio_report.parse() so the wall definition matches every other report. Prints "<mpb>\t<wall_s>"
# or nothing when the sweep is missing/incomplete -- callers must treat empty as a hard error.
off_optimum () {
  local Q="$1"
  python3 - "$OUT" "$Q" "${OFFITERS:-$ITERS}" "$SWEEP" <<'PY'
import sys, os, glob, importlib.util
out, q, iters, sweep = sys.argv[1], sys.argv[2], int(sys.argv[3]), sys.argv[4].split()
here = os.path.dirname(os.path.abspath(__file__)) if '__file__' in dir() else None
HANDOFF = os.environ.get("HANDOFF_DIR")
spec = importlib.util.spec_from_file_location("gr", os.path.join(HANDOFF, "gen_ratio_report.py"))
gr = importlib.util.module_from_spec(spec); spec.loader.exec_module(gr)
best = None
for m in sweep:
    d = f"{out}/{q}-off-{m}"
    lg = f"{d}/run.log"
    if not os.path.exists(lg):
        continue
    n = sum(1 for ln in open(lg, errors='ignore') if f"ITER {q} " in ln)
    if n < iters:
        continue
    els = [e for e in glob.glob(f"{d}/el/*") if 'inprogress' not in e]
    if not els:
        continue
    try:
        w = gr.parse(d, q)["wall"] / 1000.0
    except Exception:
        continue
    if best is None or w < best[1]:
        best = (m, w)
if best:
    print(f"{best[0]}\t{best[1]:.1f}")
PY
}

derive_parts () {
  local FROM="$1"
  python3 "$RULE" "$FROM" --json \
    | python3 -c '
import json, sys
try:
    d = json.load(sys.stdin)
except Exception as e:
    sys.stderr.write("derive_parts: could not parse rule output: %s\n" % e)
    sys.exit(0)
print("\t".join([str(d["new_shuffle"]), str(d["action"]), str(d["reason"])]))'
}
echo ">> jar=$JAR"; echo ">> gpu=$GPU  iters=$ITERS  sweep=[$SWEEP]  ceiling=$CEILING  target=${TARGET:-default(1g)}  jobs=${#JOBS[@]}"

# one arm.  $1 out-dir  $2 query  $3 iters  $4 maxPartitionBytes  $5 extra-conf  $6 extra-java-opts
run () {
  local OUTD="$1" Q="$2" IT="$3" MPB="$4" XC="$5" XJ="$6"
  # Idempotent skip -- but ONLY when the existing arm was built with the SAME maxPartitionBytes and
  # the same shuffle.partitions. Skipping on iteration count alone silently reuses an arm produced
  # under different config (this bit us: pbase/pparts built at 128m would have been reused as though
  # they were at the swept optimum). `grep -ac` exits 1 at count 0, so `|| echo 0` would emit "0\n0"
  # and break -ge; use a single-line count instead.
  local NDONE; NDONE=$(grep -acE "ITER $Q [0-9]+ [0-9]+" "$OUTD/run.log" 2>/dev/null; true)
  NDONE=${NDONE:-0}
  if [ "${NDONE//[^0-9]/}" -ge "$IT" ] 2>/dev/null; then
    local PREVMPB PREVPARTS WANTPARTS
    PREVMPB=$(grep -ao "spark\.sql\.files\.maxPartitionBytes=[^ ]*" "$OUTD/run.log" 2>/dev/null | head -1 | cut -d= -f2)
    PREVPARTS=$(grep -ao "spark\.sql\.shuffle\.partitions=[0-9]*" "$OUTD/run.log" 2>/dev/null | head -1 | cut -d= -f2)
    WANTPARTS=$(printf '%s' "$XC" | grep -o "spark\.sql\.shuffle\.partitions=[0-9]*" | cut -d= -f2)
    if [ "${PREVMPB:-$MPB}" = "$MPB" ] && [ "${PREVPARTS:-}" = "${WANTPARTS:-}" ]; then
      echo "  skip (done): $OUTD"; return
    fi
    echo "  !! STALE ARM: $OUTD was built with mpb=${PREVMPB:-?} parts=${PREVPARTS:-default}, but this"
    echo "  !! stage wants mpb=$MPB parts=${WANTPARTS:-default}. NOT reusing it. Move it aside and re-run."
    return
  fi
  local avail; avail=$(df -BG --output=avail /data | tail -1 | tr -dc '0-9')
  [ "${avail:-0}" -lt 20 ] && { echo "!! /data < 20G free — abort"; exit 3; }
  mkdir -p "$OUTD/el"
  local NPRE=""
  # -s none = no CPU sampling (default, small traces). --nsys-cpu adds CPU sampling + OS runtime,
  # needed when the GPU is idle most of the wall time and the bottleneck is host-side: measured on
  # cs01, GPU util median 0% and idle >5% for 72-74% of samples, so cuda/nvtx alone cannot see where
  # the time goes. Traces are much larger; use few iterations.
  [ "$NSYS" = yes ] && NPRE="/usr/local/bin/nsys profile -t cuda,nvtx -s none -o $OUTD/prof --force-overwrite true"
  [ "$NSYS" = cpu ] && NPRE="/usr/local/bin/nsys profile -t cuda,nvtx,osrt -s cpu --cpuctxsw=process-tree -o $OUTD/prof --force-overwrite true"
  echo "  run $Q mpb=$MPB iters=$IT nsys=$NSYS -> $OUTD  $(date +%T)"
  $NPRE "$SPARK_HOME/bin/spark-shell" --master 'local[16]' --driver-memory 32G \
    --conf spark.driver.maxResultSize=2GB --conf spark.local.dir="$LOCALDIR" \
    --conf spark.plugins=com.nvidia.spark.SQLPlugin --conf spark.rapids.sql.enabled=true \
    --conf spark.rapids.sql.variableFloatAgg.enabled=false \
    --conf spark.sql.files.maxPartitionBytes="$MPB" $TARGETCONF $XCONF $AQECONF \
    --conf spark.rapids.sql.metrics.level=DEBUG --conf spark.rapids.sql.explain=NONE \
    --conf spark.rapids.filecache.enabled=false --conf spark.rapids.memory.pinnedPool.size=8g \
    --conf spark.rapids.sql.concurrentGpuTasks=2 \
    --conf spark.shuffle.manager=com.nvidia.spark.rapids.spark353.RapidsShuffleManager \
    --conf spark.eventLog.enabled=true --conf "spark.eventLog.dir=file:$OUTD/el" $XC \
    --driver-java-options "-Dbench.query=$Q -Dbench.iters=$IT -Dbench.base=$DATA $XJ" \
    --jars "$JAR" --driver-class-path "$JAR" \
    -i "$BENCH" < /dev/null > "$OUTD/run.log" 2>&1
  echo "    $Q: $(grep -aoE "ITER $Q [0-9]+ [0-9]+" "$OUTD/run.log" | awk '{print $4}' | tr '\n' ' ')"
  # spill/disk flag (do NOT delete — just report; Spark cleans its own localdir on clean exit):
  echo "    disk: localdir=$(du -sh "$LOCALDIR" 2>/dev/null | cut -f1)  /data avail=$(df -BG --output=avail /data | tail -1 | tr -d ' ')"
}

for JOB in "${JOBS[@]}"; do
  QUERIES="${JOB%%|*}"; REST="${JOB#*|}"; DATA="${REST%%|*}"; BENCH="${REST##*|}"
  [ -d "$DATA" ] || { echo "!! data not found: $DATA"; exit 2; }
  [ -f "$BENCH" ] || { echo "!! bench not found: $BENCH"; exit 2; }
  echo ">> === dataset: data=$DATA  bench=$(basename "$BENCH")  queries=[$QUERIES] ==="
  for Q in $QUERIES; do
    echo "########## $Q  $(date +%T) ##########"
    # 0) GPU-fallback smoke (1 iter, explain=NOT_ON_GPU) — verify 100% GPU / no CPU fallback
    { [ "$ONLY" = all ] || [ "$ONLY" = smoke ]; } && \
      run "$OUT/$Q-smoke" "$Q" 1 1g "--conf spark.rapids.sql.explain=NOT_ON_GPU" ""
    # 1) OFF split sweep (autotuner off) — establishes optSplit
    { [ "$ONLY" = all ] || [ "$ONLY" = off ]; } && \
      for m in $SWEEP; do run "$OUT/$Q-off-$m" "$Q" "$ITERS" "$m" "" ""; done
    # 2) autotuner: ratio strategy, ratioBasis=listed, ceiling=$CEILING, floor=min (start split 256m)
    { [ "$ONLY" = all ] || [ "$ONLY" = ratio ]; } && \
      run "$OUT/$Q-ftt-ratio" "$Q" "$ITERS" 256m \
        "--conf spark.rapids.sql.scan.splitAutotuner.historyPath=$OUT/$Q-ftt-ratio/history.tsv" \
        "-Drapids.autotuner.strategy=ratio -Drapids.autotuner.ratioBasis=listed -Drapids.autotuner.ceiling=$CEILING -Drapids.autotuner.floor=min"
    # 3) partition-count rule, 2x2 with the split autotuner (partition_rule_full.py, applied rolling
    #    from each arm's OWN prior run). dWall must compare SAME-SPLIT, so each -parts arm is paired
    #    with the arm it derived from: pbase<->pparts (autotuner OFF), ftt-ratio<->ratio-parts (ON).
    { [ "$ONLY" = all ] || [ "$ONLY" = parts ]; } && {
      # Baseline split comes from the OFF SWEEP's optimum -- never a hand-picked value.
      IFS=$'\t' read -r OPTMPB OPTWALL < <(off_optimum "$Q")
      if [ -z "${OPTMPB:-}" ]; then
        echo "  !! [parts] $Q: no completed OFF sweep under $OUT ($Q-off-<${SWEEP// /|}> with >= $ITERS iters)."
        echo "  !! The partition baseline MUST be the swept OFF optimum (doc pipeline: smoke -> OFF sweep"
        echo "  !! -> ratio). Run --only=off for $Q first. Refusing to substitute a default split."
        continue
      fi
      echo "  [parts] $Q OFF-optimum split = $OPTMPB (avg-of-warm ${OPTWALL}s) -- baseline for the partition arms"
      [ -n "${OFFITERS:-}" ] && [ "${OFFITERS}" -lt "$ITERS" ] && \
        echo "  !! [parts] $Q baseline came from a >=${OFFITERS}-iteration sweep while arms run $ITERS." \
             "Short sweeps produced the documented fake pv03g optimum -- carry this caveat into the report."
      run "$OUT/$Q-pbase" "$Q" "$ITERS" "$OPTMPB" "" ""
      IFS=$'\t' read -r P ACT WHY < <(derive_parts "$OUT/$Q-pbase")
      echo "  [rule] $Q autotuner-OFF: ${ACT:-none} -> parts=${P:-unchanged}  ($WHY)"
      if [ -n "${P:-}" ]; then
        run "$OUT/$Q-pparts" "$Q" "$ITERS" "$OPTMPB" \
          "--conf spark.sql.shuffle.partitions=$P" ""
      else
        echo "  !! [rule] $Q: derive_parts produced NOTHING -- arm SKIPPED (this is a harness"
        echo "  !! failure, not a KEEP decision; do not read the missing arm as 'no change')"
      fi
      if [ -d "$OUT/$Q-ftt-ratio/el" ]; then
        IFS=$'\t' read -r PR ACTR WHYR < <(derive_parts "$OUT/$Q-ftt-ratio")
        echo "  [rule] $Q autotuner-ON: ${ACTR:-none} -> parts=${PR:-unchanged}  ($WHYR)"
        [ -z "${PR:-}" ] && echo "  !! [rule] $Q: derive_parts produced NOTHING for the ON half -- arm SKIPPED (harness failure)"
        [ -n "${PR:-}" ] && run "$OUT/$Q-ratio-parts" "$Q" "$ITERS" 256m \
          "--conf spark.rapids.sql.scan.splitAutotuner.historyPath=$OUT/$Q-ratio-parts/history.tsv --conf spark.sql.shuffle.partitions=$PR" \
          "-Drapids.autotuner.strategy=ratio -Drapids.autotuner.ratioBasis=listed -Drapids.autotuner.ceiling=$CEILING -Drapids.autotuner.floor=min"
      else
        echo "  [rule] $Q: no $Q-ftt-ratio arm yet -- skipping the autotuner-ON half"
      fi
    }
  done
done
echo "ALL DONE $(date +%T) -> $OUT"
