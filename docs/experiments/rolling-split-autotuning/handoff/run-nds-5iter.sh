#!/usr/bin/env bash
# NDS SF100, 5 iterations each for autotuner OFF and ON, as 5 SEPARATE spark-submits per condition
# (local nds_power.py has no --iterations). Page cache pre-warmed once before all submits, and every
# submit is a fresh JVM -- so across the 5 submits the ONLY thing that changes is the autotuner history:
#   OFF: no historyPath -> fixed 128 MiB every submit (5 near-identical baseline runs).
#   ON : shared FRESH history file -> submit 1 is cold-start (learns, 128 MiB first-sighting),
#        submits 2-5 are warm (DECIDED, learned splits).
# Aligned by iteration index: cold-vs-cold = OFF-1 vs ON-1 ; warm-vs-warm = OFF-{2..5} vs ON-{2..5}.
set -euo pipefail

REPO=/home/kuhu/Reps/spark-rapids
AB=/home/kuhu/Reps/ab
BENCH=/home/kuhu/Reps/spark-rapids-benchmarks/nds
export SPARK_HOME=/home/kuhu/Downloads/spark-3.5.3-bin-hadoop3
export JAVA_HOME=/usr/lib/jvm/java-1.17.0-openjdk-amd64
export CUDA_VISIBLE_DEVICES=GPU-1aaa66fd-0c1e-935b-fe65-2c9ca7357504   # A5000; never T400
DATA="$AB/nds_sf100/parquet_sf100_decimal_fresh_20260623"
STREAM="$AB/query_0.sql"
JAR="$REPO/dist/target/rapids-4-spark_2.12-26.08.0-SNAPSHOT-cuda12.jar"
LISTENER="$AB/resources/nds-benchmark-listener-1.0-SNAPSHOT.jar"
TS=$(date +%Y%m%d_%H%M%S)
BASE="$REPO/data/nds-5iter-$TS"
HIST_ON="$BASE/hist-on.tsv"   # fresh: does not pre-exist -> ON iteration 1 is a true cold-start
mkdir -p "$BASE"

echo "=== pre-warming page cache (all SF100 tables) ==="
find "$DATA" -name '*.parquet' -print0 | xargs -0 cat > /dev/null 2>&1 || true

# $1 label (e.g. off-1), $2 historyPath ("" = autotuner off)
run_pass() {
  local pass="$1" hist="${2:-}"
  echo ""; echo "########## $pass (history=${hist:-NONE}) ##########"
  local ELDIR="$BASE/el-$pass" WORK="$BASE/work-$pass"; mkdir -p "$ELDIR" "$WORK/tmp"
  local HISTCONF=(); [ -n "$hist" ] && HISTCONF=(--conf "spark.rapids.sql.scan.splitAutotuner.historyPath=$hist")
  "$SPARK_HOME/bin/spark-submit" \
    --master 'local[16]' --driver-memory 16G \
    --conf spark.driver.maxResultSize=2GB \
    --conf spark.executor.cores=16 --conf spark.executor.memory=16G \
    --conf spark.plugins=com.nvidia.spark.SQLPlugin --conf spark.rapids.sql.enabled=true \
    --conf spark.rapids.sql.metrics.level=DEBUG \
    "${HISTCONF[@]}" \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.rapids.memory.pinnedPool.size=8g \
    --conf spark.rapids.sql.concurrentGpuTasks=2 \
    --conf spark.shuffle.manager=com.nvidia.spark.rapids.spark353.RapidsShuffleManager \
    --conf spark.rapids.filecache.enabled=false \
    --conf spark.sql.catalogImplementation=in-memory \
    --conf spark.local.dir="$WORK/tmp" \
    --conf spark.eventLog.enabled=true --conf spark.eventLog.dir="$ELDIR" \
    --jars "$JAR,$LISTENER" \
    --driver-class-path "$JAR:$LISTENER" \
    --conf spark.executor.extraClassPath="$JAR:$LISTENER" \
    "$BENCH/nds_power.py" "$DATA" "$STREAM" "$WORK/times-$pass.csv" \
    --input_format parquet --json_summary_folder "$WORK/json" --allow_failure \
    > "$BASE/run-$pass.log" 2>&1
  echo "  $pass done -> $BASE/run-$pass.log"
}

# OFF: 5 fresh submits, autotuner disabled
for i in 1 2 3 4 5; do run_pass "off-$i" ""; done
# ON: 5 fresh submits sharing one fresh history file (submit 1 cold-start, 2-5 warm)
for i in 1 2 3 4 5; do run_pass "on-$i" "$HIST_ON"; done

echo ""; echo "BASE=$BASE"
