#!/usr/bin/env bash
# batchSizeBytes 1g vs 2g, autotuner ON with the DEFAULT 4 GiB ceiling (ratio-based split sizing
# honored: split = batchSizeBytes/ratio, clamped to [floor, 4 GiB]). Tests sending ~2 GiB GPU batches
# vs 1 GiB, without collapsing parallelism. Same jar/history/machine, back-to-back.
#   off  -> autotuner disabled, 128 MiB baseline
#   cold -> autotuner on, populate history ratios (batchSizeBytes default 1g)
#   b1g  -> autotuner on, batchSizeBytes=1g (= current 1.80x config, anchor)
#   b2g  -> autotuner on, batchSizeBytes=2g (bigger GPU batches, splits ~2x but 4 GiB-capped)
set -euo pipefail

REPO=$(cd "$(dirname "$0")/../../../.." && pwd)
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
BASE="$REPO/data/nds-batchsize-$TS"
HIST="$BASE/history.tsv"
mkdir -p "$BASE"

echo "=== pre-warming page cache ==="
find "$DATA" -name '*.parquet' -print0 | xargs -0 cat > /dev/null 2>&1 || true

# $1 label, $2 autotuner history on (yes|no), $3 batchSizeBytes value ("" = default 1g)
run_pass() {
  local pass="$1" hist="$2" bval="${3:-}"
  echo ""; echo "########## $pass (autotuner=$hist batchSizeBytes=${bval:-default}) ##########"
  local ELDIR="$BASE/el-$pass" WORK="$BASE/work-$pass"; mkdir -p "$ELDIR" "$WORK/tmp"
  local HISTCONF=()
  [ "$hist" = yes ] && HISTCONF=(--conf "spark.rapids.sql.scan.splitAutotuner.historyPath=$HIST")
  local BATCHCONF=()
  [ -n "$bval" ] && BATCHCONF=(--conf "spark.rapids.sql.batchSizeBytes=$bval")
  "$SPARK_HOME/bin/spark-submit" \
    --master 'local[16]' --driver-memory 16G \
    --conf spark.driver.maxResultSize=2GB \
    --conf spark.executor.cores=16 --conf spark.executor.memory=16G \
    --conf spark.plugins=com.nvidia.spark.SQLPlugin --conf spark.rapids.sql.enabled=true \
    --conf spark.rapids.sql.metrics.level=DEBUG \
    "${HISTCONF[@]}" "${BATCHCONF[@]}" \
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

run_pass off  no  ""
run_pass cold yes ""
run_pass b1g  yes "1g"
run_pass b2g  yes "2g"

echo ""; echo "BASE=$BASE"
