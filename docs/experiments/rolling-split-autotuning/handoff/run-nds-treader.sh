#!/usr/bin/env bash
# Reader-scoped batch size test. spark.rapids.sql.scan.targetDecodedBytesPerTask (T) sets the reader's
# output batch AND read cap (maxReadBatchSizeBytes raised to >=T) AND the autotuner split target.
# Downstream operators (joins/aggregates) stay on the global 1 GiB batch. DEFAULT 4 GiB split ceiling
# (ratio-based sizing kept; NOT ceiling=none). Same jar/history/machine, back-to-back.
#   off  -> autotuner disabled, 128 MiB baseline
#   cold -> autotuner on, populate history ratios
#   t1g  -> T=1g (reader batch 1 GiB = anchor)
#   t2g  -> T=2g
#   t4g  -> T=4g   (note: cudf per-column limit ~2 GiB may cap the actual batch below 4 GiB)
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
BASE="$REPO/data/nds-treader-$TS"
HIST="$BASE/history.tsv"
mkdir -p "$BASE"

echo "=== pre-warming page cache ==="
find "$DATA" -name '*.parquet' -print0 | xargs -0 cat > /dev/null 2>&1 || true

# $1 label, $2 autotuner history (yes|no), $3 T value ("" = don't set)
run_pass() {
  local pass="$1" hist="$2" tval="${3:-}"
  echo ""; echo "########## $pass (autotuner=$hist T=${tval:-default}) ##########"
  local ELDIR="$BASE/el-$pass" WORK="$BASE/work-$pass"; mkdir -p "$ELDIR" "$WORK/tmp"
  local HISTCONF=(); [ "$hist" = yes ] && HISTCONF=(--conf "spark.rapids.sql.scan.splitAutotuner.historyPath=$HIST")
  local TCONF=(); [ -n "$tval" ] && TCONF=(--conf "spark.rapids.sql.scan.targetDecodedBytesPerTask=$tval")
  "$SPARK_HOME/bin/spark-submit" \
    --master 'local[16]' --driver-memory 16G \
    --conf spark.driver.maxResultSize=2GB \
    --conf spark.executor.cores=16 --conf spark.executor.memory=16G \
    --conf spark.plugins=com.nvidia.spark.SQLPlugin --conf spark.rapids.sql.enabled=true \
    --conf spark.rapids.sql.metrics.level=DEBUG \
    "${HISTCONF[@]}" "${TCONF[@]}" \
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
run_pass t1g  yes "1g"
run_pass t2g  yes "2g"
run_pass t4g  yes "4g"

echo ""; echo "BASE=$BASE"
