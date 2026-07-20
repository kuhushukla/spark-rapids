#!/usr/bin/env bash
# SIMPLER test: raise only the existing reader read-cap (spark.rapids.sql.reader.batchSizeBytes =
# maxReadBatchSizeBytes). No T knob. This changes how much the reader READS/decodes per chunk, but the
# EMITTED batch stays at the global 1 GiB (gpuTargetBatchSizeBytes) — so we expect the max output batch
# NOT to grow. Comparison partner to the T-knob run (which grows the emitted batch reader-scoped).
# Autotuner ON, default 4 GiB split ceiling, same jar/history/machine.
#   off  -> autotuner disabled, 128 MiB baseline
#   cold -> autotuner on, populate history ratios
#   r1g  -> reader.batchSizeBytes=1g   r2g -> 2g   r4g -> 4g   (read cap; default is ~2 GiB)
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
BASE="$REPO/data/nds-readcap-$TS"
HIST="$BASE/history.tsv"
mkdir -p "$BASE"

echo "=== pre-warming page cache ==="
find "$DATA" -name '*.parquet' -print0 | xargs -0 cat > /dev/null 2>&1 || true

# $1 label, $2 autotuner history (yes|no), $3 reader.batchSizeBytes value ("" = don't set)
run_pass() {
  local pass="$1" hist="$2" rval="${3:-}"
  echo ""; echo "########## $pass (autotuner=$hist reader.batchSizeBytes=${rval:-default}) ##########"
  local ELDIR="$BASE/el-$pass" WORK="$BASE/work-$pass"; mkdir -p "$ELDIR" "$WORK/tmp"
  local HISTCONF=(); [ "$hist" = yes ] && HISTCONF=(--conf "spark.rapids.sql.scan.splitAutotuner.historyPath=$HIST")
  local RCONF=(); [ -n "$rval" ] && RCONF=(--conf "spark.rapids.sql.reader.batchSizeBytes=$rval")
  "$SPARK_HOME/bin/spark-submit" \
    --master 'local[16]' --driver-memory 16G \
    --conf spark.driver.maxResultSize=2GB \
    --conf spark.executor.cores=16 --conf spark.executor.memory=16G \
    --conf spark.plugins=com.nvidia.spark.SQLPlugin --conf spark.rapids.sql.enabled=true \
    --conf spark.rapids.sql.metrics.level=DEBUG \
    "${HISTCONF[@]}" "${RCONF[@]}" \
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
run_pass r1g  yes "1g"
run_pass r2g  yes "2g"
run_pass r4g  yes "4g"

echo ""; echo "BASE=$BASE"
