#!/usr/bin/env bash
# T sweep: target decoded bytes per task (reader-scoped) = 2 GiB, 4 GiB, with the split ceiling
# removed (ceiling=none) so T actually drives split = T/ratio and the reader emits ~T-sized batches.
# Same jar/history, same machine, back-to-back. T=1g + ceiling=4g is the existing batch4g reference.
#   cold  -> populate history ratios (T/ceiling irrelevant to recorded ratios)
#   t2g   -> targetDecodedBytesPerTask=2g, ceiling=none
#   t4g   -> targetDecodedBytesPerTask=4g, ceiling=none
# WATCH: OOM/spill (chunked reader budget ~ T*4), and task-count collapse on low-ratio tables.
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
BASE="$REPO/data/nds-tsweep-$TS"
HIST="$BASE/history.tsv"
mkdir -p "$BASE"

echo "=== pre-warming page cache ==="
find "$DATA" -name '*.parquet' -print0 | xargs -0 cat > /dev/null 2>&1 || true

# $1 pass label, $2 target-decoded conf value ("" = default 1g), $3 ceiling jopt ("" = default 4g)
run_pass() {
  local pass="$1" tval="${2:-}" jopt="${3:-}"
  local tconf=""; [ -n "$tval" ] && tconf="$tval"
  echo ""; echo "########## $pass (T=${tval:-default} ceiling_jopt='$jopt') ##########"
  local ELDIR="$BASE/el-$pass" WORK="$BASE/work-$pass"; mkdir -p "$ELDIR" "$WORK/tmp"
  "$SPARK_HOME/bin/spark-submit" \
    --master 'local[16]' --driver-memory 16G \
    --conf spark.driver.maxResultSize=2GB \
    --conf spark.executor.cores=16 --conf spark.executor.memory=16G \
    --conf spark.plugins=com.nvidia.spark.SQLPlugin --conf spark.rapids.sql.enabled=true \
    --conf spark.rapids.sql.metrics.level=DEBUG \
    --conf spark.rapids.sql.scan.splitAutotuner.historyPath="$HIST" \
    ${tconf:+--conf spark.rapids.sql.scan.targetDecodedBytesPerTask=$tconf} \
    --conf spark.driver.extraJavaOptions="$jopt" \
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

run_pass cold "" ""
run_pass t2g  "2g" "-Drapids.autotuner.ceiling=none"
run_pass t4g  "4g" "-Drapids.autotuner.ceiling=none"

echo ""; echo "BASE=$BASE"
