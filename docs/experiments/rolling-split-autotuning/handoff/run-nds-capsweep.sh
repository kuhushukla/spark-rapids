#!/usr/bin/env bash
# Cap sweep: does raising the single-task split ceiling help, or OOM/fail?
# Same jar, same populated history (ratios ceiling-independent), same machine, back-to-back.
#   cold -> populate history ratios
#   cap4g / cap8g / cap16g / capnone -> warm passes with ceiling 4/8/16 GiB and unlimited
# Watch each run-*.log for OOM / task failures. OFF baseline taken separately (run-nds-allq-off.sh).
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
BASE="$REPO/data/nds-capsweep-$TS"
HIST="$BASE/history.tsv"
mkdir -p "$BASE"

echo "=== pre-warming page cache ==="
find "$DATA" -name '*.parquet' -print0 | xargs -0 cat > /dev/null 2>&1 || true

# $1 = pass label, $2 = ceiling property value (empty for cold = default 4g, unused)
run_pass() {
  local pass="$1" ceil="${2:-}"
  local jopt=""; [ -n "$ceil" ] && jopt="-Drapids.autotuner.ceiling=$ceil"
  echo ""; echo "########## $pass (ceiling='$ceil') ##########"
  local ELDIR="$BASE/el-$pass" WORK="$BASE/work-$pass"; mkdir -p "$ELDIR" "$WORK/tmp"
  "$SPARK_HOME/bin/spark-submit" \
    --master 'local[16]' --driver-memory 16G \
    --conf spark.driver.maxResultSize=2GB \
    --conf spark.executor.cores=16 --conf spark.executor.memory=16G \
    --conf spark.plugins=com.nvidia.spark.SQLPlugin --conf spark.rapids.sql.enabled=true \
    --conf spark.rapids.sql.metrics.level=DEBUG \
    --conf spark.rapids.sql.scan.splitAutotuner.historyPath="$HIST" \
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

run_pass cold    4g       # populate history (ceiling irrelevant to recorded ratios)
run_pass cap4g   4g       # anchor
run_pass cap6g   6g
run_pass cap8g   8g

echo ""; echo "BASE=$BASE"
