#!/usr/bin/env bash
# Clean split A/B across ALL 99 NDS queries at SF100.
# Vary spark.sql.files.maxPartitionBytes in {128m,1024m,4096m}; autotuner OFF (so effective split ==
# the value set, verified per-scan by the scanMaxSplitBytes event-log metric). Page cache pre-warmed
# so every run reads from RAM. Fresh JVM per split. One power run of all 99 queries per split.
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
BASE="$REPO/data/nds-allq-ab-$TS"
mkdir -p "$BASE"

echo "=== pre-warming page cache (all SF100 tables) ==="
find "$DATA" -name '*.parquet' -print0 | xargs -0 cat > /dev/null 2>&1 || true
free -h | head -2

for MB in 128m 1024m 4096m; do
  echo ""
  echo "########## split=$MB ##########"
  ELDIR="$BASE/el-$MB"; WORK="$BASE/work-$MB"; mkdir -p "$ELDIR" "$WORK/tmp"
  "$SPARK_HOME/bin/spark-submit" \
    --master 'local[16]' --driver-memory 16G \
    --conf spark.driver.maxResultSize=2GB \
    --conf spark.executor.cores=16 --conf spark.executor.memory=16G \
    --conf spark.plugins=com.nvidia.spark.SQLPlugin --conf spark.rapids.sql.enabled=true \
    --conf spark.rapids.sql.metrics.level=DEBUG \
    --conf spark.sql.files.maxPartitionBytes=$MB \
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
    "$BENCH/nds_power.py" "$DATA" "$STREAM" "$WORK/times-$MB.csv" \
    --input_format parquet --json_summary_folder "$WORK/json" --allow_failure \
    > "$BASE/run-$MB.log" 2>&1
  echo "  split=$MB done -> $BASE/run-$MB.log ; times=$WORK/times-$MB.csv"
done

echo ""
echo "BASE=$BASE"
