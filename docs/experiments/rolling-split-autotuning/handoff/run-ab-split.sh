#!/usr/bin/env bash
# Clean A/B: does the scan split affect query9 time? Vary spark.sql.files.maxPartitionBytes
# (autotuner OFF so scanMaxSplitBytes == the split we set), controlling confounds:
#   - cache: pre-warm page cache once up front so every run reads from RAM
#   - JIT/JVM/GPU: fresh JVM per split; run query9 6x per JVM; compare steady state (iters 2..6)
# Verifies the effective split via the scanMaxSplitBytes event-log metric.
set -euo pipefail

REPO=$(cd "$(dirname "$0")/../../../.." && pwd)
export SPARK_HOME=/home/kuhu/Downloads/spark-3.5.3-bin-hadoop3
export JAVA_HOME=/usr/lib/jvm/java-1.17.0-openjdk-amd64
export CUDA_VISIBLE_DEVICES=GPU-1aaa66fd-0c1e-935b-fe65-2c9ca7357504   # A5000 by UUID; never T400
DATA=/home/kuhu/Reps/ab/nds_sf100/parquet_sf100_decimal_fresh_20260623
JAR="$REPO/dist/target/rapids-4-spark_2.12-26.08.0-SNAPSHOT-cuda12.jar"
SCALA="$REPO/docs/experiments/rolling-split-autotuning/handoff/aa-query9.scala"
TS=$(date +%Y%m%d_%H%M%S)
BASE="$REPO/data/ab-split-$TS"
mkdir -p "$BASE"

echo "=== prewarming page cache (store_sales + reason) ==="
find "$DATA/store_sales" "$DATA/reason" -name '*.parquet' -print0 | xargs -0 cat > /dev/null 2>&1 || true
free -h | head -2

for MB in 128m 1024m 4096m; do
  echo ""
  echo "########## split=$MB ##########"
  ELDIR="$BASE/el-$MB"; mkdir -p "$ELDIR"
  "$SPARK_HOME/bin/spark-shell" --master 'local[16]' --driver-memory 16g \
    --conf spark.plugins=com.nvidia.spark.SQLPlugin \
    --conf spark.rapids.sql.enabled=true \
    --conf spark.rapids.sql.metrics.level=DEBUG \
    --conf spark.sql.files.maxPartitionBytes=$MB \
    --conf spark.sql.catalogImplementation=in-memory \
    --conf spark.local.dir="$BASE/tmp" \
    --conf spark.eventLog.enabled=true --conf spark.eventLog.dir="$ELDIR" \
    --jars "$JAR" -i "$SCALA" > "$BASE/run-$MB.log" 2>&1
  echo "  split=$MB done -> $BASE/run-$MB.log"
done

echo ""
echo "BASE=$BASE"
