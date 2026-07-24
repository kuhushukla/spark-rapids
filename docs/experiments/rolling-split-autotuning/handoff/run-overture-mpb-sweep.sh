#!/usr/bin/env bash
# Local maxPartitionBytes baseline sweep for the Overture scan-heavy query (autotuner OFF).
# Finds the optimal maxPartitionBytes to compare fill-to-target against. One fresh spark-shell per value.
# Local Spark 3.5.3 + the 353 dist jar (has ratioBasis code but INERT with autotuner off = vanilla scan).
set -uo pipefail

REPO=/home/kuhu/Reps/spark-rapids
export SPARK_HOME=/home/kuhu/Downloads/spark-3.5.3-bin-hadoop3
export JAVA_HOME=/usr/lib/jvm/java-1.17.0-openjdk-amd64
export CUDA_VISIBLE_DEVICES=GPU-1aaa66fd-0c1e-935b-fe65-2c9ca7357504   # A5000
JAR="$REPO/dist/target/rapids-4-spark_2.12-26.08.0-SNAPSHOT-cuda12.jar"
SCRIPT="$REPO/docs/experiments/rolling-split-autotuning/handoff/overture_scanheavy.scala"
ITERS="${ITERS:-5}"

for MPB in 128m 256m 512m 1g 2g 4g; do
  OUT="$REPO/data/overture-mpb-$MPB"; EL="$OUT/el"; mkdir -p "$EL"
  echo "########## maxPartitionBytes=$MPB ($(date +%H:%M:%S)) ##########"
  "$SPARK_HOME/bin/spark-shell" --master 'local[16]' \
    --driver-memory 32G --conf spark.driver.maxResultSize=2GB \
    --conf spark.plugins=com.nvidia.spark.SQLPlugin --conf spark.rapids.sql.enabled=true \
    --conf spark.sql.files.maxPartitionBytes=$MPB \
    --conf spark.rapids.sql.metrics.level=DEBUG \
    --conf spark.rapids.filecache.enabled=false \
    --conf spark.rapids.memory.pinnedPool.size=8g \
    --conf spark.rapids.sql.concurrentGpuTasks=2 \
    --conf spark.shuffle.manager=com.nvidia.spark.rapids.spark353.RapidsShuffleManager \
    --conf spark.eventLog.enabled=true --conf "spark.eventLog.dir=file:$EL" \
    --driver-java-options "-Dbench.iters=$ITERS" \
    --jars "$JAR" --driver-class-path "$JAR" \
    -i "$SCRIPT" < /dev/null > "$OUT/run.log" 2>&1
  echo "  $MPB times: $(grep -oE 'OVERTURE_ITER [0-9]+ [0-9]+' "$OUT/run.log" | awk '{print $3}' | tr '\n' ' ')"
done
echo "ALL DONE $(date +%H:%M:%S)"
