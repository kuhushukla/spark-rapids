#!/usr/bin/env bash
# maxPartitionBytes sweep for RW6-RW9 (overture-realworld-2.scala), autotuner OFF, local Spark 3.5.3 + 353 jar,
# A5000 only. ONE QUERY PER SESSION (proven harness): loop query x config, own run dir, 5 iters (iter1 cold,
# 2-5 warm). Matches run-overture-realworld-sweep.sh but parameterized by query.
set -uo pipefail
REPO=/home/kuhu/Reps/spark-rapids
export SPARK_HOME=/home/kuhu/Downloads/spark-3.5.3-bin-hadoop3
export JAVA_HOME=/usr/lib/jvm/java-1.17.0-openjdk-amd64
export CUDA_VISIBLE_DEVICES=GPU-1aaa66fd-0c1e-935b-fe65-2c9ca7357504   # A5000 only, never T400
JAR="$REPO/dist/target/rapids-4-spark_2.12-26.08.0-SNAPSHOT-cuda12.jar"
SCRIPT="$REPO/docs/experiments/rolling-split-autotuning/handoff/overture_rw2_bench.scala"
ITERS="${ITERS:-5}"
QUERIES="${QUERIES:-rw6 rw7 rw8 rw9}"
for Q in $QUERIES; do
for MPB in 256m 512m 1g 2g 4g; do
  OUT="$REPO/data/overture-rw2-$Q-$MPB"; EL="$OUT/el"; mkdir -p "$EL"
  echo "########## $Q maxPartitionBytes=$MPB ($(date +%H:%M:%S)) ##########"
  "$SPARK_HOME/bin/spark-shell" --master 'local[16]' --driver-memory 32G \
    --conf spark.driver.maxResultSize=2GB \
    --conf spark.plugins=com.nvidia.spark.SQLPlugin --conf spark.rapids.sql.enabled=true \
    --conf spark.sql.files.maxPartitionBytes=$MPB \
    --conf spark.rapids.sql.metrics.level=DEBUG \
    --conf spark.rapids.sql.explain=NONE \
    --conf spark.rapids.filecache.enabled=false --conf spark.rapids.memory.pinnedPool.size=8g \
    --conf spark.rapids.sql.concurrentGpuTasks=2 \
    --conf spark.shuffle.manager=com.nvidia.spark.rapids.spark353.RapidsShuffleManager \
    --conf spark.eventLog.enabled=true --conf "spark.eventLog.dir=file:$EL" \
    --driver-java-options "-Dbench.query=$Q -Dbench.iters=$ITERS" \
    --jars "$JAR" --driver-class-path "$JAR" \
    -i "$SCRIPT" < /dev/null > "$OUT/run.log" 2>&1
  echo "  $Q $MPB times: $(grep -oE "RW2_ITER $Q [0-9]+ [0-9]+" "$OUT/run.log" | awk '{print $4}' | tr '\n' ' ')"
done
done
echo "SWEEP ALL DONE $(date +%H:%M:%S)"
