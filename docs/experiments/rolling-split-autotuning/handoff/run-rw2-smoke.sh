#!/usr/bin/env bash
# Smoke test: run RW6-RW9 once each (+explain) in one session at 1g, autotuner OFF.
# Confirms GPU coverage / no OOM and gives rough per-iter timing to budget the full sweep.
set -uo pipefail
REPO=/home/kuhu/Reps/spark-rapids
export SPARK_HOME=/home/kuhu/Downloads/spark-3.5.3-bin-hadoop3
export JAVA_HOME=/usr/lib/jvm/java-1.17.0-openjdk-amd64
export CUDA_VISIBLE_DEVICES=GPU-1aaa66fd-0c1e-935b-fe65-2c9ca7357504   # A5000 only, never T400
JAR="$REPO/dist/target/rapids-4-spark_2.12-26.08.0-SNAPSHOT-cuda12.jar"
SCRIPT="$REPO/docs/experiments/rolling-split-autotuning/handoff/overture_rw2_bench.scala"
OUT="$REPO/data/overture-rw2-smoke"; EL="$OUT/el"; mkdir -p "$EL"
echo "########## RW2 SMOKE (all queries, 1 iter, +explain) $(date +%H:%M:%S) ##########"
"$SPARK_HOME/bin/spark-shell" --master 'local[16]' --driver-memory 32G \
  --conf spark.driver.maxResultSize=2GB \
  --conf spark.plugins=com.nvidia.spark.SQLPlugin --conf spark.rapids.sql.enabled=true \
  --conf spark.sql.files.maxPartitionBytes=1g \
  --conf spark.rapids.sql.metrics.level=DEBUG \
  --conf spark.rapids.filecache.enabled=false --conf spark.rapids.memory.pinnedPool.size=8g \
  --conf spark.rapids.sql.concurrentGpuTasks=2 \
  --conf spark.shuffle.manager=com.nvidia.spark.rapids.spark353.RapidsShuffleManager \
  --conf spark.eventLog.enabled=true --conf "spark.eventLog.dir=file:$EL" \
  --driver-java-options "-Dbench.query=all -Dbench.iters=1 -Dbench.explain=true" \
  --jars "$JAR" --driver-class-path "$JAR" \
  -i "$SCRIPT" < /dev/null > "$OUT/run.log" 2>&1
echo "exit=$? $(date +%H:%M:%S)"
echo "=== timings ==="; grep -E 'RW2_ITER' "$OUT/run.log"
echo "=== CPU fallbacks (operators NOT on GPU) ==="; grep -icE 'cannot run on GPU|will run on CPU|not supported on GPU' "$OUT/run.log"
