#!/usr/bin/env bash
# Self-tuning positive control: run fill-to-target from SUBOPTIMAL starting maxPartitionBytes (128m and 4g,
# the worst baselines). If ftt is self-tuning, warm iters should DECIDE the same 741 MB split (777255414)
# and land at the same ~6.6-6.7s warm, independent of the starting maxPartitionBytes.
# ratioBasis=listed, ceiling=8g, floor=min. Local Spark 3.5.3 + 353 jar, A5000 only.
set -uo pipefail
REPO=/home/kuhu/Reps/spark-rapids
export SPARK_HOME=/home/kuhu/Downloads/spark-3.5.3-bin-hadoop3
export JAVA_HOME=/usr/lib/jvm/java-1.17.0-openjdk-amd64
export CUDA_VISIBLE_DEVICES=GPU-1aaa66fd-0c1e-935b-fe65-2c9ca7357504   # A5000 only, never T400
JAR="$REPO/dist/target/rapids-4-spark_2.12-26.08.0-SNAPSHOT-cuda12.jar"
SCRIPT="$REPO/docs/experiments/rolling-split-autotuning/handoff/overture_scanheavy.scala"
ITERS="${ITERS:-5}"

run_ftt () {   # $1=name  $2=maxPartitionBytes
  local name="$1" mpb="$2"
  local OUT="$REPO/data/overture-$name"; local EL="$OUT/el"; mkdir -p "$EL"
  echo "########## $name (maxPartitionBytes=$mpb) ($(date +%H:%M:%S)) ##########"
  "$SPARK_HOME/bin/spark-shell" --master 'local[16]' --driver-memory 32G \
    --conf spark.driver.maxResultSize=2GB \
    --conf spark.plugins=com.nvidia.spark.SQLPlugin --conf spark.rapids.sql.enabled=true \
    --conf spark.sql.files.maxPartitionBytes=$mpb \
    --conf spark.rapids.sql.metrics.level=DEBUG \
    --conf spark.rapids.filecache.enabled=false --conf spark.rapids.memory.pinnedPool.size=8g \
    --conf spark.rapids.sql.concurrentGpuTasks=2 \
    --conf spark.shuffle.manager=com.nvidia.spark.rapids.spark353.RapidsShuffleManager \
    --conf spark.eventLog.enabled=true --conf "spark.eventLog.dir=file:$EL" \
    --conf "spark.rapids.sql.scan.splitAutotuner.historyPath=$OUT/history.tsv" \
    --driver-java-options "-Dbench.iters=$ITERS -Drapids.autotuner.ratioBasis=listed -Drapids.autotuner.ceiling=8g -Drapids.autotuner.floor=min" \
    --jars "$JAR" --driver-class-path "$JAR" \
    -i "$SCRIPT" < /dev/null > "$OUT/run.log" 2>&1
  local sp=$(grep -oE "split_bytes=[0-9]+ raw_target=[0-9]+ ceiling=[0-9]+ parallelism_cap=[0-9]+ bound_by=[a-z_]+" "$OUT/run.log" | head -1)
  echo "  $name times: $(grep -oE 'OVERTURE_ITER [0-9]+ [0-9]+' "$OUT/run.log" | awk '{print $3}' | tr '\n' ' ')"
  echo "  $name DECIDED: $sp"
}

run_ftt "ftt-mpb128m" 128m
run_ftt "ftt-mpb4g"   4g
echo "ALL DONE $(date +%H:%M:%S)"
