#!/usr/bin/env bash
# Autotuner ON (fill-to-target) for the REAL-WORLD Overture query, from two suboptimal starts (128m, 4g).
# Tests whether ftt self-tunes to near this query's optimum (512m baseline). ceiling=8g, floor=min.
set -uo pipefail
REPO=/home/kuhu/Reps/spark-rapids
export SPARK_HOME=/home/kuhu/Downloads/spark-3.5.3-bin-hadoop3
export JAVA_HOME=/usr/lib/jvm/java-1.17.0-openjdk-amd64
export CUDA_VISIBLE_DEVICES=GPU-1aaa66fd-0c1e-935b-fe65-2c9ca7357504   # A5000 only
JAR="$REPO/dist/target/rapids-4-spark_2.12-26.08.0-SNAPSHOT-cuda12.jar"
SCRIPT="$REPO/docs/experiments/rolling-split-autotuning/handoff/overture_realworld_bench.scala"
ITERS="${ITERS:-5}"
run_ftt () {   # $1=name $2=start mpb
  local name="$1" mpb="$2"; local OUT="$REPO/data/overture-$name"; mkdir -p "$OUT/el"
  echo "########## $name (start maxPartitionBytes=$mpb) ($(date +%H:%M:%S)) ##########"
  "$SPARK_HOME/bin/spark-shell" --master 'local[16]' --driver-memory 32G \
    --conf spark.plugins=com.nvidia.spark.SQLPlugin --conf spark.rapids.sql.enabled=true \
    --conf spark.sql.files.maxPartitionBytes=$mpb --conf spark.rapids.sql.metrics.level=DEBUG \
    --conf spark.rapids.filecache.enabled=false --conf spark.rapids.memory.pinnedPool.size=8g \
    --conf spark.rapids.sql.concurrentGpuTasks=2 \
    --conf spark.shuffle.manager=com.nvidia.spark.rapids.spark353.RapidsShuffleManager \
    --conf spark.eventLog.enabled=true --conf "spark.eventLog.dir=file:$OUT/el" \
    --conf "spark.rapids.sql.scan.splitAutotuner.historyPath=$OUT/history.tsv" \
    --driver-java-options "-Dbench.iters=$ITERS -Drapids.autotuner.ratioBasis=listed -Drapids.autotuner.ceiling=8g -Drapids.autotuner.floor=min" \
    --jars "$JAR" --driver-class-path "$JAR" \
    -i "$SCRIPT" < /dev/null > "$OUT/run.log" 2>&1
  echo "  $name times: $(grep -oE 'OVERTURE_ITER [0-9]+ [0-9]+' "$OUT/run.log" | awk '{print $3}' | tr '\n' ' ')"
  echo "  $name DECIDED: $(grep -oE 'split_bytes=[0-9]+ raw_target=[0-9]+ ceiling=[0-9]+ parallelism_cap=[0-9]+ bound_by=[a-z_]+' "$OUT/run.log" | head -1)"
}
run_ftt "rw-ftt-128m" 128m
run_ftt "rw-ftt-4g"   4g
echo "ALL DONE $(date +%H:%M:%S)"
