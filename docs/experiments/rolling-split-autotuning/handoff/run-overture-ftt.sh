#!/usr/bin/env bash
# OFF@1g (optimal baseline) vs fill-to-target on the Overture scan-heavy query, local Spark 3.5.3, A5000 only.
# 3 arms, one fresh spark-shell each, 5 iters (iter1 cold; for ftt iter1 COLD_START learns, 2-5 apply):
#   off-1g     : autotuner OFF, maxPartitionBytes=1g (the sweep optimum)
#   ftt-8g     : ratioBasis=listed, ceiling=8g,   floor=min  (fill batches; few big tasks)
#   ftt-core1  : ratioBasis=listed, ceiling=core1, floor=min  (ratio capped to >= cores tasks)
# All use maxPartitionBytes=1g so cold iter1 (COLD_START/OFF) == the baseline split. Fresh historyPath per arm.
set -uo pipefail
REPO=/home/kuhu/Reps/spark-rapids
export SPARK_HOME=/home/kuhu/Downloads/spark-3.5.3-bin-hadoop3
export JAVA_HOME=/usr/lib/jvm/java-1.17.0-openjdk-amd64
export CUDA_VISIBLE_DEVICES=GPU-1aaa66fd-0c1e-935b-fe65-2c9ca7357504   # A5000 only, never T400
JAR="$REPO/dist/target/rapids-4-spark_2.12-26.08.0-SNAPSHOT-cuda12.jar"
SCRIPT="$REPO/docs/experiments/rolling-split-autotuning/handoff/overture_scanheavy.scala"
ITERS="${ITERS:-5}"

run_arm () {   # $1=name  $2=extra --driver-java-options  $3=extra --conf (autotuner historyPath or empty)
  local name="$1" jopts="$2" hconf="$3"
  local OUT="$REPO/data/overture-$name"; local EL="$OUT/el"; mkdir -p "$EL"
  echo "########## $name ($(date +%H:%M:%S)) ##########"
  "$SPARK_HOME/bin/spark-shell" --master 'local[16]' --driver-memory 32G \
    --conf spark.driver.maxResultSize=2GB \
    --conf spark.plugins=com.nvidia.spark.SQLPlugin --conf spark.rapids.sql.enabled=true \
    --conf spark.sql.files.maxPartitionBytes=1g \
    --conf spark.rapids.sql.metrics.level=DEBUG \
    --conf spark.rapids.filecache.enabled=false --conf spark.rapids.memory.pinnedPool.size=8g \
    --conf spark.rapids.sql.concurrentGpuTasks=2 \
    --conf spark.shuffle.manager=com.nvidia.spark.rapids.spark353.RapidsShuffleManager \
    --conf spark.eventLog.enabled=true --conf "spark.eventLog.dir=file:$EL" \
    $hconf \
    --driver-java-options "-Dbench.iters=$ITERS $jopts" \
    --jars "$JAR" --driver-class-path "$JAR" \
    -i "$SCRIPT" < /dev/null > "$OUT/run.log" 2>&1
  echo "  $name times: $(grep -oE 'OVERTURE_ITER [0-9]+ [0-9]+' "$OUT/run.log" | awk '{print $3}' | tr '\n' ' ')"
}

run_arm "off-1g"    ""                                                                             ""
run_arm "ftt-8g"    "-Drapids.autotuner.ratioBasis=listed -Drapids.autotuner.ceiling=8g -Drapids.autotuner.floor=min"    "--conf spark.rapids.sql.scan.splitAutotuner.historyPath=$REPO/data/overture-ftt-8g/history.tsv"
run_arm "ftt-core1" "-Drapids.autotuner.ratioBasis=listed -Drapids.autotuner.ceiling=core1 -Drapids.autotuner.floor=min" "--conf spark.rapids.sql.scan.splitAutotuner.historyPath=$REPO/data/overture-ftt-core1/history.tsv"
echo "ALL DONE $(date +%H:%M:%S)"
