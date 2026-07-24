#!/usr/bin/env bash
# Autotuner ON (fill-to-target) for RW6-RW9. ONE QUERY PER SESSION with its OWN history.tsv (so COLD_START->warm
# is clean per query and no cross-query table-history contamination; rw6/rw7/rw8 all scan `segment`). Two starts
# (128m, 4g) to show convergence independent of start. 5 iters (iter1 COLD_START/default split, 2-5 warm/converged).
# Flags match run-overture-rw-ftt.sh: ratioBasis=listed (fill-to-target), ceiling=8g, floor=min.
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
for START in 128m 4g; do
  OUT="$REPO/data/overture-rw2-$Q-ftt-$START"; EL="$OUT/el"; mkdir -p "$EL"
  HIST="$OUT/history.tsv"        # own history per (query,start) — never shared
  echo "########## $Q ftt start=$START ($(date +%H:%M:%S)) ##########"
  "$SPARK_HOME/bin/spark-shell" --master 'local[16]' --driver-memory 32G \
    --conf spark.driver.maxResultSize=2GB \
    --conf spark.plugins=com.nvidia.spark.SQLPlugin --conf spark.rapids.sql.enabled=true \
    --conf spark.sql.files.maxPartitionBytes=$START \
    --conf spark.rapids.sql.metrics.level=DEBUG \
    --conf spark.rapids.sql.explain=NONE \
    --conf spark.rapids.filecache.enabled=false --conf spark.rapids.memory.pinnedPool.size=8g \
    --conf spark.rapids.sql.concurrentGpuTasks=2 \
    --conf spark.shuffle.manager=com.nvidia.spark.rapids.spark353.RapidsShuffleManager \
    --conf "spark.rapids.sql.scan.splitAutotuner.historyPath=$HIST" \
    --conf spark.eventLog.enabled=true --conf "spark.eventLog.dir=file:$EL" \
    --driver-java-options "-Dbench.query=$Q -Dbench.iters=$ITERS -Drapids.autotuner.ratioBasis=listed -Drapids.autotuner.ceiling=8g -Drapids.autotuner.floor=min" \
    --jars "$JAR" --driver-class-path "$JAR" \
    -i "$SCRIPT" < /dev/null > "$OUT/run.log" 2>&1
  echo "  $Q ftt=$START DECIDED: $(grep -oE 'split_bytes=[0-9]+ .*bound_by=[a-z]+' "$OUT/run.log" | tail -1)"
done
done
echo "FTT ALL DONE $(date +%H:%M:%S)"
