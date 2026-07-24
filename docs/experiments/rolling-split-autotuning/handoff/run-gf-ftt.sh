#!/usr/bin/env bash
# Autotuner ON (fill-to-target) for GF1/GF2. One query per session, own history.tsv per (query,start), 2 starts
# (128m, 4g) for start-independence. 5 iters (iter1 COLD_START, 2-5 warm). ABSOLUTE eventLog dir.
set -uo pipefail
REPO=/home/kuhu/Reps/spark-rapids
export SPARK_HOME=/home/kuhu/Downloads/spark-3.5.3-bin-hadoop3
export JAVA_HOME=/usr/lib/jvm/java-1.17.0-openjdk-amd64
export CUDA_VISIBLE_DEVICES=GPU-1aaa66fd-0c1e-935b-fe65-2c9ca7357504   # A5000 only, never T400
JAR="$REPO/dist/target/rapids-4-spark_2.12-26.08.0-SNAPSHOT-cuda12.jar"
SCRIPT="$REPO/docs/experiments/rolling-split-autotuning/handoff/overture_gf_bench.scala"
ITERS="${ITERS:-5}"
QUERIES="${QUERIES:-gf1 gf2}"
for Q in $QUERIES; do
for START in 128m 4g; do
  OUT="$REPO/data/overture-gf-$Q-ftt-$START"; EL="$OUT/el"; mkdir -p "$EL"; HIST="$OUT/history.tsv"
  echo "########## $Q ftt start=$START ($(date +%H:%M:%S)) ##########"
  "$SPARK_HOME/bin/spark-shell" --master 'local[16]' --driver-memory 32G \
    --conf spark.driver.maxResultSize=2GB \
    --conf spark.plugins=com.nvidia.spark.SQLPlugin --conf spark.rapids.sql.enabled=true \
    --conf spark.sql.files.maxPartitionBytes=$START \
    --conf spark.rapids.sql.metrics.level=DEBUG --conf spark.rapids.sql.explain=NONE \
    --conf spark.rapids.filecache.enabled=false --conf spark.rapids.memory.pinnedPool.size=8g \
    --conf spark.rapids.sql.concurrentGpuTasks=2 \
    --conf spark.shuffle.manager=com.nvidia.spark.rapids.spark353.RapidsShuffleManager \
    --conf "spark.rapids.sql.scan.splitAutotuner.historyPath=$HIST" \
    --conf spark.eventLog.enabled=true --conf "spark.eventLog.dir=file:$EL" \
    --driver-java-options "-Dbench.query=$Q -Dbench.iters=$ITERS -Drapids.autotuner.ratioBasis=listed -Drapids.autotuner.ceiling=8g -Drapids.autotuner.floor=min" \
    --jars "$JAR" --driver-class-path "$JAR" \
    -i "$SCRIPT" < /dev/null > "$OUT/run.log" 2>&1
  echo "  $Q ftt=$START: $(grep -oE 'split_bytes=[0-9]+ raw_target=[0-9]+ .*bound_by=[a-z_]+' "$OUT/run.log" | tail -1)"
done
done
echo "GF FTT ALL DONE $(date +%H:%M:%S)"
