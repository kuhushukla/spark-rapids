#!/usr/bin/env bash
# One-query positive control: run query28 (store_sales-heavy) with T=4g, comparing the cudf per-column
# limit at default (2 GiB) vs 512m. If the emitted batch follows columnSizeBytes, the per-column limit
# is the cap; if it stays ~1274 MiB, the cap is elsewhere (data/split-limited).
set -euo pipefail

REPO=$(cd "$(dirname "$0")/../../../.." && pwd)
AB=/home/kuhu/Reps/ab
BENCH=/home/kuhu/Reps/spark-rapids-benchmarks/nds
export SPARK_HOME=/home/kuhu/Downloads/spark-3.5.3-bin-hadoop3
export JAVA_HOME=/usr/lib/jvm/java-1.17.0-openjdk-amd64
export CUDA_VISIBLE_DEVICES=GPU-1aaa66fd-0c1e-935b-fe65-2c9ca7357504
DATA="$AB/nds_sf100/parquet_sf100_decimal_fresh_20260623"
STREAM="$REPO/docs/experiments/rolling-split-autotuning/handoff/mini-stream-q28.sql"
JAR="$REPO/dist/target/rapids-4-spark_2.12-26.08.0-SNAPSHOT-cuda12.jar"
LISTENER="$AB/resources/nds-benchmark-listener-1.0-SNAPSHOT.jar"
TS=$(date +%Y%m%d_%H%M%S)
BASE="$REPO/data/nds-q28col-$TS"; mkdir -p "$BASE"
HIST="$BASE/history.tsv"
# reuse the learned ratios so T=4g produces the big split (non-destructive copy)
SRCHIST=$(ls -t "$REPO"/data/nds-treader-*/history.tsv | head -1)
cp -n "$SRCHIST" "$HIST"
echo "seeded history from $SRCHIST ($(wc -l < "$HIST") rows)"

find "$DATA" -name '*.parquet' -print0 | xargs -0 cat > /dev/null 2>&1 || true

# $1 label, $2 columnSizeBytes conf value ("" = default)
run_pass() {
  local pass="$1" cval="${2:-}"
  echo ""; echo "########## q28 $pass (columnSizeBytes=${cval:-default 2g}) ##########"
  local ELDIR="$BASE/el-$pass" WORK="$BASE/work-$pass"; mkdir -p "$ELDIR" "$WORK/tmp"
  local CCONF=(); [ -n "$cval" ] && CCONF=(--conf "spark.rapids.sql.columnSizeBytes=$cval")
  "$SPARK_HOME/bin/spark-submit" \
    --master 'local[16]' --driver-memory 16G --conf spark.driver.maxResultSize=2GB \
    --conf spark.executor.cores=16 --conf spark.executor.memory=16G \
    --conf spark.plugins=com.nvidia.spark.SQLPlugin --conf spark.rapids.sql.enabled=true \
    --conf spark.rapids.sql.metrics.level=DEBUG \
    --conf spark.rapids.sql.scan.splitAutotuner.historyPath="$HIST" \
    --conf spark.rapids.sql.scan.targetDecodedBytesPerTask=4g \
    "${CCONF[@]}" \
    --conf spark.sql.adaptive.enabled=true --conf spark.rapids.memory.pinnedPool.size=8g \
    --conf spark.rapids.sql.concurrentGpuTasks=2 \
    --conf spark.shuffle.manager=com.nvidia.spark.rapids.spark353.RapidsShuffleManager \
    --conf spark.rapids.filecache.enabled=false --conf spark.sql.catalogImplementation=in-memory \
    --conf spark.local.dir="$WORK/tmp" \
    --conf spark.eventLog.enabled=true --conf spark.eventLog.dir="$ELDIR" \
    --jars "$JAR,$LISTENER" --driver-class-path "$JAR:$LISTENER" \
    --conf spark.executor.extraClassPath="$JAR:$LISTENER" \
    "$BENCH/nds_power.py" "$DATA" "$STREAM" "$WORK/times-$pass.csv" \
    --input_format parquet --json_summary_folder "$WORK/json" --allow_failure \
    > "$BASE/run-$pass.log" 2>&1
  echo "  $pass done"
}

run_pass ctrl   ""
run_pass col512 "512m"
echo ""; echo "BASE=$BASE"
