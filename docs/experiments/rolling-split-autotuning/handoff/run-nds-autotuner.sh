#!/usr/bin/env bash
# Real NDS (TPC-DS-like) power run with the scan split autotuner enabled, on the A5000.
# Runs nds_power.py TWICE over the same history file:
#   Run 1 (cold): no history -> COLD_START, default 128 MiB splits, records per-table ratios.
#   Run 2 (warm): history present -> DECIDED, autotuned per-table splits.
# Compares real query runtimes cold vs warm.
#
# Usage:
#   bash docs/experiments/rolling-split-autotuning/handoff/run-nds-autotuner.sh [query_subset]
#   e.g. bash .../run-nds-autotuner.sh query9,query67,query76      (default subset)
#        bash .../run-nds-autotuner.sh ALL                          (full 99-query power run)
#
# Config mirrors the prior manual maxPartitionBytes sweep
# (nds_sf100/scan-batch-sizing-local-results-20260623) for comparability, EXCEPT:
#   - GPU pinned to the A5000 by UUID (prior run had no GPU pinning; see mechanics doc).
#   - spark.rapids.filecache.enabled=false so the cold->warm delta is attributable to
#     split sizing, not file caching.
#   - Uses THIS branch's freshly-built plugin JAR (contains the autotuner).

set -euo pipefail

SUBSET="${1:-query9,query67,query76}"
REPO=$(cd "$(dirname "$0")/../../../.." && pwd)
AB=/home/kuhu/Reps/ab
BENCH=/home/kuhu/Reps/spark-rapids-benchmarks/nds
TS=$(date +%Y%m%d_%H%M%S)
RESULTS_DIR="$REPO/docs/experiments/rolling-split-autotuning/results"
WORK="$REPO/data/nds-autotuner-${TS}"
LOG="$REPO/data/nds-autotuner-${TS}.log"
HIST="$REPO/data/nds-scan-split-history.tsv"
EVENTLOG_DIR=/home/kuhu/logdir
DATA="$AB/nds_sf100/parquet_sf100_decimal_fresh_20260623"
STREAM="$AB/query_0.sql"
PLUGIN_JAR="$REPO/dist/target/rapids-4-spark_2.12-26.08.0-SNAPSHOT-cuda12.jar"
LISTENER_JAR="$AB/resources/nds-benchmark-listener-1.0-SNAPSHOT.jar"

export SPARK_HOME=/home/kuhu/Downloads/spark-3.5.3-bin-hadoop3
export JAVA_HOME=/usr/lib/jvm/java-1.17.0-openjdk-amd64
# A5000 by UUID (NOT numeric index — CUDA FASTEST_FIRST ordering makes "1" == T400).
export CUDA_VISIBLE_DEVICES=GPU-1aaa66fd-0c1e-935b-fe65-2c9ca7357504

mkdir -p "$WORK" "$RESULTS_DIR" "$EVENTLOG_DIR"
# Fresh history for a clean cold start.
rm -f "$HIST"

echo "=== NDS autotuner run $TS ===" | tee "$LOG"
echo "Subset:   $SUBSET" | tee -a "$LOG"
echo "Data:     $DATA" | tee -a "$LOG"
echo "JAR:      $PLUGIN_JAR" | tee -a "$LOG"
echo "GPU:      A5000 (GPU-1aaa66fd)" | tee -a "$LOG"
echo "batchSizeBytes: ${BATCH:-1g}" | tee -a "$LOG"
nvidia-smi --query-gpu=index,name,memory.free --format=csv,noheader | tee -a "$LOG"

SUBSET_ARG=()
if [ "$SUBSET" != "ALL" ]; then SUBSET_ARG=(--sub_queries "$SUBSET"); fi

run_pass() {
  local pass="$1"   # cold | warm
  echo "" | tee -a "$LOG"
  echo "########## NDS $pass ##########" | tee -a "$LOG"
  "$SPARK_HOME/bin/spark-submit" \
    --master 'local[16]' \
    --conf spark.driver.maxResultSize=2GB \
    --conf spark.driver.memory=16G \
    --conf spark.executor.cores=16 \
    --conf spark.executor.instances=1 \
    --conf spark.executor.memory=16G \
    --conf spark.sql.files.maxPartitionBytes=128m \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.plugins=com.nvidia.spark.SQLPlugin \
    --conf spark.rapids.memory.host.spillStorageSize=16G \
    --conf spark.rapids.memory.pinnedPool.size=8g \
    --conf spark.rapids.sql.concurrentGpuTasks=2 \
    --conf spark.sql.adaptive.coalescePartitions.minPartitionSize=32mb \
    --conf spark.sql.adaptive.advisoryPartitionSizeInBytes=160mb \
    --conf spark.shuffle.manager=com.nvidia.spark.rapids.spark353.RapidsShuffleManager \
    --conf spark.rapids.sql.multiThreadedRead.numThreads=64 \
    --conf spark.rapids.filecache.enabled=false \
    --conf spark.rapids.sql.batchSizeBytes="${BATCH:-1g}" \
    --conf spark.rapids.sql.metrics.level=DEBUG \
    --conf spark.rapids.sql.scan.splitAutotuner.historyPath="$HIST" \
    --conf spark.sql.catalogImplementation=in-memory \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir="$EVENTLOG_DIR" \
    --conf spark.local.dir="$WORK/spark-tmp" \
    --jars "$PLUGIN_JAR,$LISTENER_JAR" \
    --driver-class-path "$PLUGIN_JAR:$LISTENER_JAR" \
    --conf spark.executor.extraClassPath="$PLUGIN_JAR:$LISTENER_JAR" \
    "$BENCH/nds_power.py" \
    "$DATA" \
    "$STREAM" \
    "$WORK/times-${pass}.csv" \
    --input_format parquet \
    "${SUBSET_ARG[@]}" \
    --json_summary_folder "$WORK/json-${pass}" \
    2>&1 | tee -a "$LOG"
}

run_pass cold
run_pass warm

echo "" | tee -a "$LOG"
echo "=== Autotuner decisions ===" | tee -a "$LOG"
grep -aE "COLD_START|DECIDED|RECORDED|SKIPPED" "$LOG" | sed 's/.*ScanSplitAutotuner\] //' | tee -a "$LOG" || true

echo "" | tee -a "$LOG"
echo "=== Per-query times (cold vs warm) ===" | tee -a "$LOG"
echo "times-cold.csv:" | tee -a "$LOG"; cat "$WORK/times-cold.csv" 2>/dev/null | tee -a "$LOG" || true
echo "times-warm.csv:" | tee -a "$LOG"; cat "$WORK/times-warm.csv" 2>/dev/null | tee -a "$LOG" || true
echo "" | tee -a "$LOG"
echo "Log:      $LOG"
echo "History:  $HIST"
echo "Work dir: $WORK"
