#!/usr/bin/env bash
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

if [[ $# -lt 1 || $# -gt 3 ]]; then
  echo "usage: $0 RUN_ID [DATASET|all] [LIMIT_WINDOWS]" >&2
  exit 2
fi

RUN_ID="$1"
DATASET_FILTER="${2:-all}"
LIMIT_WINDOWS="${3:-}"
REPO="$(git rev-parse --show-toplevel)"
EXP="$REPO/docs/experiments/rolling-split-autotuning"
SPARK_HOME="${SPARK_HOME:-/home/roberte/src/spark_3.5.5}"
RAPIDS_JAR="${RAPIDS_JAR:-$REPO/dist/target/rapids-4-spark_2.12-26.08.0-SNAPSHOT-cuda12.jar}"
ROOT="/data/tmp/rolling-split-autotuning-$RUN_ID"

if [[ -e "$ROOT" ]]; then
  echo "refusing to reuse $ROOT" >&2
  exit 2
fi
test -x "$SPARK_HOME/bin/spark-submit"
test -f "$RAPIDS_JAR"
mkdir -p "$ROOT"/{raw,analysis,stdout,provenance,spark-local,java-tmp,warehouse,eventlog}

DATASETS=(yellow green for-hire high-volume-for-hire fre-crt-stacr-dnhq)
if [[ "$DATASET_FILTER" != "all" ]]; then
  DATASETS=("$DATASET_FILTER")
fi

{
  echo "run_id=$RUN_ID"
  echo "repo_revision=$(git rev-parse HEAD)"
  echo "spark_home=$SPARK_HOME"
  echo "rapids_jar=$RAPIDS_JAR"
  echo "scratch_root=$ROOT"
  echo "dataset_filter=$DATASET_FILTER"
  echo "limit_windows=${LIMIT_WINDOWS:-none}"
} > "$ROOT/provenance/environment.txt"
nvidia-smi --query-gpu=name,memory.total,driver_version --format=csv,noheader   > "$ROOT/provenance/gpu.txt"
sha256sum   "$EXP/README.md"   "$EXP/manifest.yaml"   "$EXP/schedule.json"   "$EXP/scripts/benchmark.py"   "$EXP/scripts/validate_cpu.py"   "$EXP/scripts/analyze.py"   "$EXP/scripts/validate_preregistration.py"   "$EXP/scripts/run_experiment.sh"   "$REPO/docs/experiments/uncompressed-size-shmoo/scripts/extract_scan_metrics.py"   "$RAPIDS_JAR" > "$ROOT/provenance/executed-input-sha256.txt"

COMMON=(
  --master 'local[8]'
  --driver-memory 48g
  --conf "spark.local.dir=$ROOT/spark-local"
  --conf "spark.sql.warehouse.dir=$ROOT/warehouse"
  --conf "spark.driver.extraJavaOptions=-Djava.io.tmpdir=$ROOT/java-tmp"
  --conf "spark.executor.extraJavaOptions=-Djava.io.tmpdir=$ROOT/java-tmp"
  --conf spark.sql.adaptive.enabled=false
  --conf spark.sql.shuffle.partitions=32
  --conf spark.sql.files.minPartitionNum=1
  --conf spark.sql.files.openCostInBytes=4194304
  --conf spark.sql.files.maxPartitionBytes=134217728
)

PACKAGES=()
for DATASET in "${DATASETS[@]}"; do
  DATASET_ROOT="$ROOT/$DATASET"
  EVENT_DIR="$ROOT/eventlog/$DATASET"
  mkdir -p "$DATASET_ROOT" "$EVENT_DIR"
  LIMIT_ARGS=()
  if [[ -n "$LIMIT_WINDOWS" ]]; then
    LIMIT_ARGS=(--limit-windows "$LIMIT_WINDOWS")
  fi

  SPARK_LOCAL_DIRS="$ROOT/spark-local" "$SPARK_HOME/bin/spark-submit"     "${COMMON[@]}"     --conf spark.plugins=com.nvidia.spark.SQLPlugin     --conf "spark.driver.extraClassPath=$RAPIDS_JAR"     --conf "spark.executor.extraClassPath=$RAPIDS_JAR"     --conf spark.eventLog.enabled=true     --conf "spark.eventLog.dir=file://$EVENT_DIR"     --conf spark.rapids.sql.metrics.level=DEBUG     --conf spark.rapids.sql.explain=ALL     --conf spark.rapids.sql.concurrentGpuTasks=2     --conf spark.rapids.sql.concurrentGpuTasks.dynamic=false     --conf spark.rapids.sql.batchSizeBytes=1073741824     --conf spark.rapids.sql.reader.batchSizeBytes=2147483648     --conf spark.rapids.sql.reader.batchSizeRows=2147483647     --jars "$RAPIDS_JAR"     "$EXP/scripts/benchmark.py"     --schedule "$EXP/schedule.json"     --dataset "$DATASET"     --journal "$DATASET_ROOT/run-journal.jsonl"     --results "$DATASET_ROOT/results.jsonl"     --history-output "$DATASET_ROOT/history.json"     "${LIMIT_ARGS[@]}" > "$ROOT/stdout/$DATASET-gpu.txt" 2>&1

  mapfile -t EVENT_LOGS < <(find "$EVENT_DIR" -maxdepth 1 -type f)
  if [[ "${#EVENT_LOGS[@]}" -ne 1 ]]; then
    echo "expected one event log for $DATASET, found ${#EVENT_LOGS[@]}" >&2
    exit 1
  fi
  gzip -n -c "${EVENT_LOGS[0]}" > "$DATASET_ROOT/eventlog.json.gz"
  python3     "$REPO/docs/experiments/uncompressed-size-shmoo/scripts/extract_scan_metrics.py"     --event-log "$DATASET_ROOT/eventlog.json.gz"     --journal "$DATASET_ROOT/results.jsonl"     --output "$DATASET_ROOT/scan-summary.json"

  SPARK_LOCAL_DIRS="$ROOT/spark-local" "$SPARK_HOME/bin/spark-submit"     "${COMMON[@]}"     "$EXP/scripts/validate_cpu.py"     --schedule "$EXP/schedule.json"     --dataset "$DATASET"     --gpu-results "$DATASET_ROOT/results.jsonl"     --output "$DATASET_ROOT/cpu-validation.json"     "${LIMIT_ARGS[@]}" > "$ROOT/stdout/$DATASET-cpu.txt" 2>&1

  PACKAGES+=(--dataset-package     "$DATASET=$DATASET_ROOT/results.jsonl,$DATASET_ROOT/scan-summary.json,$DATASET_ROOT/cpu-validation.json")
done

python3 "$EXP/scripts/analyze.py"   "${PACKAGES[@]}"   --output "$ROOT/analysis/result.json"

find "$ROOT" -type f ! -path "$ROOT/provenance/artifact-sha256.txt" -print0   | sort -z   | xargs -0 sha256sum > "$ROOT/provenance/artifact-sha256.txt"
echo "$ROOT"
