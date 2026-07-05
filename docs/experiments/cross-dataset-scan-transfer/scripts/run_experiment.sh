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

if [[ $# -ne 1 ]]; then
  echo "usage: $0 RUN_ID" >&2
  exit 2
fi

RUN_ID="$1"
REPO="$(git rev-parse --show-toplevel)"
EXP="$REPO/docs/experiments/cross-dataset-scan-transfer"
SPARK_HOME="${SPARK_HOME:-/home/roberte/src/spark_3.5.5}"
RAPIDS_JAR="${RAPIDS_JAR:-$REPO/dist/target/rapids-4-spark_2.12-26.08.0-SNAPSHOT-cuda12.jar}"
ROOT="/data/tmp/cross-dataset-scan-transfer-$RUN_ID"
LOCAL="$ROOT/spark-local"
EVENT_DIR="$ROOT/eventlog"
RAW="$ROOT/raw"
ANALYSIS="$ROOT/analysis"
STDOUT="$ROOT/stdout"
PROVENANCE="$ROOT/provenance"

if [[ -e "$ROOT" ]]; then
  echo "refusing to reuse $ROOT" >&2
  exit 2
fi
test -x "$SPARK_HOME/bin/spark-submit"
test -f "$RAPIDS_JAR"
mkdir -p "$LOCAL" "$EVENT_DIR" "$RAW" "$ANALYSIS" "$STDOUT" "$PROVENANCE"

{
  echo "run_id=$RUN_ID"
  echo "repo_revision=$(git rev-parse HEAD)"
  echo "spark_home=$SPARK_HOME"
  echo "rapids_jar=$RAPIDS_JAR"
  echo "data_root=/data/public/tlc-trip-record-data"
  echo "scratch_root=$ROOT"
} > "$PROVENANCE/environment.txt"
sha256sum \
  "$EXP/manifest.yaml" \
  "$EXP/schedule.json" \
  "$EXP/scripts/benchmark.py" \
  "$EXP/scripts/analyze.py" \
  "$REPO/docs/experiments/uncompressed-size-shmoo/scripts/extract_scan_metrics.py" \
  "$RAPIDS_JAR" > "$PROVENANCE/sha256.txt"
nvidia-smi --query-gpu=name,memory.total,driver_version --format=csv,noheader \
  > "$PROVENANCE/gpu.txt"

COMMON=(
  --master 'local[8]'
  --driver-memory 24g
  --conf "spark.local.dir=$LOCAL"
  --conf "spark.sql.warehouse.dir=$ROOT/warehouse"
  --conf "spark.driver.extraJavaOptions=-Djava.io.tmpdir=$ROOT/java-tmp"
  --conf "spark.executor.extraJavaOptions=-Djava.io.tmpdir=$ROOT/java-tmp"
  --conf spark.sql.adaptive.enabled=false
  --conf spark.sql.shuffle.partitions=32
  --conf spark.sql.files.minPartitionNum=1
  --conf spark.sql.files.openCostInBytes=4194304
  --conf spark.sql.files.maxPartitionBytes=536870912
)

SPARK_LOCAL_DIRS="$LOCAL" "$SPARK_HOME/bin/spark-submit" \
  "${COMMON[@]}" \
  "$EXP/scripts/benchmark.py" \
  --schedule "$EXP/schedule.json" \
  --journal "$RAW/cpu-journal.jsonl" \
  --mode cpu > "$STDOUT/cpu.txt" 2>&1

SPARK_LOCAL_DIRS="$LOCAL" "$SPARK_HOME/bin/spark-submit" \
  "${COMMON[@]}" \
  --conf spark.plugins=com.nvidia.spark.SQLPlugin \
  --conf "spark.driver.extraClassPath=$RAPIDS_JAR" \
  --conf "spark.executor.extraClassPath=$RAPIDS_JAR" \
  --conf spark.eventLog.enabled=true \
  --conf "spark.eventLog.dir=file://$EVENT_DIR" \
  --conf spark.rapids.sql.metrics.level=DEBUG \
  --conf spark.rapids.sql.explain=ALL \
  --conf spark.rapids.sql.concurrentGpuTasks=2 \
  --conf spark.rapids.sql.concurrentGpuTasks.dynamic=false \
  --conf spark.rapids.sql.batchSizeBytes=1073741824 \
  --conf spark.rapids.sql.reader.batchSizeBytes=2147483648 \
  --conf spark.rapids.sql.reader.batchSizeRows=2147483647 \
  --jars "$RAPIDS_JAR" \
  "$EXP/scripts/benchmark.py" \
  --schedule "$EXP/schedule.json" \
  --journal "$RAW/gpu-journal.jsonl" \
  --mode gpu > "$STDOUT/gpu.txt" 2>&1

mapfile -t EVENT_LOGS < <(find "$EVENT_DIR" -maxdepth 1 -type f)
if [[ "${#EVENT_LOGS[@]}" -ne 1 ]]; then
  echo "expected one event log, found ${#EVENT_LOGS[@]}" >&2
  exit 1
fi
gzip -n -c "${EVENT_LOGS[0]}" > "$RAW/eventlog.json.gz"

python3 \
  "$REPO/docs/experiments/uncompressed-size-shmoo/scripts/extract_scan_metrics.py" \
  --event-log "$RAW/eventlog.json.gz" \
  --journal "$RAW/gpu-journal.jsonl" \
  --output "$ANALYSIS/scan-summary.json"

python3 "$EXP/scripts/analyze.py" \
  --cpu-journal "$RAW/cpu-journal.jsonl" \
  --gpu-journal "$RAW/gpu-journal.jsonl" \
  --scan-summary "$ANALYSIS/scan-summary.json" \
  --output "$ANALYSIS/result.json"

sha256sum "$RAW"/* "$ANALYSIS"/* "$STDOUT"/* > "$PROVENANCE/artifact-sha256.txt"
echo "$ROOT"
