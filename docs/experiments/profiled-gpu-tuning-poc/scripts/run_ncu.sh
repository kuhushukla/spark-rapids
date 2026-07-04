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
REPO="$(git rev-parse --show-toplevel)"
EXP="$REPO/docs/experiments/profiled-gpu-tuning-poc"
PARENT="$REPO/docs/experiments/uncompressed-size-shmoo"
SPARK=/home/roberte/src/spark_3.5.5/bin/spark-submit
JAR="$REPO/dist/target/rapids-4-spark_2.12-26.08.0-SNAPSHOT-cuda12.jar"
DATA="$REPO/taxi-data-sharded"
BENCH="$PARENT/scripts/benchmark.py"

if [[ "$#" -ne 1 ]]; then
  echo "usage: $0 <128|2048|16384>" >&2
  exit 2
fi
P="$1"
case "$P" in
  128|2048|16384) ;;
  *) echo "unsupported partition treatment: $P" >&2; exit 2 ;;
esac

ATTEMPT="$EXP/attempts/ncu-$P-001"
mkdir -p "$ATTEMPT/raw" "$ATTEMPT/analysis" "$ATTEMPT/stdout"
test ! -e "$ATTEMPT/raw/journal.jsonl"
test ! -e "$ATTEMPT/eventlog"
test ! -e "$ATTEMPT/raw/parquet-decode.ncu-rep"

ncu --target-processes all --nvtx --nvtx-include "Parquet decode" \
  --set basic --launch-count 40 --force-overwrite \
  --export "$ATTEMPT/raw/parquet-decode" \
  "$SPARK" --master 'local[8]' --driver-memory 24g \
  --conf spark.plugins=com.nvidia.spark.SQLPlugin \
  --conf "spark.driver.extraClassPath=$JAR" \
  --conf "spark.executor.extraClassPath=$JAR" \
  --jars "$JAR" "$BENCH" \
  --data-dir "$DATA" \
  --schedule "$EXP/preregistration/ncu-$P.json" \
  --journal "$ATTEMPT/raw/journal.jsonl" \
  --event-log-dir "$ATTEMPT/eventlog" \
  --concurrent-gpu-tasks 4 --dynamic-concurrency true \
  > "$ATTEMPT/stdout/ncu.txt" 2>&1

EVENT_LOG="$(find "$ATTEMPT/eventlog" -type f | head -n 1)"
gzip -n -c "$EVENT_LOG" > "$ATTEMPT/raw/eventlog.json.gz"
rm -rf "$ATTEMPT/eventlog"
ncu --import "$ATTEMPT/raw/parquet-decode.ncu-rep" --csv --page raw \
  > "$ATTEMPT/analysis/kernels.csv" 2> "$ATTEMPT/stdout/ncu-export.txt"
