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

ATTEMPT="$EXP/attempts/jfr-001"
mkdir -p "$ATTEMPT/raw" "$ATTEMPT/stdout"
test ! -e "$ATTEMPT/raw/journal.jsonl"
test ! -e "$ATTEMPT/eventlog"
test ! -e "$ATTEMPT/raw/profile.jfr"

nvidia-smi --query-gpu=timestamp,utilization.gpu,utilization.memory,memory.used,power.draw,clocks.sm,clocks.mem --format=csv -lms 100 -f "$ATTEMPT/raw/nvidia-smi.csv" &
SMI_PID=$!
trap 'kill "$SMI_PID" 2>/dev/null || true' EXIT

"$SPARK" --master 'local[8]' --driver-memory 24g \
  --conf spark.plugins=com.nvidia.spark.SQLPlugin \
  --conf "spark.driver.extraClassPath=$JAR" \
  --conf "spark.executor.extraClassPath=$JAR" \
  --conf "spark.driver.extraJavaOptions=-XX:StartFlightRecording=filename=$ATTEMPT/raw/profile.jfr,settings=profile,dumponexit=true" \
  --jars "$JAR" "$BENCH" \
  --data-dir "$DATA" \
  --schedule "$EXP/preregistration/jfr-schedule.json" \
  --journal "$ATTEMPT/raw/journal.jsonl" \
  --event-log-dir "$ATTEMPT/eventlog" \
  --concurrent-gpu-tasks 4 --dynamic-concurrency true \
  > "$ATTEMPT/stdout/spark.txt" 2>&1

kill "$SMI_PID" 2>/dev/null || true
wait "$SMI_PID" 2>/dev/null || true
trap - EXIT

EVENT_LOG="$(find "$ATTEMPT/eventlog" -type f | head -n 1)"
gzip -n -c "$EVENT_LOG" > "$ATTEMPT/raw/eventlog.json.gz"
rm -rf "$ATTEMPT/eventlog"
