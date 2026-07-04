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
if [[ "$#" -ne 1 ]]; then
  echo "usage: $0 <128|2048|16384>" >&2
  exit 2
fi
P="$1"
case "$P" in
  128|2048|16384) ;;
  *) echo "unsupported partition treatment: $P" >&2; exit 2 ;;
esac

REPO="$(git rev-parse --show-toplevel)"
EXP="$REPO/docs/experiments/profiled-gpu-tuning-poc"
PARENT="$REPO/docs/experiments/uncompressed-size-shmoo"
ATTEMPT="$EXP/attempts/nsys-$P-001"
SPARK=/home/roberte/src/spark_3.5.5/bin/spark-submit
JAR="$REPO/dist/target/rapids-4-spark_2.12-26.08.0-SNAPSHOT-cuda12.jar"
NSYS=/opt/nvidia/nsight-compute/2025.2.1/host/target-linux-x64/nsys

mkdir -p "$ATTEMPT/raw" "$ATTEMPT/analysis" "$ATTEMPT/stdout"
test ! -e "$ATTEMPT/raw/journal.jsonl"
test ! -e "$ATTEMPT/eventlog"
test ! -e "$ATTEMPT/raw/profile.nsys-rep"

"$NSYS" profile --trace=cuda,nvtx,osrt --sample=none --cpuctxsw=none \
  --gpu-metrics-devices=0 --gpu-metrics-frequency=1000 \
  --stats=true --export=sqlite --force-overwrite=true \
  --output "$ATTEMPT/raw/profile" \
  "$SPARK" --master 'local[8]' --driver-memory 24g \
  --conf spark.plugins=com.nvidia.spark.SQLPlugin \
  --conf "spark.driver.extraClassPath=$JAR" \
  --conf "spark.executor.extraClassPath=$JAR" \
  --conf "spark.driver.extraJavaOptions=-Dai.rapids.cudf.nvtx.enabled=true" \
  --conf "spark.executor.extraJavaOptions=-Dai.rapids.cudf.nvtx.enabled=true" \
  --jars "$JAR" "$PARENT/scripts/benchmark.py" \
  --data-dir "$REPO/taxi-data-sharded" \
  --schedule "$EXP/preregistration/cupti-$P.json" \
  --journal "$ATTEMPT/raw/journal.jsonl" \
  --event-log-dir "$ATTEMPT/eventlog" \
  --concurrent-gpu-tasks 4 --dynamic-concurrency true \
  > "$ATTEMPT/stdout/nsys.txt" 2>&1

EVENT_LOG="$(find "$ATTEMPT/eventlog" -type f | head -n 1)"
gzip -n -c "$EVENT_LOG" > "$ATTEMPT/raw/eventlog.json.gz"
rm -rf "$ATTEMPT/eventlog"
mv "$ATTEMPT/raw/profile.sqlite" "$ATTEMPT/analysis/profile.sqlite"
