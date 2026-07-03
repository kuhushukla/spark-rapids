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

: "${SPARK_HOME:?Set SPARK_HOME to Spark 3.5.5}"
: "${RAPIDS_JAR:?Set RAPIDS_JAR to the tested RAPIDS assembly JAR}"
: "${RUN_ID:?Set RUN_ID to a unique stable identifier}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${ROOT}/../../.." && pwd)"
EXPECTED_DATA="${REPO_ROOT}/yellow_tripdata_2020-01.parquet"
DATA="${DATA:-${EXPECTED_DATA}}"
if [[ "$(realpath "${DATA}")" != "${EXPECTED_DATA}" ]]; then
  echo "DATA must be the exact repository-root snapshot: ${EXPECTED_DATA}" >&2
  exit 2
fi
[[ -f "${DATA}" ]] || { echo "Missing dataset: ${DATA}" >&2; exit 2; }
[[ -f "${RAPIDS_JAR}" ]] || { echo "Missing RAPIDS JAR: ${RAPIDS_JAR}" >&2; exit 2; }

RUN_PARENT="${RUN_PARENT:-/tmp/max-partition-bytes}"
RUN_ROOT="${RUN_PARENT}/${RUN_ID}"
if [[ -e "${RUN_ROOT}" ]]; then
  echo "Refusing to reuse run directory: ${RUN_ROOT}" >&2
  exit 2
fi
mkdir -p   "${RUN_ROOT}/analysis"   "${RUN_ROOT}/eventlogs/cpu"   "${RUN_ROOT}/eventlogs/gpu"   "${RUN_ROOT}/provenance"   "${RUN_ROOT}/raw"   "${RUN_ROOT}/stdout"

WRAPPER_JOURNAL="${RUN_ROOT}/raw/wrapper-attempts.jsonl"
printf '{"event":"wrapper_start","run_id":"%s"}\n' "${RUN_ID}" >> "${WRAPPER_JOURNAL}"
DONE=0
on_exit() {
  code=$?
  if [[ ${DONE} -eq 0 ]]; then
    printf '{"event":"wrapper_terminal","status":"error_or_abort","exit_code":%d}\n'       "${code}" >> "${WRAPPER_JOURNAL}"
  fi
}
trap on_exit EXIT

{
  uname -a
  echo "repo_sha $(git -C "${REPO_ROOT}" rev-parse HEAD)"
  echo "dataset_path ${DATA}"
  sha256sum "${DATA}"
  sha256sum "${RAPIDS_JAR}"
  "${SPARK_HOME}/bin/spark-submit" --version
  java -version
  nvidia-smi --query-gpu=name,memory.total,driver_version --format=csv,noheader
  lscpu
  sed -n '1,5p' /proc/meminfo
} > "${RUN_ROOT}/provenance/environment.txt" 2>&1
sha256sum "${ROOT}/run_experiment.sh" "${ROOT}"/scripts/*.py   > "${RUN_ROOT}/provenance/executed-code-sha256.txt"

timeout --signal=TERM --kill-after=30s 300s   "${SPARK_HOME}/bin/spark-submit" --master local[8]   "${ROOT}/scripts/inspect_parquet.py"   --data "${DATA}"   --output "${RUN_ROOT}/analysis/parquet-metadata.json"   > "${RUN_ROOT}/stdout/metadata.stdout.txt"   2> "${RUN_ROOT}/stdout/metadata.stderr.txt"
printf '{"event":"metadata_complete","status":"success"}\n' >> "${WRAPPER_JOURNAL}"

timeout --signal=TERM --kill-after=30s 300s   "${SPARK_HOME}/bin/spark-submit" --master local[8]   "${ROOT}/scripts/benchmark.py"   --mode cpu   --data "${DATA}"   --event-log-dir "${RUN_ROOT}/eventlogs/cpu"   --journal "${RUN_ROOT}/raw/cpu-journal.jsonl"   --output "${RUN_ROOT}/analysis/cpu-reference.json"   > "${RUN_ROOT}/stdout/cpu.stdout.txt"   2> "${RUN_ROOT}/stdout/cpu.stderr.txt"
printf '{"event":"cpu_complete","status":"success"}\n' >> "${WRAPPER_JOURNAL}"

mapfile -t cpu_logs < <(find "${RUN_ROOT}/eventlogs/cpu" -maxdepth 1 -type f -name 'local-*')
if [[ ${#cpu_logs[@]} -ne 1 ]]; then
  echo "Expected exactly one CPU event log, found ${#cpu_logs[@]}" >&2
  exit 3
fi
CPU_SHA="$(python3 "${ROOT}/scripts/extract_cpu_sha.py"   "${RUN_ROOT}/analysis/cpu-reference.json")"

timeout --signal=TERM --kill-after=30s 1200s   "${SPARK_HOME}/bin/spark-submit" --master local[8]   --jars "${RAPIDS_JAR}"   --conf spark.plugins=com.nvidia.spark.SQLPlugin   --conf spark.driver.extraClassPath="${RAPIDS_JAR}"   --conf spark.executor.extraClassPath="${RAPIDS_JAR}"   "${ROOT}/scripts/benchmark.py"   --mode gpu   --expected-result-sha "${CPU_SHA}"   --data "${DATA}"   --event-log-dir "${RUN_ROOT}/eventlogs/gpu"   --journal "${RUN_ROOT}/raw/gpu-journal.jsonl"   --output "${RUN_ROOT}/analysis/gpu-benchmark.json"   > "${RUN_ROOT}/stdout/gpu.stdout.txt"   2> "${RUN_ROOT}/stdout/gpu.stderr.txt"
printf '{"event":"gpu_complete","status":"success"}\n' >> "${WRAPPER_JOURNAL}"

mapfile -t gpu_logs < <(find "${RUN_ROOT}/eventlogs/gpu" -maxdepth 1 -type f -name 'local-*')
if [[ ${#gpu_logs[@]} -ne 1 ]]; then
  echo "Expected exactly one GPU event log, found ${#gpu_logs[@]}" >&2
  exit 3
fi

gzip -n -c "${cpu_logs[0]}" > "${RUN_ROOT}/raw/cpu-eventlog.json.gz"
gzip -n -c "${gpu_logs[0]}" > "${RUN_ROOT}/raw/gpu-eventlog.json.gz"

python3 "${ROOT}/scripts/validate_experiment.py"   --cpu-journal "${RUN_ROOT}/raw/cpu-journal.jsonl"   --gpu-journal "${RUN_ROOT}/raw/gpu-journal.jsonl"   --cpu-output "${RUN_ROOT}/analysis/cpu-reference.json"   --gpu-output "${RUN_ROOT}/analysis/gpu-benchmark.json"   --cpu-plans "${RUN_ROOT}/analysis/cpu-reference.json.plans.json"   --gpu-plans "${RUN_ROOT}/analysis/gpu-benchmark.json.plans.json"   --cpu-event-log "${RUN_ROOT}/raw/cpu-eventlog.json.gz"   --gpu-event-log "${RUN_ROOT}/raw/gpu-eventlog.json.gz"   --output "${RUN_ROOT}/analysis/validated-analysis.json"
printf '{"event":"validation_complete","status":"success"}\n' >> "${WRAPPER_JOURNAL}"

python3 "${ROOT}/scripts/validate_experiment.py"   --cpu-journal "${RUN_ROOT}/raw/cpu-journal.jsonl"   --gpu-journal "${RUN_ROOT}/raw/gpu-journal.jsonl"   --cpu-output "${RUN_ROOT}/analysis/cpu-reference.json"   --gpu-output "${RUN_ROOT}/analysis/gpu-benchmark.json"   --cpu-plans "${RUN_ROOT}/analysis/cpu-reference.json.plans.json"   --gpu-plans "${RUN_ROOT}/analysis/gpu-benchmark.json.plans.json"   --cpu-event-log "${RUN_ROOT}/raw/cpu-eventlog.json.gz"   --gpu-event-log "${RUN_ROOT}/raw/gpu-eventlog.json.gz"   --output "${RUN_ROOT}/analysis/replay-analysis.json"
cmp "${RUN_ROOT}/analysis/validated-analysis.json"   "${RUN_ROOT}/analysis/replay-analysis.json"
rm "${RUN_ROOT}/analysis/replay-analysis.json"
printf '{"event":"replay_complete","status":"success"}\n' >> "${WRAPPER_JOURNAL}"

(
  cd "${RUN_ROOT}"
  find analysis raw stdout provenance -type f \
    ! -path 'provenance/checksums.txt' \
    ! -path 'raw/wrapper-attempts.jsonl' -print0 \
    | sort -z | xargs -0 sha256sum
) > "${RUN_ROOT}/provenance/checksums.txt"
(
  cd "${RUN_ROOT}"
  sha256sum -c provenance/checksums.txt
)
printf '{"event":"packaging_verified","status":"success","timestamp":"%s"}\n' \
  "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "${WRAPPER_JOURNAL}"
printf '{"event":"wrapper_terminal","status":"success","timestamp":"%s"}\n' \
  "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "${WRAPPER_JOURNAL}"
DONE=1
echo "Artifacts: ${RUN_ROOT}"
