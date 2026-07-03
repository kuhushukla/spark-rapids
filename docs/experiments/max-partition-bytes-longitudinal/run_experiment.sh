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

if [[ "${LONGITUDINAL_DEADLINE_ACTIVE:-0}" != "1" ]]; then
  exec timeout --signal=TERM --kill-after=30s 1800s     env LONGITUDINAL_DEADLINE_ACTIVE=1 "$0"
fi

: "${SPARK_HOME:?Set SPARK_HOME to Spark 3.5.5}"
: "${RAPIDS_JAR:?Set RAPIDS_JAR to the tested RAPIDS assembly JAR}"
: "${RUN_ID:?Set RUN_ID to a unique stable identifier}"
if [[ ! "${RUN_ID}" =~ ^[A-Za-z0-9._-]+$ ]]; then
  echo "RUN_ID must contain only letters, digits, dot, underscore, or dash" >&2
  exit 2
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${ROOT}/../../.." && pwd)"
DATA_DIR="${DATA_DIR:-${REPO_ROOT}/taxi-data}"
EXPECTED_DATA_DIR="${REPO_ROOT}/taxi-data"
if [[ "$(realpath "${DATA_DIR}")" != "${EXPECTED_DATA_DIR}" ]]; then
  echo "DATA_DIR must be the repository taxi-data directory" >&2
  exit 2
fi
[[ -d "${DATA_DIR}" ]] || { echo "Missing data directory" >&2; exit 2; }
[[ -f "${RAPIDS_JAR}" ]] || { echo "Missing RAPIDS JAR" >&2; exit 2; }

CENSUS="${ROOT}/analysis/stage0-census.json"
CPU_PLANNING="${ROOT}/analysis/stage0-planning-compliance.json"
GPU_PLANNING="${ROOT}/analysis/stage0-gpu-planning-compliance.json"
STAGE0="${ROOT}/analysis/stage0-verdict.json"
SOURCES="${ROOT}/analysis/source-sha256.json"
REGISTRY="${ROOT}/preregistration/episode-registry.json"
SCHEDULE="${ROOT}/preregistration/schedule.json"
PREREGISTRATION="${ROOT}/preregistration/preregistration.json"
for input in "${CENSUS}" "${CPU_PLANNING}" "${GPU_PLANNING}" "${STAGE0}" "${SOURCES}" "${REGISTRY}" "${SCHEDULE}" "${PREREGISTRATION}"; do
  [[ -f "${input}" ]] || { echo "Missing frozen input: ${input}" >&2; exit 2; }
done

RUN_PARENT="${RUN_PARENT:-/tmp/max-partition-bytes-longitudinal}"
RUN_ROOT="${RUN_PARENT}/${RUN_ID}"
if [[ -e "${RUN_ROOT}" ]]; then
  echo "Refusing to reuse run directory: ${RUN_ROOT}" >&2
  exit 2
fi
mkdir -p   "${RUN_ROOT}/analysis"   "${RUN_ROOT}/eventlogs/cpu"   "${RUN_ROOT}/eventlogs/gpu"   "${RUN_ROOT}/provenance"   "${RUN_ROOT}/raw"   "${RUN_ROOT}/stdout"

WRAPPER_JOURNAL="${RUN_ROOT}/raw/wrapper-attempts.jsonl"
WRAPPER_START_EPOCH="$(date +%s)"
printf '{"event":"wrapper_start","run_id":"%s","timestamp":"%s","timeout_seconds":1800}\n'   "${RUN_ID}" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "${WRAPPER_JOURNAL}"
DONE=0
on_exit() {
  code=$?
  if [[ ${DONE} -eq 0 ]]; then
    printf '{"event":"wrapper_terminal","status":"error_or_abort","exit_code":%d,"timestamp":"%s"}\n'       "${code}" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "${WRAPPER_JOURNAL}"
  fi
}
trap on_exit EXIT

python3 "${ROOT}/scripts/verify_preregistration.py"   --experiment-root "${ROOT}"   --spark-home "${SPARK_HOME}"   --rapids-jar "${RAPIDS_JAR}"   --snapshot "${PREREGISTRATION}"   --output "${RUN_ROOT}/analysis/preregistration-verification.json"
printf '{"event":"preregistration_verified","status":"success"}\n' >> "${WRAPPER_JOURNAL}"

python3 "${ROOT}/scripts/validate_stage0.py"   --census "${CENSUS}"   --planning-compliance "${CPU_PLANNING}"   --gpu-planning-compliance "${GPU_PLANNING}"   --source-hashes "${SOURCES}"   --output "${RUN_ROOT}/analysis/stage0-replay.json"
cmp "${STAGE0}" "${RUN_ROOT}/analysis/stage0-replay.json"
rm "${RUN_ROOT}/analysis/stage0-replay.json"
printf '{"event":"stage0_replay","status":"success"}\n' >> "${WRAPPER_JOURNAL}"

python3 "${ROOT}/scripts/hash_sources.py"   --data-dir "${DATA_DIR}"   --output "${RUN_ROOT}/analysis/source-sha256.json"
cmp "${SOURCES}" "${RUN_ROOT}/analysis/source-sha256.json"
printf '{"event":"source_identity","status":"success"}\n' >> "${WRAPPER_JOURNAL}"

{
  uname -a
  echo "repo_sha $(git -C "${REPO_ROOT}" rev-parse HEAD)"
  echo "branch $(git -C "${REPO_ROOT}" branch --show-current)"
  echo "dataset_path ${DATA_DIR}"
  sha256sum "${RAPIDS_JAR}"
  "${SPARK_HOME}/bin/spark-submit" --version
  java -version
  nvidia-smi --query-gpu=name,memory.total,driver_version --format=csv,noheader
  lscpu
  sed -n '1,5p' /proc/meminfo
} > "${RUN_ROOT}/provenance/environment.txt" 2>&1

sha256sum "${ROOT}/run_experiment.sh" "${ROOT}"/scripts/*.py   > "${RUN_ROOT}/provenance/executed-code-sha256.txt"
sha256sum "${CENSUS}" "${CPU_PLANNING}" "${GPU_PLANNING}" "${STAGE0}" "${SOURCES}" "${REGISTRY}" "${SCHEDULE}" "${PREREGISTRATION}"   > "${RUN_ROOT}/provenance/frozen-input-sha256.txt"

timeout --signal=TERM --kill-after=30s 300s   "${SPARK_HOME}/bin/spark-submit" --master local[8]   "${ROOT}/scripts/benchmark.py"   --mode cpu   --census "${CENSUS}"   --registry "${REGISTRY}"   --schedule "${SCHEDULE}"   --event-log-dir "${RUN_ROOT}/eventlogs/cpu"   --journal "${RUN_ROOT}/raw/cpu-journal.jsonl"   --output "${RUN_ROOT}/analysis/cpu-reference.json"   --plans "${RUN_ROOT}/analysis/cpu-plans.json"   > "${RUN_ROOT}/stdout/cpu.stdout.txt"   2> "${RUN_ROOT}/stdout/cpu.stderr.txt"
printf '{"event":"cpu_complete","status":"success"}\n' >> "${WRAPPER_JOURNAL}"

mapfile -t cpu_logs < <(find "${RUN_ROOT}/eventlogs/cpu" -maxdepth 1 -type f -name 'local-*')
if [[ ${#cpu_logs[@]} -ne 1 ]]; then
  echo "Expected exactly one CPU event log" >&2
  exit 3
fi

GPU_START_EPOCH="$(date +%s)"
timeout --signal=TERM --kill-after=30s 1200s   "${SPARK_HOME}/bin/spark-submit" --master local[8]   --jars "${RAPIDS_JAR}"   --conf spark.plugins=com.nvidia.spark.SQLPlugin   --conf spark.driver.extraClassPath="${RAPIDS_JAR}"   --conf spark.executor.extraClassPath="${RAPIDS_JAR}"   "${ROOT}/scripts/benchmark.py"   --mode gpu   --census "${CENSUS}"   --registry "${REGISTRY}"   --schedule "${SCHEDULE}"   --cpu-reference "${RUN_ROOT}/analysis/cpu-reference.json"   --event-log-dir "${RUN_ROOT}/eventlogs/gpu"   --journal "${RUN_ROOT}/raw/gpu-journal.jsonl"   --output "${RUN_ROOT}/analysis/gpu-benchmark.json"   --plans "${RUN_ROOT}/analysis/gpu-plans.json"   > "${RUN_ROOT}/stdout/gpu.stdout.txt"   2> "${RUN_ROOT}/stdout/gpu.stderr.txt"
GPU_ELAPSED_SECONDS="$(( $(date +%s) - GPU_START_EPOCH ))"
if [[ ${GPU_ELAPSED_SECONDS} -gt 1230 ]]; then
  echo "GPU phase exceeded 1230 seconds" >&2
  exit 4
fi
printf '{"event":"gpu_complete","status":"success","elapsed_seconds":%d}\n'   "${GPU_ELAPSED_SECONDS}" >> "${WRAPPER_JOURNAL}"

mapfile -t gpu_logs < <(find "${RUN_ROOT}/eventlogs/gpu" -maxdepth 1 -type f -name 'local-*')
if [[ ${#gpu_logs[@]} -ne 1 ]]; then
  echo "Expected exactly one GPU event log" >&2
  exit 3
fi

gzip -n -c "${cpu_logs[0]}" > "${RUN_ROOT}/raw/cpu-eventlog.json.gz"
gzip -n -c "${gpu_logs[0]}" > "${RUN_ROOT}/raw/gpu-eventlog.json.gz"

python3 "${ROOT}/scripts/validate_experiment.py"   --census "${CENSUS}"   --registry "${REGISTRY}"   --schedule "${SCHEDULE}"   --cpu-journal "${RUN_ROOT}/raw/cpu-journal.jsonl"   --gpu-journal "${RUN_ROOT}/raw/gpu-journal.jsonl"   --cpu-output "${RUN_ROOT}/analysis/cpu-reference.json"   --gpu-output "${RUN_ROOT}/analysis/gpu-benchmark.json"   --cpu-plans "${RUN_ROOT}/analysis/cpu-plans.json"   --gpu-plans "${RUN_ROOT}/analysis/gpu-plans.json"   --cpu-event-log "${RUN_ROOT}/raw/cpu-eventlog.json.gz"   --gpu-event-log "${RUN_ROOT}/raw/gpu-eventlog.json.gz"   --stage0-verdict "${STAGE0}"   --preregistration "${PREREGISTRATION}"   --prereg-verification "${RUN_ROOT}/analysis/preregistration-verification.json"   --output "${RUN_ROOT}/analysis/validated-analysis.json"
printf '{"event":"validation_complete","status":"success"}\n' >> "${WRAPPER_JOURNAL}"

python3 "${ROOT}/scripts/validate_experiment.py"   --census "${CENSUS}"   --registry "${REGISTRY}"   --schedule "${SCHEDULE}"   --cpu-journal "${RUN_ROOT}/raw/cpu-journal.jsonl"   --gpu-journal "${RUN_ROOT}/raw/gpu-journal.jsonl"   --cpu-output "${RUN_ROOT}/analysis/cpu-reference.json"   --gpu-output "${RUN_ROOT}/analysis/gpu-benchmark.json"   --cpu-plans "${RUN_ROOT}/analysis/cpu-plans.json"   --gpu-plans "${RUN_ROOT}/analysis/gpu-plans.json"   --cpu-event-log "${RUN_ROOT}/raw/cpu-eventlog.json.gz"   --gpu-event-log "${RUN_ROOT}/raw/gpu-eventlog.json.gz"   --stage0-verdict "${STAGE0}"   --preregistration "${PREREGISTRATION}"   --prereg-verification "${RUN_ROOT}/analysis/preregistration-verification.json"   --output "${RUN_ROOT}/analysis/replay-analysis.json"
cmp "${RUN_ROOT}/analysis/validated-analysis.json"   "${RUN_ROOT}/analysis/replay-analysis.json"
rm "${RUN_ROOT}/analysis/replay-analysis.json"
printf '{"event":"replay_complete","status":"success"}\n' >> "${WRAPPER_JOURNAL}"

WRAPPER_ELAPSED_SECONDS="$(( $(date +%s) - WRAPPER_START_EPOCH ))"
if [[ ${WRAPPER_ELAPSED_SECONDS} -gt 1800 ]]; then
  echo "Whole workflow exceeded 1800 seconds" >&2
  exit 4
fi
printf '{"event":"budget_check","status":"success","wrapper_elapsed_seconds":%d,"gpu_elapsed_seconds":%d}\n'   "${WRAPPER_ELAPSED_SECONDS}" "${GPU_ELAPSED_SECONDS}" >> "${WRAPPER_JOURNAL}"
printf '{"event":"wrapper_terminal","status":"success","timestamp":"%s"}\n'   "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "${WRAPPER_JOURNAL}"
python3 "${ROOT}/scripts/validate_wrapper.py"   --journal "${WRAPPER_JOURNAL}"   --output "${RUN_ROOT}/analysis/wrapper-verdict.json"

(
  cd "${RUN_ROOT}"
  find analysis raw stdout provenance -type f     ! -path 'provenance/checksums.txt' -print0     | sort -z | xargs -0 sha256sum
) > "${RUN_ROOT}/provenance/checksums.txt"
(
  cd "${RUN_ROOT}"
  sha256sum -c provenance/checksums.txt
)
DONE=1
echo "Artifacts: ${RUN_ROOT}"
