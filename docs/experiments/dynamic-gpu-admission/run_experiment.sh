#!/usr/bin/env bash
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES.
# Licensed under the Apache License, Version 2.0.
set -euo pipefail

: "${SPARK_HOME:?Set SPARK_HOME to the Spark 3.5.5 distribution}"
: "${RAPIDS_JAR:?Set RAPIDS_JAR to the tested RAPIDS assembly JAR}"
: "${RUN_ID:?Set RUN_ID to a new stable identifier, for example 20260703T120000Z}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUN_PARENT="${RUN_PARENT:-/tmp/dynamic-gpu-admission}"
RUN_ROOT="${RUN_PARENT}/${RUN_ID}"
EVENT_DIR="${RUN_ROOT}/eventlogs"
OUTPUT_DIR="${RUN_ROOT}/stdout"

if [[ -e "${RUN_ROOT}" ]]; then
  echo "Refusing to reuse existing run directory: ${RUN_ROOT}" >&2
  exit 2
fi
mkdir -p "${EVENT_DIR}" "${OUTPUT_DIR}"

for mode in dynamic static; do
  "${SPARK_HOME}/bin/spark-submit" \
    --master local[8] \
    --jars "${RAPIDS_JAR}" \
    --conf spark.plugins=com.nvidia.spark.SQLPlugin \
    --conf spark.driver.extraClassPath="${RAPIDS_JAR}" \
    --conf spark.executor.extraClassPath="${RAPIDS_JAR}" \
    "${ROOT}/scripts/smoke_dynamic_admission.py" \
    "${mode}" --event-log-dir "${EVENT_DIR}" \
    | tee "${OUTPUT_DIR}/${mode}.txt"
done

mapfile -t event_logs < <(find "${EVENT_DIR}" -maxdepth 1 -type f -name 'local-*' | sort)
if [[ ${#event_logs[@]} -ne 2 ]]; then
  echo "Expected exactly two event logs; found ${#event_logs[@]}" >&2
  exit 3
fi

python3 "${ROOT}/scripts/parse_eventlog.py" "${event_logs[@]}" \
  --output "${RUN_ROOT}/eventlog-summary.json"
python3 "${ROOT}/scripts/validate_experiment.py" \
  --eventlog-summary "${RUN_ROOT}/eventlog-summary.json" \
  --dynamic-stdout "${OUTPUT_DIR}/dynamic.txt" \
  --static-stdout "${OUTPUT_DIR}/static.txt" \
  --output "${RUN_ROOT}/validated-summary.json"

sha256sum "${event_logs[@]}" > "${RUN_ROOT}/eventlog-sha256.txt"
sha256sum "${OUTPUT_DIR}"/*.txt > "${RUN_ROOT}/stdout-sha256.txt"
sha256sum "${RAPIDS_JAR}" > "${RUN_ROOT}/rapids-jar-sha256.txt"
sha256sum "${ROOT}"/scripts/*.py "${ROOT}/run_experiment.sh" \
  > "${RUN_ROOT}/executed-code-sha256.txt"
echo "Artifacts: ${RUN_ROOT}"
