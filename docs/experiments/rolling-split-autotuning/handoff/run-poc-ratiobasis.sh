#!/usr/bin/env bash
# Single-query POC on sparkh SF3k: compare scan-split SIZING modes on store_sales (query9,
# which is store_sales-only so the split change is isolated). One mode per invocation.
#
#   listed     ratio = decoded/listed   (current design; target one decoded batch/task)
#   bytesread  ratio = decoded/readBytes (literal swap; projection removed -> smaller splits)
#   rb1g/2g/4g split = readBudget/read_selectivity (bound compressed reads/task; budget = 1/2/4 GiB)
#   off        autotuner disabled (Spark's own maxSplitBytes) -> speedup denominator
#
# All autotuner modes share ceiling=8g so they are directly comparable and safe (no 1-giant-task OOM).
# 5 iterations in one session: iter1 cold (learns ratio), iter2-5 warm (applies the mode). Warm-vs-warm
# is compared across modes. Fresh history path per mode (baked into each template) so no cross-mode bleed.
#
# READ-ONLY cluster data. This script never deletes/writes HDFS data or the shared history; ab only
# reads them. Output CSVs + event logs land in local kuhu-poc-<mode>-results/ (new dirs).
set -euo pipefail

AB=/home/kuhu/Reps/ab
JAR=/home/kuhu/Reps/spark-rapids/data/jars/rapids-ratiobasis-357.jar
MODE="${1:?usage: run-poc-ratiobasis.sh <off|listed|bytesread|rb1g|rb2g|rb4g>}"

case "$MODE" in
  off)       TPL=gpu.template ;;
  listed|bytesread|rb1g|rb2g|rb4g|brfloor) TPL="gpu-poc-${MODE}.template" ;;
  *) echo "unknown mode: $MODE"; exit 2 ;;
esac

[ -f "$JAR" ] || { echo "jar not found: $JAR (build 357 first)"; exit 3; }

OUT="/home/kuhu/Reps/spark-rapids/data/poc-${MODE}-results"
cd "$AB"
set -x
python3 ab.py --platform onprem-h \
  --test_template "templates/onprem-h/${TPL}" \
  --test_jar "$JAR" \
  --queries query9 \
  --iterations 5 \
  --runs 1 \
  --capture_eventlog \
  --output "$OUT"
