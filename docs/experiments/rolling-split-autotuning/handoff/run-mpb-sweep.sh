#!/usr/bin/env bash
# maxPartitionBytes sweep on sparkh SF3k, FULL NDS, autotuner OFF (no ratio, no code changes active).
# One maxPartitionBytes value per invocation: 128m | 256m | 512m | 1g | 2g | 4g.
#
# Each mpb template = the stock onprem-h gpu.template with ONLY spark.sql.files.maxPartitionBytes
# changed, plus spark.rapids.sql.metrics.level=DEBUG (measurement only, so scan-stage time, output
# batch counts, and the ESSENTIAL scanMaxSplitBytes metric land in the event log). No autotuner
# historyPath -> the split is Spark's own maxSplitBytes (ScanSplitAutotuner.decide returns
# sparkDefault when the store is empty). The jar's ratioBasis/floor/T-knob code is inert with no
# confs set (verified: targetSizeBytes=gpuTargetBatchSize, maxReadBatchSizeBytes=max(2GiB,1GiB)=vanilla).
#
# 5 iterations in one session: iter1 cold, iters 2-5 warm. capture_eventlog for scan metrics.
# READ-ONLY cluster data; ab only reads HDFS + copies the event log back. Output: local dir only.
set -euo pipefail

AB=/home/kuhu/Reps/ab
JAR=/home/kuhu/Reps/spark-rapids/data/jars/rapids-ratiobasis-357.jar
MPB="${1:?usage: run-mpb-sweep.sh <128m|256m|512m|1g|2g|4g>}"
TPL="gpu-mpb-${MPB}.template"

[ -f "$AB/templates/onprem-h/$TPL" ] || { echo "no template $TPL"; exit 2; }
[ -f "$JAR" ] || { echo "jar not found: $JAR"; exit 3; }

OUT="/home/kuhu/Reps/spark-rapids/data/mpb-${MPB}-results"
cd "$AB"
set -x
python3 ab.py --platform onprem-h \
  --test_template "templates/onprem-h/${TPL}" \
  --test_jar "$JAR" \
  --iterations 5 \
  --runs 1 \
  --capture_eventlog \
  --output "$OUT"
