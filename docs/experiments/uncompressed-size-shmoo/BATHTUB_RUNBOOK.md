# Bathtub follow-up runbook

The follow-up was preregistered in commit `ca79472ae` and executed on 2026-07-03.
All paths below are relative to the repository root.

## Runtime

- Spark 3.5.5, local[8]
- NVIDIA RTX A6000, 49,140 MiB
- RAPIDS jar:
  `dist/target/rapids-4-spark_2.12-26.08.0-SNAPSHOT-cuda12.jar`
- Embedded RAPIDS revision: `4c66f7214`
- Jar SHA-256:
  `9ca72145dd8e7d19bf8338a2a5646eb7ca2a66541d2ee590be9beff5831d5441`
- Initial GPU concurrency: 4
- Dynamic GPU concurrency: true
- Driver memory: 24 GiB

## Common launch prefix

```bash
SPARK=/home/roberte/src/spark_3.5.5/bin/spark-submit
JAR="$(pwd)/dist/target/rapids-4-spark_2.12-26.08.0-SNAPSHOT-cuda12.jar"

"$SPARK" --master 'local[8]' --driver-memory 24g \
  --conf spark.plugins=com.nvidia.spark.SQLPlugin \
  --conf "spark.driver.extraClassPath=$JAR" \
  --conf "spark.executor.extraClassPath=$JAR" \
  --jars "$JAR" \
  docs/experiments/uncompressed-size-shmoo/scripts/benchmark.py \
  --concurrent-gpu-tasks 4 --dynamic-concurrency true
```

The following argument tails were appended to that prefix:

| Attempt | Data | Schedule | Journal | Event-log directory |
|---|---|---|---|---|
| bathtub-mechanism-001 | taxi-data-sharded | preregistration/bathtub-mechanism-schedule.json | attempts/bathtub-mechanism-001/journal.jsonl | attempts/bathtub-mechanism-001/eventlog |
| bathtub-batch-001 | taxi-data-sharded | preregistration/bathtub-batch-schedule.json | attempts/bathtub-batch-001/journal.jsonl | attempts/bathtub-batch-001/eventlog |
| bathtub-layout-sharded-001 | taxi-data-sharded | preregistration/bathtub-layout-sharded-schedule.json | attempts/bathtub-layout-sharded-001/journal.jsonl | attempts/bathtub-layout-sharded-001/eventlog |
| bathtub-layout-source-001 | taxi-data | preregistration/bathtub-layout-source-schedule.json | attempts/bathtub-layout-source-001/journal.jsonl | attempts/bathtub-layout-source-001/eventlog |

For each row, paths under `preregistration/` and `attempts/` are relative to
`docs/experiments/uncompressed-size-shmoo`. After execution, journals and deterministic
gzip event logs were moved under each attempt's `raw/` directory. The lifecycle JSON
binds the final raw files and frozen schedule by SHA-256.

## Replay

From `docs/experiments/uncompressed-size-shmoo`:

```bash
python3 scripts/analyze_bathtub_followup.py \
  --mechanism-metrics attempts/bathtub-mechanism-001/analysis/scan-metrics.json \
  --batch-metrics attempts/bathtub-batch-001/analysis/scan-metrics.json \
  --sharded-layout-metrics attempts/bathtub-layout-sharded-001/analysis/scan-metrics.json \
  --source-layout-metrics attempts/bathtub-layout-source-001/analysis/scan-metrics.json \
  --output /tmp/bathtub-followup-analysis.json \
  --markdown /tmp/bathtub-followup-results.md
```

The complete extraction, lifecycle, plotting, and analysis replay commands are in
`manifest.yaml`.
