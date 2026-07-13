#!/usr/bin/env bash
# Run the scan split autotuner two-pass NDS test.
# Always targets the RTX A5000 (GPU 1), disables Hive/Derby, enables event logging.
#
# Usage:
#   bash docs/experiments/rolling-split-autotuning/handoff/run-autotuner-test.sh
#
# Output:
#   data/autotuner-run-<timestamp>.log   — full Spark driver log
#   data/spark-eventlog-<timestamp>/     — event log (for History Server / plan inspection)
#   docs/experiments/rolling-split-autotuning/results/run-<timestamp>.md  — parsed results

set -euo pipefail

REPO=$(cd "$(dirname "$0")/../../../.." && pwd)
TS=$(date +%Y%m%d_%H%M%S)
LOG="$REPO/data/autotuner-run-${TS}.log"
EVENTLOG_DIR="/home/kuhu/logdir"
RESULTS_DIR="$REPO/docs/experiments/rolling-split-autotuning/results"

mkdir -p "$REPO/data/spark-tmp" "$RESULTS_DIR"

echo "=== Autotuner test $TS ===" | tee "$LOG"
echo "Log:      $LOG"
echo "EventLog: $EVENTLOG_DIR"
echo "CUDA_VISIBLE_DEVICES: A5000 by UUID (GPU-1aaa66fd...)"
echo "GPU free memory:"
nvidia-smi --query-gpu=index,name,memory.free --format=csv,noheader 2>/dev/null || echo "  nvidia-smi not accessible"
echo ""

# Clear history for a fresh cold-start run
if [ -f "$REPO/data/scan-split-history.tsv" ]; then
  cp "$REPO/data/scan-split-history.tsv" "$REPO/data/scan-split-history.tsv.bak.${TS}"
  truncate -s 0 "$REPO/data/scan-split-history.tsv"
  echo "History cleared (backup: scan-split-history.tsv.bak.${TS})"
fi

export SPARK_HOME=/home/kuhu/Downloads/spark-3.5.3-bin-hadoop3
export JAVA_HOME=/usr/lib/jvm/java-1.17.0-openjdk-amd64
unset SPARK_ENV_LOADED  # force re-sourcing of spark-env.sh each run

# Target the A5000 by UUID. Numeric CUDA indices are unreliable: CUDA's default
# CUDA_DEVICE_ORDER=FASTEST_FIRST reverses nvidia-smi's PCI ordering, so
# CUDA_VISIBLE_DEVICES=1 actually selects the T400. UUID is unambiguous.
A5000_UUID=GPU-1aaa66fd-0c1e-935b-fe65-2c9ca7357504

CUDA_VISIBLE_DEVICES="$A5000_UUID" "$SPARK_HOME/bin/spark-shell" \
  --master local[4] \
  --driver-memory 4g \
  --conf spark.plugins=com.nvidia.spark.SQLPlugin \
  --conf spark.rapids.sql.enabled=true \
  --conf spark.rapids.sql.scan.splitAutotuner.historyPath="$REPO/data/scan-split-history.tsv" \
  --conf spark.local.dir="$REPO/data/spark-tmp" \
  --conf spark.sql.shuffle.partitions=200 \
  --conf spark.rapids.sql.metrics.level=DEBUG \
  --conf spark.sql.catalogImplementation=in-memory \
  ${EXTRA_CONFS:-} \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir="$EVENTLOG_DIR" \
  --jars "$REPO/dist/target/rapids-4-spark_2.12-26.08.0-SNAPSHOT-cuda12.jar" \
  -i "$REPO/docs/experiments/rolling-split-autotuning/handoff/nds-autotuner-test.scala" \
  2>&1 | tee -a "$LOG"

echo ""
echo "=== Run complete. Parsing results... ==="

# Capture the batchSizeBytes actually used (from EXTRA_CONFS, else the 1 GiB default)
BATCH_BYTES=$(echo "${EXTRA_CONFS:-}" | grep -oE 'batchSizeBytes=[0-9]+' | tail -1 | cut -d= -f2)
BATCH_BYTES=${BATCH_BYTES:-1073741824}

# Parse results into timestamped markdown
python3 - "$LOG" "$TS" "$RESULTS_DIR" "$REPO/data/scan-split-history.tsv" "$BATCH_BYTES" <<'PYEOF'
import sys, re, base64, os

log_path, ts, results_dir, history_path = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
batch_bytes = int(sys.argv[5])
log = open(log_path).read()

# Extract timings from "done in Xms" lines
timing_re = re.compile(r'=== (store_sales|web_sales) (run\d) done in (\d+)ms')
timings = {}
for m in timing_re.finditer(log):
    timings[(m.group(1), m.group(2))] = int(m.group(3))

# Extract autotuner decisions
cold_re   = re.compile(r'COLD_START table=(\S+) listed_bytes=(\d+) split_bytes=(\d+)')
decided_re = re.compile(r'DECIDED table=(\S+) listed_bytes=(\d+) expansion_ratio=([0-9.]+) predicted_decoded_bytes=(\d+) split_bytes=(\d+) spark_default=(\d+)')
recorded_re = re.compile(r'RECORDED table=(\S+) listed_bytes=(\d+) decoded_bytes=(\d+) decoded_rows=(\d+)')

colds    = {m.group(1).split('/')[-1]: m for m in cold_re.finditer(log)}
decideds = {m.group(1).split('/')[-1]: m for m in decided_re.finditer(log)}
recordeds= list(recorded_re.finditer(log))

# Extract task counts per stage from progress bar lines
stage_tasks = re.findall(r'\[Stage (\d+):.*?/ (\d+)\]', log)
# get max tasks per stage
stage_max = {}
for sid, cnt in stage_tasks:
    stage_max[int(sid)] = max(stage_max.get(int(sid), 0), int(cnt))

def fmt_mib(b): return f"{int(b)/1024**2:.0f} MiB"
def fmt_s(ms): return f"{int(ms)/1000:.1f}s" if ms else "N/A"
def fmt_batch(b):
    gib = b / 1024**3
    return f"{gib:.0f} GiB" if gib == int(gib) else (f"{gib:.2f} GiB" if gib >= 1 else fmt_mib(b))

lines = [
    f"# Scan Split Autotuner — Run {ts}",
    "",
    "## Configuration",
    "| Parameter | Value |",
    "|---|---|",
    "| Spark | 3.5.3 |",
    "| GPU | NVIDIA RTX A5000 24 GB (selected by UUID GPU-1aaa66fd) |",
    "| Parallelism | local[4] |",
    "| spark.sql.files.maxPartitionBytes (default) | 128 MiB |",
    f"| spark.rapids.sql.batchSizeBytes | {fmt_batch(batch_bytes)} |",
    "",
    "## Autotuner Decisions",
    "| Table | Run 1 split | Run 2 split | Expansion ratio | Predicted decoded | Spark default |",
    "|---|---|---|---|---|---|",
]

for table in ['store_sales', 'web_sales']:
    c = colds.get(table)
    d = decideds.get(table)
    r1_split = fmt_mib(c.group(3)) if c else "N/A"
    r2_split = fmt_mib(d.group(5)) if d else "N/A (cold)"
    ratio    = f"{float(d.group(3)):.3f}" if d else "—"
    pred     = fmt_mib(d.group(4)) if d else "—"
    default  = fmt_mib(d.group(6)) if d else "128 MiB"
    lines.append(f"| {table} | {r1_split} | {r2_split} | {ratio} | {pred} | {default} |")

lines += [
    "",
    "## Query Runtimes",
    "| Table | Run 1 | Run 2 | Delta | Speedup |",
    "|---|---|---|---|---|",
]

for table in ['store_sales', 'web_sales']:
    r1 = timings.get((table, 'run1'))
    r2 = timings.get((table, 'run2'))
    delta = (r1 - r2) if r1 and r2 else None
    speedup = f"{r1/r2:.2f}x" if r1 and r2 else "—"
    lines.append(f"| {table} | {fmt_s(r1)} | {fmt_s(r2)} | {fmt_s(delta) if delta else 'N/A'} | {speedup} |")

lines += [
    "",
    "## Task Counts",
    "| Stage | Tasks | Notes |",
    "|---|---|---|",
]
for sid, cnt in sorted(stage_max.items()):
    lines.append(f"| {sid} | {cnt} | |")

lines += [
    "",
    "## History File",
    "| Table | Listed | Decoded | Rows | Ratio |",
    "|---|---|---|---|---|",
]
if os.path.exists(history_path):
    for line in open(history_path):
        parts = line.strip().split('\t')
        if len(parts) >= 5:
            label = base64.urlsafe_b64decode(parts[0] + '==').decode()
            table = label.split('/')[-1]
            listed, decoded, rows = int(parts[1]), int(parts[2]), int(parts[3])
            ratio = decoded/listed if listed else 0
            lines.append(f"| {table} | {listed/1024**3:.2f} GiB | {decoded/1024**3:.2f} GiB | {int(rows):,} | {ratio:.3f} |")

out = results_dir + f"/run-{ts}.md"
open(out, 'w').write('\n'.join(lines) + '\n')
print(f"Results written to {out}")
PYEOF
