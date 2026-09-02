#!/usr/bin/env bash
# Periodic status in the report's own format. Safe to run mid-run.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUTROOT="${1:-/data/window-learning-20260824}"
LED="$HERE/../results/ledger-$(basename "$OUTROOT").tsv"
python3 "$HERE/build_ledger.py" "$OUTROOT" "$LED" >/dev/null 2>&1
python3 "$HERE/gen_window_report.py" --ledger "$LED" --outroot "$OUTROOT" >/dev/null 2>&1
echo "== progress $(date +%H:%M:%S)"
tot=$(( $(python3 "$HERE/wincfg.py" sweep_jobs | wc -l) * $(python3 "$HERE/wincfg.py" grid | wc -w) ))
don=$(ls -d "$OUTROOT"/sweep-* 2>/dev/null | wc -l)
seq=$(( $(python3 "$HERE/wincfg.py" sequences | wc -l) * 6 ))
sdn=$(ls -d "$OUTROOT"/*_to_* 2>/dev/null | wc -l)
echo "   sweep arms $don/$tot    learn arms $sdn/$seq"
echo
sed -n '/^## /,$p' "$HERE/../../rolling-split-autotuning/results/window-report-$(basename "$OUTROOT").md" 2>/dev/null
