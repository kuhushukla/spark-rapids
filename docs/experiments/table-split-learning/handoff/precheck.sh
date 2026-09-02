#!/usr/bin/env bash
# Verify before spending GPU time. Non-zero exit on any failure.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
W() { python3 "$HERE/wincfg.py" "$@"; }

JAR="${RAPIDS_JAR:-/home/kuhu/Reps/spark-rapids/dist/rapids-353-local-keep.jar}"
SPARK_HOME="${TSL_SPARK_HOME:-/home/kuhu/Downloads/spark-3.5.3-bin-hadoop3}"
GPU_UUID="${GPU_UUID:-GPU-1aaa66fd-0c1e-935b-fe65-2c9ca7357504}"
DATA="$(W dataset)"
fail=0
say() { printf "  %-38s %s\n" "$1" "$2"; [ "${2:0:4}" = FAIL ] && fail=1; return 0; }

echo "env:"
[ -f "$JAR" ] && say jar OK || say jar "FAIL $JAR"
L="$(unzip -l "$JAR" 2>/dev/null || true)"
case "$L" in *ScanSplitHeuristic*) say split-heuristic-in-jar OK;; *) say split-heuristic-in-jar FAIL;; esac
case "$L" in *spark353*) say jar-shim-353 OK;; *) say jar-shim-353 FAIL;; esac
V="$(head -1 "$SPARK_HOME/RELEASE" 2>/dev/null | awk '{print $2}')"
[ "$V" = 3.5.3 ] && say spark-3.5.3 OK || say spark-3.5.3 "FAIL got ${V:-none}"
[ -d "$DATA" ] && say dataset OK || say dataset "FAIL $DATA"
# An orphaned JVM from a killed run holds the pool and every arm then dies on
# "pool allocation ... less than allocation of". TaskStop does not kill the child JVM.
F="$(nvidia-smi --query-gpu=memory.free --format=csv,noheader,nounits -i "$GPU_UUID" 2>/dev/null | tr -dc 0-9)"
if [ "${F:-0}" -ge 20000 ]; then say "gpu-free ${F}MiB" OK; else
  say "gpu-free ${F:-?}MiB" FAIL
  nvidia-smi --query-compute-apps=pid,used_memory --format=csv,noheader 2>/dev/null | sed 's/^/      holding: /'
fi

echo "config:"
C="$(W check_cover)"; [ "$C" = OK ] && say cover-contiguous OK || say cover-contiguous "FAIL $C"
for w in $(W windows); do say "pred[$w]" "$(W pred "$w")"; done

[ $fail -ne 0 ] && { echo "PRECHECK FAILED"; exit 1; }

echo "data (counting rows per window):"
COVER="$(W cover)"
OUT=$(DATA="$DATA" HERE="$HERE" COVER="$COVER" CUDA_VISIBLE_DEVICES="" \
  "$SPARK_HOME/bin/spark-shell" --master 'local[8]' --driver-memory 8g \
  --conf spark.ui.enabled=false <<'SCALA' 2>/dev/null | grep -E "^WIN(CHECK|SUM)"
import sys.process._
val data = sys.env("DATA"); val here = sys.env("HERE")
val df = spark.read.parquet(data); val total = df.count()
var sum = 0L
sys.env("COVER").split(" ").filter(_.nonEmpty).foreach { n =>
  val p = Seq("python3", s"$here/wincfg.py", "pred", n).!!.trim
  val c = (if (p.isEmpty) df else df.where(p)).count()
  sum += c
  println(s"WINCHECK $n $c ${100.0*c/total}")
}
println(s"WINSUM $sum $total")
System.exit(0)
SCALA
)
echo "$OUT" | awk '/^WINCHECK/ {printf "  %-38s %d rows (%.1f%%)\n", $2, $3, $4}'
S=$(echo "$OUT" | awk '/^WINSUM/ {print $2}'); T=$(echo "$OUT" | awk '/^WINSUM/ {print $3}')
if [ -n "$S" ] && [ "$S" = "$T" ]; then
  echo "  cover sums to table                    OK ($S rows)"
  echo "PRECHECK PASSED"
else
  echo "  cover sums to table                    FAIL (windows=$S table=$T)"
  echo "PRECHECK FAILED"; exit 1
fi
