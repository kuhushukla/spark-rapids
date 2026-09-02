#!/usr/bin/env bash
# Wikimedia clickstream acquirer: all wiki editions x a month range, downloaded in parallel from a
# mirror, then converted to parquet (cols: previous, current, link_type, n; all wikis unioned).
# Output:
#   $OUT/raw/clickstream-<wiki>-<YYYY-MM>.tsv.gz   (resumable; existing files skipped)
#   $OUT/parquet/
# Needs: curl, wget, xargs, GNU date, and a Spark dist for --convert.
#
# Usage: download_clickstream.sh [--out=DIR] [--mirror=BASE] [--start-month=YYYY-MM] [--end-month=YYYY-MM]
#            [--parallel=N] [--spark-home=PATH] [--max-files=N] [--convert=yes|no] [--list-only]
#   default mirror = Your.org (full "other" mirror). Alt: https://files.scatter.red/wikimedia/other/clickstream
#   --max-files=N keeps only the first N enumerated files (smoke-sized sample).
set -uo pipefail
OUT=/data/wiki-clickstream
MIRROR=https://dumps.wikimedia.your.org/other/clickstream
START=2017-11
END=2025-06
PAR=8
SPARK_HOME="${BENCH_SPARK_HOME:-/home/kuhu/Downloads/spark-3.5.3-bin-hadoop3}"
CONVERT=yes
LIST_ONLY=no
MAXFILES=0
for a in "$@"; do case "$a" in
  --out=*)         OUT="${a#*=}" ;;
  --mirror=*)      MIRROR="${a#*=}" ;;
  --start-month=*) START="${a#*=}" ;;
  --end-month=*)   END="${a#*=}" ;;
  --parallel=*)    PAR="${a#*=}" ;;
  --spark-home=*)  SPARK_HOME="${a#*=}" ;;
  --max-files=*)   MAXFILES="${a#*=}" ;;
  --convert=*)     CONVERT="${a#*=}" ;;
  --list-only)     LIST_ONLY=yes ;;
  -h|--help)       sed -n '2,17p' "$0"; exit 0 ;;
  *) echo "unknown arg: $a  (see --help)"; exit 2 ;;
esac; done
RAW="$OUT/raw"; mkdir -p "$RAW"
URLS="$OUT/_urls.txt"; printf '' > "$URLS"
echo ">> config: out=$OUT mirror=$MIRROR range=$START..$END parallel=$PAR max-files=$MAXFILES convert=$CONVERT"

# ---- 1) enumerate every clickstream file across the month range from the mirror index ----
months=(); d="$START-01"
while : ; do m=$(date -d "$d" +%Y-%m); months+=("$m"); [ "$m" = "$END" ] && break; d=$(date -d "$d +1 month" +%Y-%m-01); done
echo ">> enumerating ${#months[@]} months (all wikis) from mirror ..."
for m in "${months[@]}"; do
  curl -fsS "$MIRROR/$m/" 2>/dev/null \
    | grep -oE "clickstream-[a-z0-9_-]+-$m\.tsv\.gz" | sort -u \
    | sed "s#^#$MIRROR/$m/#" >> "$URLS" \
    || echo "   [skip] $m index unavailable"
done
if [ "$MAXFILES" -gt 0 ] 2>/dev/null; then
  head -"$MAXFILES" "$URLS" > "$URLS.cap" && mv "$URLS.cap" "$URLS"
fi
N=$(wc -l < "$URLS"); echo ">> $N files enumerated -> $URLS"
if [ "$LIST_ONLY" = yes ]; then echo "(list-only) done."; exit 0; fi
[ "$N" -eq 0 ] && { echo "!! no files enumerated — check --mirror"; exit 3; }

# ---- 2) parallel download from the uncapped mirror (wget -c = resume, skips complete files) ----
echo ">> downloading with $PAR parallel streams -> $RAW"
xargs -P "$PAR" -n1 -a "$URLS" wget -q -c -P "$RAW"
GOT=$(ls "$RAW"/*.tsv.gz 2>/dev/null | wc -l)
echo ">> raw files present: $GOT / $N"; du -sh "$RAW"

# ---- 3) convert raw -> parquet ----
if [ "$CONVERT" = yes ] && [ -x "$SPARK_HOME/bin/spark-shell" ]; then
  echo ">> converting -> $OUT/parquet using $SPARK_HOME"
  mkdir -p "$OUT/_sparklocal"
  OUT="$OUT" "$SPARK_HOME/bin/spark-shell" --master 'local[*]' --driver-memory 16g \
    --conf spark.local.dir="$OUT/_sparklocal" <<'SCALA'
import org.apache.spark.sql.functions._
val out = sys.env("OUT")
// TAB-delimited text; split on TAB (titles contain literal ", so not CSV-safe).
spark.read.text(s"$out/raw")
  .select(split(col("value"), "\t").as("c"))
  .select(col("c")(0).as("previous"), col("c")(1).as("current"),
          col("c")(2).as("link_type"), col("c")(3).cast("long").as("n"))
  .write.mode("overwrite").parquet(s"$out/parquet")
println(s"CLICKSTREAM_ROWS=${spark.read.parquet(s"$out/parquet").count()}")
System.exit(0)
SCALA
  echo ">> parquet ready: $OUT/parquet"
else
  echo ">> conversion skipped (convert=$CONVERT or no spark-shell)."
fi
echo ">> done -> $OUT"
