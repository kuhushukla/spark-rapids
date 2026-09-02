#!/usr/bin/env bash
# Wikimedia pageview_complete acquirer: daily <agent> files over a month range, downloaded in parallel
# from a mirror, then converted to partitioned parquet.
#   $OUT/raw/<YYYY-MM>/pageviews-YYYYMMDD-<agent>.bz2   (resumable; existing files skipped)
#   $OUT/parquet/year=YYYY/month=MM/day=DD/agent=<agent>/part-*.parquet
#     cols: wiki_code, article_title, page_id, access_method, daily_total, hourly_counts
# Needs: curl, wget, xargs, GNU date, and a Spark dist for --convert.
#
# Usage: download_pageviews.sh [--out=DIR] [--mirror=BASE] [--start-month=YYYY-MM] [--end-month=YYYY-MM]
#            [--agent=user] [--parallel=N] [--spark-home=PATH] [--max-files=N] [--convert=yes|no] [--list-only]
#   default range = 2025-01..2025-01. Widen via --start-month/--end-month.
#   --max-files=N keeps only the first N daily files per month (smoke-sized sample).
set -uo pipefail
OUT=/data/wiki-pageviews
MIRROR=https://dumps.wikimedia.your.org/other/pageview_complete
START=2025-01
END=2025-01
AGENT=user
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
  --agent=*)       AGENT="${a#*=}" ;;
  --parallel=*)    PAR="${a#*=}" ;;
  --spark-home=*)  SPARK_HOME="${a#*=}" ;;
  --max-files=*)   MAXFILES="${a#*=}" ;;
  --convert=*)     CONVERT="${a#*=}" ;;
  --list-only)     LIST_ONLY=yes ;;
  -h|--help)       sed -n '2,15p' "$0"; exit 0 ;;
  *) echo "unknown arg: $a  (see --help)"; exit 2 ;;
esac; done
RAW="$OUT/raw"; mkdir -p "$RAW"
echo ">> config: out=$OUT mirror=$MIRROR range=$START..$END agent=$AGENT parallel=$PAR max-files=$MAXFILES convert=$CONVERT"

# ---- month window ----
months=(); d="$START-01"
while : ; do m=$(date -d "$d" +%Y-%m); months+=("$m"); [ "$m" = "$END" ] && break; d=$(date -d "$d +1 month" +%Y-%m-01); done
echo ">> ${#months[@]} month(s): ${months[*]}"

# ---- 1) enumerate + parallel-download the daily <agent> files, per month, from the mirror ----
TOTN=0; OKM=0
for m in "${months[@]}"; do
  y="${m%%-*}"; dst="$RAW/$m"; mkdir -p "$dst"
  urls=$(curl -fsS "$MIRROR/$y/$m/" 2>/dev/null | grep -oE "pageviews-[0-9]{8}-$AGENT\.bz2" | sort -u | sed "s#^#$MIRROR/$y/$m/#")
  [ "$MAXFILES" -gt 0 ] 2>/dev/null && urls=$(printf '%s\n' "$urls" | head -"$MAXFILES")
  cnt=$(printf '%s\n' "$urls" | grep -c . || true)
  [ "$cnt" -eq 0 ] && { echo "   [skip] $m: no $AGENT files at mirror"; continue; }
  TOTN=$((TOTN+cnt)); OKM=$((OKM+1))
  echo ">> $m: $cnt files"
  [ "$LIST_ONLY" = yes ] && continue
  printf '%s\n' "$urls" | xargs -P "$PAR" -n1 wget -q -c -P "$dst"
done
echo ">> months with data: $OKM/${#months[@]}   total files: $TOTN"
[ "$LIST_ONLY" = yes ] && { echo "(list-only) done."; exit 0; }
[ "$TOTN" -eq 0 ] && { echo "!! nothing enumerated — check --mirror"; exit 3; }
echo ">> raw on disk:"; du -sh "$RAW" 2>&1

# ---- 2) convert -> partitioned parquet ----
if [ "$CONVERT" = yes ] && [ -x "$SPARK_HOME/bin/spark-shell" ]; then
  echo ">> converting -> $OUT/parquet (partitioned year/month/day/agent) using $SPARK_HOME"
  mkdir -p "$OUT/_sparklocal"
  OUT="$OUT" "$SPARK_HOME/bin/spark-shell" --master 'local[*]' --driver-memory 16g \
    --conf spark.local.dir="$OUT/_sparklocal" <<'SCALA'
import org.apache.spark.sql.functions._
val out = sys.env("OUT")
// space-delimited text; split on space, 6 cols; derive year/month/day/agent from the filename.
spark.read.option("recursiveFileLookup","true").text(s"$out/raw")
  .withColumn("f", input_file_name())
  .withColumn("c", split(col("value"), " "))
  .select(col("c")(0).as("wiki_code"), col("c")(1).as("article_title"), col("c")(2).as("page_id"),
          col("c")(3).as("access_method"), col("c")(4).cast("long").as("daily_total"),
          col("c")(5).as("hourly_counts"),
          regexp_extract(col("f"), "pageviews-([0-9]{4})[0-9]{4}-", 1).as("year"),
          regexp_extract(col("f"), "pageviews-[0-9]{4}([0-9]{2})[0-9]{2}-", 1).as("month"),
          regexp_extract(col("f"), "pageviews-[0-9]{6}([0-9]{2})-", 1).as("day"),
          regexp_extract(col("f"), "pageviews-[0-9]{8}-([a-z-]+)\\.bz2", 1).as("agent"))
  .write.mode("overwrite").partitionBy("year","month","day","agent").parquet(s"$out/parquet")
println(s"PAGEVIEWS_ROWS=${spark.read.parquet(s"$out/parquet").count()}")
System.exit(0)
SCALA
  echo ">> parquet ready: $OUT/parquet"
else
  echo ">> conversion skipped (convert=$CONVERT or no spark-shell)."
fi
echo ">> done -> $OUT"
