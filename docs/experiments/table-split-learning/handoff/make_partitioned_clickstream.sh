#!/usr/bin/env bash
# One-time: rebuild the clickstream parquet copy partitioned by (wiki, ym), mirroring the raw layout
# (clickstream-<wiki>-<YYYY>-<MM>.tsv.gz, 1797 files, 40 wikis, 104 months). Nothing is dropped.
#
# Partitioning is what makes data windows expressible: the autotuner's history key is
# rootPaths.headOption (GpuFileSourceScanExec.scala:585-587, not pruned) while listedBytes comes from
# dynamicallySelectedPartitions (line 588, post-pruning). So different partition filters share one
# history key with different listed bytes.
#
# Note: 1797 partitions over 135.5 GiB is ~77 MiB/file vs ~403 MiB in the unpartitioned copy, so
# baselines must be re-run against this copy rather than compared to old numbers.
#
# Writes to a NEW directory; never touches the existing parquet copy.
set -euo pipefail

SRC="${SRC:-/data/wiki-clickstream/raw}"
OUT="${OUT:-/data/wiki-clickstream/parquet-part}"
SPARK_HOME="${TSL_SPARK_HOME:-/home/kuhu/Downloads/spark-3.5.3-bin-hadoop3}"
LOCALDIR="${LOCALDIR:-/data/wiki-clickstream/_sparklocal}"

[ -d "$SRC" ] || { echo "!! raw dir not found: $SRC"; exit 2; }
if [ -d "$OUT" ] && [ -n "$(ls -A "$OUT" 2>/dev/null)" ] && [ "${FORCE:-0}" != 1 ]; then
  echo "!! $OUT exists and is not empty. Re-run with FORCE=1 to overwrite it."; exit 2
fi
echo ">> src=$SRC ($(ls "$SRC"/*.tsv.gz 2>/dev/null | wc -l) gz files)  out=$OUT  spark=$SPARK_HOME"
mkdir -p "$LOCALDIR"

SRC="$SRC" OUT="$OUT" "$SPARK_HOME/bin/spark-shell" --master 'local[16]' --driver-memory 32g \
  --conf spark.local.dir="$LOCALDIR" <<'SCALA'
import org.apache.spark.sql.functions._
val src = sys.env("SRC"); val out = sys.env("OUT")
val NAME = ".*clickstream-([A-Za-z0-9_]+)-(\\d{4}-\\d{2})\\.tsv\\.gz$"
// Chains are parenthesised: spark-shell evaluates one LINE per statement, so an unparenthesised
// `val df = spark.read.text(src)` closes there and the following .withColumn lines never attach.
// split on TAB, not CSV: titles contain literal quotes. Same parsing as download_clickstream.sh:65-70.
val df = (spark.read.text(src)
  .withColumn("f", input_file_name())
  .withColumn("wiki", regexp_extract(col("f"), NAME, 1))
  .withColumn("ym",   regexp_extract(col("f"), NAME, 2))
  .select(split(col("value"), "\t").as("c"), col("wiki"), col("ym"))
  .select(col("c")(0).as("previous"), col("c")(1).as("current"),
          col("c")(2).as("link_type"), col("c")(3).cast("long").as("n"),
          col("wiki"), col("ym")))
require(df.schema.fieldNames.toSet == Set("previous","current","link_type","n","wiki","ym"),
        s"unexpected schema: ${df.schema.fieldNames.mkString(",")}")
// A regex miss would land rows in __HIVE_DEFAULT_PARTITION__; that is checked after the write
// instead of here, because filtering up front costs a full parse pass over all 1797 gz files.
// one writer task per (wiki, ym) -> one file per partition, matching the raw layout
(df.repartition(col("wiki"), col("ym"))
  .write.mode("overwrite").partitionBy("wiki", "ym").parquet(out))
val back = spark.read.parquet(out)
println(s"CLICKSTREAM_ROWS=${back.count()}")
println(s"CLICKSTREAM_PARTS=${back.select("wiki","ym").distinct().count()}")
System.exit(0)
SCALA

[ -d "$OUT" ] || { echo "!! conversion produced nothing"; exit 1; }
# a filename-regex miss would show up as a default-partition bucket
if find "$OUT" -maxdepth 2 -name '*__HIVE_DEFAULT_PARTITION__*' | grep -q .; then
  echo "!! __HIVE_DEFAULT_PARTITION__ present: the filename regex missed some files"; exit 1
fi
echo ">> written: $OUT"; du -sh "$OUT"
echo ">> files: $(find "$OUT" -name '*.parquet' | wc -l)  (raw gz files: $(ls "$SRC"/*.tsv.gz | wc -l))"
find "$OUT" -name '*.parquet' -printf '%s\n' | sort -n | \
  awk '{a[NR]=$1} END {printf ">> file size min=%.0fMiB median=%.0fMiB max=%.0fMiB\n", a[1]/1048576, a[int(NR/2)]/1048576, a[NR]/1048576}'
