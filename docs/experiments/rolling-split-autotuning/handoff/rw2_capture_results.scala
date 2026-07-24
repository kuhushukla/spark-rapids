// Capture the actual RESULT rows of RW6-RW9 (for the report "what the query answers" sections). Queries inlined
// (no :load — REPL command fails under -i). Prints top rows as pipe-delimited lines prefixed RES_<query>.
val BASE = "/home/kuhu/Reps/spark-rapids/overture_2026-07-22"
spark.read.parquet(s"$BASE/places/type=place").createOrReplaceTempView("places")
spark.read.parquet(s"$BASE/addresses/type=address").createOrReplaceTempView("address")
spark.read.parquet(s"$BASE/divisions/type=division").createOrReplaceTempView("division")
spark.read.parquet(s"$BASE/transportation/type=segment").createOrReplaceTempView("segment")
spark.read.parquet(s"$BASE/transportation/type=connector").createOrReplaceTempView("connector")

val Q = Map(
  "rw6" -> """WITH src AS (
      SELECT s.dataset AS dataset, s.confidence AS conf, s.update_time AS ut FROM segment   LATERAL VIEW explode(sources) t AS s
      UNION ALL SELECT s.dataset, s.confidence, s.update_time FROM connector LATERAL VIEW explode(sources) t AS s
      UNION ALL SELECT s.dataset, s.confidence, s.update_time FROM places    LATERAL VIEW explode(sources) t AS s
      UNION ALL SELECT s.dataset, s.confidence, s.update_time FROM address   LATERAL VIEW explode(sources) t AS s
      UNION ALL SELECT s.dataset, s.confidence, s.update_time FROM division  LATERAL VIEW explode(sources) t AS s )
    SELECT dataset, COUNT(*) AS records, ROUND(AVG(conf),3) AS avg_confidence,
      MIN(SUBSTRING(ut,1,4)) AS oldest_year, MAX(SUBSTRING(ut,1,4)) AS newest_year,
      ROUND(100.0*AVG(CASE WHEN SUBSTRING(ut,1,4)>='2024' THEN 1 ELSE 0 END),1) AS pct_updated_2024plus
    FROM src GROUP BY dataset ORDER BY records DESC""",
  "rw7" -> """WITH s AS ( SELECT seg.class AS class, SUBSTRING(src.update_time,1,4) AS yr
      FROM segment seg LATERAL VIEW explode(seg.sources) t AS src WHERE seg.class IS NOT NULL )
    SELECT class, COUNT(*) AS source_records,
      ROUND(100.0*AVG(CASE WHEN yr>='2024' THEN 1 ELSE 0 END),1) AS pct_2024plus,
      ROUND(100.0*AVG(CASE WHEN yr< '2022' THEN 1 ELSE 0 END),1) AS pct_before_2022,
      MIN(yr) AS oldest_year, MAX(yr) AS newest_year
    FROM s GROUP BY class ORDER BY source_records DESC""",
  "rw8" -> """SELECT class, COUNT(*) AS named_segments,
      ROUND(AVG(size(map_keys(names.common))),2) AS avg_languages,
      ROUND(100.0*AVG(CASE WHEN size(map_keys(names.common))>=2 THEN 1 ELSE 0 END),1) AS pct_multilingual
    FROM segment WHERE class IS NOT NULL AND names.primary IS NOT NULL
    GROUP BY class ORDER BY named_segments DESC""",
  "rw9" -> """SELECT categories.primary AS category, COUNT(*) AS pois,
      ROUND(100.0*AVG(CASE WHEN element_at(addresses,1).locality IS NOT NULL THEN 1 ELSE 0 END),1) AS pct_locality,
      ROUND(100.0*AVG(CASE WHEN element_at(addresses,1).postcode IS NOT NULL THEN 1 ELSE 0 END),1) AS pct_postcode,
      ROUND(100.0*AVG(CASE WHEN element_at(addresses,1).locality IS NOT NULL AND element_at(addresses,1).postcode IS NOT NULL THEN 1 ELSE 0 END),1) AS pct_addressable
    FROM places WHERE categories.primary IS NOT NULL GROUP BY categories.primary HAVING COUNT(*)>=5000 ORDER BY pct_addressable DESC""")

def dump(tag: String, n: Int) = {
  val df = spark.sql(Q(tag))
  println(s"RESHDR_$tag " + df.columns.mkString("|"))
  df.take(n).foreach { r => println(s"RES_$tag " + (0 until r.length).map(i => Option(r.get(i)).map(_.toString).getOrElse("")).mkString("|")) }
  println(s"RESN_$tag " + df.count())
}
dump("rw6", 15); dump("rw7", 15); dump("rw8", 15); dump("rw9", 20)
System.exit(0)
