// POSITIVE CONTROL: confirm BJ1/BJ2/BJ3 plan to a big SortMergeJoin (both sides
// shuffled+sorted), NOT a broadcast. Then MEASURE BJ1's join output (bounded).
val BASE = "/home/kuhu/Reps/spark-rapids/overture_2026-07-22"
spark.read.parquet(s"$BASE/places/type=place").createOrReplaceTempView("places")
spark.read.parquet(s"$BASE/addresses/type=address").createOrReplaceTempView("address")
spark.read.parquet(s"$BASE/transportation/type=segment").createOrReplaceTempView("segment")
spark.read.parquet(s"$BASE/transportation/type=connector").createOrReplaceTempView("connector")
def hd(t:String){ println("\n#### "+t+" ####") }

hd("BJ1 segment.connectors ⋈ connector  (expect SortMergeJoin)")
spark.sql("""WITH refs AS (SELECT c.connector_id cid, s.id seg_id FROM segment s LATERAL VIEW explode(s.connectors) t AS c WHERE c.connector_id IS NOT NULL)
  SELECT cid,COUNT(*) d FROM (SELECT r.cid FROM refs r JOIN connector k ON r.cid=k.id) GROUP BY cid""").explain()

hd("BJ2 refs self-join on connector_id  (expect SortMergeJoin)")
spark.sql("""WITH refs AS (SELECT c.connector_id cid, s.id seg_id FROM segment s LATERAL VIEW explode(s.connectors) t AS c WHERE c.connector_id IS NOT NULL)
  SELECT COUNT(*) e FROM refs a JOIN refs b ON a.cid=b.cid AND a.seg_id<b.seg_id""").explain()

hd("BJ3 address ⋈ places on (country,postcode)  (expect SortMergeJoin)")
spark.sql("""WITH a AS (SELECT id addr_id,country,postcode FROM address WHERE country IS NOT NULL AND postcode IS NOT NULL),
  p AS (SELECT element_at(addresses,1).country country, element_at(addresses,1).postcode postcode FROM places WHERE element_at(addresses,1).postcode IS NOT NULL)
  SELECT a.country,COUNT(*) c FROM a JOIN p ON a.country=p.country AND a.postcode=p.postcode GROUP BY a.country""").explain()

// MEASURE BJ1 join output rows (bounded ~897M): matched refs + referential integrity
hd("BJ1 MEASURE")
val total = spark.sql("""SELECT SUM(size(connectors)) FROM segment""").head.getLong(0)
val matched = spark.sql("""WITH refs AS (SELECT c.connector_id cid FROM segment s LATERAL VIEW explode(s.connectors) t AS c WHERE c.connector_id IS NOT NULL)
  SELECT COUNT(*) FROM refs r JOIN connector k ON r.cid=k.id""").head.getLong(0)
println(f"BJ1 exploded refs        = $total%,d")
println(f"BJ1 join OUTPUT (matched)= $matched%,d")
println(f"BJ1 refs to MISSING node = ${total-matched}%,d")
System.exit(0)
