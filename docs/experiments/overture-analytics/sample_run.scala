// Smoke test: run the real queries against ONE parquet file per theme so we
// verify they execute (no analysis errors, real field paths) and return sane
// rows. Counts are a sample, not global totals.
val BASE = "/home/kuhu/Reps/spark-rapids/overture_2026-07-22"
spark.read.parquet(s"$BASE/places/type=place/part-00000-721bdadd-4327-5b81-bc83-aa244c71ceaa-c000.zstd.parquet").createOrReplaceTempView("places")
spark.read.parquet(s"$BASE/divisions/type=division").createOrReplaceTempView("division")
spark.read.parquet(s"$BASE/addresses/type=address").limit(2000000).createOrReplaceTempView("address")
spark.read.parquet(s"$BASE/transportation/type=segment").limit(2000000).createOrReplaceTempView("segment")

spark.sql("""SELECT categories.primary AS category, brand.names.primary AS brand,
  element_at(addresses,1).country AS country, confidence, operating_status,
  (size(websites)>0 OR size(phones)>0 OR size(emails)>0 OR size(socials)>0) AS has_contact,
  CAST(FLOOR(((bbox.xmin+bbox.xmax)/2)/0.1) AS INT) AS cx,
  CAST(FLOOR(((bbox.ymin+bbox.ymax)/2)/0.1) AS INT) AS cy FROM places""").createOrReplaceTempView("poi")
spark.sql("""SELECT country, street,
  CAST(FLOOR(((bbox.xmin+bbox.xmax)/2)/0.1) AS INT) AS cx,
  CAST(FLOOR(((bbox.ymin+bbox.ymax)/2)/0.1) AS INT) AS cy FROM address""").createOrReplaceTempView("addr")

println("== Q2 top brands (sample) ==")
spark.sql("""WITH b AS (SELECT country,brand,COUNT(*) n FROM poi WHERE brand IS NOT NULL AND country IS NOT NULL GROUP BY country,brand),
 ranked AS (SELECT country,brand,n,RANK() OVER (PARTITION BY country ORDER BY n DESC) rk FROM b)
 SELECT country,brand,n,rk FROM ranked WHERE rk<=3 ORDER BY n DESC LIMIT 15""").show(false)

println("== Q3 commercial mix (sample) ==")
spark.sql("""WITH m AS (SELECT country,category,COUNT(*) n FROM poi WHERE country IS NOT NULL AND category IS NOT NULL GROUP BY country,category),
 ranked AS (SELECT country,category,n,ROUND(100.0*n/SUM(n) OVER (PARTITION BY country),2) pct,RANK() OVER (PARTITION BY country ORDER BY n DESC) rk FROM m)
 SELECT country,category,n,pct FROM ranked WHERE rk<=3 ORDER BY n DESC LIMIT 15""").show(false)

println("== Q6 most common street (sample) ==")
spark.sql("""WITH s AS (SELECT country,street,COUNT(*) n FROM addr WHERE country IS NOT NULL AND street IS NOT NULL GROUP BY country,street),
 r AS (SELECT country,street,n,ROW_NUMBER() OVER (PARTITION BY country ORDER BY n DESC) rn FROM s)
 SELECT country,street,n FROM r WHERE rn=1 ORDER BY n DESC LIMIT 15""").show(false)

println("== Q9 how online (sample) ==")
spark.sql("""SELECT country,COUNT(*) pois,ROUND(100.0*AVG(CASE WHEN has_contact THEN 1 ELSE 0 END),2) pct_online
 FROM poi WHERE country IS NOT NULL GROUP BY country HAVING COUNT(*)>=200 ORDER BY pct_online DESC LIMIT 15""").show(false)

println("== Q17 intersection degree distribution (sample) ==")
spark.sql("""WITH refs AS (SELECT c.connector_id FROM segment LATERAL VIEW explode(connectors) t AS c WHERE c.connector_id IS NOT NULL),
 deg AS (SELECT connector_id,COUNT(*) degree FROM refs GROUP BY connector_id)
 SELECT degree,COUNT(*) n_nodes,ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER (),3) pct FROM deg GROUP BY degree ORDER BY degree LIMIT 12""").show(false)

System.exit(0)
