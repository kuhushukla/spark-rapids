// EXPLAIN-only run: confirms the DOMINANT operator per query family from the
// real physical plan (Catalyst planning; does not scan the 125 GiB of data).
val BASE = "/home/kuhu/Reps/spark-rapids/overture_2026-07-22"
spark.conf.set("spark.sql.adaptive.enabled", "false")  // show static plan operators

spark.read.parquet(s"$BASE/places/type=place").createOrReplaceTempView("places")
spark.read.parquet(s"$BASE/divisions/type=division").createOrReplaceTempView("division")
spark.read.parquet(s"$BASE/addresses/type=address").createOrReplaceTempView("address")
spark.read.parquet(s"$BASE/transportation/type=segment").createOrReplaceTempView("segment")

spark.sql("""SELECT categories.primary AS category, brand.names.primary AS brand,
  element_at(addresses,1).country AS country,
  CAST(FLOOR(((bbox.xmin+bbox.xmax)/2)/0.1) AS INT) AS cx,
  CAST(FLOOR(((bbox.ymin+bbox.ymax)/2)/0.1) AS INT) AS cy FROM places""").createOrReplaceTempView("poi")
spark.sql("""SELECT country, street,
  CAST(FLOOR(((bbox.xmin+bbox.xmax)/2)/0.1) AS INT) AS cx,
  CAST(FLOOR(((bbox.ymin+bbox.ymax)/2)/0.1) AS INT) AS cy FROM address""").createOrReplaceTempView("addr")
spark.sql("""WITH counts AS (SELECT cx,cy,country,COUNT(*) n FROM addr WHERE country IS NOT NULL GROUP BY cx,cy,country),
  ranked AS (SELECT cx,cy,country,ROW_NUMBER() OVER (PARTITION BY cx,cy ORDER BY n DESC) rk FROM counts)
  SELECT cx,cy,country FROM ranked WHERE rk=1""").createOrReplaceTempView("cell_country")

def head(t:String){ println("\n\n#### "+t+" ####") }

head("Q6 street-name-per-country (family A, big aggregate)")
spark.sql("""WITH s AS (SELECT country,street,COUNT(*) n FROM addr WHERE country IS NOT NULL AND street IS NOT NULL GROUP BY country,street),
  r AS (SELECT country,street,n,ROW_NUMBER() OVER (PARTITION BY country ORDER BY n DESC) rn FROM s)
  SELECT country,street,n FROM r WHERE rn=1""").explain("formatted")

head("Q11 business-vs-bedroom (family B, grid join)")
spark.sql("""WITH pg AS (SELECT cx,cy,COUNT(*) pois FROM poi GROUP BY cx,cy), ag AS (SELECT cx,cy,COUNT(*) addrs FROM addr GROUP BY cx,cy)
  SELECT COALESCE(pg.cx,ag.cx) cx, COALESCE(pg.cy,ag.cy) cy, COALESCE(pois,0) pois, COALESCE(addrs,0) addrs
  FROM pg FULL OUTER JOIN ag ON pg.cx=ag.cx AND pg.cy=ag.cy""").explain("formatted")

head("Q17 intersection-degree (family C, explode + aggregate)")
spark.sql("""WITH refs AS (SELECT c.connector_id FROM segment LATERAL VIEW explode(connectors) t AS c WHERE c.connector_id IS NOT NULL),
  deg AS (SELECT connector_id,COUNT(*) degree FROM refs GROUP BY connector_id)
  SELECT degree,COUNT(*) n_nodes FROM deg GROUP BY degree ORDER BY degree""").explain("formatted")

head("Q4 road-class-by-country (segment + crosswalk join)")
spark.sql("""WITH seg AS (SELECT class, CAST(FLOOR(((bbox.xmin+bbox.xmax)/2)/0.1) AS INT) cx, CAST(FLOOR(((bbox.ymin+bbox.ymax)/2)/0.1) AS INT) cy FROM segment WHERE class IS NOT NULL)
  SELECT k.country,s.class,COUNT(*) n FROM seg s JOIN cell_country k ON s.cx=k.cx AND s.cy=k.cy GROUP BY k.country,s.class""").explain("formatted")

System.exit(0)
