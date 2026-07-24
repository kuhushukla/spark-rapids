// Verify the JOIN details: crosswalk build-side size, broadcast-vs-SMJ choice,
// and how many segment rows survive the inner join (coverage).
val BASE = "/home/kuhu/Reps/spark-rapids/overture_2026-07-22"
val C = 0.1
spark.read.parquet(s"$BASE/addresses/type=address").createOrReplaceTempView("address")
spark.read.parquet(s"$BASE/transportation/type=segment").createOrReplaceTempView("segment")
println("autoBroadcastJoinThreshold = " + spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))
println("adaptive.enabled = " + spark.conf.get("spark.sql.adaptive.enabled"))

spark.sql(s"""
  WITH counts AS (
    SELECT CAST(FLOOR(((bbox.xmin+bbox.xmax)/2)/$C) AS INT) cx,
           CAST(FLOOR(((bbox.ymin+bbox.ymax)/2)/$C) AS INT) cy, country, COUNT(*) n
    FROM address WHERE country IS NOT NULL GROUP BY 1,2,3),
  ranked AS (SELECT cx,cy,country,ROW_NUMBER() OVER (PARTITION BY cx,cy ORDER BY n DESC) rk FROM counts)
  SELECT cx,cy,country FROM ranked WHERE rk=1
""").createOrReplaceTempView("cell_country")
spark.sql("CACHE TABLE cell_country")
val cwRows = spark.table("cell_country").count()
val cwBytes = spark.sessionState.executePlan(spark.table("cell_country").queryExecution.analyzed)
  .optimizedPlan.stats.sizeInBytes
println(f"crosswalk build side: $cwRows%,d rows, est ${cwBytes.toLong/1e6}%.2f MB")

// The Q4 join, realistic (AQE on, default threshold). Show the chosen plan.
val q4 = spark.sql(s"""
  WITH seg AS (SELECT class,
      CAST(FLOOR(((bbox.xmin+bbox.xmax)/2)/$C) AS INT) cx,
      CAST(FLOOR(((bbox.ymin+bbox.ymax)/2)/$C) AS INT) cy
    FROM segment WHERE class IS NOT NULL)
  SELECT k.country, s.class, COUNT(*) n
  FROM seg s JOIN cell_country k ON s.cx=k.cx AND s.cy=k.cy
  GROUP BY k.country, s.class""")
q4.explain("cost")

// How many segment rows survive the inner join (coverage of the crosswalk)?
val segTotal = spark.sql("SELECT COUNT(*) FROM segment").head.getLong(0)
val segJoined = spark.sql(s"""
  SELECT COUNT(*) FROM (
    SELECT CAST(FLOOR(((bbox.xmin+bbox.xmax)/2)/$C) AS INT) cx,
           CAST(FLOOR(((bbox.ymin+bbox.ymax)/2)/$C) AS INT) cy FROM segment) s
  JOIN cell_country k ON s.cx=k.cx AND s.cy=k.cy""").head.getLong(0)
println(f"segment rows total     = $segTotal%,d")
println(f"segment rows JOINED    = $segJoined%,d  (${100.0*segJoined/segTotal}%.1f%% survive; rest are in cells with no addresses)")
System.exit(0)
