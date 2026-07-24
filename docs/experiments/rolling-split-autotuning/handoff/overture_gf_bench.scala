// Benchmark harness for the geometry-FULL Overture queries (overture-geometry-full.scala): GF1/GF2/GF3.
// Queries inlined (no :load). -Dbench.query=gf1|gf2|gf3|all, -Dbench.iters=N, -Dbench.explain=true.
// Prints  GF_ITER <query> <i> <ms>  rows=<n>.
val BASE = "/home/kuhu/Reps/spark-rapids/overture_2026-07-22"
spark.read.parquet(s"$BASE/places/type=place").createOrReplaceTempView("places")
spark.read.parquet(s"$BASE/addresses/type=address").createOrReplaceTempView("address")
spark.read.parquet(s"$BASE/divisions/type=division").createOrReplaceTempView("division")
spark.read.parquet(s"$BASE/transportation/type=segment").createOrReplaceTempView("segment")
spark.read.parquet(s"$BASE/transportation/type=connector").createOrReplaceTempView("connector")

val queries = Map(
  "gf1" -> """
    SELECT class, COUNT(*) AS segments,
      ROUND(AVG(length(geometry)),1) AS avg_wkb_bytes, MAX(length(geometry)) AS max_wkb_bytes,
      (COUNT(*) - approx_count_distinct(md5(geometry))) AS approx_duplicate_geometries,
      ROUND(100.0*AVG(CASE WHEN names.primary IS NOT NULL THEN 1 ELSE 0 END),1) AS pct_named,
      ROUND(100.0*AVG(CASE WHEN size(connectors)>0 THEN 1 ELSE 0 END),1) AS pct_connectors,
      ROUND(100.0*AVG(CASE WHEN size(speed_limits)>0 THEN 1 ELSE 0 END),1) AS pct_speed,
      ROUND(100.0*AVG(CASE WHEN size(access_restrictions)>0 THEN 1 ELSE 0 END),1) AS pct_access,
      ROUND(100.0*AVG(CASE WHEN size(road_surface)>0 THEN 1 ELSE 0 END),1) AS pct_surface,
      ROUND(100.0*AVG(CASE WHEN size(road_flags)>0 THEN 1 ELSE 0 END),1) AS pct_flags,
      ROUND(100.0*AVG(CASE WHEN size(width_rules)>0 THEN 1 ELSE 0 END),1) AS pct_width,
      ROUND(100.0*AVG(CASE WHEN size(prohibited_transitions)>0 THEN 1 ELSE 0 END),1) AS pct_turns,
      ROUND(100.0*AVG(CASE WHEN size(routes)>0 THEN 1 ELSE 0 END),1) AS pct_routes,
      ROUND(100.0*AVG(CASE WHEN size(destinations)>0 THEN 1 ELSE 0 END),1) AS pct_destinations,
      ROUND(100.0*AVG(CASE WHEN size(sources)>0 THEN 1 ELSE 0 END),1) AS pct_sources,
      ROUND(AVG(bbox.xmax - bbox.xmin),4) AS avg_bbox_w
    FROM segment WHERE class IS NOT NULL GROUP BY class ORDER BY segments DESC""",
  "gf2" -> """
    WITH g AS (
      SELECT 'segment' AS theme, hex(substring(geometry,1,5)) AS hdr FROM segment
      UNION ALL SELECT 'connector', hex(substring(geometry,1,5)) FROM connector
      UNION ALL SELECT 'address',   hex(substring(geometry,1,5)) FROM address
      UNION ALL SELECT 'place',     hex(substring(geometry,1,5)) FROM places
      UNION ALL SELECT 'division',  hex(substring(geometry,1,5)) FROM division )
    SELECT theme, hdr AS wkb_header,
      CASE hdr WHEN '0101000000' THEN 'Point' WHEN '0102000000' THEN 'LineString'
        WHEN '0103000000' THEN 'Polygon' WHEN '0104000000' THEN 'MultiPoint'
        WHEN '0105000000' THEN 'MultiLineString' WHEN '0106000000' THEN 'MultiPolygon'
        WHEN '0107000000' THEN 'GeometryCollection' ELSE 'other/EWKB' END AS geometry_type,
      COUNT(*) AS features
    FROM g GROUP BY theme, hdr ORDER BY theme, features DESC""",
  "gf3" -> """
    SELECT country, COUNT(*) AS address_points,
      COUNT(DISTINCT md5(geometry)) AS distinct_locations,
      ROUND(100.0*(COUNT(*) - COUNT(DISTINCT md5(geometry)))/COUNT(*),1) AS pct_stacked_duplicates,
      ROUND(100.0*AVG(CASE WHEN street IS NOT NULL THEN 1 ELSE 0 END),1) AS pct_street
    FROM address WHERE country IS NOT NULL GROUP BY country HAVING COUNT(*)>=100000
    ORDER BY pct_stacked_duplicates DESC""")

val which = sys.props.getOrElse("bench.query","all")
val M     = sys.props.getOrElse("bench.iters","5").toInt
val doExplain = sys.props.getOrElse("bench.explain","false").toBoolean
val toRun = if (which=="all") Seq("gf1","gf2","gf3") else Seq(which)
for (name <- toRun) {
  val q = queries(name)
  if (doExplain) { println(s"########## EXPLAIN $name ##########"); spark.sql(q).explain() }
  for (i <- 1 to M) {
    val t0 = System.nanoTime()
    val n = spark.sql(q).collect().length
    val ms = (System.nanoTime()-t0)/1e6
    println(f"GF_ITER $name%s $i%d ${ms}%.0f  rows=$n%d")
  }
}
System.exit(0)
