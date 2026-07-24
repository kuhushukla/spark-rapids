// Measure the join/explode cardinalities on the FULL dataset (aggregates only).
val BASE = "/home/kuhu/Reps/spark-rapids/overture_2026-07-22"
val C = 0.1
def cell(c:String) = s"CAST(FLOOR((($c.xmin+$c.xmax)/2)/$C) AS INT)"
def cy(c:String)   = s"CAST(FLOOR((($c.ymin+$c.ymax)/2)/$C) AS INT)"

val places = spark.read.parquet(s"$BASE/places/type=place")
val addr   = spark.read.parquet(s"$BASE/addresses/type=address")
val seg    = spark.read.parquet(s"$BASE/transportation/type=segment")
val conn   = spark.read.parquet(s"$BASE/transportation/type=connector")
places.createOrReplaceTempView("places"); addr.createOrReplaceTempView("address")
seg.createOrReplaceTempView("segment");   conn.createOrReplaceTempView("connector")

println(f"places rows      = ${places.count()}%,d")
println(f"address rows     = ${addr.count()}%,d")
println(f"segment rows     = ${seg.count()}%,d")
println(f"connector rows   = ${conn.count()}%,d   (node universe)")

// distinct grid cells (0.1 deg) each side
val poiCells = spark.sql(s"SELECT COUNT(*) FROM (SELECT DISTINCT ${cell("bbox")} cx, ${cy("bbox")} cyy FROM places)").head.getLong(0)
val addrCells = spark.sql(s"SELECT COUNT(*) FROM (SELECT DISTINCT ${cell("bbox")} cx, ${cy("bbox")} cyy FROM address)").head.getLong(0)
val segCells = spark.sql(s"SELECT COUNT(*) FROM (SELECT DISTINCT ${cell("bbox")} cx, ${cy("bbox")} cyy FROM segment)").head.getLong(0)
println(f"poi  distinct cells = $poiCells%,d")
println(f"addr distinct cells = $addrCells%,d")
println(f"seg  distinct cells = $segCells%,d")

// crosswalk build side: distinct (cell,country) rows in, distinct cells out
val cwIn = spark.sql(s"SELECT COUNT(*) FROM (SELECT DISTINCT ${cell("bbox")} cx, ${cy("bbox")} cyy, country FROM address WHERE country IS NOT NULL)").head.getLong(0)
val cwCells = spark.sql(s"SELECT COUNT(*) FROM (SELECT DISTINCT ${cell("bbox")} cx, ${cy("bbox")} cyy FROM address WHERE country IS NOT NULL)").head.getLong(0)
println(f"crosswalk agg-input (cell,country) rows = $cwIn%,d")
println(f"crosswalk cells (build side of Q4/19/23) = $cwCells%,d")

// Q11 join output = |poi_cells UNION addr_cells| (full outer on cell)
val q11out = spark.sql(s"""SELECT COUNT(*) FROM (
  SELECT DISTINCT ${cell("bbox")} cx, ${cy("bbox")} cyy FROM places
  UNION SELECT DISTINCT ${cell("bbox")} cx, ${cy("bbox")} cyy FROM address)""").head.getLong(0)
println(f"Q11 full-outer-join output cells = $q11out%,d")

// Q17 explode fan-out: total connector references (no shuffle, just sum of sizes)
val refs = spark.sql("SELECT SUM(size(connectors)) FROM segment").head.getLong(0)
println(f"Q17 explode fan-out (total connector refs) = $refs%,d")
println(f"Q17 avg degree = ${refs.toDouble/conn.count()}%.2f refs per node")

System.exit(0)
