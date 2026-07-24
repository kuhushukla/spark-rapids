// Interleaved maxPartitionBytes probe for the real-world query — cancels cross-config drift (thermal/cache/JVM).
// One session; each round runs every config back-to-back (maxPartitionBytes set per query, read at planning).
val BASE = "/home/kuhu/Reps/spark-rapids/overture_2026-07-22"
spark.read.parquet(s"$BASE/transportation/type=segment").createOrReplaceTempView("segment")
val q = """
SELECT subtype, class, COUNT(*) AS segments,
  ROUND(100.0*COUNT(names.primary)/COUNT(*),1)                                  AS pct_named,
  ROUND(100.0*SUM(CASE WHEN size(speed_limits)>0 THEN 1 ELSE 0 END)/COUNT(*),1) AS pct_speed_limit,
  ROUND(AVG(CASE WHEN size(connectors)>0 THEN size(connectors) ELSE 0 END),2)   AS avg_connectors
FROM segment GROUP BY subtype, class ORDER BY segments DESC
"""
val configs = Seq("256m","512m","1g","2g")
val rounds  = sys.props.getOrElse("bench.rounds","10").toInt
// warm the page cache / JIT once (not timed)
spark.conf.set("spark.sql.files.maxPartitionBytes","512m"); spark.sql(q).collect()
for (r <- 1 to rounds) {
  for (c <- configs) {
    spark.conf.set("spark.sql.files.maxPartitionBytes", c)
    val t0 = System.nanoTime(); spark.sql(q).collect(); val ms = (System.nanoTime()-t0)/1e6
    println(f"INTERLEAVED $c%-5s round $r%d ${ms}%.0f")
  }
}
System.exit(0)
