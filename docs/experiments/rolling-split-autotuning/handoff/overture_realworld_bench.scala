// Benchmark harness for the REAL-WORLD Overture query (road-network coverage by class) — timing loop.
// Driven by spark-shell -i; maxPartitionBytes / eventLog / plugin confs from --conf. Iterations via
// -Dbench.iters (default 5): iter1 cold, 2..M warm. Prints OVERTURE_ITER lines.
val BASE = "/home/kuhu/Reps/spark-rapids/overture_2026-07-22"
spark.read.parquet(s"$BASE/transportation/type=segment").createOrReplaceTempView("segment")
val q = """
SELECT
  subtype, class,
  COUNT(*)                                                                    AS segments,
  ROUND(100.0 * COUNT(names.primary) / COUNT(*), 1)                           AS pct_named,
  ROUND(100.0 * SUM(CASE WHEN size(speed_limits) > 0 THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_speed_limit,
  ROUND(AVG(CASE WHEN size(connectors) > 0 THEN size(connectors) ELSE 0 END), 2)       AS avg_connectors
FROM segment
GROUP BY subtype, class
ORDER BY segments DESC
"""
spark.sql(q).explain()
val M = sys.props.getOrElse("bench.iters", "5").toInt
for (i <- 1 to M) {
  val t0 = System.nanoTime()
  spark.sql(q).collect()
  val ms = (System.nanoTime() - t0) / 1e6
  println(f"OVERTURE_ITER $i%d ${ms}%.0f")
}
System.exit(0)
