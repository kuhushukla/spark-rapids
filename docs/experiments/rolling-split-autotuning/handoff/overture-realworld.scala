// Real-world scan-heavy query on Overture segments: road-network coverage/composition by class.
// "For each road subtype+class, how many segments, what % are named, what % have a speed limit, and how
//  connected are they?" — a genuine data-coverage question. Null-safe (size()<=0 => absent). GROUP BY on a
// low-cardinality real key (~tens of groups) so the shuffle stays tiny and the query stays scan-dominated.
val BASE = "/home/kuhu/Reps/spark-rapids/overture_2026-07-22"
spark.read.parquet(s"$BASE/transportation/type=segment").createOrReplaceTempView("segment")
val q = """
SELECT
  subtype,
  class,
  COUNT(*)                                                                    AS segments,
  ROUND(100.0 * COUNT(names.primary) / COUNT(*), 1)                           AS pct_named,
  ROUND(100.0 * SUM(CASE WHEN size(speed_limits) > 0 THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_speed_limit,
  ROUND(AVG(CASE WHEN size(connectors) > 0 THEN size(connectors) ELSE 0 END), 2)       AS avg_connectors
FROM segment
GROUP BY subtype, class
ORDER BY segments DESC
"""
spark.sql(q).explain()      // confirm GpuFileSourceScan + only a small GpuColumnarExchange
spark.sql(q).show(40, false)
System.exit(0)
