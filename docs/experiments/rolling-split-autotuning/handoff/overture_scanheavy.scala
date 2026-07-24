// Scan-heavy Overture segment query — timing loop for the maxPartitionBytes baseline sweep.
// Driven by spark-shell -i; maxPartitionBytes / eventLog / plugin confs come from spark-shell --conf.
// Iterations via -Dbench.iters (default 5): iter1 cold, iters 2..M warm. Prints OVERTURE_ITER lines.
val BASE = "/home/kuhu/Reps/spark-rapids/overture_2026-07-22"
spark.read.parquet(s"$BASE/transportation/type=segment").createOrReplaceTempView("segment")

val q = """
SELECT
  COUNT(*)                          AS segments,
  SUM(size(connectors))             AS total_connector_refs,
  AVG(size(access_restrictions))    AS avg_access_restr,
  AVG(size(speed_limits))           AS avg_speed_limits,
  COUNT(names.primary)              AS named_segments,
  SUM(size(sources))                AS total_sources,
  AVG(bbox.xmax - bbox.xmin)        AS avg_bbox_width_deg
FROM segment
"""

// confirm GpuFileSourceScan, no big GpuShuffleExchange
spark.sql(q).explain()

val M = sys.props.getOrElse("bench.iters", "5").toInt
for (i <- 1 to M) {
  val t0 = System.nanoTime()
  val r = spark.sql(q).collect()
  val ms = (System.nanoTime() - t0) / 1e6
  println(f"OVERTURE_ITER $i%d ${ms}%.0f")
}
System.exit(0)
