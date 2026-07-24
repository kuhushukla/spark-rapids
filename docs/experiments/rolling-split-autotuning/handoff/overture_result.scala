// Print the actual result row of the Overture segment profiling query.
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
spark.sql(q).show(false)
System.exit(0)
