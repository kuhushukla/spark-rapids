// =============================================================================
// Real-world question: "How complete and routing-ready is the world's road
// network, by road class?"  For each road class (motorway, residential, ...):
// what share of segments are named, speed-limited, surface-typed, and access-
// restricted, and how well-noded (connectors/segment) are they? This is the
// data-quality audit a routing/maps team runs to find where attribution is thin.
//
// Scan-heavy BY NECESSITY (not by contrivance): answering coverage across the
// whole network means reading the big attribute columns for all 348.7M segments.
//   connectors 12.24 + names 1.25 + access_restrictions 0.54 + speed_limits 0.42
//   + road_surface 0.17 + class 0.12  ~= 14.7 GB scanned (geometry 38.9 GB pruned).
// Memory-safe: GROUP BY class (~dozen values) -> one tiny shuffle, no join/explode.
// =============================================================================
val BASE = "/home/kuhu/Reps/spark-rapids/overture_2026-07-22"
spark.read.parquet(s"$BASE/transportation/type=segment").createOrReplaceTempView("segment")

val roadAudit = spark.sql("""
  SELECT
    class,
    COUNT(*)                                                                   AS segments,
    ROUND(AVG(size(connectors)), 2)                                            AS avg_connectors,
    ROUND(100.0*AVG(CASE WHEN names.primary IS NOT NULL     THEN 1 ELSE 0 END), 1) AS pct_named,
    ROUND(100.0*AVG(CASE WHEN size(speed_limits) > 0        THEN 1 ELSE 0 END), 1) AS pct_speed_limited,
    ROUND(100.0*AVG(CASE WHEN size(road_surface) > 0        THEN 1 ELSE 0 END), 1) AS pct_surface_known,
    ROUND(100.0*AVG(CASE WHEN size(access_restrictions) > 0 THEN 1 ELSE 0 END), 1) AS pct_access_restricted
  FROM segment
  WHERE class IS NOT NULL
  GROUP BY class
  ORDER BY segments DESC
""")

roadAudit.explain()          // expect GpuFileSourceScan -> GpuHashAggregate, tiny GpuShuffleExchange
roadAudit.show(50, false)
