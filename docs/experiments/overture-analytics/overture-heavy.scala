// =============================================================================
// Overture Maps — HEAVY real-world queries (wide scans, 16-32 GB each).
// These read (almost) every non-geometry column of a theme because the QUESTION
// genuinely requires inspecting every field (completeness/integrity audits) or
// deduplicating hundreds of millions of ids. geometry (WKB) still pruned.
// Memory-safe: GROUP BY low-cardinality key; the one big shuffle is the
// COUNT(DISTINCT id) integrity checks (that's the intended runtime).
// Scan sizes MEASURED from Parquet column-chunk footers.
// =============================================================================
val BASE = "/home/kuhu/Reps/spark-rapids/overture_2026-07-22"
spark.read.parquet(s"$BASE/addresses/type=address").createOrReplaceTempView("address")
spark.read.parquet(s"$BASE/transportation/type=segment").createOrReplaceTempView("segment")
spark.read.parquet(s"$BASE/transportation/type=connector").createOrReplaceTempView("connector")

// -----------------------------------------------------------------------------
// HW-ROAD  Road-network attribute completeness, by class  (~31.8 GB)
// QUESTION: For the global road network, which attributes are actually populated
//   enough to build routing/analytics on, and how does that differ by road class?
//   Reports the fill rate of EVERY segment attribute per class -> the field-
//   usability audit an engineer runs before trusting the data.
// SCAN: reads all non-geometry segment columns (connectors 12.24, id 7.53,
//   bbox 4.76, sources 3.72, names 1.25, + every list column) = ~31.8 GB / 348.7M.
val hwRoad = spark.sql("""
  SELECT
    class,
    COUNT(*)                                                                          AS segments,
    COUNT(id)                                                                         AS with_id,
    ROUND(AVG(bbox.xmax - bbox.xmin), 4)                                              AS avg_bbox_w,
    ROUND(100.0*AVG(CASE WHEN subclass IS NOT NULL           THEN 1 ELSE 0 END),1)    AS pct_subclass,
    ROUND(100.0*AVG(CASE WHEN names.primary IS NOT NULL      THEN 1 ELSE 0 END),1)    AS pct_named,
    ROUND(100.0*AVG(CASE WHEN size(connectors) > 0           THEN 1 ELSE 0 END),1)    AS pct_connectors,
    ROUND(100.0*AVG(CASE WHEN size(speed_limits) > 0         THEN 1 ELSE 0 END),1)    AS pct_speed,
    ROUND(100.0*AVG(CASE WHEN size(access_restrictions) > 0  THEN 1 ELSE 0 END),1)    AS pct_access,
    ROUND(100.0*AVG(CASE WHEN size(road_surface) > 0         THEN 1 ELSE 0 END),1)    AS pct_surface,
    ROUND(100.0*AVG(CASE WHEN size(road_flags) > 0           THEN 1 ELSE 0 END),1)    AS pct_flags,
    ROUND(100.0*AVG(CASE WHEN size(width_rules) > 0          THEN 1 ELSE 0 END),1)    AS pct_width,
    ROUND(100.0*AVG(CASE WHEN size(level_rules) > 0          THEN 1 ELSE 0 END),1)    AS pct_level,
    ROUND(100.0*AVG(CASE WHEN size(prohibited_transitions)>0 THEN 1 ELSE 0 END),1)    AS pct_turns,
    ROUND(100.0*AVG(CASE WHEN size(routes) > 0               THEN 1 ELSE 0 END),1)    AS pct_routes,
    ROUND(100.0*AVG(CASE WHEN size(destinations) > 0         THEN 1 ELSE 0 END),1)    AS pct_destinations,
    ROUND(100.0*AVG(CASE WHEN size(rail_flags) > 0           THEN 1 ELSE 0 END),1)    AS pct_rail,
    ROUND(100.0*AVG(CASE WHEN size(subclass_rules) > 0       THEN 1 ELSE 0 END),1)    AS pct_subclass_rules,
    ROUND(100.0*AVG(CASE WHEN size(sources) > 0              THEN 1 ELSE 0 END),1)    AS pct_sources
  FROM segment
  WHERE class IS NOT NULL
  GROUP BY class
  ORDER BY segments DESC
""")

// -----------------------------------------------------------------------------
// HW-ADDR  Address completeness & ID integrity, by country  (~16.3 GB)
// QUESTION: How trustworthy is the global address layer? Per country: how many
//   address points, are their GERS ids unique (any duplicates?), and what share
//   carry street / number / postcode / unit — i.e. are geocodable, not just dots?
// SCAN: id 9.77 + bbox 5.14 + number 0.72 + street 0.47 + unit 0.08 + postcode
//   0.08 + country = ~16.3 GB / 472.7M. The COUNT(DISTINCT id) is the heavy shuffle.
val hwAddr = spark.sql("""
  SELECT
    country,
    COUNT(*)                                                              AS addresses,
    COUNT(DISTINCT id)                                                    AS distinct_ids,
    (COUNT(*) - COUNT(DISTINCT id))                                       AS duplicate_ids,
    ROUND(100.0*AVG(CASE WHEN street   IS NOT NULL THEN 1 ELSE 0 END),1)  AS pct_street,
    ROUND(100.0*AVG(CASE WHEN number   IS NOT NULL THEN 1 ELSE 0 END),1)  AS pct_number,
    ROUND(100.0*AVG(CASE WHEN postcode IS NOT NULL THEN 1 ELSE 0 END),1)  AS pct_postcode,
    ROUND(100.0*AVG(CASE WHEN unit     IS NOT NULL THEN 1 ELSE 0 END),1)  AS pct_unit,
    ROUND(AVG(bbox.xmax - bbox.xmin), 6)                                  AS avg_bbox_w
  FROM address
  WHERE country IS NOT NULL
  GROUP BY country
  HAVING COUNT(*) >= 100000
  ORDER BY addresses DESC
""")

// -----------------------------------------------------------------------------
// HW-NODE  Connector (road-node) integrity & provenance  (~18.4 GB)
// QUESTION: The connector layer is the glue that makes roads routable. How many
//   nodes are there, are their ids unique, who sourced them, and how confident /
//   fresh are those sources? A hidden duplicate or low-confidence node breaks
//   routing, so this is the integrity check on the network's join keys.
// SCAN: id 8.64 + bbox 6.13 + sources 3.65 = ~18.4 GB / 416.8M connectors.
val hwNode = spark.sql("""
  WITH n AS (
    SELECT id,
           element_at(sources, 1).dataset    AS dataset,
           element_at(sources, 1).confidence AS conf
    FROM connector
  )
  SELECT
    COALESCE(dataset, 'unknown')                                    AS dataset,
    COUNT(*)                                                        AS nodes,
    COUNT(DISTINCT id)                                              AS distinct_ids,
    (COUNT(*) - COUNT(DISTINCT id))                                 AS duplicate_ids,
    ROUND(AVG(conf), 3)                                             AS avg_confidence,
    ROUND(100.0*AVG(CASE WHEN conf < 0.5 THEN 1 ELSE 0 END), 1)     AS pct_low_conf
  FROM n
  GROUP BY COALESCE(dataset, 'unknown')
  ORDER BY nodes DESC
""")

// ---- run one: hwRoad.explain(); hwRoad.show(50, false) ----
