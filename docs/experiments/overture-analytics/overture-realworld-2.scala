// =============================================================================
// Overture Maps — more real-world scan-heavy queries (memory-safe).
// Each states the QUESTION it answers. All are scan + GROUP BY on a low-
// cardinality key (dataset / class / category / year) -> map-side partial
// aggregation, only tiny aggregates shuffle. No join, no OOM. geometry pruned.
// Scan sizes MEASURED from Parquet column-chunk footers.
// Run in a live shell:  :load overture-realworld-2.scala   (edit BASE if needed)
// =============================================================================
val BASE = "/home/kuhu/Reps/spark-rapids/overture_2026-07-22"
spark.read.parquet(s"$BASE/places/type=place").createOrReplaceTempView("places")
spark.read.parquet(s"$BASE/addresses/type=address").createOrReplaceTempView("address")
spark.read.parquet(s"$BASE/divisions/type=division").createOrReplaceTempView("division")
spark.read.parquet(s"$BASE/transportation/type=segment").createOrReplaceTempView("segment")
spark.read.parquet(s"$BASE/transportation/type=connector").createOrReplaceTempView("connector")

// -----------------------------------------------------------------------------
// RW6  *** the scan-heavy one (~9.8 GB) — reads `sources` across ALL 5 themes ***
// QUESTION: Who actually mapped the world's basemap? Across roads, nodes, POIs,
//   addresses and admin areas, which upstream datasets/providers contribute the
//   data, how much of each, how confident are they, and how fresh (last update
//   year)? This is the provenance-bias audit — it shows how dependent the map is
//   on any single source (e.g. OSM) and where the data is going stale.
// SCAN: sources = segment 3.72 + connector 3.65 + places 2.32 + division 0.06 +
//   address 0.01 = ~9.8 GB, exploded over ~1.32B rows.
val rw6_provenance = spark.sql("""
  WITH src AS (
    SELECT s.dataset AS dataset, s.confidence AS conf, s.update_time AS ut FROM segment   LATERAL VIEW explode(sources) t AS s
    UNION ALL
    SELECT s.dataset, s.confidence, s.update_time FROM connector LATERAL VIEW explode(sources) t AS s
    UNION ALL
    SELECT s.dataset, s.confidence, s.update_time FROM places    LATERAL VIEW explode(sources) t AS s
    UNION ALL
    SELECT s.dataset, s.confidence, s.update_time FROM address   LATERAL VIEW explode(sources) t AS s
    UNION ALL
    SELECT s.dataset, s.confidence, s.update_time FROM division  LATERAL VIEW explode(sources) t AS s
  )
  SELECT
    dataset,
    COUNT(*)                                                   AS records,
    ROUND(AVG(conf), 3)                                        AS avg_confidence,
    MIN(SUBSTRING(ut, 1, 4))                                   AS oldest_year,
    MAX(SUBSTRING(ut, 1, 4))                                   AS newest_year,
    ROUND(100.0*AVG(CASE WHEN SUBSTRING(ut,1,4) >= '2024' THEN 1 ELSE 0 END), 1) AS pct_updated_2024plus
  FROM src
  GROUP BY dataset
  ORDER BY records DESC
""")

// -----------------------------------------------------------------------------
// RW7  Road-network freshness by class (~3.8 GB)
// QUESTION: How stale is each part of the road network? For each road class, what
//   is the last-update-year distribution of its source records — which classes are
//   actively maintained vs frozen years ago?
// SCAN: class 0.12 + sources 3.72 = ~3.8 GB over 348.7M segments (exploded).
val rw7_roadFreshness = spark.sql("""
  WITH s AS (
    SELECT seg.class AS class, SUBSTRING(src.update_time, 1, 4) AS yr
    FROM segment seg LATERAL VIEW explode(seg.sources) t AS src
    WHERE seg.class IS NOT NULL
  )
  SELECT
    class,
    COUNT(*)                                                             AS source_records,
    ROUND(100.0*AVG(CASE WHEN yr >= '2024' THEN 1 ELSE 0 END), 1)        AS pct_2024plus,
    ROUND(100.0*AVG(CASE WHEN yr <  '2022' THEN 1 ELSE 0 END), 1)        AS pct_before_2022,
    MIN(yr) AS oldest_year, MAX(yr) AS newest_year
  FROM s
  GROUP BY class
  ORDER BY source_records DESC
""")

// -----------------------------------------------------------------------------
// RW8  Multilingual naming coverage of the road network, by class (~1.4 GB)
// QUESTION: How multilingual is road naming? For each class, of the roads that
//   have a name, how many also carry alternate-language names (names.common map),
//   and how many languages on average? Shows i18n richness vs English/local-only.
// SCAN: class 0.12 + names 1.25 = ~1.4 GB over 348.7M segments.
val rw8_multilingualRoads = spark.sql("""
  SELECT
    class,
    COUNT(*)                                                                       AS named_segments,
    ROUND(AVG(size(map_keys(names.common))), 2)                                    AS avg_languages,
    ROUND(100.0*AVG(CASE WHEN size(map_keys(names.common)) >= 2 THEN 1 ELSE 0 END), 1) AS pct_multilingual
  FROM segment
  WHERE class IS NOT NULL AND names.primary IS NOT NULL
  GROUP BY class
  ORDER BY named_segments DESC
""")

// -----------------------------------------------------------------------------
// RW9  POI address completeness by category (~1.0 GB)
// QUESTION: Which kinds of business carry a usable street address vs only a point?
//   For each category, what share of POIs have a locality and a postcode in their
//   embedded address — i.e. are geocodable/deliverable, not just a dot on a map?
// SCAN: categories 0.30 + addresses 0.72 = ~1.0 GB over 74.2M places.
val rw9_poiAddressing = spark.sql("""
  SELECT
    categories.primary AS category,
    COUNT(*)           AS pois,
    ROUND(100.0*AVG(CASE WHEN element_at(addresses,1).locality IS NOT NULL THEN 1 ELSE 0 END), 1) AS pct_locality,
    ROUND(100.0*AVG(CASE WHEN element_at(addresses,1).postcode IS NOT NULL THEN 1 ELSE 0 END), 1) AS pct_postcode,
    ROUND(100.0*AVG(CASE WHEN element_at(addresses,1).locality IS NOT NULL
                          AND element_at(addresses,1).postcode IS NOT NULL THEN 1 ELSE 0 END), 1) AS pct_addressable
  FROM places
  WHERE categories.primary IS NOT NULL
  GROUP BY categories.primary
  HAVING COUNT(*) >= 5000
  ORDER BY pct_addressable DESC
""")

// ---- to run one: rwN.explain() then rwN.show(50, false) ----
