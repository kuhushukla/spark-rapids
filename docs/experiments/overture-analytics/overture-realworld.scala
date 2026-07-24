// =============================================================================
// Overture Maps — real-world analytical queries (scan + group-by, memory-safe)
// Every query states the QUESTION it answers. All are single-table scans with a
// GROUP BY on a low-cardinality key -> one small shuffle, no join/explode, so
// they never OOM (unlike the big-join set). Scan sizes are MEASURED from the
// Parquet column-chunk footers. geometry (WKB) is always pruned.
// Run in a live shell:  :load overture-realworld.scala   (edit BASE if needed)
// =============================================================================
val BASE = "/home/kuhu/Reps/spark-rapids/overture_2026-07-22"
spark.read.parquet(s"$BASE/places/type=place").createOrReplaceTempView("places")
spark.read.parquet(s"$BASE/addresses/type=address").createOrReplaceTempView("address")
spark.read.parquet(s"$BASE/transportation/type=segment").createOrReplaceTempView("segment")

// -----------------------------------------------------------------------------
// RW1  *** the scan-heavy one (~14.7 GB) — recommended GPU-scan stress ***
// QUESTION: How complete and routing-ready is the world's road network, by road
//   class? For each class (motorway, residential, ...), what share of segments
//   are named, speed-limited, surface-typed, and access-restricted, and how well
//   noded (connectors/segment) are they? This is the data-quality audit a routing
//   team runs to find where road attribution is too thin to route on.
// SCAN: connectors 12.24 + names 1.25 + access_restrictions 0.54 + speed_limits
//   0.42 + road_surface 0.17 + class 0.12 = ~14.7 GB over 348.7M segments.
val rw1_roadReadiness = spark.sql("""
  SELECT
    class,
    COUNT(*)                                                                       AS segments,
    ROUND(AVG(size(connectors)), 2)                                                AS avg_connectors,
    ROUND(100.0*AVG(CASE WHEN names.primary IS NOT NULL     THEN 1 ELSE 0 END), 1) AS pct_named,
    ROUND(100.0*AVG(CASE WHEN size(speed_limits) > 0        THEN 1 ELSE 0 END), 1) AS pct_speed_limited,
    ROUND(100.0*AVG(CASE WHEN size(road_surface) > 0        THEN 1 ELSE 0 END), 1) AS pct_surface_known,
    ROUND(100.0*AVG(CASE WHEN size(access_restrictions) > 0 THEN 1 ELSE 0 END), 1) AS pct_access_restricted
  FROM segment
  WHERE class IS NOT NULL
  GROUP BY class
  ORDER BY segments DESC
""")

// -----------------------------------------------------------------------------
// RW2  Digital presence of businesses, by category (~2.4 GB)
// QUESTION: Which kinds of business are most likely to be reachable online —
//   i.e. have a website, phone, email, or social handle? Shows which sectors are
//   digitized vs invisible on the web.
// SCAN: categories 0.30 + websites 0.68 + phones 0.40 + emails 0.41 + socials
//   0.62 = ~2.4 GB over 74.2M places.
val rw2_digitalPresence = spark.sql("""
  SELECT
    categories.primary AS category,
    COUNT(*)           AS pois,
    ROUND(100.0*AVG(CASE WHEN size(websites) > 0 THEN 1 ELSE 0 END), 1) AS pct_website,
    ROUND(100.0*AVG(CASE WHEN size(phones)   > 0 THEN 1 ELSE 0 END), 1) AS pct_phone,
    ROUND(100.0*AVG(CASE WHEN size(socials)  > 0 THEN 1 ELSE 0 END), 1) AS pct_social,
    ROUND(100.0*AVG(CASE WHEN size(websites)>0 OR size(phones)>0
                          OR size(emails)>0 OR size(socials)>0 THEN 1 ELSE 0 END), 1) AS pct_any_contact
  FROM places
  WHERE categories.primary IS NOT NULL
  GROUP BY categories.primary
  HAVING COUNT(*) >= 5000
  ORDER BY pct_any_contact DESC
""")

// -----------------------------------------------------------------------------
// RW3  Addressing completeness by country (~1.3 GB)
// QUESTION: Which countries have complete street-level addressing (street +
//   house number + postcode) versus sparse, partial coverage? Tells you where the
//   address layer is trustworthy enough for geocoding/delivery.
// SCAN: country 0.001 + street 0.47 + number 0.72 + postcode 0.08 = ~1.3 GB
//   over 472.7M addresses.
val rw3_addressingCompleteness = spark.sql("""
  SELECT
    country,
    COUNT(*)                                                                AS addresses,
    ROUND(100.0*AVG(CASE WHEN street   IS NOT NULL THEN 1 ELSE 0 END), 1)   AS pct_street,
    ROUND(100.0*AVG(CASE WHEN number   IS NOT NULL THEN 1 ELSE 0 END), 1)   AS pct_number,
    ROUND(100.0*AVG(CASE WHEN postcode IS NOT NULL THEN 1 ELSE 0 END), 1)   AS pct_postcode,
    ROUND(100.0*AVG(CASE WHEN street IS NOT NULL AND number IS NOT NULL
                          AND postcode IS NOT NULL THEN 1 ELSE 0 END), 1)    AS pct_fully_addressed
  FROM address
  WHERE country IS NOT NULL
  GROUP BY country
  HAVING COUNT(*) >= 100000
  ORDER BY pct_fully_addressed DESC
""")

// -----------------------------------------------------------------------------
// RW4  Global business landscape & data confidence, by category (~0.68 GB)
// QUESTION: What are the most common types of POI/business in the world, and how
//   confident is Overture in each? Low-confidence, high-count categories are where
//   the map is noisiest.
// SCAN: categories 0.30 + confidence 0.37 = ~0.68 GB over 74.2M places.
val rw4_businessLandscape = spark.sql("""
  SELECT
    categories.primary       AS category,
    COUNT(*)                 AS pois,
    ROUND(AVG(confidence),3) AS avg_confidence,
    ROUND(100.0*AVG(CASE WHEN confidence < 0.5 THEN 1 ELSE 0 END), 1) AS pct_low_confidence
  FROM places
  WHERE categories.primary IS NOT NULL
  GROUP BY categories.primary
  ORDER BY pois DESC
""")

// -----------------------------------------------------------------------------
// RW5  Road surface composition by class (~0.3 GB)
// QUESTION: What is the paved / unpaved / unknown surface mix of each road class?
//   Surface drives routing speed and vehicle suitability; unknowns are gaps.
// SCAN: class 0.12 + road_surface 0.17 = ~0.3 GB over 348.7M segments.
val rw5_surfaceMix = spark.sql("""
  SELECT
    class,
    COALESCE(element_at(road_surface, 1).value, 'unknown') AS surface,
    COUNT(*) AS segments
  FROM segment
  WHERE class IS NOT NULL
  GROUP BY class, COALESCE(element_at(road_surface, 1).value, 'unknown')
  ORDER BY class, segments DESC
""")

// ---- to run one: rwN.explain() then rwN.show(50, false) ----
// (Left un-executed on purpose so you choose what to run and when.)
