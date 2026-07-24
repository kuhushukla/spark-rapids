// =============================================================================
// Overture Maps — FULL-dataset queries that use SQL on the WKB geometry column
// WITHOUT decoding coordinates. Every geometry op below is GPU-accelerated
// (verified in sql-plugin GpuOverrides.scala):
//   length(binary)      -> GpuLength       (line 3684)  byte size = shape complexity
//   md5(binary)         -> GpuMd5          (line 2548)  hash = duplicate detection
//   substring(binary..) -> GpuSubstring    (line 3282)  read WKB header bytes
//   hex(binary)         -> GpuHex          (line 1017)  header bytes -> groupable
// Binary Parquet reads are GPU-native (TypeSig.BINARY, GpuOverrides ~889-931).
//
// Together these scan the WHOLE 128.8 GB (77 GB attributes + 51.6 GB geometry).
// Memory-safe: GROUP BY low-cardinality key; the COUNT(DISTINCT md5(geometry))
// duplicate checks are the intended heavy shuffle / runtime.
// =============================================================================
val BASE = "/home/kuhu/Reps/spark-rapids/overture_2026-07-22"
spark.read.parquet(s"$BASE/places/type=place").createOrReplaceTempView("places")
spark.read.parquet(s"$BASE/addresses/type=address").createOrReplaceTempView("address")
spark.read.parquet(s"$BASE/divisions/type=division").createOrReplaceTempView("division")
spark.read.parquet(s"$BASE/transportation/type=segment").createOrReplaceTempView("segment")
spark.read.parquet(s"$BASE/transportation/type=connector").createOrReplaceTempView("connector")

// -----------------------------------------------------------------------------
// GF1  *** the whole-theme scan (~70.7 GB: all segment columns incl geometry) ***
// QUESTION: Give a complete data-quality + geometry profile of the world road
//   network, by class: how well-attributed is each class (fill rates), how
//   geometrically complex are its shapes (avg/max WKB byte size ~ vertex count),
//   and are there duplicate/copied geometries (integrity)? This is the full
//   profile a team builds before trusting or ingesting the layer.
// SCAN: every non-geometry column (~31.8 GB) + geometry (~38.9 GB) = ~70.7 GB / 348.7M.
val gf1_roadFullProfile = spark.sql("""
  SELECT
    class,
    COUNT(*)                                                                       AS segments,
    -- geometry (WKB bytes, never decoded):
    ROUND(AVG(length(geometry)), 1)                                                AS avg_wkb_bytes,
    MAX(length(geometry))                                                          AS max_wkb_bytes,
    (COUNT(*) - COUNT(DISTINCT md5(geometry)))                                     AS duplicate_geometries,
    -- attribute completeness (forces reading every other column):
    ROUND(100.0*AVG(CASE WHEN names.primary IS NOT NULL      THEN 1 ELSE 0 END),1) AS pct_named,
    ROUND(100.0*AVG(CASE WHEN size(connectors) > 0           THEN 1 ELSE 0 END),1) AS pct_connectors,
    ROUND(100.0*AVG(CASE WHEN size(speed_limits) > 0         THEN 1 ELSE 0 END),1) AS pct_speed,
    ROUND(100.0*AVG(CASE WHEN size(access_restrictions) > 0  THEN 1 ELSE 0 END),1) AS pct_access,
    ROUND(100.0*AVG(CASE WHEN size(road_surface) > 0         THEN 1 ELSE 0 END),1) AS pct_surface,
    ROUND(100.0*AVG(CASE WHEN size(road_flags) > 0           THEN 1 ELSE 0 END),1) AS pct_flags,
    ROUND(100.0*AVG(CASE WHEN size(width_rules) > 0          THEN 1 ELSE 0 END),1) AS pct_width,
    ROUND(100.0*AVG(CASE WHEN size(prohibited_transitions)>0 THEN 1 ELSE 0 END),1) AS pct_turns,
    ROUND(100.0*AVG(CASE WHEN size(routes) > 0               THEN 1 ELSE 0 END),1) AS pct_routes,
    ROUND(100.0*AVG(CASE WHEN size(destinations) > 0         THEN 1 ELSE 0 END),1) AS pct_destinations,
    ROUND(100.0*AVG(CASE WHEN size(sources) > 0              THEN 1 ELSE 0 END),1) AS pct_sources,
    ROUND(AVG(bbox.xmax - bbox.xmin), 4)                                           AS avg_bbox_w
  FROM segment
  WHERE class IS NOT NULL
  GROUP BY class
  ORDER BY segments DESC
""")

// -----------------------------------------------------------------------------
// GF2  *** all-theme geometry scan (~51.6 GB: the geometry column, 5 themes) ***
// QUESTION: What geometry TYPES (point / linestring / polygon / multipolygon)
//   make up each Overture theme? Read straight from the 5-byte WKB header
//   (byte-order + type code) WITHOUT decoding any coordinates. Tells you the
//   shape vocabulary of the basemap and flags any non-standard/EWKB encodings.
// SCAN: geometry across places+division+address+segment+connector = ~51.6 GB / ~1.32B rows.
// NB: type mapping assumes standard OGC little-endian WKB; verify vs the raw
//     header column in the output before trusting the label.
val gf2_geometryTypes = spark.sql("""
  WITH g AS (
    SELECT 'segment'   AS theme, hex(substring(geometry,1,5)) AS hdr FROM segment
    UNION ALL SELECT 'connector', hex(substring(geometry,1,5)) FROM connector
    UNION ALL SELECT 'address',   hex(substring(geometry,1,5)) FROM address
    UNION ALL SELECT 'place',     hex(substring(geometry,1,5)) FROM places
    UNION ALL SELECT 'division',  hex(substring(geometry,1,5)) FROM division
  )
  SELECT
    theme,
    hdr AS wkb_header,
    CASE hdr
      WHEN '0101000000' THEN 'Point'      WHEN '0102000000' THEN 'LineString'
      WHEN '0103000000' THEN 'Polygon'    WHEN '0104000000' THEN 'MultiPoint'
      WHEN '0105000000' THEN 'MultiLineString'
      WHEN '0106000000' THEN 'MultiPolygon'
      WHEN '0107000000' THEN 'GeometryCollection'
      ELSE 'other/EWKB' END AS geometry_type,
    COUNT(*) AS features
  FROM g
  GROUP BY theme, hdr
  ORDER BY theme, features DESC
""")

// -----------------------------------------------------------------------------
// GF3  Duplicate / stacked address points, by country  (~21.8 GB: all of address)
// QUESTION: How many address points are exact-duplicate geometries stacked on the
//   same coordinate (a common integrity problem), and how does that vary by
//   country? A high duplicate rate means the point count overstates real coverage.
// SCAN: geometry (~5.5 GB) + id (9.8) + bbox (5.1) + street/number/postcode = ~21.8 GB / 472.7M.
//   COUNT(DISTINCT md5(geometry)) is the heavy shuffle.
val gf3_addressDuplicates = spark.sql("""
  SELECT
    country,
    COUNT(*)                                          AS address_points,
    COUNT(DISTINCT md5(geometry))                     AS distinct_locations,
    ROUND(100.0*(COUNT(*) - COUNT(DISTINCT md5(geometry))) / COUNT(*), 1) AS pct_stacked_duplicates,
    ROUND(100.0*AVG(CASE WHEN street IS NOT NULL THEN 1 ELSE 0 END), 1)   AS pct_street
  FROM address
  WHERE country IS NOT NULL
  GROUP BY country
  HAVING COUNT(*) >= 100000
  ORDER BY pct_stacked_duplicates DESC
""")

// ---- run one: gf1_roadFullProfile.explain(); gf1_roadFullProfile.show(50, false) ----
// Confirm the geometry ops show as GpuLength / GpuMd5 / GpuSubstring / GpuHex in explain().
