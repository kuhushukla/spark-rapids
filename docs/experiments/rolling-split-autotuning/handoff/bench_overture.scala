// Overture Maps dataset benchmark queries (multi-table: places, address, division, segment, connector).
// Selected via -Dbench.query (or "all"); -Dbench.iters, -Dbench.base, -Dbench.explain.
// bench.base default matches download_overture.sh output layout ($OUT/<theme>/type=<t>). These queries are
// MULTI-TABLE, so the ratio autotuner may pick a different split per table (see report per-table appendix).
// Prints  ITER <q> <i> <ms> rows=<n>.
//
// Real-world questions per query:
//   gf1 — Per road class: geometry footprint + attribute coverage (compute/memory-heavy on segment). [scan-heavy]
//   gf2 — WKB geometry-type distribution per theme (reads the geometry header byte across all 5 tables).
//   gf3 — Per country: address-point stacking (duplicate locations) + street coverage. [address]
//   hs1 — How much geometry data does each theme carry? (length(geometry) across all 5 tables). [scan-bound]
//   hs2 — Per road class: geometry footprint + name/attribute coverage (heavy scan on segment). [scan-bound]
//   hs3 — Heaviest single scan: segment geometry + names + ALL 10 nested arrays + bbox. [scan-bound]
//   rw6 — Provenance across all themes: records/confidence/recency per source dataset (explode sources).
//   rw7 — Per road class: source-record recency distribution (explode segment.sources).
//   rw8 — Per road class: multilingual name coverage (map_keys(names.common)).
//   rw9 — Per POI category: address completeness (locality/postcode present). [places]
//   ovJ1— Which junctions join the most road segments? (explode segment.connectors, GROUP BY the
//         connector id -> high-cardinality string key). [shuffle-heavy; EXPAND-bucket candidate]
val BASE = sys.props.getOrElse("bench.base", "/data/overture")
spark.read.parquet(s"$BASE/places/type=place").createOrReplaceTempView("places")
spark.read.parquet(s"$BASE/addresses/type=address").createOrReplaceTempView("address")
spark.read.parquet(s"$BASE/divisions/type=division").createOrReplaceTempView("division")
spark.read.parquet(s"$BASE/transportation/type=segment").createOrReplaceTempView("segment")
spark.read.parquet(s"$BASE/transportation/type=connector").createOrReplaceTempView("connector")

val queries = Map(
  "gf1" -> """
    SELECT class, COUNT(*) AS segments,
      ROUND(AVG(length(geometry)),1) AS avg_wkb_bytes, MAX(length(geometry)) AS max_wkb_bytes,
      (COUNT(*) - approx_count_distinct(md5(geometry))) AS approx_duplicate_geometries,
      ROUND(100.0*AVG(CASE WHEN names.primary IS NOT NULL THEN 1 ELSE 0 END),1) AS pct_named,
      ROUND(100.0*AVG(CASE WHEN size(connectors)>0 THEN 1 ELSE 0 END),1) AS pct_connectors,
      ROUND(100.0*AVG(CASE WHEN size(speed_limits)>0 THEN 1 ELSE 0 END),1) AS pct_speed,
      ROUND(100.0*AVG(CASE WHEN size(access_restrictions)>0 THEN 1 ELSE 0 END),1) AS pct_access,
      ROUND(100.0*AVG(CASE WHEN size(road_surface)>0 THEN 1 ELSE 0 END),1) AS pct_surface,
      ROUND(100.0*AVG(CASE WHEN size(road_flags)>0 THEN 1 ELSE 0 END),1) AS pct_flags,
      ROUND(100.0*AVG(CASE WHEN size(width_rules)>0 THEN 1 ELSE 0 END),1) AS pct_width,
      ROUND(100.0*AVG(CASE WHEN size(prohibited_transitions)>0 THEN 1 ELSE 0 END),1) AS pct_turns,
      ROUND(100.0*AVG(CASE WHEN size(routes)>0 THEN 1 ELSE 0 END),1) AS pct_routes,
      ROUND(100.0*AVG(CASE WHEN size(destinations)>0 THEN 1 ELSE 0 END),1) AS pct_destinations,
      ROUND(100.0*AVG(CASE WHEN size(sources)>0 THEN 1 ELSE 0 END),1) AS pct_sources,
      ROUND(AVG(bbox.xmax - bbox.xmin),4) AS avg_bbox_w
    FROM segment WHERE class IS NOT NULL GROUP BY class ORDER BY segments DESC""",

  "gf2" -> """
    WITH g AS (
      SELECT 'segment' AS theme, hex(substring(geometry,1,5)) AS hdr FROM segment
      UNION ALL SELECT 'connector', hex(substring(geometry,1,5)) FROM connector
      UNION ALL SELECT 'address',   hex(substring(geometry,1,5)) FROM address
      UNION ALL SELECT 'place',     hex(substring(geometry,1,5)) FROM places
      UNION ALL SELECT 'division',  hex(substring(geometry,1,5)) FROM division )
    SELECT theme, hdr AS wkb_header,
      CASE hdr WHEN '0101000000' THEN 'Point' WHEN '0102000000' THEN 'LineString'
        WHEN '0103000000' THEN 'Polygon' WHEN '0104000000' THEN 'MultiPoint'
        WHEN '0105000000' THEN 'MultiLineString' WHEN '0106000000' THEN 'MultiPolygon'
        WHEN '0107000000' THEN 'GeometryCollection' ELSE 'other/EWKB' END AS geometry_type,
      COUNT(*) AS features
    FROM g GROUP BY theme, hdr ORDER BY theme, features DESC""",

  "gf3" -> """
    SELECT country, COUNT(*) AS address_points,
      COUNT(DISTINCT md5(geometry)) AS distinct_locations,
      ROUND(100.0*(COUNT(*) - COUNT(DISTINCT md5(geometry)))/COUNT(*),1) AS pct_stacked_duplicates,
      ROUND(100.0*AVG(CASE WHEN street IS NOT NULL THEN 1 ELSE 0 END),1) AS pct_street
    FROM address WHERE country IS NOT NULL GROUP BY country HAVING COUNT(*)>=100000
    ORDER BY pct_stacked_duplicates DESC""",

  "hs1" -> """WITH g AS (
      SELECT 'segment'   AS theme, length(geometry) AS wkb FROM segment
      UNION ALL SELECT 'connector', length(geometry) FROM connector
      UNION ALL SELECT 'address',   length(geometry) FROM address
      UNION ALL SELECT 'place',     length(geometry) FROM places
      UNION ALL SELECT 'division',  length(geometry) FROM division )
    SELECT theme, COUNT(*) AS features, SUM(wkb) AS total_wkb_bytes,
           ROUND(AVG(wkb),1) AS avg_wkb, MAX(wkb) AS max_wkb
    FROM g GROUP BY theme ORDER BY total_wkb_bytes DESC""",

  "hs2" -> """SELECT class, COUNT(*) AS segments,
      SUM(length(geometry)) AS wkb_bytes, ROUND(AVG(length(geometry)),1) AS avg_wkb,
      SUM(length(names.primary)) AS name_bytes,
      ROUND(100.0*AVG(CASE WHEN size(connectors)>0     THEN 1 ELSE 0 END),1) AS pct_connectors,
      ROUND(100.0*AVG(CASE WHEN size(speed_limits)>0   THEN 1 ELSE 0 END),1) AS pct_speed,
      ROUND(100.0*AVG(CASE WHEN size(access_restrictions)>0 THEN 1 ELSE 0 END),1) AS pct_access
    FROM segment WHERE class IS NOT NULL
    GROUP BY class ORDER BY wkb_bytes DESC""",

  "hs3" -> """SELECT class, COUNT(*) AS segments,
      SUM(length(geometry)) AS wkb_bytes, ROUND(AVG(length(geometry)),1) AS avg_wkb,
      SUM(length(names.primary)) AS name_bytes,
      SUM(size(sources)) AS n_sources, SUM(size(connectors)) AS n_connectors,
      SUM(size(speed_limits)) AS n_speed, SUM(size(access_restrictions)) AS n_access,
      SUM(size(road_surface)) AS n_surface, SUM(size(road_flags)) AS n_flags,
      SUM(size(width_rules)) AS n_width, SUM(size(prohibited_transitions)) AS n_turns,
      SUM(size(routes)) AS n_routes, SUM(size(destinations)) AS n_dest,
      ROUND(AVG(bbox.xmax - bbox.xmin),4) AS avg_bbox_w
    FROM segment WHERE class IS NOT NULL
    GROUP BY class ORDER BY wkb_bytes DESC LIMIT 100""",

  "rw6" -> """
    WITH src AS (
      SELECT s.dataset AS dataset, s.confidence AS conf, s.update_time AS ut FROM segment   LATERAL VIEW explode(sources) t AS s
      UNION ALL SELECT s.dataset, s.confidence, s.update_time FROM connector LATERAL VIEW explode(sources) t AS s
      UNION ALL SELECT s.dataset, s.confidence, s.update_time FROM places    LATERAL VIEW explode(sources) t AS s
      UNION ALL SELECT s.dataset, s.confidence, s.update_time FROM address   LATERAL VIEW explode(sources) t AS s
      UNION ALL SELECT s.dataset, s.confidence, s.update_time FROM division  LATERAL VIEW explode(sources) t AS s )
    SELECT dataset, COUNT(*) AS records, ROUND(AVG(conf),3) AS avg_confidence,
      MIN(SUBSTRING(ut,1,4)) AS oldest_year, MAX(SUBSTRING(ut,1,4)) AS newest_year,
      ROUND(100.0*AVG(CASE WHEN SUBSTRING(ut,1,4)>='2024' THEN 1 ELSE 0 END),1) AS pct_updated_2024plus
    FROM src GROUP BY dataset ORDER BY records DESC""",

  "rw7" -> """
    WITH s AS ( SELECT seg.class AS class, SUBSTRING(src.update_time,1,4) AS yr
      FROM segment seg LATERAL VIEW explode(seg.sources) t AS src WHERE seg.class IS NOT NULL )
    SELECT class, COUNT(*) AS source_records,
      ROUND(100.0*AVG(CASE WHEN yr>='2024' THEN 1 ELSE 0 END),1) AS pct_2024plus,
      ROUND(100.0*AVG(CASE WHEN yr< '2022' THEN 1 ELSE 0 END),1) AS pct_before_2022,
      MIN(yr) AS oldest_year, MAX(yr) AS newest_year
    FROM s GROUP BY class ORDER BY source_records DESC""",

  "rw8" -> """
    SELECT class, COUNT(*) AS named_segments,
      ROUND(AVG(size(map_keys(names.common))),2) AS avg_languages,
      ROUND(100.0*AVG(CASE WHEN size(map_keys(names.common))>=2 THEN 1 ELSE 0 END),1) AS pct_multilingual
    FROM segment WHERE class IS NOT NULL AND names.primary IS NOT NULL
    GROUP BY class ORDER BY named_segments DESC""",

  // ovJ1 — Which junctions join the most road segments, and what classes meet there? Explodes
  // segment.connectors (list<struct<connector_id:string, at:double>>) and groups by the connector id,
  // a HIGH-CARDINALITY string key over ~350M segments -> the only overture query whose shuffle is not
  // near-zero. Added 2026-08-12 as an EXPAND-bucket candidate (needs >200 GiB in one
  // GpuColumnarExchange to trigger the ColumnarExchange term). Exchange size UNVERIFIED until probed.
  // STATUS: NEVER RUN. Added as an EXPAND-bucket candidate; its exchange size has not been
  // probed, so whether it exceeds the 200 GiB trigger at a 1 GiB target is unknown.
  "ovJ1" -> """
    WITH sc AS (
      SELECT c.connector_id AS connector_id, seg.class AS class, length(seg.geometry) AS wkb_bytes
      FROM segment seg LATERAL VIEW explode(seg.connectors) t AS c
      WHERE seg.class IS NOT NULL )
    SELECT connector_id, COUNT(*) AS segments_meeting,
           COUNT(DISTINCT class) AS distinct_classes,
           SUM(wkb_bytes) AS total_wkb_bytes
    FROM sc GROUP BY connector_id
    HAVING COUNT(*) >= 3
    ORDER BY segments_meeting DESC, connector_id LIMIT 100""",

  "rw9" -> """
    SELECT categories.primary AS category, COUNT(*) AS pois,
      ROUND(100.0*AVG(CASE WHEN element_at(addresses,1).locality IS NOT NULL THEN 1 ELSE 0 END),1) AS pct_locality,
      ROUND(100.0*AVG(CASE WHEN element_at(addresses,1).postcode IS NOT NULL THEN 1 ELSE 0 END),1) AS pct_postcode,
      ROUND(100.0*AVG(CASE WHEN element_at(addresses,1).locality IS NOT NULL AND element_at(addresses,1).postcode IS NOT NULL THEN 1 ELSE 0 END),1) AS pct_addressable
    FROM places WHERE categories.primary IS NOT NULL GROUP BY categories.primary HAVING COUNT(*)>=5000 ORDER BY pct_addressable DESC"""
)

val which = sys.props.getOrElse("bench.query", "all")
val M     = sys.props.getOrElse("bench.iters", "5").toInt
val doEx  = sys.props.getOrElse("bench.explain", "false").toBoolean
val order = Seq("gf1","gf2","gf3","hs1","hs2","hs3","rw6","rw7","rw8","rw9","ovJ1")
for (name <- (if (which == "all") order else Seq(which))) {
  val q = queries(name)
  if (doEx) { println(s"########## EXPLAIN $name ##########"); spark.sql(q).explain() }
  for (i <- 1 to M) {
    val t0 = System.nanoTime()
    val n  = spark.sql(q).collect().length
    val ms = (System.nanoTime() - t0) / 1e6
    println(f"ITER $name%s $i%d ${ms}%.0f  rows=$n%d")
  }
}
System.exit(0)
