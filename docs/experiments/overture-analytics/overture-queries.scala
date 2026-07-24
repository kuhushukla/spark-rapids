// =============================================================================
// Overture Maps — analytical queries (no-WKB constraint)
// Answers a representative subset of docs/experiments/overture-analytics/
//   interesting-questions-no-wkb.md across all five families (A join, B grid,
//   C graph, D bbox, E flagship).
//
// Run:  $SPARK_HOME/bin/spark-shell -i overture-queries.scala
//   (add --jars <rapids-jar> --conf spark.plugins=com.nvidia.spark.rapids.SQLPlugin
//    to run on GPU; the SQL is identical either way.)
//
// GROUNDED SCHEMA FACTS (read from the Parquet footers, not assumed):
//   * places      has NO top-level country/region. country lives in
//                 addresses[].country (well filled); addresses[].region is
//                 mostly null. Category = categories.primary.
//   * division    population filled for ~673k / 4.66M rows; ISO region code is
//                 on subtype='region' rows; country totals on subtype='country'.
//   * segment/connector carry NO country/region at all -> segments are
//                 attributed to a country via a grid(cell)->country crosswalk
//                 built from addresses (points that DO carry country).
//   * operating_status is ~99.6% null -> Q8 is coverage-bound (shown, flagged).
//   * geometry (WKB binary) is pruned from every projection; all "location"
//                 comes from bbox center or from list columns.
//
// Each query is annotated:  -- OPS: <logical operators>   -- DOMINANT: <op>
// "DOMINANT" = the operator that moves the most rows/bytes (structural; the
// companion EXPLAIN run confirms it in the physical plan).
// =============================================================================

import org.apache.spark.sql.functions._

val BASE = "/home/kuhu/Reps/spark-rapids/overture_2026-07-22"
val CELL = 0.1   // grid cell size in degrees (~11 km at the equator). State it.

spark.conf.set("spark.sql.parquet.enableVectorizedReader", "true")
// Broadcast the small dimension tables (division ~4.7M rows but the per-country
// slice is tiny; crosswalk ~1-2M cells). Let the optimizer decide; keep default.

// ---- register the six themes as views, geometry column never selected --------
spark.read.parquet(s"$BASE/places/type=place").createOrReplaceTempView("places")
spark.read.parquet(s"$BASE/divisions/type=division").createOrReplaceTempView("division")
spark.read.parquet(s"$BASE/addresses/type=address").createOrReplaceTempView("address")
spark.read.parquet(s"$BASE/transportation/type=segment").createOrReplaceTempView("segment")
spark.read.parquet(s"$BASE/transportation/type=connector").createOrReplaceTempView("connector")

// Reusable projected views that prune geometry and lift the fields we need.
// places: take the FIRST address element for country (a POI has one location).
spark.sql("""
  SELECT
    categories.primary                         AS category,
    brand.names.primary                        AS brand,
    element_at(addresses, 1).country           AS country,
    confidence,
    operating_status,
    (size(websites) > 0 OR size(phones) > 0
      OR size(emails) > 0 OR size(socials) > 0) AS has_contact,
    CAST(FLOOR(((bbox.xmin + bbox.xmax) / 2) / 0.1) AS INT) AS cx,
    CAST(FLOOR(((bbox.ymin + bbox.ymax) / 2) / 0.1) AS INT) AS cy
  FROM places
""").createOrReplaceTempView("poi")

spark.sql("""
  SELECT
    country, street,
    CAST(FLOOR(((bbox.xmin + bbox.xmax) / 2) / 0.1) AS INT) AS cx,
    CAST(FLOOR(((bbox.ymin + bbox.ymax) / 2) / 0.1) AS INT) AS cy
  FROM address
""").createOrReplaceTempView("addr")

// Grid cell -> country crosswalk: majority country of the addresses in each cell.
// OPS: scan(address 472M) -> hash-agg(count by cell,country) -> window(rank per cell)
// DOMINANT: the shuffle hash-aggregate over 472M address points.
// NOTE (MEASURED on full data): the crosswalk collapses 472.7M addresses to only
// 208,681 cells (2.8 MiB) -> with AQE on it BROADCASTS (BroadcastHashJoin), so the
// 348.7M segment rows stream past it (no big-side shuffle). Only 46.9% of segments
// survive the inner join; the rest lie in cells with NO addresses (rural roads) ->
// real coverage caveat for Q4/Q19/Q23.
spark.sql("""
  WITH counts AS (
    SELECT cx, cy, country, COUNT(*) AS n
    FROM addr WHERE country IS NOT NULL
    GROUP BY cx, cy, country
  ), ranked AS (
    SELECT cx, cy, country,
           ROW_NUMBER() OVER (PARTITION BY cx, cy ORDER BY n DESC) AS rk
    FROM counts
  )
  SELECT cx, cy, country FROM ranked WHERE rk = 1
""").createOrReplaceTempView("cell_country")

// Per-country population (subtype='country'); small dimension table.
spark.sql("""
  SELECT country, MAX(population) AS population
  FROM division WHERE subtype = 'country' AND population IS NOT NULL
  GROUP BY country
""").createOrReplaceTempView("pop_country")

// =============================================================================
// FAMILY A — attribute joins (exact, fully GPU)
// =============================================================================

// -- Q1  Restaurants / schools / pharmacies per 100k people, by country. [join]
// -- OPS: scan+filter+project (poi) -> hash-agg (count by country,category)
// --      -> broadcast-join (pop_country) -> project (ratio)
// -- DOMINANT: scan + hash-aggregate of the 74M-row places table.
val q1 = spark.sql("""
  SELECT p.country, p.category, p.n AS places,
         c.population,
         ROUND(p.n * 100000.0 / c.population, 2) AS per_100k
  FROM (
    SELECT country, category, COUNT(*) AS n
    FROM poi
    WHERE country IS NOT NULL
      AND category IN ('restaurant','school','pharmacy','grocery_store','hospital')
    GROUP BY country, category
  ) p
  JOIN pop_country c ON p.country = c.country
  WHERE c.population > 200000
  ORDER BY p.category, per_100k DESC
""")

// -- Q2  Brand penetration by country: top brands per nation. [join]
// -- OPS: scan+filter -> hash-agg (count by country,brand) -> window(rank) -> filter
// -- DOMINANT: shuffle hash-aggregate on (country, brand).
val q2 = spark.sql("""
  WITH b AS (
    SELECT country, brand, COUNT(*) AS n
    FROM poi WHERE brand IS NOT NULL AND country IS NOT NULL
    GROUP BY country, brand
  ), ranked AS (
    SELECT country, brand, n,
           RANK() OVER (PARTITION BY country ORDER BY n DESC) AS rk
    FROM b
  )
  SELECT country, brand, n, rk FROM ranked WHERE rk <= 5
  ORDER BY country, rk
""")

// -- Q3  Commercial mix per country: category share of POIs. [join]
// -- OPS: scan -> hash-agg (count by country,category) -> window(share)
// -- DOMINANT: shuffle hash-aggregate on (country, category).
val q3 = spark.sql("""
  WITH m AS (
    SELECT country, category, COUNT(*) AS n
    FROM poi WHERE country IS NOT NULL AND category IS NOT NULL
    GROUP BY country, category
  ), ranked AS (
    SELECT country, category, n,
           ROUND(100.0 * n / SUM(n) OVER (PARTITION BY country), 2) AS pct_of_country,
           RANK() OVER (PARTITION BY country ORDER BY n DESC) AS rk
    FROM m
  )
  SELECT country, category, n, pct_of_country FROM ranked WHERE rk <= 8
  ORDER BY country, n DESC
""")

// -- Q6  Most common street name per country. [join]  (addresses = 472M rows)
// -- OPS: scan -> hash-agg (count by country,street) -> window(top-1 per country)
// -- DOMINANT: the shuffle hash-aggregate over 472M address rows (heaviest agg).
val q6 = spark.sql("""
  WITH s AS (
    SELECT country, street, COUNT(*) AS n
    FROM addr WHERE country IS NOT NULL AND street IS NOT NULL
    GROUP BY country, street
  ), ranked AS (
    SELECT country, street, n,
           ROW_NUMBER() OVER (PARTITION BY country ORDER BY n DESC) AS rn
    FROM s
  )
  SELECT country, street, n FROM ranked WHERE rn = 1
  ORDER BY n DESC
""")

// -- Q8  Open-vs-closed economic vitality: operating_status shares. [join]
// --     COVERAGE CAVEAT: operating_status ~99.6% null -> mostly 'unknown'.
// -- OPS: scan -> project(coalesce) -> hash-agg (count by country,status)
// -- DOMINANT: scan + hash-aggregate of places.
val q8 = spark.sql("""
  SELECT country,
         COALESCE(operating_status, 'unknown') AS status,
         COUNT(*) AS n
  FROM poi WHERE country IS NOT NULL
  GROUP BY country, COALESCE(operating_status, 'unknown')
  ORDER BY country, n DESC
""")

// -- Q9  How online is commerce, by country: fraction of POIs with any contact. [join]
// -- OPS: scan -> project(has_contact bool) -> hash-agg (avg by country)
// -- DOMINANT: scan + hash-aggregate of places.
val q9 = spark.sql("""
  SELECT country,
         COUNT(*) AS pois,
         ROUND(100.0 * AVG(CASE WHEN has_contact THEN 1 ELSE 0 END), 2) AS pct_online
  FROM poi WHERE country IS NOT NULL
  GROUP BY country
  HAVING COUNT(*) >= 1000
  ORDER BY pct_online DESC
""")

// =============================================================================
// FAMILY A/grid — segment questions need a cell->country crosswalk join
// =============================================================================

// -- Q4  Road-class composition by country (count-based, no length). [join]+[grid]
// -- OPS: scan(segment 348M) -> project(cell) -> join(cell_country)
// --      -> hash-agg(count by country,class) -> window(share)
// -- DOMINANT (MEASURED): scanning 348.7M segment rows (class+bbox = 4.88 GB) +
// --   the final group-by. The join is a cheap BroadcastHashJoin (crosswalk 2.8 MiB);
// --   inner join keeps 163.7M / 348.7M segments (46.9%).
val q4 = spark.sql("""
  WITH seg AS (
    SELECT class,
           CAST(FLOOR(((bbox.xmin+bbox.xmax)/2)/0.1) AS INT) AS cx,
           CAST(FLOOR(((bbox.ymin+bbox.ymax)/2)/0.1) AS INT) AS cy
    FROM segment WHERE class IS NOT NULL
  ), byc AS (
    SELECT k.country, s.class, COUNT(*) AS n
    FROM seg s JOIN cell_country k ON s.cx = k.cx AND s.cy = k.cy
    GROUP BY k.country, s.class
  ), ranked AS (
    SELECT country, class, n,
           ROUND(100.0 * n / SUM(n) OVER (PARTITION BY country), 2) AS pct,
           RANK() OVER (PARTITION BY country ORDER BY n DESC) AS rk
    FROM byc
  )
  SELECT country, class, n, pct FROM ranked WHERE rk <= 6
  ORDER BY country, n DESC
""")

// =============================================================================
// FAMILY B — grid-binned (exact counts per cell, approximate location)
// =============================================================================

// -- Q11/Q12  Business districts vs bedroom suburbs + commercial intensity. [grid]
// --   POI-density grid FULL OUTER JOIN address-density grid; classify each cell.
// -- OPS: 2x scan -> 2x hash-agg(count by cell) -> FULL OUTER JOIN on cell
// --      -> project(ratio, class)
// -- DOMINANT (MEASURED): the 472.7M-row address hash-aggregate (-> 208,681 cells).
// --   The join is SMALL: 481,115 poi-cells ⋈ 208,681 addr-cells -> 528,074 cells.
// --   (Earlier "large-vs-large join" was wrong: grids collapse to <0.5M rows.)
val q11 = spark.sql("""
  WITH pg AS (SELECT cx, cy, COUNT(*) AS pois  FROM poi  GROUP BY cx, cy),
       ag AS (SELECT cx, cy, COUNT(*) AS addrs FROM addr GROUP BY cx, cy)
  SELECT
    COALESCE(pg.cx, ag.cx) AS cx, COALESCE(pg.cy, ag.cy) AS cy,
    COALESCE(pois,0)  AS pois,
    COALESCE(addrs,0) AS addrs,
    ROUND(1000.0 * COALESCE(pois,0) / NULLIF(addrs,0), 2) AS pois_per_1k_addr,
    CASE
      WHEN COALESCE(addrs,0) = 0 THEN 'commercial_only'
      WHEN COALESCE(pois,0)  = 0 THEN 'residential_only'
      WHEN pois * 1000.0 / addrs >= 200 THEN 'commercial'
      WHEN pois * 1000.0 / addrs <= 20  THEN 'residential'
      ELSE 'mixed'
    END AS cell_class
  FROM pg FULL OUTER JOIN ag ON pg.cx = ag.cx AND pg.cy = ag.cy
  WHERE COALESCE(pois,0) + COALESCE(addrs,0) >= 50
""")
q11.createOrReplaceTempView("cell_profile")

// =============================================================================
// FAMILY C — graph topology (exact, needs no geometry)
// =============================================================================

// -- Q17  Sprawl vs grid: intersection DEGREE distribution. [graph]
// --   degree(connector) = # segments referencing it (explode connectors list).
// -- OPS: scan(segment) -> EXPLODE(connectors ~700M+ refs)
// --      -> hash-agg(count by connector_id = degree)
// --      -> hash-agg(count by degree = histogram)
// -- DOMINANT: the EXPLODE + shuffle hash-aggregate over ~700M exploded rows.
val q17 = spark.sql("""
  WITH refs AS (
    SELECT c.connector_id
    FROM segment LATERAL VIEW explode(connectors) t AS c
    WHERE c.connector_id IS NOT NULL
  ), deg AS (
    SELECT connector_id, COUNT(*) AS degree
    FROM refs GROUP BY connector_id
  )
  SELECT degree, COUNT(*) AS n_nodes,
         ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 3) AS pct
  FROM deg GROUP BY degree ORDER BY degree
""")

// -- Q19  Turn-restriction complexity per country. [graph]+[join]
// -- OPS: scan(segment) -> project(size(prohibited_transitions), cell)
// --      -> join(cell_country) -> hash-agg(sum/avg by country)
// -- DOMINANT: scan of segment + join to crosswalk.
val q19 = spark.sql("""
  WITH seg AS (
    SELECT size(prohibited_transitions) AS restr,
           CAST(FLOOR(((bbox.xmin+bbox.xmax)/2)/0.1) AS INT) AS cx,
           CAST(FLOOR(((bbox.ymin+bbox.ymax)/2)/0.1) AS INT) AS cy
    FROM segment
  )
  SELECT k.country,
         COUNT(*) AS segments,
         SUM(CASE WHEN s.restr > 0 THEN 1 ELSE 0 END) AS segs_with_restriction,
         ROUND(100.0 * SUM(CASE WHEN s.restr > 0 THEN 1 ELSE 0 END) / COUNT(*), 3)
           AS pct_restricted
  FROM seg s JOIN cell_country k ON s.cx = k.cx AND s.cy = k.cy
  GROUP BY k.country
  HAVING COUNT(*) >= 10000
  ORDER BY pct_restricted DESC
""")

// =============================================================================
// FAMILY D — bbox-extent (approximate size, no decode)
// =============================================================================

// -- Q23  Sprawl of features: segment bbox diagonal as a rough length proxy. [bbox]
// --   APPROXIMATE: degrees, ignores curvature/projection. Per country.
// -- OPS: scan(segment) -> project(diagonal, cell) -> join(cell_country)
// --      -> hash-agg(avg/percentile by country)
// -- DOMINANT: scan of segment + join to crosswalk.
val q23 = spark.sql("""
  WITH seg AS (
    SELECT SQRT(POW(bbox.xmax-bbox.xmin,2) + POW(bbox.ymax-bbox.ymin,2)) AS diag_deg,
           CAST(FLOOR(((bbox.xmin+bbox.xmax)/2)/0.1) AS INT) AS cx,
           CAST(FLOOR(((bbox.ymin+bbox.ymax)/2)/0.1) AS INT) AS cy
    FROM segment
  )
  SELECT k.country,
         COUNT(*) AS segments,
         ROUND(AVG(s.diag_deg), 5)                          AS avg_diag_deg,
         ROUND(PERCENTILE_APPROX(s.diag_deg, 0.95), 5)      AS p95_diag_deg
  FROM seg s JOIN cell_country k ON s.cx = k.cx AND s.cy = k.cy
  GROUP BY k.country
  HAVING COUNT(*) >= 10000
  ORDER BY avg_diag_deg DESC
""")

// =============================================================================
// FAMILY E — flagship multi-way
// =============================================================================

// -- Q25  Coarse 15-minute city. [grid]+[join]  APPROXIMATE (cell 0.1 deg, ~11km).
// --   For each cell, the set of daily-need categories present in the cell OR any
// --   of its 8 neighbours; then per country: avg distinct daily-need categories
// --   reachable from a populated (address-bearing) cell.
// -- OPS: scan(poi)+filter -> hash-agg(distinct cats per cell)
// --      -> EXPLODE(9 neighbour offsets) -> JOIN(addr cells) -> agg-distinct
// --      -> join(cell_country) -> hash-agg per country
// -- DOMINANT: the neighbourhood self-join fan-out (9x) + distinct aggregate.
val q25 = spark.sql("""
  WITH need AS (
    SELECT cx, cy, category
    FROM poi
    WHERE category IN ('restaurant','grocery_store','pharmacy','school',
                       'hospital','cafe','bank_credit_union','park','bus_stop')
      AND category IS NOT NULL
  ),
  -- fan each POI cell out to the 9 cells it "serves" (itself + 8 neighbours)
  served AS (
    SELECT cx + dx AS cx, cy + dy AS cy, category
    FROM need
      LATERAL VIEW explode(array(-1,0,1)) ox AS dx
      LATERAL VIEW explode(array(-1,0,1)) oy AS dy
  ),
  reach AS (
    SELECT cx, cy, COUNT(DISTINCT category) AS n_needs
    FROM served GROUP BY cx, cy
  ),
  -- only cells that actually have addresses (people) count
  populated AS (SELECT DISTINCT cx, cy, country FROM addr WHERE country IS NOT NULL)
  SELECT p.country,
         COUNT(*)                       AS populated_cells,
         ROUND(AVG(COALESCE(r.n_needs,0)), 3) AS avg_needs_reachable
  FROM populated p LEFT JOIN reach r ON p.cx = r.cx AND p.cy = r.cy
  GROUP BY p.country
  HAVING COUNT(*) >= 100
  ORDER BY avg_needs_reachable DESC
""")

// -------- materialize a few (comment out the heavy ones for a quick run) ------
println("== Q1 restaurants/schools/pharmacies per 100k =="); q1.show(30, false)
println("== Q2 top brands per country ==");                  q2.show(30, false)
println("== Q3 commercial mix per country ==");              q3.show(30, false)
println("== Q6 most common street per country ==");          q6.show(30, false)
println("== Q9 how online, by country ==");                  q9.show(30, false)
println("== Q11 cell profile (business vs bedroom) ==");     q11.orderBy(desc("pois")).show(20, false)
println("== Q17 intersection degree distribution ==");       q17.show(20, false)
println("== Q4 road-class composition by country ==");       q4.show(30, false)
println("== Q19 turn-restriction complexity ==");            q19.show(20, false)
println("== Q23 segment bbox diagonal by country ==");       q23.show(20, false)
println("== Q25 coarse 15-minute city ==");                  q25.show(20, false)
