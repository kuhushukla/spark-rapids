// =============================================================================
// Overture Maps — BIG-JOIN workload (large-vs-large SortMergeJoins)
// Companion to overture-queries.scala. Those queries pre-aggregate before
// joining, so every join collapses to a tiny broadcast (build side <0.5M rows).
// These queries deliberately join at ROW level on HIGH-CARDINALITY keys so BOTH
// sides stay far above spark.sql.autoBroadcastJoinThreshold -> the optimizer must
// pick a shuffle SortMergeJoin (or ShuffledHashJoin), exercising the GPU shuffle/
// join path instead of a broadcast. Confirmed by EXPLAIN (see bigjoins_check.scala).
//
// Why these are big (MEASURED cardinalities, full data):
//   segment.connectors explodes to 897,200,683 refs
//   connector table     = 416,768,381 rows (id key ~unique)  -> reading id = 8.6 GB
//   addresses w/ postcode ~ 0.694 * 472.7M ~ 328M
//   places   w/ postcode ~ 0.933 * 74.2M  ~ 69M
//
// Tuning knobs that matter here (unlike the broadcast queries):
//   spark.sql.autoBroadcastJoinThreshold (keep default 10MB; do NOT raise, or
//       BJ1's right side could try to broadcast 8.6 GB), shuffle.partitions,
//       and on GPU: spark.rapids.shuffle.mode / multithreaded shuffle.
// =============================================================================

import org.apache.spark.sql.functions._
val BASE = "/home/kuhu/Reps/spark-rapids/overture_2026-07-22"

spark.read.parquet(s"$BASE/places/type=place").createOrReplaceTempView("places")
spark.read.parquet(s"$BASE/addresses/type=address").createOrReplaceTempView("address")
spark.read.parquet(s"$BASE/transportation/type=segment").createOrReplaceTempView("segment")
spark.read.parquet(s"$BASE/transportation/type=connector").createOrReplaceTempView("connector")

// -----------------------------------------------------------------------------
// BJ1  Topology enrichment / referential integrity  [graph]  (answers Q17/Q18)
//   Explode every segment's connector list, then JOIN each reference to the
//   connector table on connector_id. This is the road graph's incidence join.
//   LEFT  ~897.2M exploded refs   RIGHT 416.8M connectors (id ~unique)
//   OUTPUT ~897.2M rows (bounded: each ref matches exactly one connector).
// -- OPS: scan(segment.connectors 12.2GB) -> Generate(explode) ->
// --      SortMergeJoin(ref.connector_id = connector.id)  <-- BIG shuffle join,
// --      both sides sorted+shuffled on a 400M-distinct string key ->
// --      hash-agg(degree per connector) -> hash-agg(degree histogram)
// -- DOMINANT: the 897.2M ⋈ 416.8M SortMergeJoin (two large shuffles + sorts).
// -- NB referential integrity: LEFT JOIN + count nulls = refs to missing nodes.
val bj1 = spark.sql("""
  WITH refs AS (
    SELECT c.connector_id AS cid, s.id AS seg_id
    FROM segment s LATERAL VIEW explode(s.connectors) t AS c
    WHERE c.connector_id IS NOT NULL
  ), joined AS (
    SELECT r.cid, r.seg_id, k.id AS matched
    FROM refs r JOIN connector k ON r.cid = k.id      -- big-vs-big equi-join
  ), deg AS (
    SELECT cid, COUNT(*) AS degree FROM joined GROUP BY cid
  )
  SELECT degree, COUNT(*) AS n_nodes,
         ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 3) AS pct
  FROM deg GROUP BY degree ORDER BY degree
""")

// -----------------------------------------------------------------------------
// BJ2  Road-network ADJACENCY via segment self-join on connector_id  [graph]
//   Two segments are adjacent iff they share a connector. Self-join the exploded
//   references on connector_id -> every pair of segments meeting at a node.
//   LEFT 897.2M ⋈ RIGHT 897.2M on connector_id.
//   OUTPUT = sum_over_nodes(degree*(degree-1)) — MANY BILLIONS; this is the
//   heaviest join in the set. We COUNT edges + per-node fan-out rather than
//   materialize them. Cap hubs with the degree filter to bound skew if needed.
// -- OPS: 2x Generate(explode) -> SortMergeJoin(a.cid=b.cid, a.seg<b.seg)
// --      -> hash-agg. DOMINANT: the self SortMergeJoin (skewed on hub nodes).
val bj2 = spark.sql("""
  WITH refs AS (
    SELECT c.connector_id AS cid, s.id AS seg_id
    FROM segment s LATERAL VIEW explode(s.connectors) t AS c
    WHERE c.connector_id IS NOT NULL
  )
  SELECT COUNT(*) AS adjacency_edges
  FROM refs a JOIN refs b
    ON a.cid = b.cid AND a.seg_id < b.seg_id           -- self big-vs-big join
""")

// -----------------------------------------------------------------------------
// BJ3  Exact commercial intensity by postcode  [join]  (answers Q12/Q15 exactly)
//   No grid: join addresses to POIs that share (country, postcode). Both sides
//   large; postcode is many-to-many within a country, so the join fans out.
//   LEFT ~328M addresses ⋈ RIGHT ~69M POIs on (country, postcode).
//   OUTPUT can be very large inside dense postcodes -> we aggregate the joined
//   stream immediately (POIs reachable per address), never materializing pairs.
// -- OPS: scan(address country+postcode) + scan(places addresses/categories) ->
// --      SortMergeJoin on (country,postcode) <-- BIG shuffle join ->
// --      hash-agg(count distinct POIs / category per address) -> agg per country
// -- DOMINANT: the (country,postcode) SortMergeJoin of the two large tables.
val bj3 = spark.sql("""
  WITH a AS (
    SELECT id AS addr_id, country, postcode
    FROM address WHERE country IS NOT NULL AND postcode IS NOT NULL
  ), p AS (
    SELECT element_at(addresses,1).country AS country,
           element_at(addresses,1).postcode AS postcode,
           categories.primary AS category
    FROM places
    WHERE element_at(addresses,1).postcode IS NOT NULL
      AND categories.primary IS NOT NULL
  ), reach AS (
    SELECT a.country, a.addr_id, COUNT(*) AS pois_in_postcode,
           COUNT(DISTINCT p.category) AS distinct_categories
    FROM a JOIN p ON a.country = p.country AND a.postcode = p.postcode
    GROUP BY a.country, a.addr_id
  )
  SELECT country,
         COUNT(*)                          AS addresses,
         ROUND(AVG(pois_in_postcode), 2)   AS avg_pois_per_addr,
         ROUND(AVG(distinct_categories),2) AS avg_categories_per_addr
  FROM reach GROUP BY country
  HAVING COUNT(*) >= 10000
  ORDER BY avg_pois_per_addr DESC
""")

println("== BJ1 degree via segment⋈connector (897M ⋈ 417M) ==")
bj1.show(15, false)
println("== BJ2 road adjacency edges (self-join 897M ⋈ 897M) ==")
bj2.show(false)
println("== BJ3 commercial intensity by postcode (328M ⋈ 69M) ==")
bj3.show(30, false)
