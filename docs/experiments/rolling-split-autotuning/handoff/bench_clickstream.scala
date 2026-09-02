// Clickstream dataset benchmark queries (table: clickstream — cols previous, current, link_type, n).
// Selected via -Dbench.query (or "all"); -Dbench.iters, -Dbench.base, -Dbench.explain.
// bench.base default matches download_clickstream.sh output ($OUT/parquet). Prints  ITER <q> <i> <ms> rows=<n>.
//
// Real-world questions per query:
//   cs01  — Which source->target navigation transitions receive the most clicks? (GROUP BY previous,current,
//           link_type; high-cardinality -> large shuffle) [shuffle-bound]
//   cs02  — By entry source (search / external / other-wiki / article), how many clicks split by navigation
//           type? (reads `previous`; ~6 buckets x link_type -> tiny shuffle) [scan-heavy]
//   cs03  — Which articles receive the most reader traffic (inflow)? (GROUP BY current -> millions of groups)
//           [scan+shuffle]
//   csH   — Top-100 target articles by inflow (+ referer-title byte stats). Reads both big string columns.
//           [scan+shuffle]
//   csH3  — Link-type totals over a 3x UNION-ALL self-read of both big string columns. [scan-dominated]
//   cs04  — Which articles draw the most traffic from INTERNAL wiki links rather than external search
//           referrals? link_type='link' prunes no row groups (identical min AND max in every one), so
//           it varies selectivity by reading fewer ROWS, not fewer bytes. [scan+shuffle]
//   cs05  — Which articles are fed by high-volume navigation paths (>1M clicks on one source->target
//           edge)? The only predicate here that eliminates row groups: `n` has a different max per row
//           group (3.2M..489M) sharing a min, so `n > 1000000` skips ~30%. [scan+shuffle]
//   cs01_etl — cs01 aggregation with no ORDER BY/LIMIT, written to parquet (ETL variant).
val BASE   = sys.props.getOrElse("bench.base",   "/data/wiki-clickstream/parquet")
val OUTDIR = sys.props.getOrElse("bench.outdir", "/data/wiki-clickstream/etl-out")
// Data window applied at view registration, so every query's SQL text stays byte-identical.
// Via --conf, not -D: --driver-java-options splits on whitespace and "ym < '2022-01'" would arrive
// as three arguments.
val WHERE = spark.conf.getOption("spark.bench.where").getOrElse(sys.props.getOrElse("bench.where", "")).trim
val base = spark.read.parquet(BASE)
(if (WHERE.nonEmpty) base.where(WHERE) else base).createOrReplaceTempView("clickstream")
println(s"BENCH_BASE=$BASE  BENCH_WHERE=${if (WHERE.isEmpty) "<none>" else WHERE}")

val AGG = """SELECT previous, current, link_type, SUM(n) AS clicks
             FROM clickstream
             GROUP BY previous, current, link_type"""

val CS02 = """SELECT CASE WHEN previous LIKE 'other-%' THEN previous ELSE 'article' END AS entry_source,
                     link_type, SUM(n) AS clicks, COUNT(*) AS transitions
              FROM clickstream
              GROUP BY CASE WHEN previous LIKE 'other-%' THEN previous ELSE 'article' END, link_type
              ORDER BY clicks DESC LIMIT 100"""

val CS03 = """SELECT current, SUM(n) AS inflow_clicks, COUNT(*) AS incoming_transitions
              FROM clickstream
              GROUP BY current
              ORDER BY inflow_clicks DESC, current LIMIT 100"""

val queries = Map(
  "cs01"     -> (AGG + "\n             ORDER BY clicks DESC, previous, current LIMIT 100"),
  "cs02"     -> CS02,
  "cs03"     -> CS03,
  "cs01_etl" -> AGG,
  "csH" -> """SELECT current AS article, COUNT(*) AS in_transitions, SUM(n) AS inflow_clicks,
                SUM(length(previous)) AS referer_title_bytes, MAX(length(previous)) AS max_referer_len,
                ROUND(AVG(length(previous)),1) AS avg_referer_len
              FROM clickstream GROUP BY current ORDER BY inflow_clicks DESC LIMIT 100""",
  "cs04" -> """SELECT current AS article, COUNT(*) AS in_transitions, SUM(n) AS clicks,
                ROUND(AVG(length(previous)),1) AS avg_referer_len
              FROM clickstream WHERE link_type = 'link'
              GROUP BY current ORDER BY clicks DESC, article LIMIT 100""",
  "cs05" -> """SELECT current AS article, COUNT(*) AS major_paths, SUM(n) AS clicks,
                MAX(n) AS biggest_path, ROUND(AVG(length(previous)),1) AS avg_referer_len
              FROM clickstream WHERE n > 1000000
              GROUP BY current ORDER BY clicks DESC, article LIMIT 100""",
  "csH3" -> """SELECT link_type, COUNT(*) AS transitions, SUM(n) AS clicks,
                 SUM(length(previous)) AS prev_bytes, SUM(length(current)) AS cur_bytes
               FROM ( SELECT link_type, previous, current, n FROM clickstream
                      UNION ALL SELECT link_type, previous, current, n FROM clickstream
                      UNION ALL SELECT link_type, previous, current, n FROM clickstream )
               GROUP BY link_type ORDER BY clicks DESC"""
)

val which = sys.props.getOrElse("bench.query", queries.keys.head)
val M     = sys.props.getOrElse("bench.iters", "5").toInt
val doEx  = sys.props.getOrElse("bench.explain", "false").toBoolean
for (name <- (if (which == "all") queries.keys.toSeq.sorted else Seq(which))) {
  val q = queries(name)
  if (doEx) { println(s"########## EXPLAIN $name ##########"); spark.sql(q).explain() }
  for (i <- 1 to M) {
    val t0 = System.nanoTime()
    val n =
      if (name.endsWith("_etl")) { spark.sql(q).write.mode("overwrite").parquet(s"$OUTDIR/$name"); -1 }
      else spark.sql(q).collect().length
    val ms = (System.nanoTime() - t0) / 1e6
    println(f"ITER $name%s $i%d ${ms}%.0f  rows=$n%d")
  }
}
System.exit(0)
