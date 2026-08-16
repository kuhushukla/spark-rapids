// Pageviews dataset benchmark queries (table: pageviews — cols wiki_code, article_title, page_id,
// access_method, daily_total, hourly_counts, + year/month/day partition cols).
// Selected via -Dbench.query (or "all"); -Dbench.iters, -Dbench.base, -Dbench.explain.
// bench.base default matches download_pageviews.sh output ($OUT/parquet). Prints  ITER <q> <i> <ms> rows=<n>.
//
// Real-world questions per query:
//   pv02  — Hourly traffic share by wiki x access method (decode the hourly_counts token string into per-hour
//           views, then each hour's share of the day). [scan+compute]
//   pv03g — Weekend-vs-weekday pageview change per page (daily rollup, then weekend/weekday split per page).
//           [shuffle]
//   pv05g — Per-page daily-view variability: coefficient of variation / normalized range across observed days.
//           [shuffle]
//   pv06  — Access-method concentration per page (desktop vs mobile-web vs mobile-app gap). [shuffle]
//   pv07g — Weekend-vs-weekday change per page BY ACCESS METHOD (pv03g's rollup + access_method in the
//           key -> larger shuffle). [shuffle, built for the partition EXPAND path]
//           STATUS: DOES NOT RUN. Crashes on iteration 2 inside RapidsShuffleThreadedWriter
//           .doCommitAllPartitions, then the JVM aborts. Never completed an arm.
//   pv09g — Decoded-vs-reported daily-total mismatch (sum the decoded hourly array vs daily_total, find the
//           rows that disagree). [scan-heavy, near-full read]
//   pvU1  — How peaky is traffic? Average share of a page's daily views landing in its single busiest hour,
//           per wiki x access method. Heavy per-row array compute, low-card GROUP BY. [scan-heavy, U-curve]
//   pvU2  — Daytime vs nighttime split: fraction of views in UTC hours 06-17 vs the rest, per wiki x access
//           method. [scan-heavy, U-curve]
//   pvH   — Heavy scan: reads BOTH big string columns (article_title + hourly_counts) + daily_total, light
//           per-row work, GROUP BY wiki_code. [scan-heavy]
val BASE = sys.props.getOrElse("bench.base", "/data/wiki-pageviews/parquet")
spark.read.parquet(BASE).createOrReplaceTempView("pageviews")

// Shared decode: hourly_counts (wide token string) -> array of the 24 hourly view counts.
val HRS = "transform(regexp_extract_all(hourly_counts, '([A-X][0-9]+)', 1), x -> CAST(substr(x, 2) AS BIGINT))"

val queries = Map(
  "pv02" -> """WITH encoded AS (
  SELECT wiki_code, access_method, explode(
    regexp_extract_all(hourly_counts, '([A-X][0-9]+)', 1)
  ) token
  FROM pageviews
), hourly AS (
  SELECT wiki_code, access_method,
         ascii(substr(token, 1, 1)) - ascii('A') hour_utc,
         CAST(substr(token, 2) AS BIGINT) views
  FROM encoded
)
SELECT wiki_code, access_method, hour_utc, SUM(views) views,
       SUM(views) / SUM(SUM(views)) OVER (PARTITION BY wiki_code, access_method) hourly_share
FROM hourly
GROUP BY wiki_code, access_method, hour_utc""",

  "pv03g" -> """WITH daily AS (
  SELECT wiki_code, page_id, article_title, access_method,
         CAST(concat_ws('-', LPAD(CAST(year AS STRING),4,'0'),
                             LPAD(CAST(month AS STRING),2,'0'),
                             LPAD(CAST(day AS STRING),2,'0')) AS DATE) date_utc,
         SUM(daily_total) views
  FROM pageviews
  GROUP BY wiki_code, page_id, article_title, access_method, year, month, day
), split AS (
  SELECT wiki_code, page_id, article_title, access_method,
         AVG(CASE WHEN dayofweek(date_utc) IN (1,7) THEN views END) weekend_avg,
         AVG(CASE WHEN dayofweek(date_utc) NOT IN (1,7) THEN views END) weekday_avg
  FROM daily GROUP BY wiki_code, page_id, article_title, access_method
)
SELECT *, weekend_avg - weekday_avg absolute_change,
       (weekend_avg - weekday_avg) / NULLIF(weekday_avg, 0) relative_change
FROM split
WHERE weekend_avg IS NOT NULL AND weekday_avg IS NOT NULL
ORDER BY ABS(relative_change) DESC, wiki_code, page_id, article_title, access_method
LIMIT 100""",

  "pv05g" -> """WITH daily AS (
  SELECT wiki_code, page_id, article_title,
         CAST(concat_ws('-', LPAD(CAST(year AS STRING),4,'0'),
                             LPAD(CAST(month AS STRING),2,'0'),
                             LPAD(CAST(day AS STRING),2,'0')) AS DATE) date_utc,
         SUM(daily_total) views
  FROM pageviews GROUP BY wiki_code, page_id, article_title, year, month, day
)
SELECT wiki_code, page_id, article_title, COUNT(*) observed_days,
       AVG(views) mean_daily_views, STDDEV_SAMP(views) daily_stddev,
       STDDEV_SAMP(views) / NULLIF(AVG(views), 0) coefficient_of_variation,
       (MAX(views) - MIN(views)) / NULLIF(AVG(views), 0) normalized_range
FROM daily GROUP BY wiki_code, page_id, article_title
HAVING COUNT(*) >= 7
ORDER BY coefficient_of_variation DESC, wiki_code, page_id, article_title
LIMIT 100""",

  "pv06" -> """WITH access AS (
  SELECT wiki_code, page_id, article_title, access_method, SUM(daily_total) views
  FROM pageviews GROUP BY wiki_code, page_id, article_title, access_method
), pivoted AS (
  SELECT wiki_code, page_id, article_title, SUM(views) total_views,
         SUM(CASE WHEN access_method = 'desktop' THEN views ELSE 0 END) desktop_views,
         SUM(CASE WHEN access_method = 'mobile-web' THEN views ELSE 0 END) mobile_web_views,
         SUM(CASE WHEN access_method = 'mobile-app' THEN views ELSE 0 END) mobile_app_views
  FROM access GROUP BY wiki_code, page_id, article_title
)
SELECT *, desktop_views / NULLIF(total_views, 0) desktop_share,
       mobile_web_views / NULLIF(total_views, 0) mobile_web_share,
       mobile_app_views / NULLIF(total_views, 0) mobile_app_share,
       greatest(desktop_views, mobile_web_views, mobile_app_views)
         - least(desktop_views, mobile_web_views, mobile_app_views) access_gap
FROM pivoted
ORDER BY access_gap DESC, wiki_code, page_id, article_title
LIMIT 100""",

  // pv07g — weekend/weekday behaviour per page BY DEVICE. Same daily rollup as pv03g with
  // access_method added to the grouping key: fewer rows collapse map-side, so the inner exchange
  // carries more than pv03g's measured 137 GiB. Built to exercise the partition-count EXPAND path
  // (needs > 200 GiB in one GpuColumnarExchange); the actual size is UNVERIFIED until it runs.
  "pv07g" -> """WITH daily AS (
  SELECT wiki_code, page_id, article_title, access_method,
         CAST(concat_ws('-', LPAD(CAST(year AS STRING),4,'0'),
                             LPAD(CAST(month AS STRING),2,'0'),
                             LPAD(CAST(day AS STRING),2,'0')) AS DATE) date_utc,
         SUM(daily_total) views
  FROM pageviews
  GROUP BY wiki_code, page_id, article_title, access_method, year, month, day
)
SELECT wiki_code, page_id, article_title, access_method,
       COUNT(*) observed_days, SUM(views) total_views,
       AVG(CASE WHEN dayofweek(date_utc) IN (1,7) THEN views END) weekend_avg,
       AVG(CASE WHEN dayofweek(date_utc) NOT IN (1,7) THEN views END) weekday_avg
FROM daily
GROUP BY wiki_code, page_id, article_title, access_method
HAVING COUNT(*) >= 7
ORDER BY total_views DESC, wiki_code, page_id, article_title, access_method
LIMIT 100""",

  "pv09g" -> s"""WITH decoded AS (
  SELECT wiki_code, page_id, article_title,
         CAST(concat_ws('-', LPAD(CAST(year AS STRING),4,'0'),
                             LPAD(CAST(month AS STRING),2,'0'),
                             LPAD(CAST(day AS STRING),2,'0')) AS DATE) date_utc,
         access_method, daily_total,
         aggregate($HRS, CAST(0 AS BIGINT), (acc, x) -> acc + x) decoded_total
  FROM pageviews
)
SELECT *, decoded_total - daily_total difference,
       (decoded_total - daily_total) / NULLIF(daily_total, 0) relative_difference
FROM decoded WHERE decoded_total <> daily_total""",

  "pvU1" -> s"""WITH ints AS (
      SELECT wiki_code, access_method, $HRS hrs FROM pageviews
    ), perrow AS (
      SELECT wiki_code, access_method,
             aggregate(hrs, CAST(0 AS BIGINT), (a, x) -> a + x)          tot,
             aggregate(hrs, CAST(0 AS BIGINT), (a, x) -> greatest(a, x)) peak
      FROM ints
    )
    SELECT wiki_code, access_method, COUNT(*) pages, SUM(tot) total_views,
           AVG(peak / NULLIF(tot, 0)) avg_peak_hour_share
    FROM perrow WHERE tot > 0
    GROUP BY wiki_code, access_method
    ORDER BY total_views DESC LIMIT 200""",

  "pvU2" -> s"""WITH ints AS (
      SELECT wiki_code, access_method, $HRS hrs FROM pageviews
    ), perrow AS (
      SELECT wiki_code, access_method,
             aggregate(hrs, CAST(0 AS BIGINT), (a, x) -> a + x) tot,
             aggregate(transform(hrs, (x, i) -> CASE WHEN i BETWEEN 6 AND 17 THEN x ELSE CAST(0 AS BIGINT) END),
                       CAST(0 AS BIGINT), (a, x) -> a + x) day_views
      FROM ints
    )
    SELECT wiki_code, access_method, COUNT(*) pages, SUM(tot) total_views,
           SUM(day_views) / NULLIF(SUM(tot), 0) daytime_share
    FROM perrow WHERE tot > 0
    GROUP BY wiki_code, access_method
    ORDER BY total_views DESC LIMIT 200""",

  "pvH" -> """SELECT wiki_code, COUNT(*) AS rows, SUM(daily_total) AS total_views,
                SUM(length(article_title)) AS title_bytes, SUM(length(hourly_counts)) AS hourly_bytes,
                ROUND(AVG(length(hourly_counts)),1) AS avg_hourly_len, ROUND(AVG(length(article_title)),1) AS avg_title_len,
                MAX(length(article_title)) AS max_title
              FROM pageviews GROUP BY wiki_code ORDER BY total_views DESC LIMIT 200"""
)

val which = sys.props.getOrElse("bench.query", queries.keys.head)
val M     = sys.props.getOrElse("bench.iters", "5").toInt
val doEx  = sys.props.getOrElse("bench.explain", "false").toBoolean
for (name <- (if (which == "all") queries.keys.toSeq.sorted else Seq(which))) {
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
