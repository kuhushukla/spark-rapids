/*
 * Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Scan split autotuner two-run test over NDS SF100.
//
// Run with (from the spark-rapids repo root):
//
//   /home/kuhu/Downloads/spark-3.5.3-bin-hadoop3/bin/spark-shell \
//     --master local[*] \
//     --jars dist/target/rapids-4-spark_2.12-26.08.0-SNAPSHOT-cuda12.jar \
//     --conf spark.plugins=com.nvidia.spark.SQLPlugin \
//     --conf spark.rapids.sql.scan.splitAutotuner.historyPath=$(pwd)/data/scan-split-history.tsv \
//     --conf spark.local.dir=$(pwd)/data/spark-tmp \
//     --conf spark.sql.shuffle.partitions=200 \
//     -i docs/experiments/rolling-split-autotuning/handoff/nds-autotuner-test.scala
//
// Expected behaviour:
//   Run 1 — logs COLD_START for both tables, writes history file.
//   Run 2 — logs DECIDED with a non-zero expansion_ratio; split_bytes differs from spark_default.
//   Query results are identical between runs.
//
// Table choice rationale:
//   store_sales (15 GiB, ~240 KB files, ~20 files/partition) — files are far below any split
//     threshold, so the partition count is entirely controlled by maxSplitBytes. This is where
//     the autotuner has the largest actuator effect. Prior benchmark: 128 MiB→47s, 512 MiB→26s.
//   web_sales   ( 5.7 GiB, ~100 KB files, ~4 files/partition) — same structure, different table
//     identity. Tests that two tables produce independent observations and decisions.
//
//   catalog_sales and inventory are NOT used: they have one large file per date partition
//   (3.3 MB and 7.8 MB respectively). Files are already below any split threshold, so
//   changing maxSplitBytes has no effect on partition count.

import org.apache.spark.sql.functions._

val DATA    = "/home/kuhu/Reps/ab/nds_sf100/parquet_sf100_decimal_fresh_20260623"
val REPODIR = "/home/kuhu/Reps/spark-rapids"
val OUT     = s"$REPODIR/data/autotuner-test-output"

// Wide projection: enough numeric columns to push decoded/listed ratio above 1.
// Narrow projections (2–3 columns) produce ratio < 1 and the autotuner returns the ceiling
// (batchSizeBytes). A wide projection exercises the full interpolation path.
def runStoreSales(tag: String): Long = {
  println(s"\n=== store_sales $tag ===")
  val t0 = System.currentTimeMillis()
  spark.sql(s"""
    SELECT
      ss_sold_date_sk,
      count(*)                     AS row_count,
      sum(ss_quantity)             AS total_qty,
      sum(ss_sales_price)          AS total_sales,
      sum(ss_ext_sales_price)      AS total_ext_sales,
      sum(ss_ext_discount_amt)     AS total_discount,
      sum(ss_ext_list_price)       AS total_list,
      sum(ss_net_paid)             AS total_net_paid,
      sum(ss_net_paid_inc_tax)     AS total_net_paid_tax,
      sum(ss_net_profit)           AS total_profit,
      sum(ss_ext_wholesale_cost)   AS total_wh_cost,
      sum(ss_wholesale_cost)       AS total_wh_cost2,
      sum(ss_ext_tax)              AS total_tax,
      sum(ss_coupon_amt)           AS total_coupon,
      sum(ss_list_price)           AS total_list2
    FROM parquet.`$DATA/store_sales`
    GROUP BY ss_sold_date_sk
  """).write.mode("overwrite").parquet(s"$OUT/store_sales_$tag")
  val elapsed = System.currentTimeMillis() - t0
  println(s"=== store_sales $tag done in ${elapsed}ms ===")
  elapsed
}

def runWebSales(tag: String): Long = {
  println(s"\n=== web_sales $tag ===")
  val t0 = System.currentTimeMillis()
  spark.sql(s"""
    SELECT
      ws_sold_date_sk,
      count(*)                      AS row_count,
      sum(ws_quantity)              AS total_qty,
      sum(ws_sales_price)           AS total_sales,
      sum(ws_ext_sales_price)       AS total_ext_sales,
      sum(ws_ext_discount_amt)      AS total_discount,
      sum(ws_ext_list_price)        AS total_list,
      sum(ws_net_paid)              AS total_net_paid,
      sum(ws_net_paid_inc_tax)      AS total_net_paid_tax,
      sum(ws_net_profit)            AS total_profit,
      sum(ws_ext_wholesale_cost)    AS total_wh_cost,
      sum(ws_wholesale_cost)        AS total_wh_cost2,
      sum(ws_ext_tax)               AS total_tax,
      sum(ws_coupon_amt)            AS total_coupon,
      sum(ws_list_price)            AS total_list2
    FROM parquet.`$DATA/web_sales`
    GROUP BY ws_sold_date_sk
  """).write.mode("overwrite").parquet(s"$OUT/web_sales_$tag")
  val elapsed = System.currentTimeMillis() - t0
  println(s"=== web_sales $tag done in ${elapsed}ms ===")
  elapsed
}

// Two runs of the same queries over the same tables.
//   Run 1: history file is empty → autotuner logs COLD_START, uses Spark default split, writes observation.
//   Run 2: history file has one observation per table → autotuner logs DECIDED, overrides split_bytes.
// Both runs must produce identical query results. The partition count (and usually elapsed time)
// will differ between runs because a different maxSplitBytes is used in createNonBucketedReadRDD.

val timings = for (run <- 1 to 2) yield {
  println(s"\n========== RUN $run ==========")
  println(s"Expected autotuner log: ${if (run == 1) "COLD_START" else "DECIDED"}")
  val ss = runStoreSales(s"run$run")
  val ws = runWebSales(s"run$run")
  (run, ss, ws)
}

println(s"""
=== Timing summary ===
         store_sales    web_sales
run1:    ${timings(0)._2}ms        ${timings(0)._3}ms
run2:    ${timings(1)._2}ms        ${timings(1)._3}ms
delta:   ${timings(0)._2 - timings(1)._2}ms        ${timings(0)._3 - timings(1)._3}ms

=== Verify history file ===
""")

val histPath = java.nio.file.Paths.get(
  spark.conf.getOption("spark.rapids.sql.scan.splitAutotuner.historyPath")
    .getOrElse(s"$REPODIR/data/scan-split-history.tsv"))
if (java.nio.file.Files.exists(histPath)) {
  val lines = java.nio.file.Files.readAllLines(histPath)
  println(s"History file: $histPath  (${lines.size()} lines)")
  lines.forEach(println)
} else {
  println(s"WARNING: history file not found at $histPath")
}

println("""
=== What to check in driver logs ===
Run 1: grep for 'COLD_START' — should appear once per table per run.
Run 2: grep for 'DECIDED'   — expansion_ratio and split_bytes should be non-default.
       grep for 'RECORDED'  — four lines total (2 tables × 2 runs).

grep -E '(COLD_START|DECIDED|RECORDED|SKIPPED)' driver.log
""")
