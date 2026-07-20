// A/A test: run NDS query9 N times in ONE session with a CONSTANT split (autotuner OFF).
// Page cache is pre-warmed by the launcher, so every iteration reads from RAM. The only thing
// that changes across iterations is JVM/JIT/GPU warmup. If iter1 >> iter2 ~= iter3 ~= ... then
// the "cold->warm" speedup is warmup, NOT the (proven-inert) split autotuner.

val DATA = "/home/kuhu/Reps/ab/nds_sf100/parquet_sf100_decimal_fresh_20260623"
spark.read.parquet(s"$DATA/store_sales").createOrReplaceTempView("store_sales")
spark.read.parquet(s"$DATA/reason").createOrReplaceTempView("reason")

val q9 = """
select case when (select count(*) from store_sales where ss_quantity between 1 and 20) > 12171089
            then (select avg(ss_ext_tax) from store_sales where ss_quantity between 1 and 20)
            else (select avg(ss_net_paid) from store_sales where ss_quantity between 1 and 20) end bucket1,
       case when (select count(*) from store_sales where ss_quantity between 21 and 40) > 111772806
            then (select avg(ss_ext_tax) from store_sales where ss_quantity between 21 and 40)
            else (select avg(ss_net_paid) from store_sales where ss_quantity between 21 and 40) end bucket2,
       case when (select count(*) from store_sales where ss_quantity between 41 and 60) > 23211533
            then (select avg(ss_ext_tax) from store_sales where ss_quantity between 41 and 60)
            else (select avg(ss_net_paid) from store_sales where ss_quantity between 41 and 60) end bucket3,
       case when (select count(*) from store_sales where ss_quantity between 61 and 80) > 56475617
            then (select avg(ss_ext_tax) from store_sales where ss_quantity between 61 and 80)
            else (select avg(ss_net_paid) from store_sales where ss_quantity between 61 and 80) end bucket4,
       case when (select count(*) from store_sales where ss_quantity between 81 and 100) > 43543186
            then (select avg(ss_ext_tax) from store_sales where ss_quantity between 81 and 100)
            else (select avg(ss_net_paid) from store_sales where ss_quantity between 81 and 100) end bucket5
from reason where r_reason_sk = 1
"""

// warm up nothing intentionally; measure every iteration including the first
val iters = 6
val times = for (i <- 1 to iters) yield {
  val t0 = System.currentTimeMillis()
  val rows = spark.sql(q9).collect()
  val ms = System.currentTimeMillis() - t0
  println(f"=== AA query9 iter $i%d done in ${ms}%d ms  (rows=${rows.length}%d) ===")
  ms
}
println("\n=== AA query9 iteration times (ms) ===")
times.zipWithIndex.foreach { case (ms, i) => println(f"iter ${i+1}%d: ${ms}%d ms") }
val first = times.head; val rest = times.tail
println(f"\niter1=${first}%d ms;  mean(iter2..)=${rest.sum/rest.length}%d ms;  " +
        f"iter1/iter2=${first.toDouble/times(1)}%.2fx")
System.exit(0)
