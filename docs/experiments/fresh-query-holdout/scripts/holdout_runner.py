#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Fresh-holdout runner: 4 never-tested query shapes x 4 maxPartitionBytes sizes.

One spark-submit invocation = one block (session). Warmup pass (all queries at
512 MiB, untimed) then all 16 cells in seeded random order. Results appended as
JSON lines. Pilot mode runs all queries once at 512 MiB plus selectivity probes.
"""
import argparse
import hashlib
import json
import os
import random
import time

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

MIB = 1024 * 1024
DATA = "/home/roberte/src/rapids-plugin-4-spark/taxi-data-sharded"
SIZES_MIB = [128, 512, 2048, 8192]
QUERIES = ["q1_multikey_agg", "q2_selfjoin", "q3_window_topn", "q4_wide_selective_filter"]

MONTHS_2009 = ["2009-%02d" % m for m in range(1, 13)]
MONTHS_2010 = ["2010-%02d" % m for m in range(1, 13)]
MONTHS_2011 = ["2011-%02d" % m for m in range(1, 13)]


def month_paths(months):
    return [os.path.join(DATA, "yellow_tripdata_" + m) for m in months]


def cents(c):
    return F.round(F.col(c) * 100).cast("long")


def milli(c):
    return F.round(F.col(c) * 1000).cast("long")


def epoch_2009(spark):
    df = spark.read.parquet(*month_paths(MONTHS_2009))
    return df.select(
        F.col("vendor_name").cast("string").alias("vendor"),
        F.col("Passenger_Count").cast("long").alias("pcount"),
        F.col("Trip_Distance").cast("double").alias("dist"),
        F.upper(F.col("Payment_Type").cast("string")).alias("ptype"),
        F.col("Fare_Amt").cast("double").alias("fare"),
        F.col("surcharge").cast("double").alias("surch"),
        F.col("mta_tax").cast("double").alias("mta"),
        F.col("Tip_Amt").cast("double").alias("tip"),
        F.col("Tolls_Amt").cast("double").alias("tolls"),
        F.col("Total_Amt").cast("double").alias("total"),
    )


def epoch_2010(spark):
    df = spark.read.parquet(*month_paths(MONTHS_2010))
    return df.select(
        F.col("vendor_id").cast("string").alias("vendor"),
        F.col("passenger_count").cast("long").alias("pcount"),
        F.col("trip_distance").cast("double").alias("dist"),
        F.upper(F.col("payment_type").cast("string")).alias("ptype"),
        F.col("fare_amount").cast("double").alias("fare"),
        F.col("surcharge").cast("double").alias("surch"),
        F.col("mta_tax").cast("double").alias("mta"),
        F.col("tip_amount").cast("double").alias("tip"),
        F.col("tolls_amount").cast("double").alias("tolls"),
        F.col("total_amount").cast("double").alias("total"),
    )


def epoch_2011(spark):
    df = spark.read.parquet(*month_paths(MONTHS_2011))
    return df.select(
        F.col("VendorID").cast("string").alias("vendor"),
        F.col("passenger_count").cast("long").alias("pcount"),
        F.col("trip_distance").cast("double").alias("dist"),
        F.upper(F.col("payment_type").cast("string")).alias("ptype"),
        F.col("fare_amount").cast("double").alias("fare"),
        F.col("extra").cast("double").alias("surch"),
        F.col("mta_tax").cast("double").alias("mta"),
        F.col("tip_amount").cast("double").alias("tip"),
        F.col("tolls_amount").cast("double").alias("tolls"),
        F.col("total_amount").cast("double").alias("total"),
    )


def trips(spark):
    return epoch_2009(spark).unionByName(epoch_2010(spark)).unionByName(epoch_2011(spark))


def q1_multikey_agg(spark):
    t = trips(spark)
    return (
        t.groupBy("vendor", "ptype", "pcount")
        .agg(
            F.count(F.lit(1)).alias("cnt"),
            F.sum(cents("fare")).alias("fare_c"),
            F.sum(cents("tip")).alias("tip_c"),
            F.sum(cents("total")).alias("total_c"),
            F.sum(milli("dist")).alias("dist_m"),
            F.min(milli("dist")).alias("min_dist_m"),
            F.max(cents("total")).alias("max_total_c"),
        )
        .orderBy("vendor", "ptype", "pcount")
    )


def q2_selfjoin(spark):
    base = trips(spark).select(
        "vendor", "ptype",
        cents("total").alias("total_c"),
        cents("fare").alias("fare_c"),
    )
    dim = base.groupBy("vendor", "ptype").agg(
        F.sum("total_c").alias("s"), F.count(F.lit(1)).alias("c")
    )
    j = base.join(dim, ["vendor", "ptype"])
    return (
        j.groupBy("vendor")
        .agg(
            F.count(F.lit(1)).alias("cnt"),
            F.sum(F.when(F.col("total_c") * F.col("c") > F.col("s"), 1).otherwise(0)).alias("above_avg"),
            F.sum("fare_c").alias("fare_c"),
        )
        .orderBy("vendor")
    )


def q3_window_topn(spark):
    df = spark.read.parquet(*month_paths(MONTHS_2011))
    d = df.select(
        F.col("PULocationID").cast("long").alias("pu"),
        F.col("DOLocationID").cast("long").alias("do"),
        cents("total_amount").alias("total_c"),
        milli("trip_distance").alias("dist_m"),
        cents("fare_amount").alias("fare_c"),
        cents("tip_amount").alias("tip_c"),
    )
    w = Window.partitionBy("pu").orderBy(
        F.desc("total_c"), F.desc("dist_m"), F.desc("fare_c")
    )
    topn = d.withColumn("rk", F.rank().over(w)).filter(F.col("rk") <= 3)
    return (
        topn.groupBy("pu")
        .agg(
            F.count(F.lit(1)).alias("cnt"),
            F.sum("total_c").alias("total_c"),
            F.sum("dist_m").alias("dist_m"),
            F.max("tip_c").alias("max_tip_c"),
        )
        .orderBy("pu")
    )


def q4_wide_selective_filter(spark):
    t = trips(spark).filter((F.col("dist") > 30.0) & (F.col("total") > 200.0))
    return t.agg(
        F.count(F.lit(1)).alias("cnt"),
        F.sum(cents("fare")).alias("fare_c"),
        F.sum(cents("tip")).alias("tip_c"),
        F.sum(cents("tolls")).alias("tolls_c"),
        F.sum(cents("total")).alias("total_c"),
        F.sum(cents("surch")).alias("surch_c"),
        F.sum(cents("mta")).alias("mta_c"),
        F.min(milli("dist")).alias("min_dist_m"),
        F.max(milli("dist")).alias("max_dist_m"),
        F.max(cents("total")).alias("max_total_c"),
        F.sum("pcount").alias("pcount_sum"),
        F.countDistinct("vendor").alias("n_vendor"),
        F.countDistinct("ptype").alias("n_ptype"),
    )


BUILDERS = {
    "q1_multikey_agg": q1_multikey_agg,
    "q2_selfjoin": q2_selfjoin,
    "q3_window_topn": q3_window_topn,
    "q4_wide_selective_filter": q4_wide_selective_filter,
}


def canonical_hash(rows):
    def render(v):
        if v is None:
            return "NULL"
        return str(v)
    lines = sorted("|".join(render(v) for v in row) for row in rows)
    text = "\n".join(lines)
    return hashlib.sha256(text.encode("utf-8")).hexdigest(), len(lines)


def run_cell(spark, out, block, mode, query, size_mib, measured, cell_index):
    spark.conf.set("spark.sql.files.maxPartitionBytes", str(size_mib * MIB))
    cell_id = "%s_b%d_%s_s%d_i%d" % (mode, block, query, size_mib, cell_index)
    spark.sparkContext.setJobGroup(cell_id, cell_id, interruptOnCancel=True)
    err = None
    for attempt in range(2):
        try:
            q = BUILDERS[query](spark)
            t0 = time.monotonic()
            rows = q.collect()
            elapsed = time.monotonic() - t0
            digest, nrows = canonical_hash(rows)
            rec = {
                "mode": mode,
                "block": block,
                "cell_id": cell_id,
                "cell_index": cell_index,
                "query": query,
                "size_mib": size_mib,
                "measured": measured,
                "attempt": attempt,
                "elapsed_s": round(elapsed, 4),
                "result_sha256": digest,
                "result_rows": nrows,
                "app_id": spark.sparkContext.applicationId,
            }
            out.write(json.dumps(rec) + "\n")
            out.flush()
            print("CELL_DONE", json.dumps(rec))
            return
        except Exception as e:  # noqa: BLE001
            err = str(e)[:2000]
            print("CELL_ERROR", cell_id, "attempt", attempt, err)
    rec = {
        "mode": mode, "block": block, "cell_id": cell_id, "query": query,
        "size_mib": size_mib, "measured": measured, "error": err,
        "app_id": spark.sparkContext.applicationId,
    }
    out.write(json.dumps(rec) + "\n")
    out.flush()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=("pilot", "measure"), required=True)
    ap.add_argument("--block", type=int, required=True)
    ap.add_argument("--seed", type=int, required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    spark = SparkSession.builder.appName(
        "fresh-holdout-%s-b%d" % (args.mode, args.block)
    ).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    out = open(args.out, "a", encoding="utf-8")
    if args.mode == "pilot":
        # selectivity probe for q4 predicate (pilot only, at 512 MiB)
        spark.conf.set("spark.sql.files.maxPartitionBytes", str(512 * MIB))
        t = trips(spark)
        total = t.count()
        surv = t.filter((F.col("dist") > 30.0) & (F.col("total") > 200.0)).count()
        probe = {"mode": "pilot_probe", "total_rows": total, "q4_survivors": surv,
                 "q4_fraction": surv / total}
        out.write(json.dumps(probe) + "\n")
        out.flush()
        print("PROBE", json.dumps(probe))
        for i, q in enumerate(QUERIES):
            run_cell(spark, out, args.block, "pilot", q, 512, False, i)
    else:
        # warmup: all queries at 512 MiB, untimed-for-analysis
        for i, q in enumerate(QUERIES):
            run_cell(spark, out, args.block, "warmup", q, 512, False, i)
        cells = [(q, s) for q in QUERIES for s in SIZES_MIB]
        random.Random(args.seed).shuffle(cells)
        for i, (q, s) in enumerate(cells):
            run_cell(spark, out, args.block, "measure", q, s, True, i)
    out.close()
    spark.stop()


if __name__ == "__main__":
    main()
