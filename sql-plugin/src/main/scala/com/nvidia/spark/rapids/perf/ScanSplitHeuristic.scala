/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.perf

import com.nvidia.spark.history.HistoryMetricCatalog
import com.nvidia.spark.rapids.RapidsConf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf

/**
 * How far one scan's decoded device bytes expand beyond the file bytes it listed.
 */
object ScanExpansionRatio extends HistoryMetric {

  protected def metricId: Int = HistoryMetricCatalog.SCAN_EXPANSION_RATIO_ID

  protected def dimension: String = "relation"

  /**
   * Keys on the table, the columns decoded, and the filters pushed into the scan - all three move
   * the ratio, so scans differing in any of them must not share a slot.
   */
  def scanKey(table: String, columns: Seq[String], filters: Seq[String]): String = {
    if (table == null || table.isEmpty) {
      ""
    } else {
      hashKey(table, columns.sorted.mkString(","), filters.sorted.mkString(","))
    }
  }
}

/**
 * What planning has in hand when it sizes one scan.
 *
 * `decodedBytes` is read once the query ends, never at planning time: it closes over metrics that
 * stay zero until Spark merges task values back. A scan planned where no node will ever report
 * (the iceberg decision site, which runs before an exec exists) passes a thunk returning 0, and
 * `observe` then contributes nothing.
 */
final case class ScanContext(
    table: String,
    columns: Seq[String],
    filters: Seq[String],
    listedBytes: Long,
    batchSizeBytes: Long,
    minPartitionNum: Long,
    maxSplitBytes: Long,
    decodedBytes: () => Long,
    plannedSplits: Long = 0L,
    completedSplits: () => Long = () => 0L)

/** Pure split arithmetic. No state, no store, no Spark. */
object ScanSplitSizer {

  val MAX_SPLIT_BYTES: Long = 4L * 1024 * 1024 * 1024
  val MIN_SPLIT_BYTES: Long = 64L * 1024 * 1024

  /** The formula: how big a split this ratio implies. Zero means no usable answer. */
  def rawSplit(ratio: Double, targetBytes: Long): Long = {
    if (ratio <= 0.0d || ratio.isNaN || ratio.isInfinite || targetBytes <= 0L) {
      0L
    } else {
      (targetBytes.toDouble / ratio).toLong
    }
  }

  /**
   * The bounds. The ceiling keeps roughly one task per core, so a low ratio cannot collapse a
   * table into a few huge tasks. The floor is an absolute minimum, so a high-expansion table can
   * take a smaller split than Spark chose.
   */
  def bound(raw: Long, listedBytes: Long, minPartitionNum: Long, maxSplitBytes: Long): Long = {
    if (raw <= 0L) {
      maxSplitBytes
    } else {
      val parallelismCeiling =
        if (minPartitionNum > 0L) math.max(1L, listedBytes / minPartitionNum) else MAX_SPLIT_BYTES
      val ceiling = math.min(MAX_SPLIT_BYTES, parallelismCeiling)
      math.max(MIN_SPLIT_BYTES, math.min(ceiling, raw))
    }
  }
}

/** Sizes a scan's split from the expansion ratio that scan last produced. */
object ScanSplitHeuristic extends HistoryHeuristic {

  type Ctx = ScanContext
  type Decision = Long

  def name: String = "scan.split"

  def metrics: Seq[HistoryMetric] = Seq(ScanExpansionRatio)

  protected def keyFor(metric: HistoryMetric, ctx: ScanContext): String =
    ScanExpansionRatio.scanKey(ctx.table, ctx.columns, ctx.filters)

  protected def staticDecision(ctx: ScanContext): Long = ctx.maxSplitBytes

  protected def decideFrom(observed: Map[HistoryMetric, Double], ctx: ScanContext): Long =
    observed.get(ScanExpansionRatio)
      .map { ratio =>
        val raw = ScanSplitSizer.rawSplit(ratio, ctx.batchSizeBytes)
        // The ratio is otherwise only visible inside the binary snapshot, which leaves no way to
        // tell a learnt decision from the static fallback in a finished run.
        logInfo(s"scan.split: table=${ctx.table} ratio=$ratio " +
          s"targetBatch=${ctx.batchSizeBytes} rawSplit=$raw")
        raw
      }
      .getOrElse(0L)

  override protected def constrain(raw: Long, ctx: ScanContext): Long =
    ScanSplitSizer.bound(raw, ctx.listedBytes, ctx.minPartitionNum, ctx.maxSplitBytes)

  /** Reports what the decision site applied, so a finished run says whether history was used. */
  private[perf] def logDecision(table: String, listedBytes: Long, decided: Long): Unit = {
    if (decided == IcebergScanSplit.NO_DECISION) {
      logInfo(s"scan.split: table=$table listed=$listedBytes -> no history, iceberg default")
    } else {
      logInfo(s"scan.split: table=$table listed=$listedBytes -> split=$decided bytes")
    }
  }

  /**
   * Records the ratio only from a scan that read every split it planned.
   *
   * `decodedBytes` comes from accumulators that hold whatever the tasks that FINISHED contributed,
   * while `listedBytes` is the whole plan. A scan that stops early therefore yields a fraction
   * over the whole, understating the ratio - and, written to history, pulling the next decision
   * further off, which pulls the one after that further still. Job 29 diverges 4.368677 ->
   * 3.148015 -> 1.547024 that way, because AQE proves its result empty and abandons the in-flight
   * scan.
   *
   * The completeness test lives here rather than in a listener because the abandonment is not
   * observable when the observation is drained: the stage is cancelled by the shutdown hook, which
   * runs AFTER SparkListenerSQLExecutionEnd. Comparing splits read against splits planned is
   * immune to that ordering - it asks the only question that matters, and the scan already knows
   * the answer.
   *
   * The test is deliberately one-sided. `plannedSplits` is the task-group count of ONE scan plan,
   * while `completedSplits` accumulates for the lifetime of the scan object and so counts every
   * reader the plan ever creates from it. A scan executed twice therefore reports roughly double
   * the planned count - job 35 read 21,256 of 10,628 - which is healthy, not truncated. Only an
   * UNDER-read means the scan stopped early, so `<` is the condition and `!=` would discard
   * perfectly good observations from any job whose scan runs more than once.
   *
   * `plannedSplits == 0` means the call site does not track splits; the check is skipped so those
   * paths keep their previous behaviour.
   */
  protected def observe(ctx: ScanContext): Map[HistoryMetric, Double] = {
    val decoded = ctx.decodedBytes()
    if (decoded <= 0L) {
      Map.empty
    } else if (ctx.plannedSplits > 0L && ctx.completedSplits() < ctx.plannedSplits) {
      logWarning(s"scan.split: table=${ctx.table} read only ${ctx.completedSplits()} of " +
        s"${ctx.plannedSplits} planned splits; not recording an expansion ratio from a scan " +
        "that did not finish")
      Map.empty
    } else {
      Map(ScanExpansionRatio -> decoded.toDouble / ctx.listedBytes.toDouble)
    }
  }

  override protected def shouldObserve(ctx: ScanContext): Boolean =
    ctx.table != null && ctx.table.nonEmpty && ctx.listedBytes > 0L
}

/**
 * The iceberg decision site, called from java where the read options are assembled.
 *
 * Iceberg plans its own splits, so the only way to size them is to set `read.split.target-size`
 * before the scan is built - which is before any exec exists. That splits the cycle in two: the
 * decision happens here, the observation in `GpuBatchScanExecBase`. Both key on the table alone,
 * because that is all this site has: columns and filters are pushed down after it runs.
 */
object IcebergScanSplit {

  /** Returned when history has nothing to say, so iceberg's own default stands untouched. */
  val NO_DECISION: Long = -1L

  /**
   * The learned split size for `table`, or `NO_DECISION`.
   *
   * `NO_DECISION` is threaded through as the static answer rather than checked for separately:
   * with no usable ratio the heuristic returns `maxSplitBytes` unchanged, and with one the bound
   * never reads it. Callers must leave the option unset on `NO_DECISION` - writing a value would
   * shadow the table's own TBLPROPERTIES.
   */
  def learnedSplitBytes(table: String, listedBytes: Long): Long = {
    if (!ScanSplitHeuristic.isEnabled || table == null || table.isEmpty) {
      NO_DECISION
    } else {
      SparkSession.getActiveSession.map { spark =>
        val sqlConf = spark.sessionState.conf
        // Same value SparkSession.leafNodeDefaultParallelism resolves, spelled out because that
        // method is package-private to org.apache.spark.sql.
        val minPartitionNum = sqlConf.filesMinPartitionNum.getOrElse(
          sqlConf.getConf(SQLConf.LEAF_NODE_DEFAULT_PARALLELISM)
            .getOrElse(spark.sparkContext.defaultParallelism))
        val ctx = ScanContext(
          table = table,
          columns = Nil,
          filters = Nil,
          listedBytes = listedBytes,
          batchSizeBytes = new RapidsConf(sqlConf).gpuTargetBatchSizeBytes,
          minPartitionNum = minPartitionNum.toLong,
          maxSplitBytes = NO_DECISION,
          // Never observed from here; the exec registers its own context.
          decodedBytes = () => 0L)
        val decided = ScanSplitHeuristic.decide(ctx, System.currentTimeMillis())
        // Logged at the decision site because this is the only place that knows the split was
        // actually applied to a scan; NO_DECISION means iceberg's own default stands.
        ScanSplitHeuristic.logDecision(table, listedBytes, decided)
        decided
      }.getOrElse(NO_DECISION)
    }
  }
}
