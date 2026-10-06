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

import scala.util.Try

import com.nvidia.spark.history.{MetricStore, MetricStores}
import com.nvidia.spark.rapids.RapidsConf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf

/**
 * What planning has in hand when it sizes one scan.
 *
 * `decodedBytes` and `completedSplits` are read once the query ends, never at planning time:
 * they close over metrics that stay zero until Spark merges task values back. A context that is
 * only decided on, never observed, passes thunks returning 0.
 *
 * @param table history key: the table name exactly as the planning hook sees it
 * @param listedBytes on-disk bytes the scan lists; the observed ratio's denominator
 * @param plannedSplits splits the scan planned, or 0 when the call site does not track them
 */
final case class ScanContext(
    table: String,
    listedBytes: Long,
    batchSizeBytes: Long,
    minPartitionNum: Long,
    maxSplitBytes: Long,
    decodedBytes: () => Long,
    plannedSplits: Long = 0L,
    completedSplits: () => Long = () => 0L)

/** Sizes a scan's split from the decode-expansion ratio its table last produced. */
class ScanSplitHeuristic(store: () => MetricStore) extends HistoryHeuristic(store) {

  type Ctx = ScanContext
  type Decision = Long

  def name: String = "scan.split"

  def metrics: Seq[HistoryMetric] = Seq(ScanExpansionRatio)

  /** Table only: the Iceberg decision runs before columns and filters are pushed down. */
  protected def keyFor(metric: HistoryMetric, ctx: ScanContext): String = ctx.table

  protected def staticDecision(ctx: ScanContext): Long = ctx.maxSplitBytes

  protected def decideFrom(observed: Map[HistoryMetric, Double], ctx: ScanContext): Long =
    observed.get(ScanExpansionRatio)
      .map { ratio =>
        val raw = ScanSplitSizer.rawSplit(ratio, ctx.batchSizeBytes)
        // The only record of the ratio a decision used, so a finished run can tell a learned
        // decision from the static fallback.
        logInfo(s"scan.split: table=${ctx.table} ratio=$ratio " +
          s"targetBatch=${ctx.batchSizeBytes} rawSplit=$raw")
        raw
      }
      .getOrElse(0L)

  override protected def constrain(raw: Long, ctx: ScanContext): Long =
    ScanSplitSizer.bound(raw, ctx.listedBytes, ctx.minPartitionNum, ctx.maxSplitBytes)

  /**
   * The Iceberg decision: the learned split for `table`, or `ScanSplitHeuristic.NO_DECISION` so
   * Iceberg's own split stands. Logs the outcome, since this is the only place that knows the
   * split was actually applied to a scan.
   */
  def decideIcebergSplit(
      table: String,
      listedBytes: Long,
      batchSizeBytes: Long,
      minPartitionNum: Long,
      nowMs: Long): Long = {
    val ctx = ScanContext(
      table = table,
      listedBytes = listedBytes,
      batchSizeBytes = batchSizeBytes,
      minPartitionNum = minPartitionNum,
      // Threaded through as the static answer: with no usable ratio `bound` returns it
      // unchanged, and with one it is never read.
      maxSplitBytes = ScanSplitHeuristic.NO_DECISION,
      // Observed by the scan itself once it runs, never from here.
      decodedBytes = () => 0L)
    val decided = decide(ctx, nowMs)
    if (decided == ScanSplitHeuristic.NO_DECISION) {
      logInfo(s"scan.split: table=$table listed=$listedBytes -> no history, iceberg default")
    } else {
      logInfo(s"scan.split: table=$table listed=$listedBytes -> split=$decided bytes")
    }
    decided
  }

  /**
   * The file-source decision: decides and registers the scan of catalog table `table`, or
   * returns `sparkMaxSplitBytes` unchanged for a path-only relation or while history is off.
   *
   * @param context builds the scan's context for its table; only called when history is used
   */
  def fileSourceSplit(
      table: Option[String],
      sparkMaxSplitBytes: Long,
      executionId: => Option[Long],
      context: String => ScanContext,
      nowMs: Long): Long = table match {
    case Some(name) if isEnabled =>
      val ctx = context(name)
      val split = decide(ctx, nowMs)
      register(executionId, ctx)
      split
    case _ => sparkMaxSplitBytes
  }

  /**
   * Records the ratio only from a scan that read every split it planned. One-sided: a scan
   * executed twice reads more than it planned, which is healthy; only an under-read means it
   * stopped early and would understate the ratio.
   */
  protected def observe(ctx: ScanContext): Map[HistoryMetric, Double] = {
    val decoded = ctx.decodedBytes()
    if (decoded <= 0L) {
      Map.empty
    } else if (ctx.plannedSplits > 0L && ctx.completedSplits() < ctx.plannedSplits) {
      logDebug(s"scan.split: table=${ctx.table} read only ${ctx.completedSplits()} of " +
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

/** The application's scan split heuristic, over the store the driver plugin installed. */
object ScanSplitHeuristic extends ScanSplitHeuristic(() => MetricStores.current()) {

  /** Returned when history has nothing to say, so Iceberg's own split stands untouched. */
  final val NO_DECISION = -1L

  /**
   * The Iceberg advisor, reached through the root-level Iceberg table wrapper: the learned split
   * size for `table` in the active session, or `NO_DECISION`. Callers must leave the read option
   * unset on `NO_DECISION`: writing a value would shadow the table's own properties.
   */
  def learnedSplitBytes(table: String, listedBytes: Long): Long = {
    if (!isEnabled || table == null || table.isEmpty) {
      NO_DECISION
    } else {
      SparkSession.getActiveSession.map { spark =>
        decideIcebergSplit(
          table = table,
          listedBytes = listedBytes,
          batchSizeBytes = new RapidsConf(spark.sessionState.conf).gpuTargetBatchSizeBytes,
          minPartitionNum = minPartitionNum(spark),
          nowMs = System.currentTimeMillis())
      }.getOrElse(NO_DECISION)
    }
  }

  /** `ScanSplitSizer.minPartitionNum` over this session's configuration. */
  def minPartitionNum(spark: SparkSession): Long =
    minPartitionNum(spark.sessionState.conf, spark.sparkContext.defaultParallelism)

  /**
   * `ScanSplitSizer.minPartitionNum` over `sqlConf`, which carries the session's Spark settings
   * too. `registeredCores` is only read when no parallelism or executor slots are configured.
   */
  def minPartitionNum(sqlConf: SQLConf, registeredCores: => Int): Long = {
    def intConf(key: String): Option[Int] =
      Option(sqlConf.getConfString(key, null)).flatMap(v => Try(v.trim.toInt).toOption)
    ScanSplitSizer.minPartitionNum(
      filesMinPartitionNum = sqlConf.filesMinPartitionNum,
      leafNodeDefaultParallelism = sqlConf.getConf(SQLConf.LEAF_NODE_DEFAULT_PARALLELISM),
      defaultParallelism = intConf("spark.default.parallelism"),
      executorInstances = intConf("spark.executor.instances"),
      executorCores = intConf("spark.executor.cores"),
      registeredCores = registeredCores)
  }
}
