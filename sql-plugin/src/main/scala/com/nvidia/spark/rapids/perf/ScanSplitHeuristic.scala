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

import scala.collection.JavaConverters._
import scala.util.Try

import com.nvidia.spark.history.{MetricStore, MetricStores}
import com.nvidia.spark.rapids.{GpuMetric, RapidsConf}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.CollectionAccumulator

/**
 * What planning has in hand when it sizes one scan.
 *
 * @param table history key: the table name exactly as the planning hook sees it
 * @param listedBytes on-disk bytes the scan lists; bounds the decided split
 * @param plannedSplits splits the scan planned, indices 0..plannedSplits-1
 * @param drainSplits at query end: returns and clears the (split, decoded, listed) reads
 * @param isCurrent false once the scan was planned again; only the current planning is drained
 */
final case class ScanContext(
    table: String,
    listedBytes: Long,
    batchSizeBytes: Long,
    minPartitionNum: Long,
    maxSplitBytes: Long,
    plannedSplits: Int = 0,
    drainSplits: () => Seq[(Int, Long, Long)] = () => Seq.empty,
    isCurrent: () => Boolean = () => true)

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
      maxSplitBytes = ScanSplitHeuristic.NO_DECISION)
    val decided = decide(ctx, nowMs)
    if (decided == ScanSplitHeuristic.NO_DECISION) {
      logInfo(s"scan.split: table=$table listed=$listedBytes -> no history, iceberg default")
    } else {
      logInfo(s"scan.split: table=$table listed=$listedBytes -> split=$decided bytes")
    }
    decided
  }

  /**
   * The file-source decision: the split for catalog table `table`, or `sparkMaxSplitBytes`
   * unchanged for a path-only relation or while history is off.
   *
   * @param context builds the scan's context for its table; only called when history is used
   */
  def fileSourceSplit(
      table: Option[String],
      sparkMaxSplitBytes: Long,
      context: String => ScanContext,
      nowMs: Long): Long = table match {
    case Some(name) if isEnabled => decide(context(name), nowMs)
    case _ => sparkMaxSplitBytes
  }

  /** Records the current planning's ratio, or logs once why not (superseded: debug only). */
  protected def observe(ctx: ScanContext): Map[HistoryMetric, Double] = {
    if (!ctx.isCurrent()) {
      logDebug(s"scan.split: table=${ctx.table} not recorded: superseded planning")
      Map.empty
    } else {
      val entries = ctx.drainSplits()
      val result = if (entries.isEmpty) {
        Left("planned but not executed in this query")
      } else {
        ScanSplitHeuristic.ratioFromSplits(ctx.plannedSplits, entries)
      }
      result match {
        case Right(ratio) => Map(ScanExpansionRatio -> ratio)
        case Left(reason) =>
          logInfo(s"scan.split: table=${ctx.table} not recorded: $reason")
          Map.empty
      }
    }
  }

  override protected def shouldObserve(ctx: ScanContext): Boolean =
    ctx.table != null && ctx.table.nonEmpty && ctx.plannedSplits > 0
}

/** The application's scan split heuristic, over the store the driver plugin installed. */
object ScanSplitHeuristic extends ScanSplitHeuristic(() => MetricStores.current()) {

  /** Returned when history has nothing to say, so Iceberg's own split stands untouched. */
  final val NO_DECISION = -1L

  /**
   * Create when a split's reader opens; on exhaustion adds (split, growth of `decoded`, listed).
   * A task's reads run one after another, so the growth is this split's. Sizes may start at -1.
   */
  def recordSplitOnExhaustion(
      split: Int,
      listed: Long,
      decoded: GpuMetric,
      acc: CollectionAccumulator[(Int, Long, Long)]): Boolean => Boolean = {
    val start = decoded.value.max(0L)
    hasMore => {
      if (!hasMore) acc.add((split, decoded.value.max(0L) - start, listed))
      hasMore
    }
  }

  /** Returns and clears `acc` atomically. */
  def drainSplits(acc: CollectionAccumulator[(Int, Long, Long)]): Seq[(Int, Long, Long)] =
    acc.synchronized {
      val entries = acc.value.asScala.toList
      acc.reset()
      entries
    }

  /**
   * sum(decoded) / sum(listed) over the first read of each split, only if exactly splits
   * 0..planned-1 were read; otherwise the reason.
   */
  def ratioFromSplits(
      planned: Int,
      entries: Seq[(Int, Long, Long)]): Either[String, Double] = {
    val firstReads = entries.groupBy(_._1).map { case (split, reads) => split -> reads.head }
    val decoded = firstReads.values.map(_._2).sum
    val listed = firstReads.values.map(_._3).sum
    val inRange = firstReads.keySet.count(split => split >= 0 && split < planned)
    if (planned <= 0) {
      Left("nothing planned")
    } else if (inRange < firstReads.size) {
      Left(s"read split indices outside 0..${planned - 1}")
    } else if (inRange < planned) {
      Left(s"read $inRange of $planned planned splits to the end")
    } else if (listed <= 0L) {
      Left("no listed bytes in the splits read")
    } else if (decoded <= 0L) {
      Left("no decoded bytes (decoded-bytes metric off or zero)")
    } else {
      Right(decoded.toDouble / listed.toDouble)
    }
  }

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
