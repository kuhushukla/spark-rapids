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

import java.time.Duration

import scala.collection.JavaConverters._
import scala.util.Try

import com.nvidia.spark.history.{MetricStore, MetricStores, Retention}
import com.nvidia.spark.rapids.{GpuMetric, RapidsConf}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.CollectionAccumulator

/**
 * Decode expansion: GPU output batch bytes / on-disk bytes of the scan's planned files. One
 * observation per completed scan, stamped at SQL execution end.
 *
 * Dimension `table`: Iceberg's `Table.name()`, or the catalog identifier for file-source tables.
 * Compared as exact bytes, so decide and observe must derive it identically.
 */
object ScanExpansionRatio extends HistoryMetric {

  /** `scan.decode_expansion_ratio` in the production catalog. */
  override val id: Int = 1

  override val version: Int = 1

  override val dimension: String = "table"

  override val retention: Retention = new Retention(Duration.ofDays(7), Duration.ofDays(14))
}

/**
 * Inputs for sizing one scan.
 *
 * @param table history key
 * @param listedBytes on-disk bytes the scan lists; bounds the split
 * @param drainSplits returns and clears the (split, decoded, listed) reads
 * @param isCurrent false once the scan was re-planned; only the current planning is recorded
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

/** Sizes a scan's split from its table's last decode-expansion ratio. */
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
        // Only record of the ratio used; distinguishes learned from static decisions.
        logInfo(s"scan.split: table=${ctx.table} ratio=$ratio " +
          s"targetBatch=${ctx.batchSizeBytes} rawSplit=$raw")
        raw
      }
      .getOrElse(0L)

  override protected def constrain(raw: Long, ctx: ScanContext): Long =
    ScanSplitSizer.bound(raw, ctx.listedBytes, ctx.minPartitionNum, ctx.maxSplitBytes)

  /** Learned Iceberg split for `table`, or `NO_DECISION`. Logs the outcome. */
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
      // Static answer: returned unchanged when there is no usable ratio.
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
   * File-source split; `sparkMaxSplitBytes` for path-only relations or while history is off.
   *
   * @param context only called when history is used
   */
  def fileSourceSplit(
      table: Option[String],
      sparkMaxSplitBytes: Long,
      context: String => ScanContext,
      nowMs: Long): Long = table match {
    case Some(name) if isEnabled => decide(context(name), nowMs)
    case _ => sparkMaxSplitBytes
  }

  /** The current planning's ratio, or logs why not. */
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

object ScanSplitHeuristic extends ScanSplitHeuristic(() => MetricStores.current()) {

  /** No learned split; Iceberg's own split stands. */
  final val NO_DECISION = -1L

  /**
   * Create when a split's reader opens; on exhaustion adds (split, growth of `decoded`, listed).
   * A task reads splits sequentially, so the growth is this split's. Metrics may start at -1.
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

  /** sum(decoded) / sum(listed) over each split's first read if exactly 0..planned-1 were read. */
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
   * The Iceberg advisor: learned split for `table` in the active session, or `NO_DECISION`. On
   * `NO_DECISION` callers must leave the read option unset, or it shadows table properties.
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

  def minPartitionNum(spark: SparkSession): Long =
    minPartitionNum(spark.sessionState.conf, spark.sparkContext.defaultParallelism)

  /** `registeredCores` is read only when no parallelism or executor slots are configured. */
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
