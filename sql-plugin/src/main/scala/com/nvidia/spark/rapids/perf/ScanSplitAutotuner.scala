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

package com.nvidia.spark.rapids.perf

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, StandardOpenOption}
import java.util.Base64

import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.GpuMetric
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.rapids.GpuFileSourceScanExec
import org.apache.spark.sql.util.QueryExecutionListener

private[rapids] case class ScanSplitRecord(
    tableLabel: String,
    listedBytes: Long,
    decodedBytes: Long,
    decodedRows: Long,
    timestampMs: Long)

/**
 * Append-only local file store for scan split observations.
 * One tab-separated line per record; tableLabel is base64-encoded.
 * Truncates any incomplete trailing line on load to handle crash-interrupted writes.
 */
private[rapids] class ScanSplitStore(path: Path) extends Logging {
  private val encoder = Base64.getUrlEncoder.withoutPadding()
  private val decoder = Base64.getUrlDecoder
  private val records = ArrayBuffer[ScanSplitRecord]()

  if (Files.exists(path)) {
    val raw = Files.readAllBytes(path)
    val lastNl = raw.lastIndexOf('\n'.toByte)
    if (lastNl + 1 < raw.length) {
      val ch = FileChannel.open(path, StandardOpenOption.WRITE)
      try {
        val lk = ch.lock()
        try { ch.truncate(lastNl + 1); ch.force(true) } finally lk.release()
      } finally ch.close()
    }
    val complete = if (lastNl >= 0) raw.take(lastNl + 1) else Array.emptyByteArray
    new String(complete, StandardCharsets.UTF_8).split("\n", -1).filter(_.nonEmpty).foreach { line =>
      val p = line.split("\t", -1)
      if (p.length == 5) {
        records += ScanSplitRecord(
          new String(decoder.decode(p(0)), StandardCharsets.UTF_8),
          p(1).toLong, p(2).toLong, p(3).toLong, p(4).toLong)
      }
    }
    logInfo(s"[ScanSplitAutotuner] loaded ${records.size} observations from $path")
  }

  def latestFor(tableLabel: String): Option[ScanSplitRecord] = synchronized {
    records.filter(_.tableLabel == tableLabel).lastOption
  }

  def append(record: ScanSplitRecord): Unit = synchronized {
    if (path.getParent != null) Files.createDirectories(path.getParent)
    val line = (Seq(
      encoder.encodeToString(record.tableLabel.getBytes(StandardCharsets.UTF_8)),
      record.listedBytes, record.decodedBytes, record.decodedRows, record.timestampMs
    ).mkString("\t") + "\n").getBytes(StandardCharsets.UTF_8)
    val ch = FileChannel.open(path,
      StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND)
    try {
      val lk = ch.lock()
      try { ch.write(ByteBuffer.wrap(line)); ch.force(true) } finally lk.release()
    } finally ch.close()
    records += record
  }
}

/**
 * Driver-side scan split autotuner.
 *
 * Enabled by setting spark.rapids.sql.scan.splitAutotuner.historyPath to a local file path.
 * decide() overrides per-scan maxSplitBytes using the expansion ratio from the latest observation
 * for the same table. Falls back to the Spark default when no history exists or arithmetic fails.
 * record() persists a new observation after a successful query.
 */
object ScanSplitAutotuner extends Logging {
  private val MIN_SPLIT_BYTES = 64L * 1024L * 1024L
  @volatile private var store: Option[ScanSplitStore] = None
  @volatile private var listenerRegistered = false

  // Scan nodes registered at planning time (createNonBucketedReadRDD). Metrics are read from
  // them in onSuccess after execution. CommandResultExec.children is empty for write queries so
  // plan traversal in the listener can never reach these nodes — we must capture them here.
  private val pendingScans =
    new java.util.concurrent.ConcurrentLinkedQueue[(String, Long, GpuFileSourceScanExec)]()

  def init(path: Path): Unit = {
    store = Some(new ScanSplitStore(path))
  }

  def close(): Unit = {
    store = None
    listenerRegistered = false
    pendingScans.clear()
  }

  def registerPendingScan(label: String, listedBytes: Long, node: GpuFileSourceScanExec): Unit = {
    if (store.isDefined && label.nonEmpty) pendingScans.offer((label, listedBytes, node))
  }

  def discardPendingScans(): Unit = pendingScans.clear()

  def drainPendingScans(): Unit = {
    var entry = pendingScans.poll()
    while (entry != null) {
      val (label, listedBytes, node) = entry
      val decodedBytes = node.metrics.get(GpuMetric.OUTPUT_BATCH_BYTES).map(_.value).getOrElse(0L)
      val decodedRows  = node.metrics.get(GpuMetric.NUM_OUTPUT_ROWS).map(_.value).getOrElse(0L)
      record(label, listedBytes, decodedBytes, decodedRows)
      entry = pendingScans.poll()
    }
  }

  // Called from decide() on the first scan after init. By that point the SparkSession exists.
  private def ensureListenerRegistered(): Unit = {
    if (!listenerRegistered) synchronized {
      if (!listenerRegistered) {
        SparkSession.getActiveSession.foreach { spark =>
          spark.listenerManager.register(new ScanObservationListener())
          logWarning(s"[ScanSplitAutotuner] ScanObservationListener registered")
          listenerRegistered = true
        }
      }
    }
  }

  /** Catalog name (db.table) if available, else the first root path string. */
  def tableLabel(catalogName: Option[String], firstRootPath: Option[String]): String =
    catalogName.orElse(firstRootPath).getOrElse("")

  /**
   * Returns the split bytes to use for this scan.
   * Ceiling is maxSplitCeiling (defaults to batchSizeBytes at the call site); floor is 64 MiB.
   * The split targets one decoded batch: raw = batchSizeBytes / expansionRatio. When data
   * compresses/projects (ratio < 1) raw exceeds batchSizeBytes, so a ceiling above batchSizeBytes
   * lets tasks actually decode to ~batchSizeBytes instead of under-filling.
   */
  def decide(
      label: String,
      listedBytes: Long,
      sparkDefault: Long,
      batchSizeBytes: Long,
      maxSplitCeiling: Long): Long = {
    if (store.isEmpty || label.isEmpty) return sparkDefault
    ensureListenerRegistered()
    val maxSplit = math.max(maxSplitCeiling, MIN_SPLIT_BYTES)
    store.get.latestFor(label) match {
      case None =>
        logWarning(s"[ScanSplitAutotuner] COLD_START table=$label " +
          s"listed_bytes=$listedBytes split_bytes=$sparkDefault history_count=0")
        sparkDefault
      case Some(rec) if rec.listedBytes <= 0 || rec.decodedBytes <= 0 =>
        logWarning(s"[ScanSplitAutotuner] SKIPPED table=$label " +
          s"reason=invalid_history split_bytes=$sparkDefault")
        sparkDefault
      case Some(rec) =>
        val expansionRatio = rec.decodedBytes.toDouble / rec.listedBytes
        if (!java.lang.Double.isFinite(expansionRatio) || expansionRatio <= 0) {
          logWarning(s"[ScanSplitAutotuner] SKIPPED table=$label " +
            s"reason=arithmetic_overflow split_bytes=$sparkDefault")
          return sparkDefault
        }
        val raw = (batchSizeBytes / expansionRatio).toLong
        if (raw <= 0) {
          logWarning(s"[ScanSplitAutotuner] SKIPPED table=$label " +
            s"reason=arithmetic_overflow split_bytes=$sparkDefault")
          return sparkDefault
        }
        val splitBytes = math.max(MIN_SPLIT_BYTES, math.min(maxSplit, raw))
        val predictedDecoded = (listedBytes * expansionRatio).toLong
        logWarning(s"[ScanSplitAutotuner] DECIDED table=$label listed_bytes=$listedBytes " +
          s"expansion_ratio=$expansionRatio predicted_decoded_bytes=$predictedDecoded " +
          s"split_bytes=$splitBytes spark_default=$sparkDefault history_count=1")
        splitBytes
    }
  }

  def record(
      label: String,
      listedBytes: Long,
      decodedBytes: Long,
      decodedRows: Long): Unit = {
    if (store.isEmpty || label.isEmpty || listedBytes <= 0) return
    if (decodedBytes <= 0) {
      logWarning(s"[ScanSplitAutotuner] SKIPPED_RECORD table=$label " +
        s"reason=zero_decoded_bytes (set spark.rapids.sql.metrics.level=DEBUG to enable)")
      return
    }
    store.get.append(
      ScanSplitRecord(label, listedBytes, decodedBytes, decodedRows, System.currentTimeMillis()))
    logWarning(s"[ScanSplitAutotuner] RECORDED table=$label listed_bytes=$listedBytes " +
      s"decoded_bytes=$decodedBytes decoded_rows=$decodedRows")
  }
}

/**
 * Collects scan metrics after each successful query and forwards them to ScanSplitAutotuner.
 * onFailure is a no-op so failed/cancelled queries never contaminate history.
 */
private[rapids] class ScanObservationListener extends QueryExecutionListener {
  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    // Scan nodes were registered at planning time via registerPendingScan; read their
    // post-execution metrics here. Plan traversal cannot be used because write commands
    // (CommandResultExec) have empty physical children — the scan runs inside command.run().
    ScanSplitAutotuner.drainPendingScans()
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    ScanSplitAutotuner.discardPendingScans()
  }
}
