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
    readBytes: Long,
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
      if (p.length == 6) {
        records += ScanSplitRecord(
          new String(decoder.decode(p(0)), StandardCharsets.UTF_8),
          p(1).toLong, p(2).toLong, p(3).toLong, p(4).toLong, p(5).toLong)
      } else if (p.length == 5) {
        // legacy rows written before readBytes column existed: readBytes = 0
        records += ScanSplitRecord(
          new String(decoder.decode(p(0)), StandardCharsets.UTF_8),
          p(1).toLong, p(2).toLong, p(3).toLong, 0L, p(4).toLong)
      }
    }
    logInfo(s"[ScanSplitAutotuner] loaded ${records.size} observations from $path")
  }

  def latestFor(tableLabel: String): Option[ScanSplitRecord] = synchronized {
    records.filter(_.tableLabel == tableLabel).lastOption
  }

  def append(record: ScanSplitRecord): Unit = synchronized {
    // Only create the parent when it isn't already a directory. Files.createDirectories throws
    // FileAlreadyExistsException when the path is a symlink to a directory (e.g. /tmp on some
    // nodes); isDirectory follows symlinks, so this guard avoids that and is a no-op when present.
    val parent = path.getParent
    if (parent != null && !Files.isDirectory(parent)) Files.createDirectories(parent)
    val line = (Seq(
      encoder.encodeToString(record.tableLabel.getBytes(StandardCharsets.UTF_8)),
      record.listedBytes, record.decodedBytes, record.decodedRows,
      record.readBytes, record.timestampMs
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
  // Absolute cap on encoded bytes read per task, so a very low ratio (decoded << listed) can't
  // collapse a table into ~1 huge task. Ratio drives the split below this.
  private val MAX_INPUT_SPLIT_BYTES = 4L * 1024L * 1024L * 1024L
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
      // Actual compressed bytes read from disk (projected columns of surviving row groups), from
      // the reader's readBufferSize metric (promoted to MODERATE). Separates projection selectivity
      // (bytesRead/listed) from decode expansion (decoded/bytesRead).
      val inputBytes = node.metrics.get("readBufferSize").map(_.value).getOrElse(0L)
      // Number of GPU batches the scan emitted; with decodedBytes gives the actual avg batch size,
      // so batch fullness vs the target can be tracked directly (not just inferred per-task).
      val numBatches = node.metrics.get(GpuMetric.NUM_OUTPUT_BATCHES).map(_.value).getOrElse(0L)
      record(label, listedBytes, decodedBytes, decodedRows, inputBytes, numBatches)
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
   *
   * Target: one decoded batch per task, `raw = batchSizeBytes / expansionRatio`. When data
   * compresses/projects (ratio < 1) `raw` exceeds batchSizeBytes, so the split legitimately needs
   * to grow past it to actually fill a batch.
   *
   * Ceiling keeps enough tasks to use the whole cluster: default core1 caps the split at
   * listedBytes/(1*minPartitionNum) so a table never runs on fewer than ~minPartitionNum (cores)
   * tasks, no matter how large `raw` gets. core<N> targets N tasks per core. A/B-selectable via
   * -Drapids.autotuner.ceiling:
   *   core<N> (core1 default) = listedBytes/(N*minPartitionNum): >= N tasks per core
   *   parcap                  = same as core1
   *   batch4g / <N>g          = flat N GiB of encoded input per task (legacy, ignores parallelism)
   *   none                    = unlimited (raw only)
   * Floor is the Spark default in the legacy g-modes (never over-split), but only 64 MiB in
   * core<N> mode, since there the point is to let the ceiling raise task count above Spark's.
   */
  def decide(
      label: String,
      listedBytes: Long,
      sparkDefault: Long,
      batchSizeBytes: Long,
      minPartitionNum: Long): Long = {
    if (store.isEmpty || label.isEmpty) return sparkDefault
    ensureListenerRegistered()
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
        // How the un-clamped target split (raw) is sized, via -Drapids.autotuner.ratioBasis:
        //   listed     (default) = batchSize / (decoded/listed): target one decoded GPU batch/task.
        //   bytesread            = batchSize / (decoded/readBytes): literal swap, divides by the
        //                          intrinsic decode_expansion, so projection is removed (splits shrink).
        //   readbudget           = readBudgetBytes / read_selectivity, read_selectivity=readBytes/listed:
        //                          size the split so each task READS ~readBudgetBytes of compressed data
        //                          (-Drapids.autotuner.readBudgetBytes, default batchSize).
        // decode_expansion / read_selectivity come from the recorded readBytes; both need DEBUG/MODERATE
        // metrics (readBufferSize) at record time, else readBytes=0 and these modes fall back to listed.
        val ratioBasis = System.getProperty("rapids.autotuner.ratioBasis", "listed")
        val readSelectivity =
          if (rec.listedBytes > 0) rec.readBytes.toDouble / rec.listedBytes else 0.0
        val decodeExpansion =
          if (rec.readBytes > 0) rec.decodedBytes.toDouble / rec.readBytes else 0.0
        val readBudgetBytes = System.getProperty(
          "rapids.autotuner.readBudgetBytes", batchSizeBytes.toString).toLong
        val raw = ratioBasis match {
          case "bytesread" if decodeExpansion > 0 =>
            (batchSizeBytes / decodeExpansion).toLong
          case "readbudget" if readSelectivity > 0 =>
            (readBudgetBytes / readSelectivity).toLong
          case _ =>
            (batchSizeBytes / expansionRatio).toLong
        }
        if (raw <= 0) {
          logWarning(s"[ScanSplitAutotuner] SKIPPED table=$label " +
            s"reason=arithmetic_overflow split_bytes=$sparkDefault")
          return sparkDefault
        }
        // Ratio-driven: split = batchSizeBytes/ratio (raw), so each task decodes to exactly one
        // batch (decoded = split*ratio = batchSizeBytes). When decoded < listed (ratio < 1) that
        // reads MORE encoded bytes per task. A/B ceiling (see decide() doc): batch4g vs parcap.
        val parallelismCap =
          if (minPartitionNum > 0) math.max(1L, listedBytes / minPartitionNum) else Long.MaxValue
        // A/B/sweep ceiling via -Drapids.autotuner.ceiling:
        //   parcap        = listedBytes/minPartitionNum (~1 task per core)
        //   core<N>       = listedBytes/(N*minPartitionNum): keep >= N tasks per core, so the
        //                   ratio-driven split can never drop task count below the cluster's
        //                   parallelism (fixes big-cluster under-parallelization / SF3000 regression)
        //   none          = unlimited (raw only)
        //   <N>g          = N GiB of encoded input (e.g. 4g, 8g, 16g)
        //   anything else = 4 GiB default
        val ceilingMode = System.getProperty("rapids.autotuner.ceiling", "core1")
        val coreOversub: Option[Long] =
          if (ceilingMode.startsWith("core") && ceilingMode.length > 4 &&
              ceilingMode.drop(4).forall(_.isDigit)) Some(math.max(1L, ceilingMode.drop(4).toLong))
          else None
        val ceiling = ceilingMode match {
          case "parcap" => parallelismCap
          case "none" => Long.MaxValue
          case _ if coreOversub.isDefined =>
            val targetTasks = math.max(1L, coreOversub.get * math.max(1L, minPartitionNum))
            math.min(MAX_INPUT_SPLIT_BYTES, math.max(1L, listedBytes / targetTasks))
          case s if s.endsWith("g") && s.length > 1 && s.dropRight(1).forall(_.isDigit) =>
            s.dropRight(1).toLong * 1024L * 1024L * 1024L
          case _ => MAX_INPUT_SPLIT_BYTES
        }
        // Floor: normally the Spark default (never regress by over-splitting). In core<N> mode the
        // whole point is to let the parallelism ceiling RAISE task count above Spark's, so the split
        // may legitimately fall below sparkDefault — floor only at the 64 MiB minimum there.
        // Floor mode via -Drapids.autotuner.floor: "default" = never-regress (max(64MiB, sparkDefault));
        // "min" = 64 MiB, so a ratio SMALLER than Spark's default split (e.g. bytesread) can actually
        // take effect instead of being clamped back up to maxPartitionBytes.
        val floorMode = System.getProperty("rapids.autotuner.floor", "default")
        val floor =
          if (floorMode == "min" || coreOversub.isDefined) MIN_SPLIT_BYTES
          else math.max(MIN_SPLIT_BYTES, sparkDefault)
        val splitBytes = math.max(floor, math.min(ceiling, raw))
        // Which constraint set the split — so we can count how often the learned ratio (raw)
        // actually drove the decision vs. the parallelism ceiling or the floor.
        val boundBy =
          if (splitBytes == raw) "ratio"
          else if (splitBytes == ceiling) "parallelism_ceiling"
          else "floor"
        // SQL execution id (thread-local on the planning thread) so ratio-usage can be bucketed
        // per query in post-processing; "na" if unavailable.
        val execId = SparkSession.getActiveSession
          .flatMap(s => Option(s.sparkContext.getLocalProperty("spark.sql.execution.id")))
          .getOrElse("na")
        val predictedDecoded = (listedBytes * expansionRatio).toLong
        logWarning(s"[ScanSplitAutotuner] DECIDED table=$label exec_id=$execId " +
          s"ratio_basis=$ratioBasis floor_mode=$floorMode listed_bytes=$listedBytes " +
          s"expansion_ratio=$expansionRatio " +
          s"read_selectivity=$readSelectivity decode_expansion=$decodeExpansion " +
          s"read_budget_bytes=$readBudgetBytes " +
          s"predicted_decoded_bytes=$predictedDecoded split_bytes=$splitBytes raw_target=$raw " +
          s"ceiling=$ceiling parallelism_cap=$parallelismCap bound_by=$boundBy " +
          s"spark_default=$sparkDefault history_count=1")
        splitBytes
    }
  }

  def record(
      label: String,
      listedBytes: Long,
      decodedBytes: Long,
      decodedRows: Long,
      inputBytes: Long = 0L,
      numBatches: Long = 0L): Unit = {
    if (store.isEmpty || label.isEmpty || listedBytes <= 0) return
    if (decodedBytes <= 0) {
      logWarning(s"[ScanSplitAutotuner] SKIPPED_RECORD table=$label " +
        s"reason=zero_decoded_bytes (set spark.rapids.sql.metrics.level=DEBUG to enable)")
      return
    }
    store.get.append(
      ScanSplitRecord(label, listedBytes, decodedBytes, decodedRows, inputBytes,
        System.currentTimeMillis()))
    // read_selectivity = actual bytes read / full file length (projection + row-group pruning).
    // decode_expansion = decoded / actual bytes read (intrinsic, projection-independent).
    val readSel = if (listedBytes > 0) inputBytes.toDouble / listedBytes else 0.0
    val decodeExp = if (inputBytes > 0) decodedBytes.toDouble / inputBytes else 0.0
    // Actual average emitted GPU batch size — batch fullness vs the target batch size.
    val avgBatchBytes = if (numBatches > 0) decodedBytes / numBatches else 0L
    logWarning(s"[ScanSplitAutotuner] RECORDED table=$label listed_bytes=$listedBytes " +
      s"decoded_bytes=$decodedBytes decoded_rows=$decodedRows input_bytes=$inputBytes " +
      s"read_selectivity=$readSel decode_expansion=$decodeExp num_batches=$numBatches " +
      s"avg_batch_bytes=$avgBatchBytes")
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
