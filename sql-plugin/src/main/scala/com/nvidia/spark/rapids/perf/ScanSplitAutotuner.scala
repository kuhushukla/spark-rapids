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
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.rapids.GpuFileSourceScanExec
import org.apache.spark.sql.util.QueryExecutionListener

private[rapids] case class ScanSplitRecord(
    tableLabel: String,
    listedBytes: Long,
    decodedBytes: Long,
    decodedRows: Long,
    readBytes: Long,
    timestampMs: Long,
    // p90 of per-task GPU memory for this scan's stage (the decision-metric family the GpuSemaphore
    // percentiles into permits/concurrency). 0 when unmeasured (legacy rows) -> peakmem-* cold-start.
    memP90Bytes: Long = 0L,
    // Usable GPU memory (RMM pool) on the executor that ran this scan, so decide() can size the
    // per-task memory budget driver-side without a GPU. 0 when unknown.
    gpuMemBudgetBytes: Long = 0L,
    // The maxSplitBytes actually used for this run, so peakmem has an exact S_prev to scale from.
    splitBytesUsed: Long = 0L,
    // Actual max concurrent GPU tasks measured for this scan's stage (dynamic; gpuMaxConcurrentGpuTasks),
    // so the per-task memory budget = usableGpuMem / concurrency uses the real value, not the config.
    concurrentTasks: Long = 0L,
    // Total GPU-semaphore-holding time (ns) summed over this scan stage's tasks (gpuTime). The gputime
    // strategy minimises gpuTimeNanos/concurrentTasks (GPU wall-time) — the only signal with an interior
    // optimum vs the split. 0 when unmeasured/legacy rows.
    gpuTimeNanos: Long = 0L,
    // Downstream shuffle-write bytes/rows for the query this scan ran in (summed over the executed plan's
    // shuffle exchanges), the expansion strategy's shuffle/explode gate. shuf_ratio = shuffleWriteBytes /
    // listedBytes; row_ratio = shuffleWriteRows / decodedRows. 0 when unmeasured (rule then grows to cap).
    shuffleWriteBytes: Long = 0L,
    shuffleWriteRows: Long = 0L,
    // GPU-buffer spill bytes (host+disk) at split_prev — the expansion spill-feedback: nonzero => the
    // previous split was too big for GPU memory => shrink. 0 when unmeasured/legacy rows.
    spillBytes: Long = 0L)

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
      val label = () => new String(decoder.decode(p(0)), StandardCharsets.UTF_8)
      if (p.length == 14) {
        // current schema: + spillBytes (expansion strategy spill-feedback)
        records += ScanSplitRecord(label(),
          p(1).toLong, p(2).toLong, p(3).toLong, p(4).toLong, p(5).toLong,
          p(6).toLong, p(7).toLong, p(8).toLong, p(9).toLong, p(10).toLong,
          p(11).toLong, p(12).toLong, p(13).toLong)
      } else if (p.length == 13) {
        // pre-spill rows: + shuffleWriteBytes, shuffleWriteRows (expansion shuffle/explode gate)
        records += ScanSplitRecord(label(),
          p(1).toLong, p(2).toLong, p(3).toLong, p(4).toLong, p(5).toLong,
          p(6).toLong, p(7).toLong, p(8).toLong, p(9).toLong, p(10).toLong,
          p(11).toLong, p(12).toLong)
      } else if (p.length == 11) {
        // pre-shuffle rows: shuffleWriteBytes/Rows = 0 (expansion grows to cap)
        records += ScanSplitRecord(label(),
          p(1).toLong, p(2).toLong, p(3).toLong, p(4).toLong, p(5).toLong,
          p(6).toLong, p(7).toLong, p(8).toLong, p(9).toLong, p(10).toLong)
      } else if (p.length == 10) {
        // pre-gpuTime rows: gpuTimeNanos = 0 (gputime cold-starts)
        records += ScanSplitRecord(label(),
          p(1).toLong, p(2).toLong, p(3).toLong, p(4).toLong, p(5).toLong,
          p(6).toLong, p(7).toLong, p(8).toLong, p(9).toLong)
      } else if (p.length == 9) {
        // pre-concurrency rows: concurrentTasks = 0 (peakmem cold-starts)
        records += ScanSplitRecord(label(),
          p(1).toLong, p(2).toLong, p(3).toLong, p(4).toLong, p(5).toLong,
          p(6).toLong, p(7).toLong, p(8).toLong)
      } else if (p.length == 6) {
        // pre-footprint rows: new fields default to 0 (peakmem-* strategies cold-start on these)
        records += ScanSplitRecord(label(),
          p(1).toLong, p(2).toLong, p(3).toLong, p(4).toLong, p(5).toLong)
      } else if (p.length == 5) {
        // legacy rows written before readBytes column existed: readBytes = 0
        records += ScanSplitRecord(label(),
          p(1).toLong, p(2).toLong, p(3).toLong, 0L, p(4).toLong)
      }
    }
    logInfo(s"[ScanSplitAutotuner] loaded ${records.size} observations from $path")
  }

  def latestFor(tableLabel: String): Option[ScanSplitRecord] = synchronized {
    records.filter(_.tableLabel == tableLabel).lastOption
  }

  /** The last up-to-2 records for this table, oldest-first — the gputime strategy needs the previous two
   * to estimate the slope of gpuTime/cc vs split (which way to move). */
  def lastTwoFor(tableLabel: String): Seq[ScanSplitRecord] = synchronized {
    records.filter(_.tableLabel == tableLabel).takeRight(2).toSeq
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
      record.readBytes, record.timestampMs,
      record.memP90Bytes, record.gpuMemBudgetBytes, record.splitBytesUsed, record.concurrentTasks,
      record.gpuTimeNanos, record.shuffleWriteBytes, record.shuffleWriteRows, record.spillBytes
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
  // Tuple: (label, listedBytes, splitBytesUsed, node).
  private val pendingScans =
    new java.util.concurrent.ConcurrentLinkedQueue[(String, Long, Long, GpuFileSourceScanExec)]()

  // Usable GPU memory = the RMM pool the GpuSemaphore itself budgets against (GpuSemaphore:282), not
  // the raw device total — so the split's memory budget matches what the system actually admits on.
  // Cached; 0 when unavailable (e.g. driver has no GPU) -> peakmem cold-starts.
  @volatile private var gpuMemBudgetCached: Long = -1L

  private def gpuMemBudget(): Long = {
    if (gpuMemBudgetCached < 0L) {
      gpuMemBudgetCached =
        try com.nvidia.spark.rapids.GpuDeviceManager.getMemorySize
        catch { case _: Throwable => 0L }
    }
    gpuMemBudgetCached
  }

  def init(path: Path): Unit = {
    store = Some(new ScanSplitStore(path))
  }

  def close(): Unit = {
    store = None
    listenerRegistered = false
    pendingScans.clear()
  }

  def registerPendingScan(
      label: String, listedBytes: Long, splitBytesUsed: Long,
      node: GpuFileSourceScanExec): Unit = {
    if (store.isDefined && label.nonEmpty) pendingScans.offer((label, listedBytes, splitBytesUsed, node))
  }

  def discardPendingScans(): Unit = pendingScans.clear()

  /** All plan nodes reachable from `p`, CROSSING AdaptiveSparkPlanExec and QueryStageExec boundaries.
   * Plain TreeNode.foreach/find stop at query-stage boundaries, so under AQE the shuffle exchanges (wrapped
   * in ShuffleQueryStageExec) and the scan (in a separate materialized input stage) are otherwise invisible
   * — which read shuffle-write as 0. Descending into a.executedPlan / q.plan makes both reachable. */
  private def descend(p: SparkPlan): Seq[SparkPlan] = {
    val inner: Seq[SparkPlan] = p match {
      case a: AdaptiveSparkPlanExec => Seq(a.executedPlan)
      case q: QueryStageExec => Seq(q.plan)
      case _ => Nil
    }
    p +: (p.children ++ inner).flatMap(descend)
  }

  /** Shuffle-write bytes/rows attributable to ONE scan: sum only the shuffle exchanges (GPU or CPU; both
   * expose Spark's shuffleBytesWritten/shuffleRecordsWritten) whose subtree (crossing query-stage
   * boundaries) contains THIS exact scan node. Per-scan by reference identity, so a multi-table join
   * attributes each side's shuffle to its own scan instead of summing the whole query onto every scan. A
   * scan with no downstream shuffle — or hidden inside a write command's command.run() — gets (0,0), and
   * expansion then grows to cap. */
  private def shuffleWriteForScan(allNodes: Seq[SparkPlan], scanNode: SparkPlan): (Long, Long) = {
    var bytes = 0L
    var rows = 0L
    allNodes.foreach { p =>
      if (p.metrics.contains("shuffleBytesWritten") && descend(p).exists(_ eq scanNode)) {
        p.metrics.get("shuffleBytesWritten").foreach(m => bytes += m.value)
        p.metrics.get("shuffleRecordsWritten").foreach(m => rows += m.value)
      }
    }
    (bytes, rows)
  }

  def drainPendingScans(qe: QueryExecution): Unit = {
    val allNodes = try descend(qe.executedPlan) catch { case _: Throwable => Seq(qe.executedPlan) }
    var entry = pendingScans.poll()
    while (entry != null) {
      val (label, listedBytes, splitBytesUsed, node) = entry
      val decodedBytes = node.metrics.get(GpuMetric.OUTPUT_BATCH_BYTES).map(_.value).getOrElse(0L)
      val decodedRows  = node.metrics.get(GpuMetric.NUM_OUTPUT_ROWS).map(_.value).getOrElse(0L)
      // Actual compressed bytes read from disk (projected columns of surviving row groups), from
      // the reader's readBufferSize metric (promoted to MODERATE). Separates projection selectivity
      // (bytesRead/listed) from decode expansion (decoded/bytesRead).
      val inputBytes = node.metrics.get("readBufferSize").map(_.value).getOrElse(0L)
      // Number of GPU batches the scan emitted; with decodedBytes gives the actual avg batch size,
      // so batch fullness vs the target can be tracked directly (not just inferred per-task).
      val numBatches = node.metrics.get(GpuMetric.NUM_OUTPUT_BATCHES).map(_.value).getOrElse(0L)
      // Decision metric: p90 of per-task GPU memory for THIS scan's stage (the family the GpuSemaphore
      // percentiles into GPU permits/concurrency). Collected by the stage-scoped TaskMemPercentileAccumulator
      // attached to the node (GpuTransitionOverrides.insertStageLevelMetrics); the autotuner picks p90.
      // Correctly per-scan; 0 when unavailable -> peakmem-* cold-start.
      val memP90 = node.getTaskMetrics.map(_.getTaskMemPercentileBytes(0.9)).getOrElse(0L)
      // Actual max concurrent GPU tasks for this scan's stage (dynamic) — the divisor for the per-task
      // memory budget (usableGpuMem / concurrency), not the concurrentGpuTasks config.
      val concurrency = node.getTaskMetrics.map(_.getMaxConcurrentGpuTasks).getOrElse(0L)
      // Usable GPU memory from the EXECUTOR (the driver has no GPU on a cluster -> gpuMemBudget()=0).
      // Fall back to the driver call only when the executor didn't report it (e.g. no scan tasks).
      val execMem = node.getTaskMetrics.map(_.getUsableGpuMemBytes).getOrElse(0L)
      val gpuMem = if (execMem > 0) execMem else gpuMemBudget()
      // Total GPU work (semaphore-holding ns) summed over this scan stage's tasks — the gputime
      // strategy's objective input (minimise gpuTime/concurrency). Recorded for all strategies.
      val gpuTimeNs = node.getTaskMetrics.map(_.getGpuTimeNanos).getOrElse(0L)
      // GPU-buffer spill (host+disk) for this scan's stage — the expansion spill-feedback safety.
      val spillBytes = node.getTaskMetrics.map(_.getGpuSpillBytes).getOrElse(0L)
      // Shuffle-write attributed to THIS scan only (exchanges whose subtree contains this scan node).
      val (shuffleWriteBytes, shuffleWriteRows) =
        try shuffleWriteForScan(allNodes, node) catch { case _: Throwable => (0L, 0L) }
      record(label, listedBytes, decodedBytes, decodedRows, inputBytes, numBatches,
        memP90, gpuMem, splitBytesUsed, concurrency, gpuTimeNs, shuffleWriteBytes, shuffleWriteRows,
        spillBytes)
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
        // Fat-row cap input (expansion strategy): array/geometry-heavy rows (high decoded_bytes/decoded_rows)
        // are compute- and working-set-heavy per row -> want smaller tasks. fatRowScale grades the GROW cap
        // down (=1 for thin rows, -> minScale for very fat rows). Computed here so DECIDED can log it.
        val bytesPerRow = if (rec.decodedRows > 0) rec.decodedBytes.toDouble / rec.decodedRows else 0.0
        val fatRowScale = {
          val refBpr = System.getProperty("rapids.autotuner.refBytesPerRow", "128").toDouble
          val minScale = System.getProperty("rapids.autotuner.minFatRowScale", "0.25").toDouble
          if (bytesPerRow > 0) math.max(minScale, math.min(1.0, refBpr / bytesPerRow)) else 1.0
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
        // Split strategy (-Drapids.autotuner.strategy). Default "ratio" = the ratio target `raw` unchanged.
        // "peakmem": grow the split to fill the per-task GPU-memory budget, but never below 1 task/core.
        //   memBudgetPerTask = usableGpuMem / concurrency        (concurrency = measured max concurrent
        //                      GPU tasks; usableGpuMem = RMM pool the GpuSemaphore budgets against)
        //   splitByMem  = S_prev * memBudgetPerTask / F_p90      (F_p90/S_prev = measured mem per byte
        //                      of split; grow until a task's p90 memory just fills its budget)
        //   splitByPar  = listedBytes / cores                    (>= 1 task per core)
        //   split       = min(splitByMem, splitByPar)            (fill memory, but keep parallelism)
        // No tuning constants — every term is measured (F_p90, S_prev, concurrency, usableGpuMem) or
        // physical (cores). Cold/missing history -> fall back to `raw`.
        val strategy = System.getProperty("rapids.autotuner.strategy", "ratio")
        val fPrev = rec.memP90Bytes
        val cores = if (minPartitionNum > 0) minPartitionNum else 1L
        // Which term set the peakmem split — "mem" (per-task memory budget) or "par" (1 task/core), or
        // "na"/"ratio" for the ratio strategy. (The 64 MiB floor is reported separately as bound_by=floor.)
        var peakmemBound = "na"
        val rawAdj = strategy match {
          case "peakmem"
              if fPrev > 0 && rec.gpuMemBudgetBytes > 0 && rec.concurrentTasks > 0 &&
                 rec.splitBytesUsed > 0 =>
            val memBudgetPerTask = rec.gpuMemBudgetBytes.toDouble / rec.concurrentTasks
            val splitByMem = (rec.splitBytesUsed.toDouble * memBudgetPerTask / fPrev).toLong
            val splitByPar = math.max(1L, listedBytes / cores)
            peakmemBound = if (splitByMem <= splitByPar) "mem" else "par"
            math.max(1L, math.min(splitByMem, splitByPar))
          // gputime: rolling hill-climb that MINIMISES J = gpuTimeNanos / concurrentTasks (GPU wall-time
          // for the scan stage) — the only measured signal with an interior optimum vs the split (memory,
          // fullness, gpuTime alone are all monotonic). Needs the last two records to estimate dJ/dSplit;
          // fixed multiplicative step (gputimeStep, default 1.4). J turns UP past the optimum, so it
          // reverses instead of running away. Run with ceiling=none, floor=min so it can explore freely.
          case "gputime" =>
            val step = System.getProperty("rapids.autotuner.gputimeStep", "1.4").toDouble
            val recsTwo = store.map(_.lastTwoFor(label)).getOrElse(Seq.empty)
            def objOk(r: ScanSplitRecord) =
              r.concurrentTasks > 0 && r.gpuTimeNanos > 0 && r.splitBytesUsed > 0
            if (recsTwo.length < 2 || !recsTwo.forall(objOk)) {
              // warm-up: nudge up from the last split to obtain a 2nd (split, J) point to climb from.
              peakmemBound = "gputime-warmup"
              val base = if (rec.splitBytesUsed > 0) rec.splitBytesUsed.toDouble else sparkDefault.toDouble
              math.max(1L, (base * step).toLong)
            } else {
              val older = recsTwo.head
              val newer = recsTwo(1)
              val j1 = older.gpuTimeNanos.toDouble / older.concurrentTasks
              val j2 = newer.gpuTimeNanos.toDouble / newer.concurrentTasks
              val dJ = j2 - j1
              val dS = (newer.splitBytesUsed - older.splitBytesUsed).toDouble
              // move to DECREASE J: grow when dJ/dS < 0 (J falls as the split grows), else shrink.
              val grow = if (dS == 0.0) true else (dJ < 0) != (dS < 0)
              peakmemBound = if (grow) "gputime-grow" else "gputime-shrink"
              val base = newer.splitBytesUsed.toDouble
              math.max(1L, (if (grow) base * step else base / step).toLong)
            }
          // expansion (VALIDATED ONE-SHOT RULE, SIZING-HEURISTIC-SPEC): closed-form, one prior run, no time.
          // expansion = max(1, decoded/listed) (materialization blow-up; listed denominator is robust to
          // column-pruned reads, unlike decode_expansion). GROW to a pool-based memory cap by default;
          // SHRINK to one batch/task when the query is shuffle-bound (shuf_ratio>1) or explode-bound
          // (row_ratio>2). Run with -Drapids.autotuner.floor=min (the cap/parCap already bound it, so
          // ceiling can be core1 or none). shuffle_write_*/decodedRows come from the recorded downstream
          // shuffle; when unplumbed/0 the rule grows to cap (documented: pv06/pv02 then mis-size).
          case "expansion" =>
            val memSafety = System.getProperty("rapids.autotuner.memSafety", "0.6").toDouble
            val expansion = math.max(1.0, expansionRatio)
            val usableGpuMem = rec.gpuMemBudgetBytes
            // Divide the pool by the GPU-semaphore concurrency TARGET (spark.rapids.sql.concurrentGpuTasks,
            // default 2) — NOT the measured cold-start concurrent_tasks (11-16 under dynamic concurrency).
            // At the target (larger) split the semaphore throttles concurrency back down toward this value,
            // so the cold-start cc over-counts memory pressure ~5.5x and undersizes falling scan-heavy
            // queries (csH, cs03). Matches the validated rule (concurrency=2); memSafety=0.6 keeps 2 tasks
            // under pool (2 x 0.3.pool = 0.6.pool < pool).
            val concurrencyGpu = SparkSession.getActiveSession
              .flatMap(s => s.conf.getOption("spark.rapids.sql.concurrentGpuTasks"))
              .flatMap(v => scala.util.Try(v.toLong).toOption).filter(_ > 0).getOrElse(2L)
            val memCap =
              if (usableGpuMem > 0)
                (memSafety * usableGpuMem / (concurrencyGpu * expansion)).toLong
              else MAX_INPUT_SPLIT_BYTES
            val parCap = math.max(1L, listedBytes / cores)
            val cap = math.min(math.min(memCap, parCap), MAX_INPUT_SPLIT_BYTES)
            val ratioSplit = math.max(1L, (batchSizeBytes / expansion).toLong)
            val shufRatio = if (rec.listedBytes > 0) rec.shuffleWriteBytes.toDouble / rec.listedBytes else 0.0
            val rowRatio = if (rec.decodedRows > 0) rec.shuffleWriteRows.toDouble / rec.decodedRows else 0.0
            // Spill feedback (memory safety, wins over the cap): if split_prev spilled GPU memory it was too
            // big -> shrink split_prev / shrinkFactor, repeats each run until spill stops.
            val spillTol = System.getProperty("rapids.autotuner.spillTolBytes", "0").toLong
            val shrinkFactor = System.getProperty("rapids.autotuner.spillShrinkFactor", "1.5").toDouble
            if (rec.spillBytes > spillTol && rec.splitBytesUsed > 0) {
              peakmemBound = "expansion-spill"
              math.max(1L, (rec.splitBytesUsed / shrinkFactor).toLong)
            } else if (shufRatio > 1.0 || rowRatio > 2.0) {
              // shrink path (shuffle-/explode-bound) — fat-row scale does NOT apply here
              peakmemBound = "expansion-shuffle"
              ratioSplit
            } else {
              // grow path — scale the cap down for fat rows (compute/array overshoot)
              peakmemBound = "expansion-cap"
              math.max(1L, (cap * fatRowScale).toLong)
            }
          case _ =>
            peakmemBound = "ratio"
            raw
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
        // rawAdj = the strategy-adjusted target (== raw for the default "ratio" strategy). The floor and
        // parallelism ceiling still clamp it, so peakmem-* never violates the parallelism guarantee.
        val splitBytes = math.max(floor, math.min(ceiling, rawAdj))
        // Which constraint set the split — so we can count how often the learned ratio (raw / the
        // strategy target) actually drove the decision vs. the parallelism ceiling or the floor.
        val boundBy =
          if (splitBytes == rawAdj) "ratio"
          else if (splitBytes == ceiling) "parallelism_ceiling"
          else "floor"
        // SQL execution id (thread-local on the planning thread) so ratio-usage can be bucketed
        // per query in post-processing; "na" if unavailable.
        val execId = SparkSession.getActiveSession
          .flatMap(s => Option(s.sparkContext.getLocalProperty("spark.sql.execution.id")))
          .getOrElse("na")
        val predictedDecoded = (listedBytes * expansionRatio).toLong
        logWarning(s"[ScanSplitAutotuner] DECIDED table=$label exec_id=$execId " +
          s"strategy=$strategy ratio_basis=$ratioBasis floor_mode=$floorMode listed_bytes=$listedBytes " +
          s"expansion_ratio=$expansionRatio " +
          s"read_selectivity=$readSelectivity decode_expansion=$decodeExpansion " +
          s"read_budget_bytes=$readBudgetBytes " +
          s"usable_gpu_mem=${rec.gpuMemBudgetBytes} concurrency_prev=${rec.concurrentTasks} " +
          s"mem_p90_prev=$fPrev gpu_time_prev=${rec.gpuTimeNanos} " +
          s"shuffle_write_bytes_prev=${rec.shuffleWriteBytes} " +
          s"shuffle_write_rows_prev=${rec.shuffleWriteRows} " +
          s"bytes_per_row=$bytesPerRow fat_row_scale=$fatRowScale " +
          s"spill_bytes_prev=${rec.spillBytes} " +
          s"split_bytes_prev=${rec.splitBytesUsed} " +
          s"predicted_decoded_bytes=$predictedDecoded split_bytes=$splitBytes raw_target=$raw " +
          s"raw_adjusted=$rawAdj peakmem_bound=$peakmemBound " +
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
      numBatches: Long = 0L,
      memP90Bytes: Long = 0L,
      gpuMemBudgetBytes: Long = 0L,
      splitBytesUsed: Long = 0L,
      concurrentTasks: Long = 0L,
      gpuTimeNanos: Long = 0L,
      shuffleWriteBytes: Long = 0L,
      shuffleWriteRows: Long = 0L,
      spillBytes: Long = 0L): Unit = {
    if (store.isEmpty || label.isEmpty || listedBytes <= 0) return
    if (decodedBytes <= 0) {
      logWarning(s"[ScanSplitAutotuner] SKIPPED_RECORD table=$label " +
        s"reason=zero_decoded_bytes (set spark.rapids.sql.metrics.level=DEBUG to enable)")
      return
    }
    store.get.append(
      ScanSplitRecord(label, listedBytes, decodedBytes, decodedRows, inputBytes,
        System.currentTimeMillis(), memP90Bytes, gpuMemBudgetBytes, splitBytesUsed, concurrentTasks,
        gpuTimeNanos, shuffleWriteBytes, shuffleWriteRows, spillBytes))
    // read_selectivity = actual bytes read / full file length (projection + row-group pruning).
    // decode_expansion = decoded / actual bytes read (intrinsic, projection-independent).
    val readSel = if (listedBytes > 0) inputBytes.toDouble / listedBytes else 0.0
    val decodeExp = if (inputBytes > 0) decodedBytes.toDouble / inputBytes else 0.0
    // Actual average emitted GPU batch size — batch fullness vs the target batch size.
    val avgBatchBytes = if (numBatches > 0) decodedBytes / numBatches else 0L
    logWarning(s"[ScanSplitAutotuner] RECORDED table=$label listed_bytes=$listedBytes " +
      s"decoded_bytes=$decodedBytes decoded_rows=$decodedRows input_bytes=$inputBytes " +
      s"read_selectivity=$readSel decode_expansion=$decodeExp num_batches=$numBatches " +
      s"avg_batch_bytes=$avgBatchBytes mem_p90_bytes=$memP90Bytes " +
      s"gpu_mem_budget_bytes=$gpuMemBudgetBytes split_bytes_used=$splitBytesUsed " +
      s"concurrent_tasks=$concurrentTasks gpu_time_ns=$gpuTimeNanos " +
      s"shuffle_write_bytes=$shuffleWriteBytes shuffle_write_rows=$shuffleWriteRows " +
      s"spill_bytes=$spillBytes")
  }
}

/**
 * Collects scan metrics after each successful query and forwards them to ScanSplitAutotuner.
 * onFailure is a no-op so failed/cancelled queries never contaminate history.
 */
private[rapids] class ScanObservationListener extends QueryExecutionListener {
  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    // Scan nodes were registered at planning time via registerPendingScan; read their
    // post-execution metrics here. Plan traversal cannot be used to find the SCANS (write commands
    // (CommandResultExec) have empty physical children — the scan runs inside command.run()), but the
    // plan IS walked for downstream shuffle-write totals (visible for read queries).
    ScanSplitAutotuner.drainPendingScans(qe)
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    ScanSplitAutotuner.discardPendingScans()
  }
}
