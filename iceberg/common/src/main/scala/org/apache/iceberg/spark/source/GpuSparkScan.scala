/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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

package org.apache.iceberg.spark.source

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.iceberg.{IcebergFormatVersionSupport, ShimUtils}
import com.nvidia.spark.rapids.perf.{ScanContext, ScanSplitHeuristic}
import org.apache.iceberg.ScanTaskGroup
import org.apache.iceberg.spark.GpuSparkReadConf
import org.apache.iceberg.types.Types

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator
import org.apache.spark.sql.connector.metric.{CustomMetric, CustomTaskMetric}
import org.apache.spark.sql.connector.read.{Batch, Scan, Statistics, SupportsReportStatistics}
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.types.StructType


abstract class GpuSparkScan(val cpuScan: Scan,
    val rapidsConf: RapidsConf,
    val queryUsesInputFile: Boolean,
) extends GpuScan with SupportsReportStatistics with Logging {
  private val readConf: GpuSparkReadConf = new GpuSparkReadConf(
    GpuSparkScanAccess.readConf(cpuScan))

  def readTimestampWithoutZone: Boolean = readConf.handleTimestampWithoutZone()

  override def readSchema(): StructType = cpuScan.readSchema()

  override def estimateStatistics(): Statistics = GpuSparkScanAccess.estimateStatistics(cpuScan)

  /**
   * Iceberg's fully-qualified table name, matching the identity the split decision was keyed on in
   * `RapidsSparkTable`. Metadata tables never participate: their decode cost is not a property of
   * the user's data.
   */
  override def historyTable: Option[String] = {
    if (GpuSparkScanAccess.isMetadataScan(cpuScan)) {
      None
    } else {
      Try(GpuSparkScanAccess.table(cpuScan).name()) match {
        case Success(name) if name != null && name.nonEmpty => Some(name)
        case _ => None
      }
    }
  }

  /**
   * On-disk bytes this scan reads, summed over the planned task groups so it already reflects
   * pruning. This is the same quantity the v1 path records (`files.map(_.getLen).sum`), which is
   * what makes `decoded / listed` a real expansion factor.
   *
   * `estimateStatistics().sizeInBytes()` was used here before. Iceberg reports that as an
   * uncompressed estimate - about 15x the on-disk size on the netflix tables - so the ratio came
   * out near 1.0 for every table whatever its compression, and `batchSizeBytes / ratio` collapsed
   * to roughly `batchSizeBytes` for all of them.
   */
  override def historyListedBytes: Long =
    Try(GpuSparkScanAccess.taskGroups(cpuScan).asScala.map(_.sizeBytes()).sum).getOrElse(0L)

  /**
   * Registers this scan so its decode-expansion ratio is recorded when the query ends.
   *
   * Registration happens here rather than in the exec because every shim's `inputRDD` forces
   * `toBatch` through its `lazy val batch`, while several shims (spark340 onward, which is what
   * Spark 4.0.2 resolves to) override `inputRDD` without calling super - a hook in the exec base
   * would silently never run on them.
   *
   * The decision itself is made in `RapidsSparkTable`, before any scan object exists, so only the
   * observation belongs here. Both sides key on the table alone: that is all the decision site
   * has, since columns and filters are pushed down after it runs.
   *
   * `metrics` is a var the exec assigns; closing over it rather than reading it now means the
   * value is picked up at drain time, whatever order assignment and this call happen in.
   */
  /**
   * Splits this scan planned, and splits it actually read.
   *
   * The accumulator is created on the driver and handed to `GpuReaderFactory`, which increments it
   * once per partition on the executors, so at query end the two numbers say whether the scan
   * finished. Without that, an abandoned scan contributes a fraction of the decoded bytes over the
   * whole of the listed bytes and poisons the learned ratio - see `ScanSplitHeuristic.observe`.
   */
  @transient lazy val splitsRead: LongAccumulator = {
    val acc = new LongAccumulator
    SparkContext.getOrCreate().register(acc, "gpuScanSplitsRead")
    acc
  }

  private def plannedSplits: Long =
    Try(GpuSparkScanAccess.taskGroups(cpuScan).size().toLong).getOrElse(0L)

  private def registerHistoryObservation(): Unit = {
    // DIAGNOSTIC (2026-10-01). Job 16 decides `no history` forever: a GPU scan runs, nothing is
    // ever recorded, and no existing log fires. Every skip on this path is silent by design -
    // historyTable is an Option, historyListedBytes swallows exceptions via getOrElse(0L), and a
    // missing executionId drops the registration - so there is no way to tell which one happened.
    // These lines name the branch taken. Remove once the cause is fixed.
    if (historyTable.isEmpty) {
      logWarning(s"scan.split/diag: no historyTable (metadataScan=" +
        s"${Try(GpuSparkScanAccess.isMetadataScan(cpuScan)).getOrElse("threw")}, " +
        s"name=${Try(GpuSparkScanAccess.table(cpuScan).name()).getOrElse("threw")}), " +
        s"scanClass=${cpuScan.getClass.getName}")
    }
    historyTable.foreach { table =>
      val listed = historyListedBytes
      val groupsTry = Try(GpuSparkScanAccess.taskGroups(cpuScan).size())
      logWarning(s"scan.split/diag: table=$table listed=$listed " +
        s"taskGroups=${groupsTry.map(_.toString).getOrElse("THREW: " + groupsTry.failed.get)} " +
        s"scanClass=${cpuScan.getClass.getName} " +
        s"executionId=${SparkSession.getActiveSession
          .flatMap(s => Option(s.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)))
          .getOrElse("NONE")}")
      if (listed > 0L) {
        val ctx = ScanContext(
          table = table,
          columns = Nil,
          filters = Nil,
          listedBytes = listed,
          // Decision-only inputs; this context never reaches `decide`.
          batchSizeBytes = 0L,
          minPartitionNum = 0L,
          maxSplitBytes = 0L,
          decodedBytes = () =>
            metrics.get(GpuMetric.GPU_OUTPUT_BATCH_BYTES).map(_.value).getOrElse(0L),
          plannedSplits = plannedSplits,
          completedSplits = () => splitsRead.value)
        // Scoped to this SQL execution so only its own end can drain it. A scan planned outside
        // an execution has no id and is not tracked, because nothing would ever drain it.
        val executionId = SparkSession.getActiveSession
          .flatMap(s => Option(s.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)))
          .flatMap(id => try Some(id.toLong) catch { case _: NumberFormatException => None })
        ScanSplitHeuristic.register(executionId, ctx)
      }
    }
  }

  override def toBatch: Batch = {
    registerHistoryObservation()
    new GpuSparkBatch(GpuSparkScanAccess.toBatch(cpuScan), this)
  }

  override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream =
    throw new UnsupportedOperationException(
      "GpuSparkScan does not support toMicroBatchStream()")

  override def description(): String = cpuScan.description()

  override def reportDriverMetrics(): Array[CustomTaskMetric] = cpuScan.reportDriverMetrics()

  override def supportedCustomMetrics(): Array[CustomMetric] = cpuScan.supportedCustomMetrics()

  protected def groupingKeyType(): Types.StructType

  protected def taskGroups(): Seq[_ <: ScanTaskGroup[_]]

  def hasNestedType: Boolean = {
    GpuSparkScanAccess.expectedSchema(cpuScan)
      .asStruct()
      .fields()
      .asScala
      .exists { field => field.`type`().isNestedType }
  }
}


object GpuSparkScan {
  def isMetadataScan(scan: Scan): Boolean = {
    GpuSparkScanAccess.isMetadataScan(scan)
  }

  def tryConvert(cpuScan: Scan, rapidsConf: RapidsConf): Try[GpuSparkScan] = {
    Try {
      if (GpuSparkScanAccess.isBatchQueryScan(cpuScan)) {
        new GpuSparkBatchQueryScan(cpuScan, rapidsConf, false)
      } else if (GpuSparkScanAccess.isCopyOnWriteScan(cpuScan)) {
        ShimUtils.newCopyOnWriteScan(cpuScan, rapidsConf, false)
      } else {
        throw new IllegalArgumentException(
          s"Currently iceberg support only supports batch query scan and copy-on-write scan, " +
            s"but got ${cpuScan.getClass.getName}")
      }
    }
  }

  def tagForGpu(meta: ScanMeta[Scan], gpuScan: Try[GpuSparkScan]): Unit = {
    if (!meta.conf.isIcebergEnabled) {
      meta.willNotWorkOnGpu("Iceberg input and output has been disabled. To enable set " +
        s"${RapidsConf.ENABLE_ICEBERG} to true")
    }

    if (!meta.conf.isIcebergReadEnabled) {
      meta.willNotWorkOnGpu("Iceberg input has been disabled. To enable set " +
        s"${RapidsConf.ENABLE_ICEBERG_READ} to true")
    }

    Try {
      IcebergFormatVersionSupport.tagForFormatVersion(
        GpuSparkScanAccess.table(meta.wrapped), meta)
    } match {
      case Failure(e) => meta.willNotWorkOnGpu(s"error examining Iceberg table version: $e")
      case _ =>
    }

    FileFormatChecks.tag(meta, meta.wrapped.readSchema(), IcebergFormatType, ReadFileOp)

    Try {
      GpuSparkScan.isMetadataScan(meta.wrapped)
    } match {
      case Success(true) => meta.willNotWorkOnGpu("scan is a metadata scan")
      case Failure(e) => meta.willNotWorkOnGpu(s"error examining CPU Iceberg scan: $e")
      case _ =>
    }

    gpuScan match {
      case Success(_) =>
        // Nested types (map, list, struct) are now supported
      case Failure(e) => meta.willNotWorkOnGpu(s"conversion to GPU scan failed: ${e.getMessage}")
    }
  }
}
