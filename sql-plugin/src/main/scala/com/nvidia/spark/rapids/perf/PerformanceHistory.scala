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

/**
 * Exact reuse key for an observed performance context.
 *
 * The read client details remain part of the key even though the POC models reads with
 * effective throughput. A serial blocking client is not interchangeable with an async client.
 */
private[rapids] case class PerformanceContext(
    instanceType: String,
    gpuName: String,
    gpuMemoryBytes: Long,
    cpuCores: Int,
    sparkVersion: String,
    rapidsVersion: String,
    javaVersion: String,
    connectorId: String,
    tableId: String,
    tableVersion: String,
    fileFormat: String,
    codec: String,
    schemaFingerprint: String,
    executionFingerprint: String,
    projectionFingerprint: String,
    partitionPredicateFingerprint: String,
    dataPredicateFingerprint: String,
    fileSystemScheme: String,
    storageContext: String,
    clientKind: String,
    asynchronousRead: Boolean,
    readerThreads: Int,
    maxOutstandingRequests: Int,
    readAheadEnabled: Boolean,
    rangeMergeGapBytes: Long,
    throttlePolicyId: String,
    cacheState: String,
    downstreamFingerprint: String,
    downstreamUnit: String)

/**
 * Service times are summed demand before overlap. sumSqlFilterNs is SQL GpuFilter
 * service only, not Parquet footer/row-group pruning. observedGpuOverlapCapacity is
 * derived from the union of these same host service intervals, never from holder count
 * or CUDA-kernel overlap.
 */
private[rapids] case class PerformanceObservation(
    context: PerformanceContext,
    observedAtMs: Long,
    compressedReadBytes: Long,
    decodedBytes: Long,
    decodedRows: Long,
    survivorBytes: Long,
    survivorRows: Long,
    downstreamWorkUnits: Long,
    outputBatches: Long,
    maxBatchBytes: Long,
    usefulTasks: Int,
    effectiveReadParallelism: Double,
    observedGpuOverlapCapacity: Double,
    maxTaskServiceNs: Long,
    sumReadNs: Long,
    sumDecodeNs: Long,
    sumSqlFilterNs: Long,
    sumDownstreamNs: Long,
    scanStageWallNs: Long,
    queryWallNs: Long,
    maxGpuHolders: Int,
    maxTaskFootprintBytes: Long,
    kernelServiceNs: Option[Long] = None,
    kernelBusyNs: Option[Long] = None)

/**
 * predictedGpuOverlapCapacity uses the same host-service domain as the observation.
 * effectiveReadParallelism applies to per-lane summed read-call service.
 */
private[rapids] case class PredictionRequest(
    context: PerformanceContext,
    compressedReadBytes: Long,
    decodedBytes: Long,
    decodedRows: Long,
    survivorBytes: Long,
    survivorRows: Long,
    downstreamWorkUnits: Long,
    outputBatches: Long,
    maxBatchBytes: Long,
    usefulTasks: Int,
    effectiveReadParallelism: Double,
    predictedGpuOverlapCapacity: Double,
    maxTaskFootprintBytes: Long,
    admissionBudgetBytes: Long)

private[rapids] case class ComponentPrediction(
    readServiceNs: Long,
    decodeServiceNs: Long,
    filterServiceNs: Long,
    downstreamServiceNs: Long,
    longestTaskLowerBoundNs: Long)

private[rapids] case class PerformancePrediction(
    stageWallNs: Long,
    queryWallNs: Long,
    components: ComponentPrediction,
    sampleCount: Int,
    sizeBucket: Int,
    matchQuality: String,
    predictedGpuCapacity: Double,
    memoryStatus: String,
    p10Scale: Option[Double],
    p90Scale: Option[Double])

private[rapids] trait PerformanceHistory extends AutoCloseable {
  def record(observation: PerformanceObservation): Unit
  def predict(request: PredictionRequest): Option[PerformancePrediction]
  def observations(context: PerformanceContext): Seq[PerformanceObservation]
  def flush(): Unit
}

private[rapids] object PerformanceHistory {
  def local(path: Path): PerformanceHistory = new LocalFilePerformanceHistory(path)
}

/**
 * A deliberately small, versioned line protocol for the POC. String fields are URL-safe
 * base64 and the remaining fields are decimal values. Persistence is hidden by
 * PerformanceHistory, so this is not a public or durable integration contract.
 */
private object ObservationCodec {
  private val Version = "v3"
  private val encoder = Base64.getUrlEncoder.withoutPadding()
  private val decoder = Base64.getUrlDecoder

  private def encode(value: String): String =
    encoder.encodeToString(value.getBytes(StandardCharsets.UTF_8))

  private def decode(value: String): String =
    new String(decoder.decode(value), StandardCharsets.UTF_8)

  def serialize(observation: PerformanceObservation): String = {
    val c = observation.context
    Seq(
      Version,
      encode(c.instanceType), encode(c.gpuName), c.gpuMemoryBytes, c.cpuCores,
      encode(c.sparkVersion), encode(c.rapidsVersion), encode(c.javaVersion),
      encode(c.connectorId), encode(c.tableId), encode(c.tableVersion),
      encode(c.fileFormat), encode(c.codec), encode(c.schemaFingerprint),
      encode(c.executionFingerprint), encode(c.projectionFingerprint), encode(c.partitionPredicateFingerprint),
      encode(c.dataPredicateFingerprint), encode(c.fileSystemScheme), encode(c.storageContext),
      encode(c.clientKind), c.asynchronousRead, c.readerThreads, c.maxOutstandingRequests,
      c.readAheadEnabled, c.rangeMergeGapBytes, encode(c.throttlePolicyId),
      encode(c.cacheState), encode(c.downstreamFingerprint), encode(c.downstreamUnit),
      observation.observedAtMs, observation.compressedReadBytes, observation.decodedBytes,
      observation.decodedRows, observation.survivorBytes, observation.survivorRows,
      observation.downstreamWorkUnits, observation.outputBatches, observation.maxBatchBytes,
      observation.usefulTasks, observation.effectiveReadParallelism,
      observation.observedGpuOverlapCapacity, observation.maxTaskServiceNs, observation.sumReadNs, observation.sumDecodeNs,
      observation.sumSqlFilterNs,
      observation.sumDownstreamNs, observation.scanStageWallNs, observation.queryWallNs,
      observation.maxGpuHolders, observation.maxTaskFootprintBytes,
      observation.kernelServiceNs.map(_.toString).getOrElse(""),
      observation.kernelBusyNs.map(_.toString).getOrElse("")
    ).mkString("\t")
  }

  def deserialize(line: String): PerformanceObservation = {
    val values = line.split("\t", -1).iterator
    def next(): String = {
      require(values.hasNext, "truncated performance history record")
      values.next()
    }
    require(next() == Version, "unsupported performance history schema")
    val context = PerformanceContext(
      decode(next()), decode(next()), next().toLong, next().toInt,
      decode(next()), decode(next()), decode(next()), decode(next()),
      decode(next()), decode(next()), decode(next()), decode(next()),
      decode(next()), decode(next()), decode(next()), decode(next()), decode(next()),
      decode(next()), decode(next()), decode(next()), next().toBoolean,
      next().toInt, next().toInt, next().toBoolean, next().toLong,
      decode(next()), decode(next()), decode(next()), decode(next()))
    def optionalLong(value: String): Option[Long] =
      if (value.isEmpty) None else Some(value.toLong)
    val observation = PerformanceObservation(
      context, next().toLong, next().toLong, next().toLong, next().toLong,
      next().toLong, next().toLong, next().toLong, next().toLong, next().toLong,
      next().toInt, next().toDouble, next().toDouble, next().toLong, next().toLong,
      next().toLong,
      next().toLong, next().toLong, next().toLong, next().toLong, next().toInt,
      next().toLong, optionalLong(next()), optionalLong(next()))
    require(!values.hasNext, "unexpected trailing performance history fields")
    observation
  }
}

private final class LocalFilePerformanceHistory(path: Path) extends PerformanceHistory {
  private val records = ArrayBuffer[PerformanceObservation]()
  private var closed = false

  if (path.getParent != null) {
    Files.createDirectories(path.getParent)
  }
  if (Files.exists(path)) {
    val contents = new String(Files.readAllBytes(path), StandardCharsets.UTF_8)
    val completeLines = contents.split("\n", -1)
    val lastCompleteIndex = if (contents.endsWith("\n")) completeLines.length else {
      completeLines.length - 1
    }
    completeLines.take(lastCompleteIndex).filter(_.trim.nonEmpty).foreach { line =>
      records += ObservationCodec.deserialize(line)
    }
  }

  override def record(observation: PerformanceObservation): Unit = synchronized {
    require(!closed, "performance history is closed")
    val bytes = (ObservationCodec.serialize(observation) + "\n")
      .getBytes(StandardCharsets.UTF_8)
    val channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
      StandardOpenOption.APPEND)
    try {
      val lock = channel.lock()
      try {
        val buffer = ByteBuffer.wrap(bytes)
        while (buffer.hasRemaining) {
          channel.write(buffer)
        }
        channel.force(true)
      } finally {
        lock.release()
      }
    } finally {
      channel.close()
    }
    records += observation
  }

  override def observations(context: PerformanceContext): Seq[PerformanceObservation] =
    synchronized {
      require(!closed, "performance history is closed")
      records.filter(_.context == context).toSeq
    }

  override def predict(request: PredictionRequest): Option[PerformancePrediction] =
    synchronized {
      require(!closed, "performance history is closed")
      val exact = records.filter(_.context == request.context)
      if (exact.isEmpty) {
        None
      } else {
        val bucket = sizeBucket(typicalBatchBytes(request.decodedBytes, request.outputBatches))
        val nearSize = exact.filter { observation =>
          val observedTypical = typicalBatchBytes(observation.decodedBytes,
            observation.outputBatches)
          Math.abs(sizeBucket(observedTypical) - bucket) <= 1
        }
        if (nearSize.isEmpty) {
          None
        } else {
          Some(Predictor.predict(request, nearSize.toSeq, bucket,
            "exact-key-size-bucket"))
        }
      }
    }

  override def flush(): Unit = synchronized {
    require(!closed, "performance history is closed")
    // record() forces every append in this intentionally simple POC.
  }

  override def close(): Unit = synchronized {
    closed = true
  }

  private def typicalBatchBytes(decodedBytes: Long, outputBatches: Long): Long =
    if (outputBatches <= 0) decodedBytes else decodedBytes / outputBatches

  private def sizeBucket(bytes: Long): Int =
    if (bytes <= 0) 0 else 63 - java.lang.Long.numberOfLeadingZeros(bytes)
}

private object Predictor {
  private def median(values: Seq[Double]): Double = {
    val sorted = values.sorted
    val middle = sorted.length / 2
    if (sorted.length % 2 == 0) (sorted(middle - 1) + sorted(middle)) / 2.0
    else sorted(middle)
  }

  private def percentile(values: Seq[Double], fraction: Double): Double = {
    val sorted = values.sorted
    sorted(Math.round(fraction * (sorted.length - 1)).toInt)
  }

  private def service(
      target: Long,
      observations: Seq[PerformanceObservation],
      units: PerformanceObservation => Long,
      time: PerformanceObservation => Long): Long = {
    val rates = observations.flatMap { observation =>
      val count = units(observation)
      val duration = time(observation)
      if (count > 0 && duration > 0) Some(count.toDouble / duration) else None
    }
    if (target <= 0 || rates.isEmpty) 0L else Math.round(target / median(rates))
  }

  def predict(
      request: PredictionRequest,
      observations: Seq[PerformanceObservation],
      bucket: Int,
      matchQuality: String): PerformancePrediction = {
    require(request.usefulTasks > 0, "useful tasks must be positive")
    require(java.lang.Double.isFinite(request.effectiveReadParallelism) &&
      request.effectiveReadParallelism > 0, "read parallelism must be finite and positive")
    require(java.lang.Double.isFinite(request.predictedGpuOverlapCapacity) &&
      request.predictedGpuOverlapCapacity > 0, "GPU capacity must be finite and positive")
    val readNs = service(request.compressedReadBytes, observations,
      _.compressedReadBytes, _.sumReadNs)
    val decodeNs = service(request.decodedBytes, observations, _.decodedBytes, _.sumDecodeNs)
    val filterNs = service(request.decodedRows, observations, _.decodedRows, _.sumSqlFilterNs)
    val downstreamNs = service(request.downstreamWorkUnits, observations,
      _.downstreamWorkUnits, _.sumDownstreamNs)
    val totalServiceNs = readNs + decodeNs + filterNs + downstreamNs

    val footprintCapacity =
      if (request.maxTaskFootprintBytes > 0 && request.admissionBudgetBytes > 0) {
        Math.floor(request.admissionBudgetBytes.toDouble / request.maxTaskFootprintBytes)
      } else {
        request.predictedGpuOverlapCapacity
      }
    val gpuCapacity = Math.max(1.0, Math.min(request.predictedGpuOverlapCapacity,
      Math.min(request.usefulTasks.toDouble, footprintCapacity)))
    val memoryStatus =
      if (request.maxTaskFootprintBytes <= 0 || request.admissionBudgetBytes <= 0) "unknown"
      else if (request.maxTaskFootprintBytes <= request.admissionBudgetBytes) "safe"
      else "unsafe"
    val readWall = readNs / request.effectiveReadParallelism
    val gpuWall = (decodeNs + filterNs + downstreamNs) / gpuCapacity
    val longestFractions = observations.flatMap { observation =>
      val total = observation.sumReadNs + observation.sumDecodeNs +
        observation.sumSqlFilterNs + observation.sumDownstreamNs
      if (total > 0 && observation.maxTaskServiceNs > 0) {
        Some(observation.maxTaskServiceNs.toDouble / total)
      } else {
        None
      }
    }
    val averageTask = Math.round(totalServiceNs.toDouble / request.usefulTasks)
    val longestTask = if (longestFractions.nonEmpty) {
      Math.max(averageTask, Math.round(totalServiceNs * median(longestFractions)))
    } else {
      averageTask
    }
    val rawBound = Math.max(Math.max(readWall, gpuWall), longestTask)

    val residualScales = observations.flatMap { observation =>
      val readCapacity = Math.max(1.0, observation.effectiveReadParallelism)
      val gpuCapacity = Math.max(1.0, observation.observedGpuOverlapCapacity)
      val historicalRead = observation.sumReadNs / readCapacity
      val historicalGpu = (observation.sumDecodeNs + observation.sumSqlFilterNs +
        observation.sumDownstreamNs) / gpuCapacity
      val historicalLongest = observation.maxTaskServiceNs.toDouble
      val historicalBound = Math.max(Math.max(historicalRead, historicalGpu),
        historicalLongest)
      if (historicalBound > 0) Some(observation.scanStageWallNs / historicalBound) else None
    }
    val scale = if (residualScales.nonEmpty) median(residualScales) else 1.0
    val stageNs = Math.round(rawBound * scale)
    val tails = observations.map { observation =>
      Math.max(0L, observation.queryWallNs - observation.scanStageWallNs).toDouble
    }
    val tailNs = if (tails.nonEmpty) Math.round(median(tails)) else 0L

    PerformancePrediction(
      stageNs,
      Math.addExact(stageNs, tailNs),
      ComponentPrediction(readNs, decodeNs, filterNs, downstreamNs, longestTask),
      observations.length,
      bucket,
      if (observations.length >= 5) matchQuality else matchQuality + "-low-sample",
      gpuCapacity,
      memoryStatus,
      if (residualScales.length >= 5) {
        Some(percentile(residualScales, 0.10) / scale)
      } else {
        None
      },
      if (residualScales.length >= 5) {
        Some(percentile(residualScales, 0.90) / scale)
      } else {
        None
      })
  }
}
