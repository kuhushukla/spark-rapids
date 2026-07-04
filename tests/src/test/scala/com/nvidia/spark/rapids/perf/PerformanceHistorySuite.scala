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

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, StandardOpenOption}

import org.scalatest.funsuite.AnyFunSuite

class PerformanceHistorySuite extends AnyFunSuite {
  private def context(
      tableVersion: String = "2009",
      tableId: String = "taxi",
      schemaFingerprint: String = "schema-sha256",
      projectionFingerprint: String = "projection-sha256",
      partitionPredicateFingerprint: String = "partition-predicate-literal-a",
      dataPredicateFingerprint: String = "data-predicate-literal-a",
      partitionPredicateShapeFingerprint: String = "partition-predicate-shape",
      dataPredicateShapeFingerprint: String = "data-predicate-shape",
      instanceType: String = "g7.4xlarge",
      gpuName: String = "NVIDIA L4"): PerformanceContext =
    PerformanceContext(
      instanceType,
      gpuName,
      24L * 1024 * 1024 * 1024,
      16,
      "3.5.5",
      "26.08.0",
      "17",
      "hadoop-s3a-v1",
      tableId,
      tableVersion,
      "parquet",
      "snappy",
      schemaFingerprint,
      "reader-and-admission-config-sha256",
      projectionFingerprint,
      partitionPredicateFingerprint,
      dataPredicateFingerprint,
      partitionPredicateShapeFingerprint,
      dataPredicateShapeFingerprint,
      "s3a",
      "bucket=example,region=us-east-1",
      "s3a-async",
      asynchronousRead = true,
      readerThreads = 16,
      maxOutstandingRequests = 32,
      readAheadEnabled = true,
      rangeMergeGapBytes = 1024 * 1024,
      "memory-pressure-v1",
      "warm-plugin-cache",
      "gpu-hash-aggregate-sha256",
      "survivor-bytes")

  private def observation(ctx: PerformanceContext): PerformanceObservation =
    PerformanceObservation(
      ctx,
      1L,
      compressedReadBytes = 1000,
      decodedBytes = 2000,
      decodedRows = 100,
      survivorBytes = 500,
      survivorRows = 25,
      downstreamWorkUnits = 500,
      outputBatches = 4,
      maxBatchBytes = 512L * 1024 * 1024,
      usefulTasks = 4,
      launchedTasks = 4,
      effectiveTaskParallelism = 2.0,
      effectiveReadParallelism = 2.0,
      observedGpuOverlapCapacity = 2.0,
      maxTaskServiceNs = 200,
      sumTaskFixedNs = 80,
      sumReadNs = 100,
      sumDecodeNs = 200,
      sumSqlFilterNs = 50,
      sumDownstreamNs = 100,
      scanStageWallNs = 350,
      queryWallNs = 400,
      maxGpuHolders = 2,
      maxTaskFootprintBytes = 1024,
      kernelServiceNs = Some(75),
      kernelBusyNs = Some(60))

  test("local history round trips observations behind the high-level API") {
    val directory = Files.createTempDirectory("performance-history")
    val path = directory.resolve("history.log")
    val expected = observation(context())

    val history = PerformanceHistory.local(path)
    history.record(expected)
    history.close()

    val reopened = PerformanceHistory.local(path)
    assert(reopened.observations(context()) === Seq(expected))
    reopened.close()
  }

  test("legacy end-to-end prediction still requires an exact context") {
    val path = Files.createTempDirectory("performance-history-key").resolve("history.log")
    val history = PerformanceHistory.local(path)
    history.record(observation(context()))

    val differentTableVersion = PredictionRequest(
      context("2010"), 2000, 4000, 200, 1000, 50, 1000, 8,
      1024L * 1024 * 1024, 4, 8, 2.0, 2.0, 2.0, 400, 2048, 4096)
    assert(history.predict(differentTableVersion).isEmpty)
    history.close()
  }

  test("data-shape prediction transfers across snapshots and hardware") {
    val path = Files.createTempDirectory("performance-history-data-shape")
      .resolve("history.log")
    val history = PerformanceHistory.local(path)
    history.record(observation(context()))

    val requestContext = context(
      tableVersion = "2010",
      partitionPredicateFingerprint = "partition-predicate-literal-b",
      dataPredicateFingerprint = "data-predicate-literal-b",
      instanceType = "different-instance",
      gpuName = "different-gpu")
    val prediction = history.predictDataShape(DataShapeRequest(requestContext, 3000)).get

    assert(prediction.decodedBytes === 6000)
    assert(prediction.decodedRows === 300)
    assert(prediction.empiricalUpperDecodedBytes === 6000)
    assert(prediction.empiricalUpperDecodedRows === 300)
    assert(prediction.sampleCount === 1)
    assert(prediction.evidenceLevel === "same-table-compatible-snapshot")
    history.close()
  }

  test("data-shape prediction transfers to another table with a compatible shape") {
    val path = Files.createTempDirectory("performance-history-cross-table")
      .resolve("history.log")
    val history = PerformanceHistory.local(path)
    history.record(observation(context()))

    val requestContext = context(tableId = "another-table", tableVersion = "v1")
    val prediction = history.predictDataShape(DataShapeRequest(requestContext, 500)).get

    assert(prediction.decodedBytes === 1000)
    assert(prediction.decodedRows === 50)
    assert(prediction.evidenceLevel === "cross-table-compatible-shape")
    assert(history.predictDataShape(DataShapeRequest(
      requestContext.copy(schemaFingerprint = "incompatible-schema"), 500)).isEmpty)
    history.close()
  }

  test("footprint prediction uses recorded task footprint instead of table identity") {
    val path = Files.createTempDirectory("performance-history-footprint")
      .resolve("history.log")
    val history = PerformanceHistory.local(path)
    history.record(observation(context()))

    val sameShape = history.predictFootprint(FootprintRequest(
      context(tableVersion = "2010"), 1024L * 1024 * 1024)).get
    assert(sameShape.taskFootprintBytes === 2048)
    assert(sameShape.empiricalUpperTaskFootprintBytes === 2048)
    assert(sameShape.evidenceLevel === "same-reader-and-shape")

    val changedShape = history.predictFootprint(FootprintRequest(
      context(
        tableId = "another-table",
        schemaFingerprint = "new-schema",
        projectionFingerprint = "new-projection"),
      1024L * 1024 * 1024)).get
    assert(changedShape.taskFootprintBytes === 2048)
    assert(changedShape.evidenceLevel === "same-reader-transferable-batch")
    history.close()
  }

  test("prediction separates service demand from overlap and fixed tail") {
    val path = Files.createTempDirectory("performance-history-predict").resolve("history.log")
    val history = PerformanceHistory.local(path)
    history.record(observation(context()))

    val request = PredictionRequest(
      context(),
      compressedReadBytes = 2000,
      decodedBytes = 4000,
      decodedRows = 200,
      survivorBytes = 1000,
      survivorRows = 50,
      downstreamWorkUnits = 1000,
      outputBatches = 8,
      maxBatchBytes = 1024L * 1024 * 1024,
      usefulTasks = 4,
      launchedTasks = 8,
      effectiveTaskParallelism = 2.0,
      effectiveReadParallelism = 2.0,
      predictedGpuOverlapCapacity = 2.0,
      predictedMaxTaskServiceNs = 400,
      maxTaskFootprintBytes = 2048,
      admissionBudgetBytes = 4096)
    val prediction = history.predict(request).get

    assert(prediction.components.taskFixedServiceNs === 160)
    assert(prediction.components.readServiceNs === 200)
    assert(prediction.components.decodeServiceNs === 400)
    assert(prediction.components.filterServiceNs === 100)
    assert(prediction.components.downstreamServiceNs === 200)
    assert(prediction.components.longestTaskLowerBoundNs === 400)
    assert(prediction.stageWallNs === 700)
    assert(prediction.queryWallNs === 750)
    assert(prediction.predictedGpuCapacity === 2.0)
    assert(prediction.memoryStatus === "safe")
    assert(prediction.p10Scale.isEmpty)
    assert(prediction.p90Scale.isEmpty)
    assert(prediction.matchQuality === "exact-key-size-bucket-low-sample")

    val collapsed = history.predict(request.copy(
      usefulTasks = 1,
      maxTaskFootprintBytes = 3000,
      admissionBudgetBytes = 2000)).get
    assert(collapsed.components.longestTaskLowerBoundNs === 920)
    assert(collapsed.predictedGpuCapacity === 1.0)
    assert(collapsed.memoryStatus === "unsafe")
    assert(collapsed.stageWallNs === 1610)
    history.close()
  }

  test("an incomplete tail is truncated before the next append") {
    val path = Files.createTempDirectory("performance-history-tail").resolve("history.log")
    val first = observation(context())
    val history = PerformanceHistory.local(path)
    history.record(first)
    history.close()
    Files.write(path, "v5\\tpartial".getBytes(StandardCharsets.UTF_8),
      StandardOpenOption.APPEND)

    val recovered = PerformanceHistory.local(path)
    assert(recovered.observations(context()) === Seq(first))
    val second = first.copy(observedAtMs = 2L)
    recovered.record(second)
    recovered.close()

    val reopened = PerformanceHistory.local(path)
    assert(reopened.observations(context()) === Seq(first, second))
    reopened.close()
  }

  test("prediction refuses a size outside the observed batch neighborhood") {
    val path = Files.createTempDirectory("performance-history-size").resolve("history.log")
    val history = PerformanceHistory.local(path)
    history.record(observation(context()))
    val request = PredictionRequest(
      context(), 2000, 1L << 40, 200, 1000, 50, 1000, 1,
      1L << 40, 4, 8, 2.0, 2.0, 2.0, 400, 2048, 4096)
    assert(history.predict(request).isEmpty)
    history.close()
  }

  test("five observations expose residual bounds") {
    val path = Files.createTempDirectory("performance-history-residual").resolve("history.log")
    val history = PerformanceHistory.local(path)
    (1L to 5L).foreach { index =>
      history.record(observation(context()).copy(observedAtMs = index))
    }
    val request = PredictionRequest(
      context(), 2000, 4000, 200, 1000, 50, 1000, 8,
      1024L * 1024 * 1024, 4, 8, 2.0, 2.0, 2.0, 400, 2048, 4096)
    val prediction = history.predict(request).get
    assert(prediction.sampleCount === 5)
    assert(prediction.p10Scale.nonEmpty)
    assert(prediction.p90Scale.nonEmpty)
    history.close()
  }

  test("corruption before the final record fails reopening") {
    val path = Files.createTempDirectory("performance-history-corrupt").resolve("history.log")
    val history = PerformanceHistory.local(path)
    history.record(observation(context()))
    history.close()
    Files.write(path, "corrupt\n".getBytes(StandardCharsets.UTF_8),
      StandardOpenOption.APPEND)
    intercept[IllegalArgumentException] {
      PerformanceHistory.local(path)
    }
  }
}
