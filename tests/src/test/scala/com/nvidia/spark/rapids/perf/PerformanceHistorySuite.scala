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

import java.nio.file.Files

import org.scalatest.funsuite.AnyFunSuite

class PerformanceHistorySuite extends AnyFunSuite {
  private def context(tableVersion: String = "2009"): PerformanceContext =
    PerformanceContext(
      "g7.4xlarge",
      "NVIDIA L4",
      24L * 1024 * 1024 * 1024,
      16,
      "3.5.5",
      "26.08.0",
      "17",
      "hadoop-s3a-v1",
      "taxi",
      tableVersion,
      "parquet",
      "snappy",
      "schema-sha256",
      "reader-and-admission-config-sha256",
      "projection-sha256",
      "partition-predicate-sha256",
      "data-predicate-sha256",
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
      effectiveReadParallelism = 2.0,
      observedGpuOverlapCapacity = 2.0,
      maxTaskServiceNs = 200,
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

  test("prediction requires the complete hardware table predicate and reader context") {
    val path = Files.createTempDirectory("performance-history-key").resolve("history.log")
    val history = PerformanceHistory.local(path)
    history.record(observation(context()))

    val differentTableVersion = PredictionRequest(
      context("2010"), 2000, 4000, 200, 1000, 50, 1000, 8,
      1024L * 1024 * 1024, 4, 2.0, 2.0, 2048, 4096)
    assert(history.predict(differentTableVersion).isEmpty)
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
      effectiveReadParallelism = 2.0,
      predictedGpuOverlapCapacity = 2.0,
      maxTaskFootprintBytes = 2048,
      admissionBudgetBytes = 4096)
    val prediction = history.predict(request).get

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
    assert(collapsed.components.longestTaskLowerBoundNs === 900)
    assert(collapsed.predictedGpuCapacity === 1.0)
    assert(collapsed.memoryStatus === "unsafe")
    assert(collapsed.stageWallNs === 1575)
    history.close()
  }
}
