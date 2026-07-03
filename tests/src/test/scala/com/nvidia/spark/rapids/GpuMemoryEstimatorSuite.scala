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

package com.nvidia.spark.rapids

import scala.collection.mutable.ArrayBuffer

import org.scalatest.funsuite.AnyFunSuite

class GpuMemoryEstimatorSuite extends AnyFunSuite {
  private val GiB = 1024L * 1024 * 1024

  test("StatEstimator returns its default without observations") {
    val estimator = new StatEstimator(minEntries = 4, defaultValue = 8 * GiB)
    assert(estimator.percentile(0.8, ArrayBuffer.empty) === 8 * GiB)
  }

  test("StatEstimator p80 is the maximum with four or fewer combined entries") {
    val lowEstimator = new StatEstimator(minEntries = 4, defaultValue = 8 * GiB)
    lowEstimator.add(2 * GiB)
    assert(lowEstimator.percentile(0.8, ArrayBuffer.empty) === 8 * GiB)

    val highEstimator = new StatEstimator(minEntries = 4, defaultValue = 8 * GiB)
    highEstimator.add(12 * GiB)
    assert(highEstimator.percentile(0.8, ArrayBuffer.empty) === 12 * GiB)

    val activeEstimator = new StatEstimator(minEntries = 4, defaultValue = 8 * GiB)
    assert(activeEstimator.percentile(0.8,
      ArrayBuffer(1 * GiB, 2 * GiB, 3 * GiB, 4 * GiB)) === 4 * GiB)
  }

  test("StatEstimator interpolates percentiles after its prior-padding period") {
    val estimator = new StatEstimator(minEntries = 4, defaultValue = 0)
    Seq(1.0, 2.0, 3.0, 4.0, 5.0).foreach(estimator.add)
    assert(math.abs(estimator.percentile(0.8, ArrayBuffer.empty) - 4.8) < 1e-12)
  }

  test("StatEstimator retains only the 200 most recent completed values") {
    val estimator = new StatEstimator(minEntries = 4, defaultValue = 1 * GiB)
    (0 until 200).foreach(_ => estimator.add(1 * GiB))
    (0 until 200).foreach(_ => estimator.add(16 * GiB))
    assert(estimator.percentile(0.8, ArrayBuffer.empty) === 16 * GiB)
  }

  test("GpuTaskMemoryEstimator jumps immediately above its prior and keeps the maximum") {
    val estimator = new GpuTaskMemoryEstimator(
      stageId = 1,
      taskId = 2,
      defaultEstimate = 4 * GiB,
      allowDynamicUpdate = true)

    estimator.update(timeLost = 0, memory = 6 * GiB)
    assert(estimator.estimate() === 6 * GiB)

    estimator.update(timeLost = 0, memory = 1 * GiB)
    assert(estimator.estimate() === 6 * GiB)
  }

  test("GpuTaskMemoryEstimator converges downward after its active-time window") {
    val estimator = new GpuTaskMemoryEstimator(
      stageId = 1,
      taskId = 2,
      defaultEstimate = 4 * GiB,
      allowDynamicUpdate = true)

    estimator.update(timeLost = 0, memory = 1 * GiB)
    Thread.sleep(150)
    assert(estimator.estimate() === 1 * GiB)
  }

  test("GpuTaskMemoryEstimator returns its prior when dynamic updates are disabled") {
    val estimator = new GpuTaskMemoryEstimator(
      stageId = 1,
      taskId = 2,
      defaultEstimate = 4 * GiB,
      allowDynamicUpdate = false)

    estimator.update(timeLost = 0, memory = 6 * GiB)
    assert(estimator.estimate() === 4 * GiB)
  }
}
