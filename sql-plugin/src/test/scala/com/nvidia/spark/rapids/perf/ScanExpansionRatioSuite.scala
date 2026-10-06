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

import com.nvidia.spark.history.{DimValue, HistoryMetricCatalog, MetricVersionId}
import org.scalatest.funsuite.AnyFunSuite

class ScanExpansionRatioSuite extends AnyFunSuite {

  test("matches its governed entry in the production catalog") {
    val entry = HistoryMetricCatalog.production().find(ScanExpansionRatio.id)
    assert(entry.isPresent, s"metric ID ${ScanExpansionRatio.id} is not in the catalog")
    assert(entry.get.name() == ScanExpansionRatio.name)
    assert(!entry.get.retired())
  }

  test("declares one STRING dimension named table, at version 1") {
    val schema = ScanExpansionRatio.schema
    assert(schema.metric() == new MetricVersionId(ScanExpansionRatio.id, 1))
    val dims = schema.dimensions().asScala.map(d => d.name() -> d.kind())
    assert(dims == Seq("table" -> DimValue.Kind.STRING))
  }

  test("recommended retention is fixed: 7 days for planning, 14 for storage") {
    val retention = ScanExpansionRatio.schema.recommendedRetention()
    assert(retention.planningMaxAge() == Duration.ofDays(7))
    assert(retention.storageRetention() == Duration.ofDays(14))
  }
}
