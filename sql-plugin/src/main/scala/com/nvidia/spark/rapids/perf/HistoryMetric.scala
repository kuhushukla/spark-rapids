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

import java.util.Collections

import com.nvidia.spark.history.{DimensionSpec, DimValue, HistoryMetricCatalog, MetricSchema,
  MetricVersionId, Retention}

/**
 * The contract of one governed history metric family at one version: the identity it is
 * recorded under, its single dimension, and how long its observations stay useful to planning.
 *
 * Contract only - value types from the history metrics API, no store access. Each family has
 * exactly one STRING dimension: "most recent reading" is expressed as `limit(1)`, which requires
 * every declared dimension to be bound, so a second dimension would have to be bound on every
 * read, and adding one is a new version.
 */
trait HistoryMetric {

  /** Governed family ID. Must match the history metrics API's production catalog entry. */
  def id: Int

  /**
   * Governed family name, read from the history metrics API's production catalog entry for `id`
   * so the ID-to-name association has a single source. A family absent from the catalog, or
   * retired there, is named by its ID; the store rejects its declaration either way.
   */
  final lazy val name: String = {
    val entry = HistoryMetricCatalog.production().find(id)
    if (entry.isPresent && !entry.get.retired()) entry.get.name() else s"uncatalogued-metric-$id"
  }

  /** Family-scoped contract version. Any change in meaning needs a new version. */
  def version: Int

  /** Name of the single STRING dimension. */
  def dimension: String

  /**
   * Recommended retention. The first declaration a store accepts fixes it for good, so it is a
   * constant of the contract rather than a configuration value. Its planning age is also the
   * oldest observation a lookup asks for.
   */
  def retention: Retention

  final lazy val metric: MetricVersionId = new MetricVersionId(id, version)

  final lazy val schema: MetricSchema = new MetricSchema(
    metric,
    Collections.singletonList(new DimensionSpec(dimension, DimValue.Kind.STRING)),
    retention)
}
