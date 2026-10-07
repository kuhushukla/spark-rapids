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

import com.nvidia.spark.history.Retention

/**
 * How far one scan's decoded device bytes expand beyond the file bytes it read. This object is
 * the only place the family's governed identity appears.
 *
 *  - quantity: GPU output batch bytes / on-disk bytes of the files the scan planned, a positive
 *    unitless ratio, one observation per completed scan, recorded when its SQL execution ends and
 *    stamped with that time
 *  - dimension `table`: the scan's table name exactly as the planning hook sees it (Iceberg's
 *    `Table.name()`, or the catalog identifier for file-source tables). Values are compared as
 *    exact bytes, so the decide and observe sides must derive it identically.
 */
object ScanExpansionRatio extends HistoryMetric {

  /** Placeholder until the governed allocation is final; must match the API catalog. */
  override val id: Int = 1

  override val version: Int = 1

  override val dimension: String = "table"

  override val retention: Retention = new Retention(Duration.ofDays(7), Duration.ofDays(14))
}
