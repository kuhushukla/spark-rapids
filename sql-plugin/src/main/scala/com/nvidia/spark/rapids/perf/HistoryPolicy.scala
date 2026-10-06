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

import com.nvidia.spark.rapids.RapidsConf

/**
 * Time budgets for using history on the planning path. They are the caller's only protection:
 * the store honors the budget it is handed and never blocks longer.
 *
 * The planning age is not here: it is each family's contract (`HistoryMetric.retention`), which
 * the store fixes at the first declaration it accepts.
 *
 * @param planningTimeout budget for one lookup on the planning path
 * @param declareBudget budget for the one declaration each family makes per store
 */
case class HistoryPolicy(
    planningTimeout: Duration,
    declareBudget: Duration = HistoryPolicy.DECLARE_BUDGET)

object HistoryPolicy {

  val DECLARE_BUDGET: Duration = Duration.ofSeconds(5)

  def fromConf(conf: RapidsConf): HistoryPolicy =
    HistoryPolicy(Duration.ofMillis(conf.historyPlanningTimeoutMs.toLong))
}
