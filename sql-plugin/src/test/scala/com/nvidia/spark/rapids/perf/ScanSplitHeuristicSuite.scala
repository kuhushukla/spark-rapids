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
import java.util.Collections

import com.nvidia.spark.history.{DimValue, MetricStore, MetricStores, Observation,
  SchemaStatus, Status, SummaryResponse}
import org.scalatest.funsuite.AnyFunSuite

/** Decide and observe against an in-memory store. Nothing here starts Spark. */
class ScanSplitHeuristicSuite extends AnyFunSuite {

  private val GiB = 1024L * 1024 * 1024
  private val MiB = 1024L * 1024
  private val table = "cat.db.events"
  private val now = 1790000000000L
  private val policy = HistoryPolicy(Duration.ofMillis(100))

  private def heuristic(store: MetricStore, enabled: Boolean = true): ScanSplitHeuristic = {
    val h = new ScanSplitHeuristic(() => store)
    if (enabled) {
      h.enable(policy)
    }
    h
  }

  /** Decides the Iceberg split for `table` over 960 GiB on 96 slots, 1 GiB batches. */
  private def decide(h: ScanSplitHeuristic, key: String = table): Long =
    h.decideIcebergSplit(key, 960 * GiB, GiB, 96L, now)

  private def observation(key: String, value: Double, atMs: Long): Observation =
    new Observation(ScanExpansionRatio.metric,
      Collections.singletonMap(ScanExpansionRatio.dimension, DimValue.of(key)), value, atMs)

  /** A store that already holds `values` for `table`, recorded at the given times. */
  private def storeWith(values: (Double, Long)*): FakeMetricStore = {
    val store = new FakeMetricStore
    store.declare(Collections.singletonList(ScanExpansionRatio.schema), Duration.ofSeconds(1))
    values.foreach { case (v, at) => store.record(observation(table, v, at)) }
    store.declareCalls.clear()
    store.offered.clear()
    store
  }

  private def scanContext(decoded: Long, listed: Long = 100 * MiB, planned: Long = 0L,
      completed: Long = 0L): ScanContext =
    ScanContext(table, listed, 0L, 0L, 0L, () => decoded, planned, () => completed)

  test("decides from the table's last reading") {
    val store = storeWith(4.0 -> (now - 1000))
    assert(decide(heuristic(store)) == 256 * MiB)
  }

  test("summary request: table bound, window [now - 7 days, now + 1), limit 1") {
    val store = storeWith(4.0 -> (now - 1000))
    decide(heuristic(store))
    assert(store.summarizeCalls.size == 1)
    val (request, timeout) = store.summarizeCalls.head
    assert(request.metric() == ScanExpansionRatio.metric)
    assert(request.bound() == Collections.singletonMap("table", DimValue.of(table)))
    // The planning age is the contract's, not a setting.
    assert(request.fromMs() == now - Duration.ofDays(7).toMillis)
    assert(request.toMs() == now + 1)
    assert(request.limit() == 1)
    assert(timeout == policy.planningTimeout)
  }

  test("uses the most recent reading, not an average of history") {
    val store = storeWith(1.0 -> (now - 3000), 8.0 -> (now - 1000), 2.0 -> (now - 2000))
    assert(decide(heuristic(store)) == 128 * MiB)
  }

  test("equal timestamps resolve to the later-recorded reading") {
    val store = storeWith(1.0 -> (now - 1000), 4.0 -> (now - 1000))
    assert(decide(heuristic(store)) == 256 * MiB)
  }

  test("readings older than the planning age are ignored") {
    val store = storeWith(4.0 -> (now - Duration.ofDays(8).toMillis))
    assert(decide(heuristic(store)) == ScanSplitHeuristic.NO_DECISION)
  }

  test("no history for the table means no decision") {
    val store = storeWith(4.0 -> (now - 1000))
    assert(decide(heuristic(store), "cat.db.other") == ScanSplitHeuristic.NO_DECISION)
    assert(decide(heuristic(new FakeMetricStore)) == ScanSplitHeuristic.NO_DECISION)
  }

  test("table keys are exact: case differs, history differs") {
    val store = storeWith(4.0 -> (now - 1000))
    assert(decide(heuristic(store), table.toUpperCase(java.util.Locale.ROOT)) ==
      ScanSplitHeuristic.NO_DECISION)
  }

  test("disabled heuristic never touches the store") {
    val store = storeWith(4.0 -> (now - 1000))
    assert(decide(heuristic(store, enabled = false)) == ScanSplitHeuristic.NO_DECISION)
    assert(store.declareCalls.isEmpty && store.summarizeCalls.isEmpty)
  }

  test("built-in no-op store: no decision, one log line") {
    val noop = MetricStores.current()
    assume(noop.info().description().contains("no-op"), "another store is installed")
    val h = heuristic(noop)
    assert(decide(h) == ScanSplitHeuristic.NO_DECISION)
    assert(decide(h) == ScanSplitHeuristic.NO_DECISION)
    assert(h.historyOf(ScanExpansionRatio).reportedReasons == Set("lookup-UNAVAILABLE"))
  }

  test("rejected declaration: no lookup, no record, one log line per code") {
    Seq(SchemaStatus.Code.INCOMPATIBLE, SchemaStatus.Code.INVALID_REQUEST,
        SchemaStatus.Code.UNAVAILABLE, SchemaStatus.Code.DENIED).foreach { code =>
      val store = storeWith(4.0 -> (now - 1000))
      store.declareCode = code
      val h = heuristic(store)
      assert(decide(h) == ScanSplitHeuristic.NO_DECISION, code)
      assert(decide(h) == ScanSplitHeuristic.NO_DECISION, code)
      h.recordAll(scanContext(decoded = 400 * MiB))
      assert(store.declareCalls.size == 1, s"$code: declared once and cached")
      assert(store.summarizeCalls.isEmpty, code)
      assert(store.offered.isEmpty, code)
      assert(h.historyOf(ScanExpansionRatio).reportedReasons == Set(s"declare-$code"))
    }
  }

  test("accepted with a warning still decides") {
    val store = storeWith(4.0 -> (now - 1000))
    store.declareReason = "first effective retention remains unchanged"
    val h = heuristic(store)
    assert(decide(h) == 256 * MiB)
    assert(h.historyOf(ScanExpansionRatio).reportedReasons == Set("declare-ACCEPTED"))
  }

  test("summarize errors and timeouts: no decision, one log line per code") {
    Seq(Status.Code.NOT_DECLARED, Status.Code.INVALID_REQUEST, Status.Code.DEADLINE_EXCEEDED,
        Status.Code.UNAVAILABLE, Status.Code.DENIED).foreach { code =>
      val store = storeWith(4.0 -> (now - 1000))
      store.summarizeError = Some(Status.of(code, "injected"))
      val h = heuristic(store)
      assert(decide(h) == ScanSplitHeuristic.NO_DECISION, code)
      assert(decide(h) == ScanSplitHeuristic.NO_DECISION, code)
      assert(h.historyOf(ScanExpansionRatio).reportedReasons == Set(s"lookup-$code"))
    }
  }

  test("malformed or failing summarize: no decision") {
    val malformed = Seq(
      FakeMetricStore.emptyResponses,
      java.util.Arrays.asList(SummaryResponse.ok(null), SummaryResponse.ok(null)),
      null)
    malformed.foreach { responses =>
      val store = storeWith(4.0 -> (now - 1000))
      store.summarizeRaw = Some(responses)
      val h = heuristic(store)
      assert(decide(h) == ScanSplitHeuristic.NO_DECISION)
      assert(h.historyOf(ScanExpansionRatio).reportedReasons == Set("lookup-malformed"))
    }
    val store = storeWith(4.0 -> (now - 1000))
    store.summarizeThrows = Some(new IllegalStateException("boom"))
    val h = heuristic(store)
    assert(decide(h) == ScanSplitHeuristic.NO_DECISION)
    assert(h.historyOf(ScanExpansionRatio).reportedReasons ==
      Set("lookup-java.lang.IllegalStateException"))
  }

  test("table names that cannot be a dimension value are not tracked") {
    val store = storeWith(4.0 -> (now - 1000))
    val h = heuristic(store)
    val tooLong = "t" * 254
    val badUtf16 = "db.\ud800"
    Seq(tooLong, badUtf16).foreach { key =>
      assert(decide(h, key) == ScanSplitHeuristic.NO_DECISION)
      h.recordAll(ScanContext(key, 100 * MiB, 0L, 0L, 0L, () => 400 * MiB))
    }
    assert(store.summarizeCalls.isEmpty && store.offered.isEmpty)
    assert(h.historyOf(ScanExpansionRatio).reportedReasons == Set("untrackable-key"))
    // 253 bytes is the largest key the API takes.
    assert(decide(h, "t" * 253) == ScanSplitHeuristic.NO_DECISION)
    assert(store.summarizeCalls.size == 1)
  }

  test("observe records decoded / listed under the table") {
    val store = new FakeMetricStore
    val h = heuristic(store)
    h.recordAll(scanContext(decoded = 400 * MiB))
    assert(store.stored.size == 1)
    val o = store.stored.head
    assert(o.metric() == ScanExpansionRatio.metric)
    assert(o.dimensions() == Collections.singletonMap("table", DimValue.of(table)))
    assert(o.value() == 4.0)
    // and the next decision uses it
    assert(h.decideIcebergSplit(table, 960 * GiB, GiB, 96L, o.timestampMs() + 1) == 256 * MiB)
  }

  test("observe skips scans that decoded nothing or stopped early") {
    val store = new FakeMetricStore
    val h = heuristic(store)
    h.recordAll(scanContext(decoded = 0L))
    h.recordAll(scanContext(decoded = 400 * MiB, planned = 10, completed = 9))
    assert(store.offered.isEmpty)
    // reading a scan more than once is healthy
    h.recordAll(scanContext(decoded = 400 * MiB, planned = 10, completed = 20))
    assert(store.stored.size == 1)
  }

  test("file-source hook: catalog tables only, and only while enabled") {
    val store = storeWith(4.0 -> (now - 1000))
    def ctx(name: String): ScanContext =
      ScanContext(name, 960 * GiB, GiB, 96L, 123L, () => 400 * MiB)
    val noContext: String => ScanContext = _ => fail("context built without a decision")
    val disabled = heuristic(store, enabled = false)
    assert(disabled.fileSourceSplit(Some(table), 123L, Some(1L), noContext, now) == 123L)
    val h = heuristic(store)
    assert(h.fileSourceSplit(None, 123L, Some(1L), noContext, now) == 123L)
    assert(store.summarizeCalls.isEmpty)
    assert(h.fileSourceSplit(Some(table), 123L, None, ctx, now) == 256 * MiB)
    assert(h.fileSourceSplit(Some("cat.db.other"), 123L, None, ctx, now) == 123L)
  }

  test("file-source hook registers the scan for observation") {
    val store = new FakeMetricStore
    val h = heuristic(store)
    HistoryObservations.start()
    try {
      h.fileSourceSplit(Some(table), 123L, Some(77L),
        name => ScanContext(name, 100 * MiB, GiB, 96L, 123L, () => 400 * MiB), now)
      HistoryObservations.listener.onOtherEvent(
        org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd(77L, 0L))
      assert(store.stored.map(_.value()) == Seq(4.0))
    } finally {
      HistoryObservations.shutdown()
    }
  }

  test("minPartitionNum reads the session settings, falling back to executor slots") {
    val conf = new org.apache.spark.sql.internal.SQLConf
    def partitions: Long = ScanSplitHeuristic.minPartitionNum(conf, 7)
    assert(partitions == 7L)
    conf.setConfString("spark.executor.instances", "6")
    assert(partitions == 7L, "both slot settings are needed")
    conf.setConfString("spark.executor.cores", " 16 ")
    assert(partitions == 96L)
    conf.setConfString("spark.default.parallelism", "200")
    assert(partitions == 200L)
    conf.setConfString("spark.sql.leafNodeDefaultParallelism", "40")
    assert(partitions == 40L)
    conf.setConfString("spark.sql.files.minPartitionNum", "500")
    assert(partitions == 500L)
  }

  test("the application's Iceberg advisor abstains while disabled") {
    assume(!ScanSplitHeuristic.isEnabled, "history-backed planning is on in this JVM")
    assert(ScanSplitHeuristic.learnedSplitBytes(table, 960 * GiB) ==
      ScanSplitHeuristic.NO_DECISION)
  }
}
