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

package org.apache.spark.sql.rapids

import java.time.Duration
import java.util.Properties

import com.nvidia.spark.rapids.perf.{FakeMetricStore, HistoryObservations, HistoryPolicy,
  ScanContext, ScanSplitHeuristic}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.scheduler.{JobFailed, JobResult, JobSucceeded, SparkListenerJobEnd,
  SparkListenerJobStart, SparkListenerStageCompleted, StageInfo}
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd

/**
 * Observations drain on SQL execution end, driven by hand-built listener events. In this package
 * only so a failed job result can be built; nothing here starts Spark.
 */
class HistoryObservationsSuite extends AnyFunSuite with BeforeAndAfterEach {

  private val MiB = 1024L * 1024
  private val listener = HistoryObservations.listener
  private var store: FakeMetricStore = _
  private var heuristic: ScanSplitHeuristic = _

  override def beforeEach(): Unit = {
    store = new FakeMetricStore
    heuristic = new ScanSplitHeuristic(() => store)
    heuristic.enable(HistoryPolicy(Duration.ofMillis(100)))
    HistoryObservations.start()
  }

  override def afterEach(): Unit = HistoryObservations.shutdown()

  /** A scan of `cat.ns.events` that decoded 4x its 100 MiB on disk. */
  private def scan: ScanContext =
    ScanContext("cat.ns.events", 100 * MiB, 0L, 0L, 0L, () => 400 * MiB)

  private def startJob(jobId: Int, executionId: Long, stages: StageInfo*): Unit = {
    val properties = new Properties()
    properties.setProperty(SQLExecution.EXECUTION_ID_KEY, executionId.toString)
    listener.onJobStart(SparkListenerJobStart(jobId, 0L, stages, properties))
  }

  private def runJob(jobId: Int, executionId: Long, result: JobResult): Unit = {
    startJob(jobId, executionId)
    listener.onJobEnd(SparkListenerJobEnd(jobId, 0L, result))
  }

  private def stage(stageId: Int, failure: Option[String]): StageInfo = {
    val info = new StageInfo(stageId, 0, s"stage $stageId", 1, Seq.empty, Seq.empty, "",
      resourceProfileId = 0)
    info.failureReason = failure
    info
  }

  test("a failed stage attempt discards the execution even if its job succeeds") {
    heuristic.register(Some(46L), scan)
    val retried = stage(3, None)
    startJob(12, 46L, stage(2, None), retried)
    // The first attempt of stage 3 fails, a retry succeeds, and the job succeeds: the retried
    // tasks were counted twice, so the ratio would be inflated.
    listener.onStageCompleted(SparkListenerStageCompleted(stage(3, Some("fetch failure"))))
    listener.onStageCompleted(SparkListenerStageCompleted(retried))
    listener.onJobEnd(SparkListenerJobEnd(12, 0L, JobSucceeded))
    listener.onOtherEvent(SparkListenerSQLExecutionEnd(46L, 0L))
    assert(store.offered.isEmpty)
  }

  test("stages that complete cleanly keep the observation") {
    heuristic.register(Some(47L), scan)
    startJob(13, 47L, stage(4, None))
    listener.onStageCompleted(SparkListenerStageCompleted(stage(4, None)))
    listener.onJobEnd(SparkListenerJobEnd(13, 0L, JobSucceeded))
    listener.onOtherEvent(SparkListenerSQLExecutionEnd(47L, 0L))
    assert(store.stored.size == 1)
  }

  test("a job failing after its execution ended is ignored") {
    heuristic.register(Some(48L), scan)
    startJob(14, 48L, stage(5, None))
    listener.onOtherEvent(SparkListenerSQLExecutionEnd(48L, 0L))
    listener.onJobEnd(SparkListenerJobEnd(14, 0L, JobFailed(new RuntimeException("late"))))
    listener.onStageCompleted(SparkListenerStageCompleted(stage(5, Some("late"))))
    assert(store.stored.size == 1)
  }

  test("records when the execution ends, once") {
    heuristic.register(Some(41L), scan)
    heuristic.register(None, scan) // planned outside an execution: not tracked
    runJob(7, 41L, JobSucceeded)
    assert(store.offered.isEmpty, "nothing is recorded before the execution ends")
    listener.onOtherEvent(SparkListenerSQLExecutionEnd(41L, 0L))
    assert(store.stored.map(_.value()) == Seq(4.0))
    listener.onOtherEvent(SparkListenerSQLExecutionEnd(41L, 0L))
    assert(store.stored.size == 1)
  }

  test("an execution with a failed or cancelled job records nothing") {
    heuristic.register(Some(42L), scan)
    runJob(8, 42L, JobSucceeded)
    runJob(9, 42L, JobFailed(new RuntimeException("cancelled")))
    listener.onOtherEvent(SparkListenerSQLExecutionEnd(42L, 0L))
    assert(store.offered.isEmpty)
  }

  test("other executions are unaffected by a failure") {
    heuristic.register(Some(43L), scan)
    heuristic.register(Some(44L), scan)
    runJob(10, 43L, JobFailed(new RuntimeException("failed")))
    runJob(11, 44L, JobSucceeded)
    listener.onOtherEvent(SparkListenerSQLExecutionEnd(43L, 0L))
    listener.onOtherEvent(SparkListenerSQLExecutionEnd(44L, 0L))
    assert(store.stored.size == 1)
  }

  test("nothing is held while observations are stopped") {
    HistoryObservations.shutdown()
    heuristic.register(Some(45L), scan)
    HistoryObservations.start()
    listener.onOtherEvent(SparkListenerSQLExecutionEnd(45L, 0L))
    assert(store.offered.isEmpty)
  }
}
