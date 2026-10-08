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

import java.util.{ArrayList => JArrayList, Collections, LinkedHashSet => JLinkedHashSet,
  List => JList, Locale}
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import com.nvidia.spark.history.{MetricStore, MetricStores}
import com.nvidia.spark.rapids.{HistoryMetricsManager, RapidsConf}
import com.nvidia.spark.rapids.iceberg.IcebergProvider

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{JobSucceeded, SparkListener, SparkListenerEvent,
  SparkListenerJobEnd, SparkListenerJobStart, SparkListenerStageCompleted}
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd

/**
 * A planning decision that reads history at planning time and records what the query did when
 * it ends. `decide` and `register` are final so fallback and execution-id scoping stay uniform.
 *
 * @param store the installed store; `MetricStores.current()` in production
 */
abstract class HistoryHeuristic(store: () => MetricStore) extends Logging {

  type Ctx
  type Decision

  def name: String

  /** Families this heuristic reads and writes. */
  def metrics: Seq[HistoryMetric]

  private lazy val histories: Map[HistoryMetric, MetricHistory] =
    metrics.map(m => m -> new MetricHistory(m, store)).toMap

  /** Set while history-backed planning is on; otherwise the static decision stands. */
  @volatile private var active: Option[HistoryPolicy] = None

  final def enable(policy: HistoryPolicy): Unit = active = Some(policy)

  final def disable(): Unit = active = None

  final def isEnabled: Boolean = active.isDefined

  /** Declares every family up front so the first planning lookup does not pay for it. */
  final def declare(): Unit = active.foreach { policy =>
    histories.values.foreach(_.declare(policy))
  }

  /** Dimension value for `ctx` in `metric`. */
  protected def keyFor(metric: HistoryMetric, ctx: Ctx): String

  /** The decision without history. */
  protected def staticDecision(ctx: Ctx): Decision

  /** Called only when `sufficient` holds. */
  protected def decideFrom(observed: Map[HistoryMetric, Double], ctx: Ctx): Decision

  protected def constrain(raw: Decision, ctx: Ctx): Decision = raw

  /** What the query did; called after it ends. */
  protected def observe(ctx: Ctx): Map[HistoryMetric, Double]

  /** Default: every family answered. */
  protected def sufficient(observed: Map[HistoryMetric, Double]): Boolean =
    observed.size == metrics.size

  protected def shouldObserve(ctx: Ctx): Boolean = true

  /**
   * History-based or static, never a blend. Abstaining families are absent from `observed`;
   * `sufficient` decides whether the rest is enough.
   */
  final def decide(ctx: Ctx, nowMs: Long): Decision = active match {
    case None => staticDecision(ctx)
    case Some(policy) =>
      val observed = metrics.flatMap { m =>
        histories(m).latest(keyFor(m, ctx), nowMs, policy).map(value => m -> value)
      }.toMap
      if (sufficient(observed)) constrain(decideFrom(observed, ctx), ctx)
      else staticDecision(ctx)
  }

  /** Queues `ctx` for observation at execution end; untracked outside a SQL execution. */
  final def register(executionId: Option[Long], ctx: Ctx): Unit = {
    if (isEnabled && shouldObserve(ctx)) {
      executionId match {
        case None => logDebug(s"$name planned outside a SQL execution; not tracked for history")
        case Some(id) => HistoryObservations.register(id, () => recordAll(ctx))
      }
    }
  }

  /** Records what `ctx` observed; called at execution end. */
  private[perf] final def recordAll(ctx: Ctx): Unit = active.foreach { policy =>
    val atMs = System.currentTimeMillis()
    observe(ctx).foreach { case (m, value) =>
      histories(m).record(keyFor(m, ctx), value, atMs, policy)
    }
  }

  /** For tests. */
  private[perf] final def historyOf(metric: HistoryMetric): MetricHistory = histories(metric)
}

/**
 * Holds planned contexts per SQL execution id and records them when the execution ends, once
 * accumulators are merged. Shared by all heuristics.
 *
 * Records nothing for an execution with a failed or cancelled job (partial accumulators) or a
 * failed stage attempt (a re-run stage can double-count plain accumulators).
 */
object HistoryObservations extends Logging {

  /** What is known about one running SQL execution. */
  private final class Execution {
    val callbacks: JList[() => Unit] = Collections.synchronizedList(new JArrayList[() => Unit]())
    @volatile var incomplete: Boolean = false
  }

  /** Ended execution ids remembered, so late registrations are dropped. */
  private val ENDED_MEMORY = 1024

  @volatile private var active: Boolean = false

  /** Running executions and their jobs and stages; dropped at execution end. */
  private val executions = new ConcurrentHashMap[Long, Execution]()
  private val jobOwner = new ConcurrentHashMap[Int, java.lang.Long]()
  private val stageOwner = new ConcurrentHashMap[Int, java.lang.Long]()

  /** Recently ended executions, oldest first. Guarded by `this`. */
  private val ended = new JLinkedHashSet[Long]()

  def start(): Unit = active = true

  def shutdown(): Unit = synchronized {
    active = false
    executions.clear()
    jobOwner.clear()
    stageOwner.clear()
    ended.clear()
  }

  /** Register on the SparkContext so every session and micro-batch is covered. */
  def listener: SparkListener = new ObservationListener

  /** (executions, job owners, stage owners), for tests. */
  private[perf] def trackedCounts: (Int, Int, Int) =
    (executions.size(), jobOwner.size(), stageOwner.size())

  /** The calling thread's SQL execution id, if any. */
  def currentExecutionId(sc: SparkContext): Option[Long] =
    Option(sc.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)).flatMap(parseId)

  private def parseId(id: String): Option[Long] =
    try Some(id.toLong) catch { case _: NumberFormatException => None }

  /** Get or create. Callers hold `this`. */
  private def running(executionId: Long): Execution = {
    val known = executions.get(executionId)
    if (known != null) {
      known
    } else {
      val created = new Execution
      executions.put(executionId, created)
      created
    }
  }

  private[perf] def register(executionId: Long, record: () => Unit): Unit = synchronized {
    // A registration for an execution that already ended would never drain.
    if (active && !ended.contains(executionId)) {
      running(executionId).callbacks.add(record)
    }
  }

  /**
   * `errorMessage` is not checked: Spark 3.3 lacks it, and a scan that read every split is valid
   * even if the query failed later.
   */
  private def executionEnded(executionId: Long): Unit = {
    val state = synchronized {
      ended.add(executionId)
      if (ended.size() > ENDED_MEMORY) {
        ended.remove(ended.iterator().next())
      }
      executions.remove(executionId)
    }
    // By owner, so an id reused by a later execution stays.
    jobOwner.values().removeIf(owner => owner.longValue() == executionId)
    stageOwner.values().removeIf(owner => owner.longValue() == executionId)
    if (state != null) {
      if (active && !state.incomplete) {
        state.callbacks.asScala.foreach { record =>
          try {
            record()
          } catch {
            case t: Throwable if MetricHistory.isContained(t) =>
              // No dimension values or provider text in diagnostics.
              logDebug(s"Observation skipped for execution $executionId: ${t.getClass.getName}")
          }
        }
      }
    }
  }

  /** Marks `owner` incomplete if still running. */
  private def markIncomplete(owner: java.lang.Long): Unit = {
    if (owner != null) {
      Option(executions.get(owner.longValue())).foreach(_.incomplete = true)
    }
  }

  private class ObservationListener extends SparkListener {
    override def onJobStart(e: SparkListenerJobStart): Unit = {
      if (active) {
        Option(e.properties)
          .flatMap(p => Option(p.getProperty(SQLExecution.EXECUTION_ID_KEY)))
          .flatMap(parseId)
          .foreach { id =>
            val tracked = HistoryObservations.synchronized {
              if (ended.contains(id)) {
                false
              } else {
                running(id)
                true
              }
            }
            if (tracked) {
              jobOwner.put(e.jobId, Long.box(id))
              e.stageIds.foreach(stageId => stageOwner.put(stageId, Long.box(id)))
            }
          }
      }
    }

    override def onJobEnd(e: SparkListenerJobEnd): Unit = {
      val owner = jobOwner.remove(e.jobId)
      if (e.jobResult != JobSucceeded) {
        markIncomplete(owner)
      }
    }

    override def onStageCompleted(e: SparkListenerStageCompleted): Unit = {
      if (e.stageInfo.failureReason.isDefined) {
        markIncomplete(stageOwner.get(e.stageInfo.stageId))
      }
    }

    override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
      case e: SparkListenerSQLExecutionEnd => executionEnded(e.executionId)
      case _ => // not ours
    }
  }
}

/**
 * Starts and stops the history-backed heuristics. External dependencies are injected for tests.
 *
 * @param setIcebergAdvisor installs (true) or removes (false) the Iceberg split advisor
 * @param currentStore the installed store; `MetricStores.current()` in production
 */
private[perf] class HistoryLifecycle(
    heuristics: Seq[HistoryHeuristic],
    setIcebergAdvisor: Boolean => Unit,
    currentStore: () => MetricStore) extends Logging {

  /** Store at `start`; None while stopped. */
  private var storeAtStart: Option[MetricStore] = None
  private var advisorInstalled = false

  /**
   * Enables the heuristics if a provider is requested. Stops any earlier start first so nothing
   * leaks across SparkContexts.
   *
   * @param policy by-name: a bad setting is never read while history is off
   * @return whether the heuristics are enabled
   */
  def start(provider: String, policy: => HistoryPolicy, addListener: SparkListener => Unit)
      : Boolean = synchronized {
    stop()
    val requested = Option(provider).map(_.trim.toLowerCase(Locale.ROOT)).getOrElse("")
    if (requested.isEmpty || requested == HistoryMetricsManager.NO_PROVIDER) {
      false
    } else {
      try {
        val resolved = policy
        storeAtStart = Some(currentStore())
        HistoryObservations.start()
        // SparkContext-level: sees every session and streaming micro-batch.
        addListener(HistoryObservations.listener)
        heuristics.foreach(_.enable(resolved))
        // Loads the Iceberg probe; may fail without Iceberg on the classpath.
        setIcebergAdvisor(true)
        advisorInstalled = true
        logInfo(s"History-backed planning requested for ${heuristics.map(_.name)}; it activates " +
          "once a history metrics provider is installed")
        true
      } catch {
        case t: Throwable if MetricHistory.isContained(t) =>
          logWarning(s"History-backed planning could not start (${t.getClass.getName}); " +
            "planning stays static")
          stop()
          false
      }
    }
  }

  /**
   * Called after provider selection. Stays on only if a provider store replaced the one seen at
   * `start`, then declares every family.
   */
  def activate(): Unit = synchronized {
    storeAtStart.foreach { before =>
      if (currentStore() eq before) {
        logInfo(s"History-backed planning for ${heuristics.map(_.name)} is off: no history " +
          "metrics provider was installed")
        stop()
      } else {
        heuristics.foreach(_.declare())
        logInfo(s"History-backed planning is active for ${heuristics.map(_.name)}")
      }
    }
  }

  def stop(): Unit = synchronized {
    if (advisorInstalled) {
      advisorInstalled = false
      try {
        setIcebergAdvisor(false)
      } catch {
        case t: Throwable if MetricHistory.isContained(t) =>
          logDebug(s"Iceberg split advisor removal failed: ${t.getClass.getName}")
      }
    }
    heuristics.foreach(_.disable())
    HistoryObservations.shutdown()
    storeAtStart = None
  }
}

/** The application's history-backed heuristics, driven by the driver plugin. */
object HistoryHeuristics {

  private val lifecycle = new HistoryLifecycle(
    heuristics = Seq(ScanSplitHeuristic),
    // The Iceberg table wrapper is outside the shim class loader, so it gets a hand-off.
    setIcebergAdvisor = install => IcebergProvider.installScanSplitAdvisor(
      if (install) Some((t: String, l: Long) => ScanSplitHeuristic.learnedSplitBytes(t, l))
      else None),
    currentStore = () => MetricStores.current())

  /** At driver plugin init. */
  def start(sc: SparkContext, conf: RapidsConf): Unit =
    lifecycle.start(conf.historyMetricsProvider, HistoryPolicy.fromConf(conf), sc.addSparkListener)

  /** After the provider manager ran. */
  def activate(): Unit = lifecycle.activate()

  /** Before the provider shuts down. */
  def stop(): Unit = lifecycle.stop()
}
