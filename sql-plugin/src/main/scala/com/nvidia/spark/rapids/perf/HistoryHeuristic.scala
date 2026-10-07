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
 * One planning decision informed by history: read the families at planning time, observe what
 * actually happened when the query ends.
 *
 * `Ctx` is whatever the planning site has in hand, so heuristics do not share a widening
 * parameter list. `decide` and `register` are final: the fallback path and the execution-id
 * scoping are not a heuristic's business.
 *
 * @param store the installed store; `MetricStores.current()` in production
 */
abstract class HistoryHeuristic(store: () => MetricStore) extends Logging {

  type Ctx
  type Decision

  def name: String

  /** Families this heuristic reads and writes. A formula may take several measured quantities. */
  def metrics: Seq[HistoryMetric]

  private lazy val histories: Map[HistoryMetric, MetricHistory] =
    metrics.map(m => m -> new MetricHistory(m, store)).toMap

  /** Set while history-backed planning is on. Until then the static decision stands. */
  @volatile private var active: Option[HistoryPolicy] = None

  final def enable(policy: HistoryPolicy): Unit = active = Some(policy)

  final def disable(): Unit = active = None

  final def isEnabled: Boolean = active.isDefined

  /** Declares every family now, so the first planning lookup does not pay for it. */
  final def declare(): Unit = active.foreach { policy =>
    histories.values.foreach(_.declare(policy))
  }

  /** The dimension value this context maps to, per family. */
  protected def keyFor(metric: HistoryMetric, ctx: Ctx): String

  /** What planning would have chosen without history. */
  protected def staticDecision(ctx: Ctx): Decision

  /** The formula. Called only when `sufficient` holds. */
  protected def decideFrom(observed: Map[HistoryMetric, Double], ctx: Ctx): Decision

  /** Bounds on the formula's output. */
  protected def constrain(raw: Decision, ctx: Ctx): Decision = raw

  /** What this query actually did, read after it ran. */
  protected def observe(ctx: Ctx): Map[HistoryMetric, Double]

  /** Whether the evidence in hand is enough to decide. Default: every family answered. */
  protected def sufficient(observed: Map[HistoryMetric, Double]): Boolean =
    observed.size == metrics.size

  /** Whether this context is worth remembering for observation. */
  protected def shouldObserve(ctx: Ctx): Boolean = true

  /**
   * The planning decision. History or the static answer, never a blend: any family that
   * abstains is simply absent, and `sufficient` decides whether what remains is enough.
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

  /**
   * Remembers this context so its query end can observe it. A context planned outside a SQL
   * execution has no id to drain against and is not tracked.
   */
  final def register(executionId: Option[Long], ctx: Ctx): Unit = {
    if (isEnabled && shouldObserve(ctx)) {
      executionId match {
        case None => logDebug(s"$name planned outside a SQL execution; not tracked for history")
        case Some(id) => HistoryObservations.register(id, () => recordAll(ctx))
      }
    }
  }

  /** Records what `ctx` observed. Called when its execution ends; directly by tests. */
  private[perf] final def recordAll(ctx: Ctx): Unit = active.foreach { policy =>
    val atMs = System.currentTimeMillis()
    observe(ctx).foreach { case (m, value) =>
      histories(m).record(keyFor(m, ctx), value, atMs, policy)
    }
  }

  /** The access layer for `metric`, for tests. */
  private[perf] final def historyOf(metric: HistoryMetric): MetricHistory = histories(metric)
}

/**
 * Holds planned contexts until their query ends, then lets each record what it observed.
 *
 * Values cannot be read at planning time: they come from accumulators that stay zero until Spark
 * merges task values back. Entries are keyed by SQL execution id so a query only ever reads its
 * own merged accumulators. One registry and one listener serve every heuristic.
 *
 * An execution records nothing if one of its jobs failed or was cancelled (partial
 * accumulators) or one of its stage attempts failed (conservative).
 */
object HistoryObservations extends Logging {

  /** What is known about one running SQL execution. */
  private final class Execution {
    val callbacks: JList[() => Unit] = Collections.synchronizedList(new JArrayList[() => Unit]())
    @volatile var incomplete: Boolean = false
  }

  /** How many ended execution ids are remembered, to drop registrations that arrive late. */
  private val ENDED_MEMORY = 1024

  @volatile private var active: Boolean = false

  /** Executions seen running and the jobs and stages they own, dropped when each one ends. */
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

  /** Registered once on the SparkContext, so every session and micro-batch is covered. */
  def listener: SparkListener = new ObservationListener

  /** Running executions, job owners and stage owners held, for tests. */
  private[perf] def trackedCounts: (Int, Int, Int) =
    (executions.size(), jobOwner.size(), stageOwner.size())

  /** The SQL execution the calling thread is running, if any. */
  def currentExecutionId(sc: SparkContext): Option[Long] =
    Option(sc.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)).flatMap(parseId)

  private def parseId(id: String): Option[Long] =
    try Some(id.toLong) catch { case _: NumberFormatException => None }

  /** The running execution `executionId`, created on first sight. Callers hold `this`. */
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
   * `SparkListenerSQLExecutionEnd.errorMessage` is deliberately not checked: Spark 3.3 lacks it,
   * and a scan that read every split is a valid observation even if the query failed later.
   */
  private def executionEnded(executionId: Long): Unit = {
    val state = synchronized {
      ended.add(executionId)
      if (ended.size() > ENDED_MEMORY) {
        ended.remove(ended.iterator().next())
      }
      executions.remove(executionId)
    }
    // Only entries this execution still owns: a stage id reused by a later execution stays.
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

  /** Marks the execution owning a job or stage incomplete, if it is still running. */
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
 * Starts and stops the history-backed heuristics. What it touches outside this package is passed
 * in, so it can be driven without Spark.
 *
 * @param heuristics every heuristic that learns from history
 * @param setIcebergAdvisor installs (true) or removes (false) the Iceberg split advisor
 * @param currentStore the installed store; `MetricStores.current()` in production
 */
private[perf] class HistoryLifecycle(
    heuristics: Seq[HistoryHeuristic],
    setIcebergAdvisor: Boolean => Unit,
    currentStore: () => MetricStore) extends Logging {

  /** The store in place when planning started; None while stopped. */
  private var storeAtStart: Option[MetricStore] = None
  private var advisorInstalled = false

  /**
   * Enables the heuristics when a history metrics provider is requested. Whatever an earlier
   * start left behind is stopped first, so nothing leaks from one SparkContext to the next.
   *
   * @param policy only read once a provider is requested, so a bad setting cannot matter when
   *               history is off
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
        // On the SparkContext, not a session's listenerManager: this sees every SparkSession
        // and every Structured Streaming micro-batch.
        addListener(HistoryObservations.listener)
        heuristics.foreach(_.enable(resolved))
        // Loads the Iceberg probe, which may fail on a classpath without Iceberg support.
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
   * Called once provider selection is over. Planning stays history-backed only if a provider
   * store replaced the one in place at start; every family is then declared before the first
   * query plans.
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
    // Every heuristic that learns from history. Add one here to enable it.
    heuristics = Seq(ScanSplitHeuristic),
    // The Iceberg table wrapper is loaded outside the plugin's shim class loader, so it receives
    // the split decision through a hand-off rather than calling the heuristic directly.
    setIcebergAdvisor = install => IcebergProvider.installScanSplitAdvisor(
      if (install) Some((t: String, l: Long) => ScanSplitHeuristic.learnedSplitBytes(t, l))
      else None),
    currentStore = () => MetricStores.current())

  /** At driver plugin init: enables planning when a provider is requested. */
  def start(sc: SparkContext, conf: RapidsConf): Unit =
    lifecycle.start(conf.historyMetricsProvider, HistoryPolicy.fromConf(conf), sc.addSparkListener)

  /** After the provider manager ran: planning stays on only if a provider store is installed. */
  def activate(): Unit = lifecycle.activate()

  /** Before the provider shuts down. */
  def stop(): Unit = lifecycle.stop()
}
