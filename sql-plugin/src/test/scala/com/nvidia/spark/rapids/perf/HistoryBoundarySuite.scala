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

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.ServiceLoader

import scala.collection.JavaConverters._

import com.nvidia.spark.history.HistoryMetricsProvider
import org.scalatest.funsuite.AnyFunSuite

/**
 * Keeps history-backed planning on the public side of the history metrics boundary: it may
 * compile only against the history metrics API, and must not name a storage backend, carry SQL,
 * or point at files. Providers are selected and owned by the driver plugin at runtime; the
 * build's enforcer rule bans every other history metrics artifact.
 *
 * Scans compiled classes and source text only; nothing here starts Spark. Patterns are
 * assembled from pieces so this file does not match itself.
 */
class HistoryBoundarySuite extends AnyFunSuite {

  private lazy val root: File = {
    val marker = "sql-plugin/src/main/scala/com/nvidia/spark/rapids/perf"
    Iterator.iterate(new File(System.getProperty("user.dir")).getAbsoluteFile)(_.getParentFile)
      .takeWhile(_ != null)
      .find(dir => new File(dir, marker).isDirectory)
      .getOrElse(fail(s"cannot find $marker above ${System.getProperty("user.dir")}"))
  }

  /** Everything this feature adds: the heuristic package, its tests, and the Iceberg hand-off. */
  private def featureFiles: Seq[File] = {
    val dirs = Seq(
      "sql-plugin/src/main/scala/com/nvidia/spark/rapids/perf",
      "sql-plugin/src/test/scala/com/nvidia/spark/rapids/perf",
      "sql-plugin/src/test/resources/com/nvidia/spark/rapids/perf")
    val files = dirs.flatMap(d => walk(new File(root, d))) ++ Seq(
      "iceberg-common/src/main/java/com/nvidia/spark/rapids/iceberg/spark/source/" +
        "IcebergSplitAdvisor.java",
      "sql-plugin/src/test/scala/org/apache/spark/sql/rapids/HistoryObservationsSuite.scala",
      "tests/src/test/spark350/scala/com/nvidia/spark/rapids/iceberg/spark/source/" +
        "IcebergSplitHandoffSuite.scala")
      .map(new File(root, _))
    files.foreach(f => assert(f.isFile, s"missing $f"))
    files
  }

  /** Files under `f`, skipping build output and hidden directories. */
  private def walk(f: File): Seq[File] =
    if (f.isDirectory) {
      Option(f.listFiles()).toSeq.flatten
        .filterNot(c => c.isDirectory && (c.getName == "target" || c.getName.startsWith(".")))
        .sortBy(_.getName)
        .flatMap(walk)
    } else {
      Seq(f)
    }

  private def read(f: File): String =
    new String(Files.readAllBytes(f.toPath), StandardCharsets.UTF_8)

  private def offending(files: Seq[File], patterns: Seq[String]): Seq[String] = {
    val compiled = patterns.map(_.r)
    files.flatMap { f =>
      read(f).split("\n").zipWithIndex.flatMap { case (line, n) =>
        compiled.find(_.findFirstIn(line).isDefined)
          .map(p => s"${root.toPath.relativize(f.toPath)}:${n + 1} matches /$p/")
      }
    }
  }

  /** Where a class was loaded from: a build output directory. */
  private def outputDir(c: Class[_]): File =
    new File(c.getProtectionDomain.getCodeSource.getLocation.toURI)

  /** Every `classes` and `test-classes` directory under a `target` directory of this checkout. */
  private def buildOutputs(dir: File, underTarget: Boolean): Seq[File] = {
    val name = dir.getName
    if (underTarget && (name == "classes" || name == "test-classes")) {
      Seq(dir)
    } else {
      Option(dir.listFiles()).toSeq.flatten
        .filter(c => c.isDirectory && !c.getName.startsWith(".") && c.getName != "src")
        .flatMap(c => buildOutputs(c, underTarget || c.getName == "target"))
    }
  }

  private def classFiles(dir: File): Seq[File] =
    walk(dir).filter(_.getName.endsWith(".class"))

  /**
   * Whether a class file refers to a class in a sub-package of the history metrics API. The API
   * is one flat package, so a lower-case segment after it means some other package. Class
   * references and descriptors carry the slash-separated name; a string handed to reflection
   * carries the dotted one.
   */
  private val apiPrefixes = Seq("com/nvidia/spark/" + "history/", "com.nvidia.spark." + "history.")

  private def refersToNonApiHistoryPackage(bytes: Array[Byte]): Boolean = {
    // One char per byte, so offsets match and ASCII names compare as text.
    val text = new String(bytes, StandardCharsets.ISO_8859_1)
    apiPrefixes.exists { prefix =>
      Iterator.iterate(text.indexOf(prefix))(i => text.indexOf(prefix, i + 1))
        .takeWhile(_ >= 0)
        .exists { i =>
          val next = i + prefix.length
          next < text.length && text.charAt(next) >= 'a' && text.charAt(next) <= 'z'
        }
    }
  }

  test("compiled classes refer only to the history metrics API, never a provider package") {
    val own = Seq(outputDir(classOf[ScanSplitHeuristic]), outputDir(getClass))
    own.foreach(d => assert(d.isDirectory, s"not a build output directory: $d"))
    val dirs = (own ++ buildOutputs(root, underTarget = false)).map(_.getCanonicalFile).distinct
    val classes = dirs.flatMap(classFiles)
    Seq("com/nvidia/spark/rapids/perf/MetricHistory.class",
        "com/nvidia/spark/rapids/perf/HistoryBoundarySuite.class").foreach { c =>
      assert(own.exists(d => new File(d, c).isFile), s"$c was not scanned")
    }
    val found = classes.filter(f => refersToNonApiHistoryPackage(Files.readAllBytes(f.toPath)))
      .map(f => root.toPath.relativize(f.toPath).toString)
    assert(found.isEmpty, found.mkString("\n"))
  }

  test("the bytecode check recognizes a provider package reference") {
    def bytes(s: String): Array[Byte] = s.getBytes(StandardCharsets.US_ASCII)
    val api = "com/nvidia/spark/" + "history/"
    assert(refersToNonApiHistoryPackage(bytes(s"\u0001L${api}local/Store;")))
    assert(!refersToNonApiHistoryPackage(bytes(s"\u0001L${api}MetricStore;")))
    assert(!refersToNonApiHistoryPackage(bytes(api)))
    val dotted = "com.nvidia.spark." + "history."
    assert(refersToNonApiHistoryPackage(bytes(s"\u0001${dotted}local.Store")))
    assert(!refersToNonApiHistoryPackage(bytes(s"\u0001${dotted}MetricStores")))
  }

  test("no storage backend, SQL, file path or prototype naming in this feature") {
    val banned = Seq(
      "(?i)" + "sql" + "ite", "(?i)" + "jd" + "bc", "(?i)" + "postg" + "res", "(?i)" + "my" + "sql",
      "(?i)" + "der" + "by", "(?i)" + "rocks" + "db",
      // file names and the provider's own path setting
      "\\." + "db(?![\\w.])", "history\\.metrics\\." + "local",
      // SQL statements
      "\\b(SEL" + "ECT|INS" + "ERT|UPD" + "ATE|DEL" + "ETE|CRE" + "ATE)\\s+[A-Z*]",
      // the earlier prototype's family name and dimension
      "scan\\.expansion\\." + "ratio", "\"rel" + "ation\"")
    val found = offending(featureFiles, banned)
    assert(found.isEmpty, found.mkString("\n"))
  }

  test("no history metrics provider on the test classpath") {
    val providers = ServiceLoader.load(classOf[HistoryMetricsProvider]).iterator().asScala
      .map(_.getClass.getName).toList
    assert(providers.isEmpty, providers.mkString(", "))
  }
}
