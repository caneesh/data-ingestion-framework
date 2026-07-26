package com.hcsc.generic.ingest.jdbc.watermark

import com.hcsc.generic.ingest.jdbc.{SharedSparkSession, WatermarkConfig, WatermarkType}
import com.hcsc.generic.ingest.jdbc.dialect.{GenericDialect, SqlServerDialect}
import org.scalatest.funsuite.AnyFunSuite

class WatermarksTest extends AnyFunSuite with SharedSparkSession {

  import spark.implicits._

  private def tsConfig(overlap: Option[BigDecimal] = None) = WatermarkConfig(
    WatermarkType.Timestamp, Seq("modified_ts"), Seq(WatermarkType.Timestamp),
    "1900-01-01 00:00:00", overlap, "memory", None, "ingest_watermarks")

  private def numConfig(overlap: Option[BigDecimal] = None) = WatermarkConfig(
    WatermarkType.Numeric, Seq("id"), Seq(WatermarkType.Numeric),
    "0", overlap, "memory", None, "ingest_watermarks")

  private def compositeConfig = WatermarkConfig(
    WatermarkType.Composite, Seq("modified_ts", "id"),
    Seq(WatermarkType.Timestamp, WatermarkType.Numeric),
    "1900-01-01 00:00:00|0", None, "memory", None, "ingest_watermarks")

  test("timestamp predicate quotes the literal and column") {
    val p = Watermarks.predicate(tsConfig(), SqlServerDialect, WatermarkValue(Seq("2026-01-15 10:30:00")))
    assert(p == "[modified_ts] > '2026-01-15 10:30:00.0'")
  }

  test("timestamp overlap widens the window backwards") {
    val p = Watermarks.predicate(tsConfig(Some(BigDecimal(300))), GenericDialect,
      WatermarkValue(Seq("2026-01-15 10:30:00")))
    assert(p == "\"modified_ts\" > '2026-01-15 10:25:00.0'")
  }

  test("numeric predicate subtracts overlap without quoting") {
    val p = Watermarks.predicate(numConfig(Some(BigDecimal(10))), GenericDialect, WatermarkValue(Seq("500")))
    assert(p == "\"id\" > 490")
  }

  test("composite predicate is lexicographic with overlap on the first column") {
    val p = Watermarks.predicate(compositeConfig, SqlServerDialect,
      WatermarkValue(Seq("2026-01-15 10:30:00", "42")))
    assert(p == "(([modified_ts] > '2026-01-15 10:30:00.0') OR " +
      "([modified_ts] = '2026-01-15 10:30:00.0' AND [id] > 42))")
  }

  test("corrupt stored watermark values fail with JDBC_004, never build SQL") {
    val ex = intercept[IllegalArgumentException] {
      Watermarks.predicate(tsConfig(), GenericDialect, WatermarkValue(Seq("1' OR '1'='1")))
    }
    assert(ex.getMessage.contains("JDBC_004"))

    val ex2 = intercept[IllegalArgumentException] {
      Watermarks.predicate(numConfig(), GenericDialect, WatermarkValue(Seq("42; DROP TABLE x")))
    }
    assert(ex2.getMessage.contains("JDBC_004"))
  }

  test("value arity must match the configured columns") {
    val ex = intercept[IllegalArgumentException] {
      Watermarks.predicate(compositeConfig, GenericDialect, WatermarkValue(Seq("2026-01-01 00:00:00")))
    }
    assert(ex.getMessage.contains("JDBC_004"))
  }

  test("computeNext finds the max watermark in the extracted data") {
    val df = Seq(
      ("2026-01-01 10:00:00", "a"),
      ("2026-03-01 10:00:00", "b"),
      ("2026-02-01 10:00:00", "c")
    ).toDF("modified_ts", "payload")
    val next = Watermarks.computeNext(df, tsConfig()).get
    assert(next.values.head.startsWith("2026-03-01 10:00:00"))
  }

  test("computeNext on composite uses lexicographic ordering") {
    val df = Seq(
      ("2026-01-01 10:00:00", 99L),
      ("2026-02-01 10:00:00", 5L),
      ("2026-02-01 10:00:00", 7L)
    ).toDF("modified_ts", "id")
    val next = Watermarks.computeNext(df, compositeConfig).get
    assert(next.values.head.startsWith("2026-02-01 10:00:00"))
    assert(next.values(1) == "7")
  }

  test("computeNext returns None for empty extracts and fails for absent columns") {
    val empty = Seq.empty[(String, String)].toDF("modified_ts", "payload")
    assert(Watermarks.computeNext(empty, tsConfig()).isEmpty)

    val wrong = Seq(("x", "y")).toDF("a", "b")
    val ex = intercept[IllegalArgumentException] { Watermarks.computeNext(wrong, tsConfig()) }
    assert(ex.getMessage.contains("JDBC_004"))
  }

  test("max is advance-only in both directions") {
    val older = WatermarkValue(Seq("2026-01-01 00:00:00"))
    val newer = WatermarkValue(Seq("2026-06-01 00:00:00"))
    assert(Watermarks.max(tsConfig(), older, newer) == newer)
    assert(Watermarks.max(tsConfig(), newer, older) == newer)
  }

  test("serialization round-trips composite values") {
    val v = WatermarkValue(Seq("2026-01-01 00:00:00", "42"))
    assert(WatermarkValue.deserialize(v.serialized) == v)
  }

  test("in-memory store keeps append-only history with latest first") {
    InMemoryWatermarkStore.clear()
    InMemoryWatermarkStore.record("e1", WatermarkValue(Seq("1")), "run1")
    InMemoryWatermarkStore.record("e1", WatermarkValue(Seq("2")), "run2")
    assert(InMemoryWatermarkStore.latest("e1").get.serialized == "2")
    assert(InMemoryWatermarkStore.entries("e1").map(_._2) == List("run2", "run1"))
    assert(InMemoryWatermarkStore.latest("other").isEmpty)
  }

  test("bounded predicate uses overlap only on the lower edge") {
    val p = Watermarks.boundedPredicate(tsConfig(Some(BigDecimal(60))), GenericDialect,
      WatermarkValue(Seq("2026-01-15 10:00:00")), WatermarkValue(Seq("2026-01-15 12:00:00")))
    assert(p == "(\"modified_ts\" > '2026-01-15 09:59:00.0') AND " +
      "NOT (\"modified_ts\" > '2026-01-15 12:00:00.0')")
  }

  test("bounded composite predicate keeps the tie-breaker on both edges") {
    val p = Watermarks.boundedPredicate(compositeConfig, SqlServerDialect,
      WatermarkValue(Seq("2026-01-01 00:00:00", "10")),
      WatermarkValue(Seq("2026-02-01 00:00:00", "20")))
    assert(p.contains("[modified_ts] > '2026-01-01 00:00:00.0'"))
    assert(p.contains("NOT ((([modified_ts] > '2026-02-01 00:00:00.0')"))
    assert(p.contains("[id] > 20"))
  }

  test("optimistic version check: stale version fails with JDBC_005") {
    InMemoryWatermarkStore.clear()
    InMemoryWatermarkStore.recordIfVersion("cas", WatermarkValue(Seq("1")), "runA", 0L)
    assert(InMemoryWatermarkStore.latestVersioned("cas").get.version == 1L)

    // A second run that read version 0 must not be able to commit
    val ex = intercept[WatermarkConflictException] {
      InMemoryWatermarkStore.recordIfVersion("cas", WatermarkValue(Seq("2")), "runB", 0L)
    }
    assert(ex.getMessage.contains("JDBC_005"))

    // The run holding the current version commits fine
    InMemoryWatermarkStore.recordIfVersion("cas", WatermarkValue(Seq("3")), "runC", 1L)
    assert(InMemoryWatermarkStore.latestVersioned("cas").get == VersionedWatermark(WatermarkValue(Seq("3")), 2L))
  }
}
