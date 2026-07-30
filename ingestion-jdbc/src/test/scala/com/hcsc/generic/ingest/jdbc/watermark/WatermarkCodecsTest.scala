package com.hcsc.generic.ingest.jdbc.watermark

import com.hcsc.generic.ingest.jdbc.{WatermarkConfig, WatermarkType}
import com.hcsc.generic.ingest.jdbc.dialect.{GenericDialect, SqlServerDialect}
import org.scalatest.funsuite.AnyFunSuite

/** Typed watermark codecs for Azure SQL column types: DATE, DATETIMEOFFSET
  * and ROWVERSION, alongside the existing TIMESTAMP and NUMERIC. */
class WatermarkCodecsTest extends AnyFunSuite {

  private def cfg(t: String, overlap: Option[BigDecimal] = None) = WatermarkConfig(
    t, Seq("wm"), Seq(t), "unused", overlap, "memory", None, "ingest_watermarks")

  test("DATE: predicate literal, overlap in days, ordering") {
    val p = Watermarks.predicate(cfg(WatermarkType.Date), GenericDialect, WatermarkValue(Seq("2026-03-15")))
    assert(p == "\"wm\" > '2026-03-15'")

    val overlapped = Watermarks.predicate(cfg(WatermarkType.Date, Some(BigDecimal(7))),
      GenericDialect, WatermarkValue(Seq("2026-03-15")))
    assert(overlapped == "\"wm\" > '2026-03-08'")

    assert(Watermarks.compare(cfg(WatermarkType.Date),
      WatermarkValue(Seq("2026-03-15")), WatermarkValue(Seq("2026-02-01"))) > 0)

    intercept[IllegalArgumentException] {
      Watermarks.predicate(cfg(WatermarkType.Date), GenericDialect, WatermarkValue(Seq("03/15/2026")))
    }
  }

  test("DATETIMEOFFSET: offset-aware parsing, literal and overlap in seconds") {
    val v = WatermarkValue(Seq("2026-03-15 10:00:00.000 +05:00"))
    val p = Watermarks.predicate(cfg(WatermarkType.DatetimeOffset), SqlServerDialect, v)
    assert(p == "[wm] > '2026-03-15 10:00:00.0000000 +05:00'")

    // ISO-8601 input is accepted too
    val iso = Watermarks.predicate(cfg(WatermarkType.DatetimeOffset), SqlServerDialect,
      WatermarkValue(Seq("2026-03-15T10:00:00+05:00")))
    assert(iso == "[wm] > '2026-03-15 10:00:00.0000000 +05:00'")

    val overlapped = Watermarks.predicate(cfg(WatermarkType.DatetimeOffset, Some(BigDecimal(3600))),
      SqlServerDialect, v)
    assert(overlapped == "[wm] > '2026-03-15 09:00:00.0000000 +05:00'")

    // Offset participates in ordering: 10:00+05:00 == 05:00Z < 06:00Z
    assert(Watermarks.compare(cfg(WatermarkType.DatetimeOffset),
      WatermarkValue(Seq("2026-03-15 10:00:00.000 +05:00")),
      WatermarkValue(Seq("2026-03-15 06:00:00.000 +00:00"))) < 0)

    intercept[IllegalArgumentException] {
      Watermarks.predicate(cfg(WatermarkType.DatetimeOffset), SqlServerDialect,
        WatermarkValue(Seq("2026-03-15 10:00:00"))) // no offset
    }
  }

  test("ROWVERSION: hex parsing, 0x literal, unsigned ordering, overlap floor") {
    val p = Watermarks.predicate(cfg(WatermarkType.RowVersion), SqlServerDialect,
      WatermarkValue(Seq("00000000000007D0")))
    assert(p == "[wm] > 0x00000000000007D0")

    // 0x prefix accepted; comparison is numeric, not lexical
    assert(Watermarks.compare(cfg(WatermarkType.RowVersion),
      WatermarkValue(Seq("0x0A")), WatermarkValue(Seq("9"))) > 0)

    val overlapped = Watermarks.predicate(cfg(WatermarkType.RowVersion, Some(BigDecimal(100))),
      SqlServerDialect, WatermarkValue(Seq("00000000000007D0")))
    assert(overlapped == "[wm] > 0x000000000000076C") // 2000 - 100 = 1900

    // Overlap can never underflow below zero
    val floored = Watermarks.predicate(cfg(WatermarkType.RowVersion, Some(BigDecimal(100))),
      SqlServerDialect, WatermarkValue(Seq("10")))
    assert(floored == "[wm] > 0x0000000000000000")

    intercept[IllegalArgumentException] {
      Watermarks.predicate(cfg(WatermarkType.RowVersion), SqlServerDialect,
        WatermarkValue(Seq("not-hex")))
    }
  }

  test("composite watermarks may mix the new codecs") {
    val composite = WatermarkConfig(
      WatermarkType.Composite, Seq("d", "rv"),
      Seq(WatermarkType.Date, WatermarkType.RowVersion),
      "2026-01-01|0", None, "memory", None, "ingest_watermarks")
    val p = Watermarks.predicate(composite, SqlServerDialect,
      WatermarkValue(Seq("2026-01-01", "FF")))
    assert(p == "(([d] > '2026-01-01') OR ([d] = '2026-01-01' AND [rv] > 0x00000000000000FF))")
  }

  test("WatermarkValue round-trips values containing the delimiter and backslashes") {
    // Fix: an unescaped '|' in a composite STRING value corrupted the stored
    // arity and jammed the entity with JDBC_004 on every later run.
    val tricky = Seq(
      WatermarkValue(Seq("2026-01-01 10:00:00.0", "REGION|042")),
      WatermarkValue(Seq("a\\b", "c|d", "trailing\\")),
      WatermarkValue(Seq("", "|", "\\|"))
    )
    tricky.foreach { v =>
      val roundTripped = WatermarkValue.deserialize(v.serialized)
      assert(roundTripped == v, s"round-trip failed for $v via '${v.serialized}'")
    }

    // Legacy stored values without escapes deserialize identically
    assert(WatermarkValue.deserialize("2026-01-01 10:00:00.0|42") ==
      WatermarkValue(Seq("2026-01-01 10:00:00.0", "42")))
  }

  test("TIMESTAMP overlap arithmetic is zone-naive across DST transitions") {
    // Fix: epoch-millis subtraction through java.sql.Timestamp re-rendered
    // wall-clock fields in the JVM default zone — an overlap spanning the
    // spring-forward gap shifted by an hour.
    val prevTz = java.util.TimeZone.getDefault
    java.util.TimeZone.setDefault(java.util.TimeZone.getTimeZone("America/Chicago"))
    try {
      // 2026-03-08 02:00 does not exist in America/Chicago (spring forward);
      // zone-naive subtraction must still produce exactly 03:30 - 3600s = 02:30.
      val result = Watermarks.applyOverlap(
        WatermarkType.Timestamp, "2026-03-08 03:30:00.5", Some(BigDecimal(3600)))
      assert(result == "2026-03-08 02:30:00.5", s"got $result")

      // Fractional seconds survive; no DST involvement either
      val plain = Watermarks.applyOverlap(
        WatermarkType.Timestamp, "2026-06-15 10:00:00.123", Some(BigDecimal(90)))
      assert(plain == "2026-06-15 09:58:30.123", s"got $plain")
    } finally java.util.TimeZone.setDefault(prevTz)
  }
}
