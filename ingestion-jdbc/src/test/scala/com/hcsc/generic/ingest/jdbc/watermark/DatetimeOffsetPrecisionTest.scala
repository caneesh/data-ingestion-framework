package com.hcsc.generic.ingest.jdbc.watermark

import com.hcsc.generic.ingest.jdbc.{WatermarkConfig, WatermarkType}
import com.hcsc.generic.ingest.jdbc.dialect.{GenericDialect, SqlServerDialect}
import org.scalatest.funsuite.AnyFunSuite

/** DATETIMEOFFSET(7) precision: predicates and captured values must carry
  * all 7 fractional digits — millisecond truncation excluded the true max
  * row from its own bounded window, deferring it to the next run. */
class DatetimeOffsetPrecisionTest extends AnyFunSuite {

  private def cfg = WatermarkConfig(
    WatermarkType.DatetimeOffset, Seq("wm"), Seq(WatermarkType.DatetimeOffset),
    "1900-01-01 00:00:00.0000000 +00:00", None, "memory", None, "ingest_watermarks")

  test("literals preserve 100ns precision end to end") {
    assert(SqlServerDialect.renderLiteral("DATETIMEOFFSET", "2026-03-15 10:00:00.1234567 +05:00") ==
      "'2026-03-15 10:00:00.1234567 +05:00'")
    // Lower precision inputs normalize by zero-padding, never truncating
    assert(GenericDialect.renderLiteral("DATETIMEOFFSET", "2026-03-15 10:00:00.123 +05:00") ==
      "'2026-03-15 10:00:00.1230000 +05:00'")
  }

  test("predicates keep the full captured precision") {
    val p = Watermarks.predicate(cfg, SqlServerDialect,
      WatermarkValue(Seq("2026-03-15 10:00:00.1234567 +05:00")))
    assert(p == "[wm] > '2026-03-15 10:00:00.1234567 +05:00'")
  }

  test("100ns-adjacent values are ordered, not equal") {
    assert(Watermarks.compare(cfg,
      WatermarkValue(Seq("2026-03-15 10:00:00.1234567 +05:00")),
      WatermarkValue(Seq("2026-03-15 10:00:00.1234566 +05:00"))) > 0)
  }

  test("legacy 3-digit stored watermarks still parse and round-trip") {
    val p = Watermarks.predicate(cfg, SqlServerDialect,
      WatermarkValue(Seq("2026-01-01 08:30:00.500 +00:00")))
    assert(p == "[wm] > '2026-01-01 08:30:00.5000000 +00:00'")
  }
}
