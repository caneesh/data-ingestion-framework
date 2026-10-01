package com.hcsc.generic.ingest.watermark

import org.scalatest.funsuite.AnyFunSuite

/** Escaped-pipe codec for stored watermarks. Copied from the jdbc suites
  * (WatermarkCodecsTest, WatermarksTest) when the type moved to core; the
  * jdbc originals are retained for now so that module's test count is
  * unchanged by the move. */
class WatermarkValueTest extends AnyFunSuite {

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

  test("serialization round-trips composite values") {
    val v = WatermarkValue(Seq("2026-01-01 00:00:00", "42"))
    assert(WatermarkValue.deserialize(v.serialized) == v)
  }
}
