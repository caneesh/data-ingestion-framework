package com.hcsc.generic.ingest.jdbc.watermark

import com.hcsc.generic.ingest.jdbc.{BoundaryConvention, JdbcSource, JdbcSourceConfig, WatermarkConfig, WatermarkType, WatermarkUpperBound}
import com.hcsc.generic.ingest.jdbc.dialect.SqlServerDialect
import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite

class HalfOpenConventionTest extends AnyFunSuite {

  private def wmConfig(convention: String) = WatermarkConfig(
    watermarkType = WatermarkType.Timestamp,
    columns = Seq("modified_ts"),
    columnTypes = Seq(WatermarkType.Timestamp),
    initialValue = "1900-01-01 00:00:00",
    overlap = None,
    storeType = "memory",
    storeDatabase = None,
    storeTable = "ingest_watermarks",
    upperBound = WatermarkUpperBound.SourceClock,
    clockZone = Some("UTC"),
    boundaryConvention = convention)

  test("HALF_OPEN renders [lower, upper): >= on both edges") {
    val predicate = Watermarks.boundedPredicate(wmConfig(BoundaryConvention.HalfOpen),
      SqlServerDialect,
      WatermarkValue(Seq("2026-01-01 09:00:00")), WatermarkValue(Seq("2026-01-02 09:00:00")))
    assert(predicate ==
      "([modified_ts] >= '2026-01-01 09:00:00.0') AND NOT ([modified_ts] >= '2026-01-02 09:00:00.0')")
  }

  test("LEGACY stays wire-identical: (lower, upper] with strict >") {
    val predicate = Watermarks.boundedPredicate(wmConfig(BoundaryConvention.Legacy),
      SqlServerDialect,
      WatermarkValue(Seq("2026-01-01 09:00:00")), WatermarkValue(Seq("2026-01-02 09:00:00")))
    assert(predicate ==
      "([modified_ts] > '2026-01-01 09:00:00.0') AND NOT ([modified_ts] > '2026-01-02 09:00:00.0')")
  }

  private def parse(extra: String) = JdbcSourceConfig.parse(ConfigFactory.parseString(
    s"""type = "jdbc"
       |url = "jdbc:sqlserver://h;databaseName=d"
       |mode = "INCREMENTAL"
       |table = "dbo.claims"
       |health_check { enabled = false }
       |incremental {
       |  watermark_type = "TIMESTAMP"
       |  watermark_columns = ["modified_ts"]
       |  initial_value = "1900-01-01 00:00:00"
       |  watermark_store { type = "memory" }
       |  $extra
       |}""".stripMargin))

  test("HALF_OPEN parse gates: requires SOURCE_CLOCK, rejects COMPOSITE") {
    val ok = parse(
      """boundary_convention = "HALF_OPEN"
        |upper_bound = "SOURCE_CLOCK"
        |clock_zone = "UTC"""".stripMargin)
    assert(ok.watermark.get.boundaryConvention == BoundaryConvention.HalfOpen)

    val maxValue = intercept[IllegalArgumentException] {
      parse("""boundary_convention = "HALF_OPEN"""")
    }
    assert(maxValue.getMessage.contains("SOURCE_CLOCK"),
      "a MAX_VALUE upper would skip the max row permanently")

    // COMPOSITE + SOURCE_CLOCK is already impossible (the source clock is a
    // single timestamp), so the composite gate is exercised on the
    // MAX_VALUE path, where it fires before the SOURCE_CLOCK requirement.
    val composite = intercept[IllegalArgumentException] {
      parse(
        """boundary_convention = "HALF_OPEN"
          |watermark_type = "COMPOSITE"
          |watermark_columns = ["modified_ts", "claim_id"]
          |column_types = ["TIMESTAMP", "NUMERIC"]""".stripMargin)
    }
    assert(composite.getMessage.contains("scalar"))

    intercept[IllegalArgumentException] { parse("""boundary_convention = "OPEN_OPEN"""") }
  }

  test("clock drift diagnostic computes signed source-minus-driver millis") {
    val now = java.sql.Timestamp.valueOf("2026-08-01 10:00:00")
      .toLocalDateTime.atZone(java.time.ZoneOffset.UTC).toInstant.toEpochMilli
    // Source clock 10 minutes behind the driver
    assert(JdbcSource.clockDriftMillis(WatermarkType.Timestamp, Some("UTC"),
      "2026-08-01 09:50:00", now).contains(-600000L))
    // DATETIMEOFFSET in the SQL Server literal shape
    assert(JdbcSource.clockDriftMillis(WatermarkType.DatetimeOffset, None,
      "2026-08-01 10:00:00.0000000 +00:00", now).contains(0L))
    // Unparseable input never fails the diagnostic
    assert(JdbcSource.clockDriftMillis(WatermarkType.Timestamp, Some("UTC"),
      "not-a-clock", now).isEmpty)
  }
}
