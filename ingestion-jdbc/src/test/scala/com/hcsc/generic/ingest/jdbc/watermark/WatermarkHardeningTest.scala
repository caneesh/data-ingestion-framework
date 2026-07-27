package com.hcsc.generic.ingest.jdbc.watermark

import com.hcsc.generic.ingest.jdbc.{JdbcSource, JdbcSourceConfig, SharedSparkSession, WatermarkConfig, WatermarkType}
import com.hcsc.generic.ingest.jdbc.dialect.GenericDialect
import com.hcsc.generic.ingest.jdbc.read.SqlFailureClassifier
import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite

import java.sql.DriverManager

/** STRING watermarks (approval-gated), commit detail persistence and the
  * LOCK_TIMEOUT classification. */
class WatermarkHardeningTest extends AnyFunSuite with SharedSparkSession {

  // ---------------------------------------------------------------------------
  // STRING watermark codec (approval-gated)
  // ---------------------------------------------------------------------------

  private def stringCfg = WatermarkConfig(
    WatermarkType.StringType, Seq("batch_id"), Seq(WatermarkType.StringType),
    "BATCH-000", None, "memory", None, "ingest_watermarks")

  test("STRING watermark requires explicit approval in config") {
    def parse(extra: String) = JdbcSourceConfig.parse(ConfigFactory.parseString(
      s"""url = "jdbc:sqlserver://h;databaseName=d"
         |table = "t"
         |mode = "INCREMENTAL"
         |incremental {
         |  watermark_type = "STRING"
         |  watermark_columns = ["batch_id"]
         |  initial_value = "BATCH-000"
         |  $extra
         |}""".stripMargin))

    assert(intercept[IllegalArgumentException] { parse("") }
      .getMessage.contains("allow_string_watermark"))
    assert(parse("allow_string_watermark = true").watermark.get.watermarkType == "STRING")
  }

  test("STRING predicate quotes and escapes; ordering is lexicographic; overlap rejected") {
    val p = Watermarks.predicate(stringCfg, GenericDialect, WatermarkValue(Seq("BATCH-042")))
    assert(p == "\"batch_id\" > 'BATCH-042'")

    assert(Watermarks.compare(stringCfg,
      WatermarkValue(Seq("BATCH-100")), WatermarkValue(Seq("BATCH-099"))) > 0)

    val escaped = Watermarks.predicate(stringCfg, GenericDialect, WatermarkValue(Seq("O'Brien")))
    assert(escaped == "\"batch_id\" > 'O''Brien'")

    val withOverlap = stringCfg.copy(overlap = Some(BigDecimal(5)))
    assert(intercept[IllegalArgumentException] {
      Watermarks.predicate(withOverlap, GenericDialect, WatermarkValue(Seq("BATCH-042")))
    }.getMessage.contains("JDBC_004"))
  }

  // ---------------------------------------------------------------------------
  // Commit detail: lower bound + query hash persisted with each commit
  // ---------------------------------------------------------------------------

  test("watermark commits persist the window lower bound and executed query hash") {
    InMemoryWatermarkStore.clear()
    val url = "jdbc:h2:mem:wm_detail_tests;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false"
    Class.forName("org.h2.Driver")
    val conn = DriverManager.getConnection(url, "sa", "")
    try {
      val stmt = conn.createStatement()
      try {
        stmt.execute("DROP TABLE IF EXISTS d_events")
        stmt.execute("CREATE TABLE d_events (id INT, modified_ts TIMESTAMP)")
        stmt.execute("INSERT INTO d_events VALUES (1, '2026-01-01 10:00:00')")
      } finally stmt.close()
    } finally conn.close()

    val conf = ConfigFactory.parseString(
      s"""
         |type = "jdbc"
         |url = "$url"
         |dialect = "generic"
         |driver = "org.h2.Driver"
         |auth { user = "sa", password = { provider = "inline", value = "" } }
         |health_check { enabled = false }
         |mode = "INCREMENTAL"
         |table = "d_events"
         |entity = "detail_feed"
         |incremental {
         |  watermark_type = "NUMERIC"
         |  watermark_columns = ["id"]
         |  initial_value = "0"
         |  watermark_store { type = "memory" }
         |}
      """.stripMargin)

    val df = JdbcSource.read(spark, conf)
    JdbcSource.advanceWatermark(spark, conf, "detail_feed", "run-d1", df)

    val detail = InMemoryWatermarkStore.latestDetail("detail_feed").get
    assert(detail.lower.contains("0"), "window lower bound persisted")
    assert(detail.queryHash.exists(_.length == 12), "executed query hash persisted")
  }

  // ---------------------------------------------------------------------------
  // LOCK_TIMEOUT classification
  // ---------------------------------------------------------------------------

  test("SQL Server lock timeout (1222) is its own non-retryable category") {
    val c = SqlFailureClassifier.classify(new java.sql.SQLException("lock timeout", "S0001", 1222))
    assert(c == SqlFailureClassifier.LockTimeout)
    assert(!c.retryable, "lock timeouts retry only with explicit approval, never blindly")
    // Query timeout remains distinct and retryable
    assert(SqlFailureClassifier.classify(new java.sql.SQLException("t/o", null, -2))
      == SqlFailureClassifier.Timeout)
  }
}
