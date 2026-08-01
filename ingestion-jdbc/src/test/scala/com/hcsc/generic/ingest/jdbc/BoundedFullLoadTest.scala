package com.hcsc.generic.ingest.jdbc

import com.hcsc.generic.ingest.jdbc.dialect.SqlServerDialect
import com.hcsc.generic.ingest.jdbc.read.QueryBuilder
import com.hcsc.generic.ingest.jdbc.watermark.{WatermarkValue, Watermarks}
import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite

/** FULL_THEN_INCREMENTAL seed pulls are bounded at the captured cutoff so
  * the handoff never double-loads rows modified during the full load. */
class BoundedFullLoadTest extends AnyFunSuite {

  private def cfg(hocon: String) = JdbcSourceConfig.parse(ConfigFactory.parseString(hocon))

  private val fullWithSeed =
    """type = "jdbc"
      |url = "jdbc:sqlserver://h;databaseName=d"
      |mode = "FULL_TABLE"
      |table = "dbo.claims"
      |health_check { enabled = false }
      |incremental {
      |  watermark_type = "TIMESTAMP"
      |  watermark_columns = ["modified_ts"]
      |  initial_value = "1900-01-01 00:00:00"
      |  watermark_store { type = "memory" }
      |}""".stripMargin

  test("the cutoff predicate is upper-only and exact (no overlap widening)") {
    val parsed = cfg(fullWithSeed)
    val predicate = Watermarks.upperBoundPredicate(parsed.watermark.get, SqlServerDialect,
      WatermarkValue(Seq("2026-01-05 10:00:00")))
    assert(predicate == "NOT ([modified_ts] > '2026-01-05 10:00:00.0')")
  }

  test("FULL_TABLE with a cutoff renders a bounded subquery; without one, the bare table") {
    val parsed = cfg(fullWithSeed)
    assert(QueryBuilder.dbtable(parsed, None) == "dbo.claims",
      "no predicate keeps the bare-table fast path")
    assert(QueryBuilder.dbtable(parsed, Some("NOT ([modified_ts] > '2026-01-05 10:00:00.0')")) ==
      "(SELECT * FROM dbo.claims WHERE (NOT ([modified_ts] > '2026-01-05 10:00:00.0'))) src")
  }

  test("bounded seeding is FULL_TABLE-only: SELECT_QUERY rejects incremental blocks at parse") {
    // rejectStrayIncremental: only FULL_TABLE parses with an incremental
    // block, so the bounded-seed path cannot exist for other modes.
    val ex = intercept[IllegalArgumentException] {
      cfg(fullWithSeed.replace("\"FULL_TABLE\"", "\"SELECT_QUERY\""))
    }
    assert(ex.getMessage.contains("JDBC_003"))
  }

  test("bound_full_load parses with a safe default of true") {
    assert(cfg(fullWithSeed).watermark.get.boundFullLoad)
    assert(!cfg(fullWithSeed.replace("initial_value = \"1900-01-01 00:00:00\"",
      "initial_value = \"1900-01-01 00:00:00\", bound_full_load = false"))
      .watermark.get.boundFullLoad)
  }
}
