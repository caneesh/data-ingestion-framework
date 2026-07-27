package com.hcsc.generic.ingest.jdbc

import com.hcsc.generic.ingest.jdbc.watermark.{InMemoryWatermarkStore, WatermarkConflictException}
import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite

import java.sql.DriverManager

/**
  * Regression tests for the expert-review findings on the bounded
  * incremental window:
  *  - the driver-side upper capture must scope to the SAME filters as the
  *    extraction query (structured filters, not just the legacy where)
  *  - read windows are per (entity, run), so concurrent runs of one entity
  *    cannot commit each other's captured upper
  */
class IncrementalWindowFixesTest extends AnyFunSuite with SharedSparkSession {

  private val url = "jdbc:h2:mem:window_fixes;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false"

  private def execute(statements: String*): Unit = {
    Class.forName("org.h2.Driver")
    val conn = DriverManager.getConnection(url, "sa", "")
    try {
      val stmt = conn.createStatement()
      try statements.foreach(stmt.execute) finally stmt.close()
    } finally conn.close()
  }

  private def conf(entity: String, runId: Option[String]) = {
    val base = ConfigFactory.parseString(
      s"""
         |type = "jdbc"
         |url = "$url"
         |dialect = "generic"
         |driver = "org.h2.Driver"
         |auth { user = "sa", password = { provider = "inline", value = "" } }
         |health_check { enabled = false }
         |mode = "INCREMENTAL"
         |table = "w_claims"
         |entity = "$entity"
         |filters = [ { column = "category", operator = "=", value = "A", type = "STRING" } ]
         |incremental {
         |  watermark_type = "NUMERIC"
         |  watermark_columns = ["id"]
         |  initial_value = "0"
         |  watermark_store { type = "memory" }
         |}
      """.stripMargin)
    runId.fold(base)(r => base.withValue("run_id",
      com.typesafe.config.ConfigValueFactory.fromAnyRef(r)))
  }

  test("captured upper respects structured filters: watermark never advances past the filtered subset") {
    InMemoryWatermarkStore.clear()
    execute(
      "DROP TABLE IF EXISTS w_claims",
      "CREATE TABLE w_claims (id INT, category VARCHAR(8))",
      "INSERT INTO w_claims VALUES (1,'A'),(2,'A'),(3,'A'),(100,'B'),(200,'B')")

    val first = JdbcSource.read(spark, conf("filtered_feed", Some("run-1")))
    assert(first.count() == 3, "first run extracts only category A")
    JdbcSource.advanceWatermark(spark, conf("filtered_feed", Some("run-1")),
      "filtered_feed", "run-1", first)

    // The committed watermark is the FILTERED max (3), not the global max
    // (200) — the old behavior advanced to 200 and every later run read 0
    // rows forever.
    assert(InMemoryWatermarkStore.latestVersioned("filtered_feed").get.value.serialized == "3")

    execute("INSERT INTO w_claims VALUES (4,'A'),(300,'B')")
    val second = JdbcSource.read(spark, conf("filtered_feed", Some("run-2")))
    assert(second.collect().map(_.getInt(0)).toSet == Set(4),
      "second run picks up exactly the new category-A row")
  }

  test("read windows are per run: an interleaved read cannot poison another run's commit") {
    InMemoryWatermarkStore.clear()
    execute(
      "DROP TABLE IF EXISTS w_claims",
      "CREATE TABLE w_claims (id INT, category VARCHAR(8))",
      "INSERT INTO w_claims VALUES (1,'A'),(2,'A')")

    // Run 1 reads (captured upper = 2) ...
    val run1Df = JdbcSource.read(spark, conf("racing_feed", Some("r1")))
    assert(run1Df.count() == 2)

    // ... then run 2 reads AFTER more rows landed (captured upper = 3).
    execute("INSERT INTO w_claims VALUES (3,'A')")
    val run2Df = JdbcSource.read(spark, conf("racing_feed", Some("r2")))
    assert(run2Df.count() == 3)

    // Run 1 commits ITS window: the stored watermark must be run 1's
    // captured upper (2), not run 2's (3) — pre-fix the shared entity key
    // meant run 2's put overwrote run 1's window.
    JdbcSource.advanceWatermark(spark, conf("racing_feed", Some("r1")), "racing_feed", "r1", run1Df)
    assert(InMemoryWatermarkStore.latestVersioned("racing_feed").get.value.serialized == "2")

    // Run 2's commit then trips the optimistic version check (JDBC_005):
    // its window was captured at version 0, but run 1 already advanced.
    intercept[WatermarkConflictException] {
      JdbcSource.advanceWatermark(spark, conf("racing_feed", Some("r2")), "racing_feed", "r2", run2Df)
    }
  }
}
