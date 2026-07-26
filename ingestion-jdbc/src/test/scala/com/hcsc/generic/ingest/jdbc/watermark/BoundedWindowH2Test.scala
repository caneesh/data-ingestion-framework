package com.hcsc.generic.ingest.jdbc.watermark

import com.hcsc.generic.ingest.jdbc.{JdbcSource, SharedSparkSession}
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.sql.DriverManager

/** Bounded-window edge cases against a dedicated H2 database: composite
  * equal-timestamp boundary protection, where-filtered upper capture, and
  * custom-SQL incremental bases. */
class BoundedWindowH2Test extends AnyFunSuite with SharedSparkSession with BeforeAndAfterAll {

  private val url = "jdbc:h2:mem:bounded_tests;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false"

  private def h2(statements: String*): Unit = {
    Class.forName("org.h2.Driver")
    val conn = DriverManager.getConnection(url, "sa", "")
    try {
      val stmt = conn.createStatement()
      try statements.foreach(stmt.execute)
      finally stmt.close()
    } finally conn.close()
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    h2(
      "DROP TABLE IF EXISTS events",
      """CREATE TABLE events (
        |  id INT,
        |  category VARCHAR(10),
        |  modified_ts TIMESTAMP
        |)""".stripMargin,
      "INSERT INTO events VALUES (1, 'A', '2026-01-01 10:00:00')",
      "INSERT INTO events VALUES (2, 'A', '2026-01-02 10:00:00')",
      "INSERT INTO events VALUES (3, 'B', '2026-01-05 10:00:00')"
    )
  }

  private def conf(extra: String): Config = ConfigFactory.parseString(
    s"""
       |type = "jdbc"
       |url = "$url"
       |dialect = "generic"
       |driver = "org.h2.Driver"
       |auth { user = "sa", password = { provider = "inline", value = "" } }
       |health_check { enabled = false }
       |mode = "INCREMENTAL"
       |$extra
    """.stripMargin)

  test("composite watermark: an equal-timestamp row with a higher key is not missed") {
    val composite = conf(
      """
        |table = "events"
        |entity = "composite_boundary"
        |incremental {
        |  watermark_type = "COMPOSITE"
        |  watermark_columns = ["modified_ts", "id"]
        |  column_types = ["TIMESTAMP", "NUMERIC"]
        |  initial_value = "1900-01-01 00:00:00|0"
        |  watermark_store { type = "memory" }
        |}
      """.stripMargin)

    val first = JdbcSource.read(spark, composite)
    assert(first.count() == 3)
    JdbcSource.advanceWatermark(spark, composite, "composite_boundary", "run-1", first)
    val committed = InMemoryWatermarkStore.latest("composite_boundary").get
    assert(committed.values.head.startsWith("2026-01-05 10:00:00"))
    assert(committed.values(1) == "3")

    // The classic timestamp-only miss: a new row with EXACTLY the committed
    // boundary timestamp but a higher key. The composite tie-breaker
    // guarantees it is extracted by the next run.
    h2("INSERT INTO events VALUES (9, 'A', '2026-01-05 10:00:00')")

    val second = JdbcSource.read(spark, composite)
    assert(second.count() == 1)
    assert(second.selectExpr("id").collect().head.getInt(0) == 9)

    JdbcSource.advanceWatermark(spark, composite, "composite_boundary", "run-2", second)
    assert(InMemoryWatermarkStore.latest("composite_boundary").get.values(1) == "9")
  }

  test("upper watermark capture honors the configured where clause") {
    val filtered = conf(
      """
        |table = "events"
        |where = "category = 'A'"
        |entity = "filtered_upper"
        |incremental {
        |  watermark_type = "NUMERIC"
        |  watermark_columns = ["id"]
        |  initial_value = "0"
        |  watermark_store { type = "memory" }
        |}
      """.stripMargin)

    val df = JdbcSource.read(spark, filtered)
    // Only category A rows extracted (ids 1, 2, 9 at this point)
    val ids = df.selectExpr("id").collect().map(_.getInt(0)).toSet
    assert(!ids.contains(3), "category B row must not be extracted")

    JdbcSource.advanceWatermark(spark, filtered, "filtered_upper", "run-1", df)
    val committed = InMemoryWatermarkStore.latest("filtered_upper").get.values.head
    // The captured upper is the max id AMONG category A rows, not the global max
    assert(committed == ids.max.toString)
  }

  test("custom SQL base supports bounded incremental extraction") {
    val customSql = conf(
      """
        |sql = "SELECT id, modified_ts FROM events WHERE category = 'A'"
        |entity = "custom_sql_inc"
        |incremental {
        |  watermark_type = "NUMERIC"
        |  watermark_columns = ["id"]
        |  initial_value = "1"
        |  watermark_store { type = "memory" }
        |}
      """.stripMargin)

    val df = JdbcSource.read(spark, customSql)
    val ids = df.selectExpr("id").collect().map(_.getInt(0)).toSet
    assert(ids == Set(2, 9)) // > 1, category A only

    JdbcSource.advanceWatermark(spark, customSql, "custom_sql_inc", "run-1", df)
    assert(InMemoryWatermarkStore.latest("custom_sql_inc").get.values.head == "9")
  }

  test("captured upper equal to the lower watermark leaves it unchanged") {
    val steady = conf(
      """
        |table = "events"
        |entity = "steady_feed"
        |incremental {
        |  watermark_type = "NUMERIC"
        |  watermark_columns = ["id"]
        |  initial_value = "0"
        |  watermark_store { type = "memory" }
        |}
      """.stripMargin)

    val first = JdbcSource.read(spark, steady)
    JdbcSource.advanceWatermark(spark, steady, "steady_feed", "run-1", first)
    val v1 = InMemoryWatermarkStore.latestVersioned("steady_feed").get

    // No new rows: second run extracts nothing and must not bump the version
    val second = JdbcSource.read(spark, steady)
    assert(second.count() == 0)
    JdbcSource.advanceWatermark(spark, steady, "steady_feed", "run-2", second)
    assert(InMemoryWatermarkStore.latestVersioned("steady_feed").get == v1)
  }
}
