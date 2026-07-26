package com.hcsc.generic.ingest.jdbc

import com.hcsc.generic.ingest.jdbc.watermark.InMemoryWatermarkStore
import com.hcsc.generic.ingest.schema.SchemaContractViolationException
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

class JdbcSourceH2Test extends AnyFunSuite with SharedSparkSession with BeforeAndAfterAll with BeforeAndAfterEach {

  override def beforeAll(): Unit = {
    super.beforeAll()
    H2TestDatabase.execute(
      "DROP TABLE IF EXISTS members",
      """CREATE TABLE members (
        |  subscriber_id VARCHAR(20),
        |  plan_hios_id VARCHAR(20),
        |  amount INT,
        |  modified_ts TIMESTAMP
        |)""".stripMargin,
      "INSERT INTO members VALUES ('S001','H1',10,'2026-01-01 10:00:00')",
      "INSERT INTO members VALUES ('S002','H2',20,'2026-02-01 10:00:00')",
      "INSERT INTO members VALUES ('S003','H3',30,'2026-03-01 10:00:00')"
    )
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    InMemoryWatermarkStore.clear()
  }

  private def conf(extra: String): Config =
    ConfigFactory.parseString(H2TestDatabase.sourceHocon(extra))

  test("FULL_TABLE reads every row") {
    val df = JdbcSource.read(spark, conf("""table = "members""""))
    assert(df.count() == 3)
    assert(df.columns.map(_.toLowerCase).toSet ==
      Set("subscriber_id", "plan_hios_id", "amount", "modified_ts"))
  }

  test("SELECT_QUERY pushes projection and predicate into the database") {
    val df = JdbcSource.read(spark, conf(
      """
        |mode = "SELECT_QUERY"
        |table = "members"
        |columns = ["subscriber_id", "amount"]
        |where = "amount >= 20"
      """.stripMargin))
    assert(df.columns.length == 2)
    assert(df.count() == 2)
  }

  test("CUSTOM_SQL executes arbitrary configured SQL") {
    val df = JdbcSource.read(spark, conf(
      """
        |mode = "CUSTOM_SQL"
        |sql = "SELECT subscriber_id, amount * 2 AS doubled FROM members WHERE amount < 30"
      """.stripMargin))
    assert(df.count() == 2)
    assert(df.columns.map(_.toLowerCase).contains("doubled"))
  }

  test("INCREMENTAL reads only rows past the stored watermark and advances after publish") {
    val incremental = conf(
      """
        |mode = "INCREMENTAL"
        |table = "members"
        |entity = "members_feed"
        |incremental {
        |  watermark_type = "TIMESTAMP"
        |  watermark_columns = ["modified_ts"]
        |  initial_value = "1900-01-01 00:00:00"
        |  watermark_store { type = "memory" }
        |}
      """.stripMargin)

    // First run: everything after the initial watermark
    val first = JdbcSource.read(spark, incremental)
    assert(first.count() == 3)

    // Publish succeeded -> pipeline advances the watermark
    JdbcSource.advanceWatermark(spark, incremental, "members_feed", "run-1", first)
    assert(InMemoryWatermarkStore.latest("members_feed").get.values.head.startsWith("2026-03-01 10:00:00"))

    // New source rows arrive
    H2TestDatabase.execute("INSERT INTO members VALUES ('S004','H4',40,'2026-04-01 10:00:00')")

    // Second run: only the new row
    val second = JdbcSource.read(spark, incremental)
    assert(second.count() == 1)
    assert(second.selectExpr("subscriber_id").collect().head.getString(0) == "S004")
  }

  test("overlap window re-reads the boundary without regressing the watermark") {
    val withOverlap = conf(
      """
        |mode = "INCREMENTAL"
        |table = "members"
        |entity = "overlap_feed"
        |incremental {
        |  watermark_type = "TIMESTAMP"
        |  watermark_columns = ["modified_ts"]
        |  initial_value = "1900-01-01 00:00:00"
        |  overlap = "86400"
        |  watermark_store { type = "memory" }
        |}
      """.stripMargin)

    JdbcSource.advanceWatermark(spark, withOverlap, "overlap_feed",
      "seed", JdbcSource.read(spark, conf("""table = "members"""")))
    val atMax = InMemoryWatermarkStore.latest("overlap_feed").get

    // Overlap of 1 day re-reads the newest row; advance must not move backwards
    val overlapRead = JdbcSource.read(spark, withOverlap)
    assert(overlapRead.count() >= 1)
    JdbcSource.advanceWatermark(spark, withOverlap, "overlap_feed", "run-2", overlapRead)
    assert(InMemoryWatermarkStore.latest("overlap_feed").get == atMax)
  }

  test("empty incremental extract leaves the watermark unchanged") {
    val incremental = conf(
      """
        |mode = "INCREMENTAL"
        |table = "members"
        |entity = "empty_feed"
        |incremental {
        |  watermark_type = "TIMESTAMP"
        |  watermark_columns = ["modified_ts"]
        |  initial_value = "2999-01-01 00:00:00"
        |  watermark_store { type = "memory" }
        |}
      """.stripMargin)
    val df = JdbcSource.read(spark, incremental)
    assert(df.count() == 0)
    JdbcSource.advanceWatermark(spark, incremental, "empty_feed", "run-1", df)
    assert(InMemoryWatermarkStore.latest("empty_feed").isEmpty)
  }

  test("NUMERIC watermarks work end to end") {
    val incremental = conf(
      """
        |mode = "INCREMENTAL"
        |table = "members"
        |entity = "numeric_feed"
        |incremental {
        |  watermark_type = "NUMERIC"
        |  watermark_columns = ["amount"]
        |  initial_value = "15"
        |  watermark_store { type = "memory" }
        |}
      """.stripMargin)
    val df = JdbcSource.read(spark, incremental)
    assert(df.count() == 3) // amounts 20, 30, 40 (S004 added by earlier test)
    JdbcSource.advanceWatermark(spark, incremental, "numeric_feed", "run-1", df)
    assert(InMemoryWatermarkStore.latest("numeric_feed").get.values.head == "40")
  }

  test("schema contract detects drift: aliases map, unknown required column fails") {
    val contract =
      """
        |schema {
        |  version = "1.0"
        |  columns = [
        |    { name = "subscriber_id", type = "string" },
        |    { name = "hios_id", type = "string", aliases = ["plan_hios_id"] },
        |    { name = "amount", type = "int" },
        |    { name = "modified_ts", type = "timestamp" }
        |  ]
        |}
      """.stripMargin

    // plan_hios_id is renamed to canonical hios_id via alias
    val df = JdbcSource.read(spark, conf(s"""table = "members"\n$contract"""))
    assert(df.columns.contains("hios_id"))
    assert(!df.columns.map(_.toLowerCase).contains("plan_hios_id"))

    // A contract expecting a column the database no longer has -> HDR_001
    val badContract =
      """
        |schema {
        |  version = "1.0"
        |  columns = [
        |    { name = "subscriber_id" },
        |    { name = "renamed_away_column" }
        |  ]
        |  header_validation { on_extra_columns = "IGNORE" }
        |}
      """.stripMargin
    val ex = intercept[SchemaContractViolationException] {
      JdbcSource.read(spark, conf(s"""table = "members"\n$badContract"""))
    }
    assert(ex.getMessage.contains("HDR_001"))
  }

  test("contract adds missing optional columns with defaults") {
    val df = JdbcSource.read(spark, conf(
      """
        |table = "members"
        |schema {
        |  version = "1.0"
        |  columns = [
        |    { name = "subscriber_id" },
        |    { name = "plan_hios_id" },
        |    { name = "amount", type = "int" },
        |    { name = "modified_ts", type = "timestamp" },
        |    { name = "exchange", required = false, default = "FFM" }
        |  ]
        |}
      """.stripMargin))
    assert(df.columns.contains("exchange"))
    assert(df.selectExpr("exchange").distinct().collect().head.getString(0) == "FFM")
  }

  test("failed health check fails fast with JDBC_001 before any Spark read") {
    val ex = intercept[IllegalStateException] {
      JdbcSource.read(spark, ConfigFactory.parseString(
        """
          |type = "jdbc"
          |url = "jdbc:h2:tcp://localhost:19998/absent"
          |dialect = "generic"
          |driver = "org.h2.Driver"
          |table = "members"
          |health_check { enabled = true }
          |retry { max_attempts = 1, backoff_ms = 1 }
        """.stripMargin))
    }
    assert(ex.getMessage.contains("JDBC_001"))
  }

  test("spark partitioned reads return complete data") {
    val df = JdbcSource.read(spark, conf(
      """
        |table = "members"
        |numPartitions = 3
        |partitionColumn = "amount"
        |lowerBound = 0
        |upperBound = 100
        |fetchsize = 10
      """.stripMargin))
    assert(df.rdd.getNumPartitions == 3)
    assert(df.count() == 4)
  }
}
