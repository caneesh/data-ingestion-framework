package com.hcsc.generic.ingest.jdbc.mssql

import com.hcsc.generic.ingest.jdbc.read.SqlFailureClassifier
import com.hcsc.generic.ingest.jdbc.watermark.InMemoryWatermarkStore
import com.hcsc.generic.ingest.jdbc.{JdbcSource, JdbcSourceConfig, SharedSparkSession}
import com.hcsc.generic.ingest.jdbc.health.JdbcHealthCheck
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.funsuite.AnyFunSuite

import SqlServerContainerSupport.dockerAvailable

/**
  * Docker-gated SQL Server integration suite exercising the JDBC source
  * against a real SQL Server via Testcontainers.
  *
  * Every test opens with `assume(dockerAvailable, ...)`: without Docker the
  * test is CANCELED (not failed), so `mvn test` stays green everywhere. The
  * container is shared for the suite (see SqlServerContainerSupport).
  *
  * These tests validate behaviors H2 cannot: SQL Server bracket quoting,
  * datetime2 sub-second precision, DATETIMEOFFSET, ROWVERSION, TOP-1 upper
  * capture, real SQLServerException classification, and CUSTOM_SQL wrapping.
  */
class SqlServerIntegrationTest
  extends AnyFunSuite
  with SharedSparkSession
  with SqlServerContainerSupport {

  private def gate(): Unit =
    assume(dockerAvailable, "Docker not available; skipping SQL Server integration tests")

  private def conf(extra: String): Config = ConfigFactory.parseString(baseHocon(extra))

  // ---------------------------------------------------------------------------
  // (a) Bracket-quoted identifier round trip: a column with a space, mapped via
  //     a structured projection { source = "Member ID", target = "member_id" }.
  //     SQL Server quotes it as [Member ID]; the target lands as member_id.
  // ---------------------------------------------------------------------------
  test("bracket-quoted identifier with a space round-trips through structured columns") {
    gate()
    execute(
      "IF OBJECT_ID('dbo.bracket_members','U') IS NOT NULL DROP TABLE dbo.bracket_members",
      """CREATE TABLE dbo.bracket_members (
        |  [Member ID] NVARCHAR(20),
        |  [amount] INT
        |)""".stripMargin,
      "INSERT INTO dbo.bracket_members ([Member ID], [amount]) VALUES (N'M001', 10)",
      "INSERT INTO dbo.bracket_members ([Member ID], [amount]) VALUES (N'M002', 20)"
    )

    val df = JdbcSource.read(spark, conf(
      """
        |mode = "SELECT_QUERY"
        |table = "dbo.bracket_members"
        |columns = [
        |  { source = "Member ID", target = "member_id" },
        |  { source = "amount",     target = "amount" }
        |]
      """.stripMargin))

    assert(df.columns.map(_.toLowerCase).toSet == Set("member_id", "amount"))
    assert(df.count() == 2)
    assert(df.selectExpr("member_id").collect().map(_.getString(0)).toSet == Set("M001", "M002"))
  }

  // ---------------------------------------------------------------------------
  // (b) datetime2 precision watermark: INCREMENTAL read + advanceWatermark with
  //     sub-second precision preserved across two runs.
  // ---------------------------------------------------------------------------
  test("datetime2 sub-second precision survives an INCREMENTAL watermark round trip") {
    gate()
    execute(
      "IF OBJECT_ID('dbo.dt2_events','U') IS NOT NULL DROP TABLE dbo.dt2_events",
      """CREATE TABLE dbo.dt2_events (
        |  id INT,
        |  modified_ts DATETIME2(7)
        |)""".stripMargin,
      "INSERT INTO dbo.dt2_events VALUES (1, '2026-01-01 10:00:00.1234567')",
      "INSERT INTO dbo.dt2_events VALUES (2, '2026-02-01 10:00:00.7654321')"
    )

    val incremental = conf(
      """
        |mode = "INCREMENTAL"
        |table = "dbo.dt2_events"
        |entity = "dt2_feed"
        |incremental {
        |  watermark_type = "TIMESTAMP"
        |  watermark_columns = ["modified_ts"]
        |  initial_value = "1900-01-01 00:00:00"
        |  on_unprotected_watermark = "WARN"
        |  watermark_store { type = "memory" }
        |}
      """.stripMargin)

    val first = JdbcSource.read(spark, incremental)
    assert(first.count() == 2)
    JdbcSource.advanceWatermark(spark, incremental, "dt2_feed", "run-1", first)

    // The captured upper is the datetime2 max; sub-second fraction is retained.
    val committed = InMemoryWatermarkStore.latest("dt2_feed").get.values.head
    assert(committed.startsWith("2026-02-01 10:00:00.765"),
      s"committed watermark '$committed' lost sub-second precision")

    // A new row strictly after the committed watermark by 1 tick only.
    execute("INSERT INTO dbo.dt2_events VALUES (3, '2026-02-01 10:00:00.7654322')")

    val second = JdbcSource.read(spark, incremental)
    assert(second.count() == 1, "only the single row past the sub-second watermark should return")
    assert(second.selectExpr("id").collect().head.getInt(0) == 3)
  }

  // ---------------------------------------------------------------------------
  // (c) DATETIMEOFFSET codec end to end.
  // ---------------------------------------------------------------------------
  test("DATETIMEOFFSET watermark codec works end to end") {
    gate()
    execute(
      "IF OBJECT_ID('dbo.dto_events','U') IS NOT NULL DROP TABLE dbo.dto_events",
      """CREATE TABLE dbo.dto_events (
        |  id INT,
        |  event_at DATETIMEOFFSET(3)
        |)""".stripMargin,
      "INSERT INTO dbo.dto_events VALUES (1, '2026-01-01 10:00:00.000 -05:00')",
      "INSERT INTO dbo.dto_events VALUES (2, '2026-01-01 12:00:00.000 +02:00')"
    )

    val incremental = conf(
      """
        |mode = "INCREMENTAL"
        |table = "dbo.dto_events"
        |entity = "dto_feed"
        |incremental {
        |  watermark_type = "DATETIMEOFFSET"
        |  watermark_columns = ["event_at"]
        |  initial_value = "1900-01-01 00:00:00.000 +00:00"
        |  watermark_store { type = "memory" }
        |}
      """.stripMargin)

    val first = JdbcSource.read(spark, incremental)
    assert(first.count() == 2)
    JdbcSource.advanceWatermark(spark, incremental, "dto_feed", "run-1", first)

    // Both rows are the same instant offset-normalized; the later instant
    // (12:00 +02:00 == 10:00 UTC vs 10:00 -05:00 == 15:00 UTC) is the max.
    val committed = InMemoryWatermarkStore.latest("dto_feed").get.values.head
    assert(committed.contains("2026-01-01"), s"unexpected committed watermark '$committed'")

    // A later-instant row must be picked up on the next run; an earlier one must not.
    execute(
      "INSERT INTO dbo.dto_events VALUES (3, '2026-01-01 09:00:00.000 -05:00')", // 14:00 UTC, before max 15:00
      "INSERT INTO dbo.dto_events VALUES (4, '2026-01-01 20:00:00.000 +00:00')"  // 20:00 UTC, after max
    )
    val second = JdbcSource.read(spark, incremental)
    val ids = second.selectExpr("id").collect().map(_.getInt(0)).toSet
    assert(ids.contains(4), s"row 4 (later instant) should be extracted; got $ids")
    assert(!ids.contains(3), s"row 3 (earlier instant) should NOT be extracted; got $ids")
  }

  // ---------------------------------------------------------------------------
  // (d) ROWVERSION incremental: two runs pick up only updated rows.
  // ---------------------------------------------------------------------------
  test("ROWVERSION incremental picks up only rows changed since the last run") {
    gate()
    execute(
      "IF OBJECT_ID('dbo.rv_members','U') IS NOT NULL DROP TABLE dbo.rv_members",
      """CREATE TABLE dbo.rv_members (
        |  id INT PRIMARY KEY,
        |  name NVARCHAR(50),
        |  rv ROWVERSION
        |)""".stripMargin,
      "INSERT INTO dbo.rv_members (id, name) VALUES (1, N'Alice')",
      "INSERT INTO dbo.rv_members (id, name) VALUES (2, N'Bob')"
    )

    val incremental = conf(
      """
        |mode = "INCREMENTAL"
        |table = "dbo.rv_members"
        |entity = "rv_feed"
        |incremental {
        |  watermark_type = "ROWVERSION"
        |  watermark_columns = ["rv"]
        |  initial_value = "0000000000000000"
        |  watermark_store { type = "memory" }
        |}
      """.stripMargin)

    val first = JdbcSource.read(spark, incremental)
    assert(first.count() == 2)
    JdbcSource.advanceWatermark(spark, incremental, "rv_feed", "run-1", first)
    val wm1 = InMemoryWatermarkStore.latest("rv_feed").get.values.head

    // Updating a row bumps its rowversion beyond the committed watermark.
    execute("UPDATE dbo.rv_members SET name = N'Robert' WHERE id = 2")

    val second = JdbcSource.read(spark, incremental)
    assert(second.count() == 1, "only the updated row should be re-extracted")
    assert(second.selectExpr("id").collect().head.getInt(0) == 2)

    JdbcSource.advanceWatermark(spark, incremental, "rv_feed", "run-2", second)
    val wm2 = InMemoryWatermarkStore.latest("rv_feed").get.values.head
    assert(BigInt(wm2, 16) > BigInt(wm1, 16), s"rowversion watermark did not advance ($wm1 -> $wm2)")

    // Nothing changed since run-2 -> empty extract.
    val third = JdbcSource.read(spark, incremental)
    assert(third.count() == 0)
  }

  // ---------------------------------------------------------------------------
  // (e) Composite watermark predicate against real SQL Server.
  // ---------------------------------------------------------------------------
  test("composite watermark predicate extracts the correct lexicographic tail") {
    gate()
    execute(
      "IF OBJECT_ID('dbo.comp_events','U') IS NOT NULL DROP TABLE dbo.comp_events",
      """CREATE TABLE dbo.comp_events (
        |  event_date DATE,
        |  seq INT,
        |  payload NVARCHAR(20)
        |)""".stripMargin,
      "INSERT INTO dbo.comp_events VALUES ('2026-01-01', 1, N'a')",
      "INSERT INTO dbo.comp_events VALUES ('2026-01-01', 2, N'b')",
      "INSERT INTO dbo.comp_events VALUES ('2026-01-02', 1, N'c')"
    )

    val incremental = conf(
      """
        |mode = "INCREMENTAL"
        |table = "dbo.comp_events"
        |entity = "comp_feed"
        |incremental {
        |  watermark_type = "COMPOSITE"
        |  watermark_columns = ["event_date", "seq"]
        |  column_types = ["DATE", "NUMERIC"]
        |  initial_value = "2026-01-01|1"
        |  watermark_store { type = "memory" }
        |}
      """.stripMargin)

    // Lower is (2026-01-01, 1): strictly-greater lexicographic order returns
    // (2026-01-01, 2) and (2026-01-02, 1) but NOT the boundary row.
    val first = JdbcSource.read(spark, incremental)
    assert(first.count() == 2)
    val rows = first.selectExpr("event_date", "seq").collect()
      .map(r => (r.getDate(0).toString, r.getInt(1))).toSet
    assert(rows == Set(("2026-01-01", 2), ("2026-01-02", 1)), s"unexpected composite tail: $rows")

    JdbcSource.advanceWatermark(spark, incremental, "comp_feed", "run-1", first)
    assert(InMemoryWatermarkStore.latest("comp_feed").get.values == Seq("2026-01-02", "1"))
  }

  // ---------------------------------------------------------------------------
  // (f) Partitioned range read returns complete data.
  // ---------------------------------------------------------------------------
  test("partitioned range read returns complete data") {
    gate()
    execute(
      "IF OBJECT_ID('dbo.part_members','U') IS NOT NULL DROP TABLE dbo.part_members",
      "CREATE TABLE dbo.part_members (id INT, amount INT)",
      "INSERT INTO dbo.part_members VALUES (1, 5), (2, 25), (3, 55), (4, 85), (5, 95)"
    )

    val df = JdbcSource.read(spark, conf(
      """
        |mode = "SELECT_QUERY"
        |table = "dbo.part_members"
        |numPartitions = 4
        |partitionColumn = "amount"
        |lowerBound = 0
        |upperBound = 100
      """.stripMargin))

    assert(df.rdd.getNumPartitions == 4)
    assert(df.count() == 5, "partitioned read must return every row, no gaps or duplicates")
    assert(df.selectExpr("id").collect().map(_.getInt(0)).toSet == Set(1, 2, 3, 4, 5))
  }

  // ---------------------------------------------------------------------------
  // (g) SQLState/vendor classification: wrong password -> JDBC_001/JDBC_002
  //     health-check failure, and SqlFailureClassifier classifies the raised
  //     SQLServerException as Authentication.
  // ---------------------------------------------------------------------------
  test("wrong password fails the health check and classifies as Authentication") {
    gate()
    val badConf = ConfigFactory.parseString(
      s"""
         |type = "jdbc"
         |url = "${container.getJdbcUrl}"
         |dialect = "sqlserver"
         |auth {
         |  user = "${container.getUsername}"
         |  password = { provider = "inline", value = "wrong-password-000" }
         |}
         |connection_properties { encrypt = "false" }
         |table = "sys.tables"
         |mode = "SELECT_QUERY"
         |health_check { enabled = true }
         |retry { max_attempts = 1, backoff_ms = 1 }
       """.stripMargin)

    // The health check surfaces JDBC_001 or JDBC_002 (the SQL Server driver
    // raises a generic SQLServerException, not SQLInvalidAuthorizationSpec, so
    // JdbcHealthCheck maps it to JDBC_001 — either prefix is an auth failure).
    val cfg = JdbcSourceConfig.parse(badConf)
    val result = JdbcHealthCheck.check(cfg)
    assert(result.isLeft, "wrong password must fail the health check")
    val message = result.left.get
    assert(message.startsWith("JDBC_001") || message.startsWith("JDBC_002"),
      s"expected JDBC_001/JDBC_002, got: $message")

    // And the underlying SQLServerException classifies as Authentication
    // (vendor code 18456 / SQLState 28000).
    val ex = intercept[java.sql.SQLException] {
      val props = new java.util.Properties()
      props.setProperty("user", container.getUsername)
      props.setProperty("password", "wrong-password-000")
      props.setProperty("encrypt", "false")
      java.sql.DriverManager.getConnection(container.getJdbcUrl, props)
    }
    assert(SqlFailureClassifier.classify(ex) == SqlFailureClassifier.Authentication,
      s"expected Authentication for vendor=${ex.getErrorCode} state=${ex.getSQLState}")
    assert(!SqlFailureClassifier.classify(ex).retryable, "auth failures must not be retryable")
  }

  // ---------------------------------------------------------------------------
  // (h) CUSTOM_SQL query wrapping executes on SQL Server.
  // ---------------------------------------------------------------------------
  test("CUSTOM_SQL query wrapping executes on SQL Server") {
    gate()
    execute(
      "IF OBJECT_ID('dbo.custom_members','U') IS NOT NULL DROP TABLE dbo.custom_members",
      "CREATE TABLE dbo.custom_members (id INT, amount INT)",
      "INSERT INTO dbo.custom_members VALUES (1, 10), (2, 20), (3, 30)"
    )

    val df = JdbcSource.read(spark, conf(
      """
        |mode = "CUSTOM_SQL"
        |sql = "SELECT id, amount * 2 AS doubled FROM dbo.custom_members WHERE amount < 30"
      """.stripMargin))

    assert(df.columns.map(_.toLowerCase).contains("doubled"))
    assert(df.count() == 2)
    assert(df.selectExpr("doubled").collect().map(_.getInt(0)).toSet == Set(20, 40))
  }
}
