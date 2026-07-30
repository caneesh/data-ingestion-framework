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

  test("bounded window: rows arriving after upper capture wait for the next run") {
    val incremental = conf(
      """
        |mode = "INCREMENTAL"
        |table = "members"
        |entity = "bounded_feed"
        |incremental {
        |  watermark_type = "TIMESTAMP"
        |  watermark_columns = ["modified_ts"]
        |  initial_value = "1900-01-01 00:00:00"
        |  watermark_store { type = "memory" }
        |}
      """.stripMargin)

    // Read captures the upper bound at read time
    val df = JdbcSource.read(spark, incremental)
    val extracted = df.count()

    // A row arrives AFTER the window was captured, before the commit
    H2TestDatabase.execute("INSERT INTO members VALUES ('S_LATE','HL',999,'2030-01-01 00:00:00')")

    // Commit uses the CAPTURED upper, not the extracted/current max — the
    // late row stays beyond the committed watermark for the next run
    JdbcSource.advanceWatermark(spark, incremental, "bounded_feed", "run-b1", df)
    val committed = InMemoryWatermarkStore.latest("bounded_feed").get.values.head
    assert(!committed.startsWith("2030"), s"committed=$committed must be the captured upper")

    val next = JdbcSource.read(spark, incremental)
    assert(next.count() == 1)
    assert(next.selectExpr("subscriber_id").collect().head.getString(0) == "S_LATE")
    assert(extracted >= 4)

    H2TestDatabase.execute("DELETE FROM members WHERE subscriber_id = 'S_LATE'")
  }

  test("concurrent watermark advancement is detected (JDBC_005)") {
    val incremental = conf(
      """
        |mode = "INCREMENTAL"
        |table = "members"
        |entity = "race_feed"
        |incremental {
        |  watermark_type = "NUMERIC"
        |  watermark_columns = ["amount"]
        |  initial_value = "0"
        |  watermark_store { type = "memory" }
        |}
      """.stripMargin)

    // Run A reads (captures version 0)
    val dfA = JdbcSource.read(spark, incremental)

    // A competing run commits first
    InMemoryWatermarkStore.recordIfVersion("race_feed",
      com.hcsc.generic.ingest.jdbc.watermark.WatermarkValue(Seq("35")), "competitor", 0L)

    // Run A's commit must now conflict instead of silently overwriting
    val ex = intercept[com.hcsc.generic.ingest.jdbc.watermark.WatermarkConflictException] {
      JdbcSource.advanceWatermark(spark, incremental, "race_feed", "run-a", dfA)
    }
    assert(ex.getMessage.contains("JDBC_005"))
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

  test("SOURCE_CLOCK upper bound: idle source still advances; empty window commits") {
    val incremental = conf(
      """
        |mode = "INCREMENTAL"
        |table = "members"
        |entity = "clock_feed"
        |incremental {
        |  watermark_type = "TIMESTAMP"
        |  watermark_columns = ["modified_ts"]
        |  initial_value = "1900-01-01 00:00:00"
        |  upper_bound = "SOURCE_CLOCK"
        |  clock_zone = "LOCAL"
        |  watermark_store { type = "memory" }
        |}
      """.stripMargin)

    // First run: everything before the source clock; commits the clock value
    val first = JdbcSource.read(spark, incremental)
    assert(first.count() >= 3)
    JdbcSource.advanceWatermark(spark, incremental, "clock_feed", "run-c1", first)
    val committed1 = java.sql.Timestamp.valueOf(
      InMemoryWatermarkStore.latest("clock_feed").get.values.head)
    assert(committed1.after(java.sql.Timestamp.valueOf("2026-04-02 00:00:00")),
      s"clock cutoff $committed1 must be the source clock, not the row max")

    // Second run with NO new rows: still advances to the fresh clock value —
    // an idle-but-alive source keeps moving (and rerun cost stays bounded)
    Thread.sleep(50)
    val second = JdbcSource.read(spark, incremental)
    assert(second.count() == 0)
    JdbcSource.advanceWatermark(spark, incremental, "clock_feed", "run-c2", second)
    val committed2 = java.sql.Timestamp.valueOf(
      InMemoryWatermarkStore.latest("clock_feed").get.values.head)
    assert(committed2.after(committed1), "empty run against a live source must advance the clock cutoff")
  }

  test("NULL watermark values fail fast (JDBC_006) instead of silent loss") {
    H2TestDatabase.execute(
      "DROP TABLE IF EXISTS null_wm",
      "CREATE TABLE null_wm (id VARCHAR(10), modified_ts TIMESTAMP)",
      "INSERT INTO null_wm VALUES ('R1', '2026-01-01 10:00:00')",
      "INSERT INTO null_wm VALUES ('R2', NULL)"
    )
    def incremental(extra: String) = conf(
      s"""
        |mode = "INCREMENTAL"
        |table = "null_wm"
        |entity = "null_wm_feed"
        |incremental {
        |  watermark_type = "TIMESTAMP"
        |  watermark_columns = ["modified_ts"]
        |  initial_value = "1900-01-01 00:00:00"
        |  watermark_store { type = "memory" }
        |  $extra
        |}
      """.stripMargin)

    val ex = intercept[IllegalStateException] {
      JdbcSource.read(spark, incremental(""))
    }
    assert(ex.getMessage.contains("JDBC_006"))
    assert(ex.getMessage.contains("NULL"))

    // Explicit override accepts the (documented) loss of NULL-watermark rows
    val df = JdbcSource.read(spark, incremental("allow_null_watermark = true"))
    assert(df.count() == 1) // only the non-NULL row is extractable

    H2TestDatabase.execute("DROP TABLE IF EXISTS null_wm")
  }

  test("FULL_TABLE with an incremental block seeds the watermark (FULL -> INCR handoff)") {
    val incrementalBlock =
      """
        |entity = "seed_feed"
        |incremental {
        |  watermark_type = "TIMESTAMP"
        |  watermark_columns = ["modified_ts"]
        |  initial_value = "1900-01-01 00:00:00"
        |  watermark_store { type = "memory" }
        |}
      """.stripMargin

    // Full load: unfiltered read (NULL rows included, no predicate), but the
    // cutoff is captured and committed as the seed after success
    val full = JdbcSource.read(spark, conf(s"""table = "members"\n$incrementalBlock"""))
    val fullCount = full.count()
    assert(fullCount >= 3)
    JdbcSource.advanceWatermark(spark, conf(s"""table = "members"\n$incrementalBlock"""),
      "seed_feed", "run-seed", full)
    val seeded = InMemoryWatermarkStore.latest("seed_feed")
    assert(seeded.isDefined, "FULL_TABLE run with incremental block must seed the watermark")

    // Handoff: the first INCREMENTAL run extracts nothing new
    val incr = JdbcSource.read(spark, conf(s"""mode = "INCREMENTAL"\ntable = "members"\n$incrementalBlock"""))
    assert(incr.count() == 0, "seeded watermark must hand off cleanly to INCREMENTAL")

    // Reseed guard: a LATER FULL_TABLE rerun against the populated store
    // must not jump the watermark (an ad-hoc full reload would otherwise
    // skip every row between the incremental cursor and "now")
    val before = InMemoryWatermarkStore.latest("seed_feed")
    val rerun = JdbcSource.read(spark, conf(s"""table = "members"\n$incrementalBlock"""))
    JdbcSource.advanceWatermark(spark, conf(s"""table = "members"\n$incrementalBlock"""),
      "seed_feed", "run-seed-2", rerun)
    assert(InMemoryWatermarkStore.latest("seed_feed") == before,
      "FULL_TABLE rerun must never reseed an existing watermark")
  }

  test("SOURCE_CLOCK upper bound rejects non-timestamp watermark types at parse") {
    val ex = intercept[IllegalArgumentException] {
      JdbcSourceConfig.parse(conf(
        """
          |mode = "INCREMENTAL"
          |table = "members"
          |incremental {
          |  watermark_type = "NUMERIC"
          |  watermark_columns = ["amount"]
          |  initial_value = "0"
          |  upper_bound = "SOURCE_CLOCK"
          |}
        """.stripMargin))
    }
    assert(ex.getMessage.contains("SOURCE_CLOCK"))
  }

  test("SOURCE_CLOCK with a TIMESTAMP watermark requires an explicit clock_zone") {
    val ex = intercept[IllegalArgumentException] {
      JdbcSourceConfig.parse(conf(
        """
          |mode = "INCREMENTAL"
          |table = "members"
          |incremental {
          |  watermark_type = "TIMESTAMP"
          |  watermark_columns = ["modified_ts"]
          |  initial_value = "1900-01-01 00:00:00"
          |  upper_bound = "SOURCE_CLOCK"
          |}
        """.stripMargin))
    }
    assert(ex.getMessage.contains("clock_zone"))
  }

  test("connection_properties may not override auth-controlled credentials") {
    // Fix: a plaintext password in connection_properties would silently win
    // over the vault-resolved credential (properties are applied last).
    val ex = intercept[IllegalArgumentException] {
      JdbcSourceConfig.parse(conf(
        """
          |table = "members"
          |connection_properties { password = "plaintext-override" }
        """.stripMargin))
    }
    assert(ex.getMessage.contains("connection_properties may not set"))
    assert(ex.getMessage.contains("password"))

    val ex2 = intercept[IllegalArgumentException] {
      JdbcSourceConfig.parse(conf(
        """
          |table = "members"
          |connection_properties { authentication = "ActiveDirectoryPassword" }
        """.stripMargin))
    }
    assert(ex2.getMessage.contains("authentication"))
  }

  test("parameters blocks under modes that never render them are rejected") {
    val ex = intercept[IllegalArgumentException] {
      JdbcSourceConfig.parse(conf(
        """
          |mode = "CUSTOM_SQL"
          |sql = "SELECT * FROM members"
          |parameters = [ { name = "run_date", type = "DATE", source = "CONFIG", value = "2026-01-01" } ]
        """.stripMargin))
    }
    assert(ex.getMessage.contains("parameters"))

    val ex2 = intercept[IllegalArgumentException] {
      JdbcSourceConfig.parse(conf(
        """
          |mode = "INCREMENTAL"
          |table = "members"
          |parameters = [ { name = "run_date", type = "DATE", source = "CONFIG", value = "2026-01-01" } ]
          |incremental {
          |  watermark_type = "TIMESTAMP"
          |  watermark_columns = ["modified_ts"]
          |  initial_value = "1900-01-01 00:00:00"
          |}
        """.stripMargin))
    }
    assert(ex2.getMessage.contains("parameters"))
  }

  test("INCREMENTAL with both table and sql configured is rejected as ambiguous") {
    val ex = intercept[IllegalArgumentException] {
      JdbcSourceConfig.parse(conf(
        """
          |mode = "INCREMENTAL"
          |table = "members"
          |sql = "SELECT * FROM members"
          |incremental {
          |  watermark_type = "TIMESTAMP"
          |  watermark_columns = ["modified_ts"]
          |  initial_value = "1900-01-01 00:00:00"
          |}
        """.stripMargin))
    }
    assert(ex.getMessage.contains("not both"))
  }

  test("parameterized INCREMENTAL sql base renders :name parameters end to end") {
    // Fix: companion queries (upper capture, NULL probe) previously executed
    // the RAW base with unrendered :name tokens — a native SQL error on the
    // very first run of the documented parameterized-incremental feature.
    val incremental = conf(
      """
        |mode = "INCREMENTAL"
        |sql = "SELECT * FROM members WHERE amount >= :floor"
        |entity = "param_feed"
        |parameters = [ { name = "floor", type = "NUMBER", source = "CONFIG", value = "20" } ]
        |incremental {
        |  watermark_type = "TIMESTAMP"
        |  watermark_columns = ["modified_ts"]
        |  initial_value = "1900-01-01 00:00:00"
        |  watermark_store { type = "memory" }
        |}
      """.stripMargin)

    val df = JdbcSource.read(spark, incremental)
    val amounts = df.selectExpr("amount").collect().map(_.getInt(0))
    assert(amounts.nonEmpty && amounts.forall(_ >= 20),
      s"the :floor parameter must be rendered into base and companion queries (got ${amounts.mkString(",")})")
    JdbcSource.advanceWatermark(spark, incremental, "param_feed", "run-p1", df)
    assert(InMemoryWatermarkStore.latest("param_feed").isDefined)
  }

  test("composite watermark with NULL trailing tie-breakers fails fast (JDBC_006)") {
    // Fix: at an exact leading-column tie, three-valued logic silently and
    // PERMANENTLY skips rows whose tie-breaker is NULL — trailing NULLs are
    // now fatal, not a warning.
    H2TestDatabase.execute(
      "DROP TABLE IF EXISTS comp_null",
      "CREATE TABLE comp_null (id VARCHAR(10), modified_ts TIMESTAMP, seq INT)",
      "INSERT INTO comp_null VALUES ('R1', '2026-01-01 10:00:00', 1)",
      "INSERT INTO comp_null VALUES ('R2', '2026-01-01 10:00:00', NULL)"
    )
    val composite = conf(
      """
        |mode = "INCREMENTAL"
        |table = "comp_null"
        |entity = "comp_null_feed"
        |incremental {
        |  watermark_type = "COMPOSITE"
        |  watermark_columns = ["modified_ts", "seq"]
        |  column_types = ["TIMESTAMP", "NUMERIC"]
        |  initial_value = "1900-01-01 00:00:00|0"
        |  watermark_store { type = "memory" }
        |}
      """.stripMargin)

    val ex = intercept[IllegalStateException] {
      JdbcSource.read(spark, composite)
    }
    assert(ex.getMessage.contains("JDBC_006"))
    assert(ex.getMessage.contains("tie-breaker"))
    H2TestDatabase.execute("DROP TABLE IF EXISTS comp_null")
  }

  test("a stray incremental block under SELECT_QUERY is rejected, not silently ignored") {
    val ex = intercept[IllegalArgumentException] {
      JdbcSourceConfig.parse(conf(
        """
          |mode = "SELECT_QUERY"
          |table = "members"
          |incremental {
          |  watermark_type = "TIMESTAMP"
          |  watermark_columns = ["modified_ts"]
          |  initial_value = "1900-01-01 00:00:00"
          |}
        """.stripMargin))
    }
    assert(ex.getMessage.contains("incremental"))
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
