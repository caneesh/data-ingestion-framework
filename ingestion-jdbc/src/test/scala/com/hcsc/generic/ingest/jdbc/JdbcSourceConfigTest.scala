package com.hcsc.generic.ingest.jdbc

import com.hcsc.generic.ingest.jdbc.dialect.SqlServerDialect
import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite

class JdbcSourceConfigTest extends AnyFunSuite {

  private def parse(hocon: String) = JdbcSourceConfig.parse(ConfigFactory.parseString(hocon))

  private val sqlServerBase =
    """
      |url = "jdbc:sqlserver://myserver.database.windows.net:1433;databaseName=claims"
      |table = "dbo.claims"
    """.stripMargin

  test("dialect inferred from url; sqlserver defaults applied") {
    val cfg = parse(sqlServerBase)
    assert(cfg.dialect == SqlServerDialect)
    assert(cfg.driver == "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    assert(cfg.mode == JdbcMode.FullTable)
    assert(cfg.connectionProperties("encrypt") == "true")
    assert(cfg.connectionProperties("trustServerCertificate") == "false")
    assert(cfg.fetchSize == 1000)
    assert(cfg.retry.maxAttempts == 3)
    assert(cfg.healthCheckEnabled)
  }

  test("companion query timeout defaults to 300s and is configurable") {
    assert(parse(sqlServerBase).companionTimeoutSeconds == 300)
    assert(parse(sqlServerBase + "\ncompanion_timeout_seconds = 60").companionTimeoutSeconds == 60)
    val ex = intercept[IllegalArgumentException] {
      parse(sqlServerBase + "\ncompanion_timeout_seconds = 0")
    }
    assert(ex.getMessage.contains("companion_timeout_seconds"))
  }

  test("mysql dialect enables server-side cursors so fetchsize is honored") {
    val cfg = parse("""url = "jdbc:mysql://host:3306/db", table = "t" """)
    assert(cfg.connectionProperties("useCursorFetch") == "true")
    // feed-level connection_properties still override the dialect default
    val overridden = parse(
      """url = "jdbc:mysql://host:3306/db", table = "t"
        |connection_properties { useCursorFetch = "false" }""".stripMargin)
    assert(overridden.connectionProperties("useCursorFetch") == "false")
  }

  test("explicit dialect must match the url prefix") {
    val ex = intercept[IllegalArgumentException] {
      parse("""url = "jdbc:postgresql://host/db", dialect = "sqlserver", table = "t" """)
    }
    assert(ex.getMessage.contains("JDBC_003"))
  }

  test("url is required") {
    val ex = intercept[IllegalArgumentException] { parse("""table = "t" """) }
    assert(ex.getMessage.contains("source.url is required"))
  }

  test("uninferable dialect requires explicit dialect") {
    val ex = intercept[IllegalArgumentException] {
      parse("""url = "jdbc:h2:mem:x", table = "t" """)
    }
    assert(ex.getMessage.contains("Cannot infer dialect"))
  }

  test("generic dialect requires an explicit driver") {
    val ex = intercept[IllegalArgumentException] {
      parse("""url = "jdbc:h2:mem:x", dialect = "generic", table = "t" """)
    }
    assert(ex.getMessage.contains("source.driver is required"))
  }

  test("invalid mode is rejected; each mode enforces its required fields") {
    assert(intercept[IllegalArgumentException] {
      parse(sqlServerBase + """mode = "GUESS"""")
    }.getMessage.contains("JDBC_003"))

    assert(intercept[IllegalArgumentException] {
      parse("""url = "jdbc:sqlserver://h;databaseName=d", mode = "FULL_TABLE"""")
    }.getMessage.contains("source.table is required"))

    assert(intercept[IllegalArgumentException] {
      parse("""url = "jdbc:sqlserver://h;databaseName=d", mode = "CUSTOM_SQL"""")
    }.getMessage.contains("source.sql is required"))
  }

  test("partitioning is atomic: all four options or none") {
    assert(intercept[IllegalArgumentException] {
      parse(sqlServerBase + """partitionColumn = "id"""")
    }.getMessage.contains("configured together"))

    // numPartitions alone is also a partial group now
    assert(intercept[IllegalArgumentException] {
      parse(sqlServerBase + """numPartitions = 8""")
    }.getMessage.contains("configured together"))

    val ok = parse(sqlServerBase +
      """
        |numPartitions = 8
        |partitionColumn = "id"
        |lowerBound = 1
        |upperBound = 1000000
      """.stripMargin)
    assert(ok.partitioning.numPartitions.contains(8))
  }

  test("partition bounds and counts are validated") {
    def withPartitioning(n: Int, lo: Long, hi: Long, extra: String = "") = sqlServerBase +
      s"""
         |numPartitions = $n
         |partitionColumn = "id"
         |lowerBound = $lo
         |upperBound = $hi
         |$extra
       """.stripMargin

    assert(intercept[IllegalArgumentException] { parse(withPartitioning(0, 1, 100)) }
      .getMessage.contains("greater than zero"))
    assert(intercept[IllegalArgumentException] { parse(withPartitioning(8, 100, 100)) }
      .getMessage.contains("less than upperBound"))
    assert(intercept[IllegalArgumentException] { parse(withPartitioning(500, 1, 100)) }
      .getMessage.contains("operational maximum"))
    // The cap can be raised deliberately
    assert(parse(withPartitioning(500, 1, 100000, "max_partitions = 512"))
      .partitioning.numPartitions.contains(500))
  }

  test("unprotected TIMESTAMP watermark warns by default and can be made to fail") {
    def incremental(policy: String) = sqlServerBase +
      s"""
         |mode = "INCREMENTAL"
         |incremental {
         |  watermark_type = "TIMESTAMP"
         |  watermark_columns = ["modified_ts"]
         |  initial_value = "1900-01-01 00:00:00"
         |  $policy
         |}
       """.stripMargin

    // Default WARN: parses (overlap-less timestamp accepted with a warning)
    assert(parse(incremental("")).watermark.isDefined)

    // FAIL policy rejects the unprotected configuration
    assert(intercept[IllegalArgumentException] {
      parse(incremental("""on_unprotected_watermark = "FAIL""""))
    }.getMessage.contains("equal-timestamp"))

    // An overlap or a composite tie-breaker satisfies the guard
    assert(parse(incremental("""overlap = "300", on_unprotected_watermark = "FAIL"""")).watermark.isDefined)
  }

  test("diagnostics.log_sql defaults off") {
    assert(!parse(sqlServerBase).logSql)
    assert(parse(sqlServerBase + """diagnostics { log_sql = true }""").logSql)
  }

  test("INCREMENTAL requires a full incremental block") {
    assert(intercept[IllegalArgumentException] {
      parse(sqlServerBase + """mode = "INCREMENTAL"""")
    }.getMessage.contains("incremental block"))

    val cfg = parse(sqlServerBase +
      """
        |mode = "INCREMENTAL"
        |incremental {
        |  watermark_type = "TIMESTAMP"
        |  watermark_columns = ["modified_ts"]
        |  initial_value = "1900-01-01 00:00:00"
        |  overlap = "300"
        |  watermark_store { type = "memory" }
        |}
      """.stripMargin)
    val wm = cfg.watermark.get
    assert(wm.watermarkType == WatermarkType.Timestamp)
    assert(wm.columns == Seq("modified_ts"))
    assert(wm.overlap.contains(BigDecimal(300)))
    assert(wm.storeType == "memory")
  }

  test("COMPOSITE watermarks need 2+ columns and matching column_types") {
    assert(intercept[IllegalArgumentException] {
      parse(sqlServerBase +
        """
          |mode = "INCREMENTAL"
          |incremental {
          |  watermark_type = "COMPOSITE"
          |  watermark_columns = ["ts"]
          |  initial_value = "x"
          |}
        """.stripMargin)
    }.getMessage.contains("at least two"))

    assert(intercept[IllegalArgumentException] {
      parse(sqlServerBase +
        """
          |mode = "INCREMENTAL"
          |incremental {
          |  watermark_type = "COMPOSITE"
          |  watermark_columns = ["ts", "id"]
          |  column_types = ["TIMESTAMP"]
          |  initial_value = "x|0"
          |}
        """.stripMargin)
    }.getMessage.contains("column_types must match"))
  }

  test("single-column watermark types reject multiple columns") {
    val ex = intercept[IllegalArgumentException] {
      parse(sqlServerBase +
        """
          |mode = "INCREMENTAL"
          |incremental {
          |  watermark_type = "NUMERIC"
          |  watermark_columns = ["a", "b"]
          |  initial_value = "0"
          |}
        """.stripMargin)
    }
    assert(ex.getMessage.contains("exactly one"))
  }

  test("auth block resolves user and secret-provider password") {
    System.setProperty("jdbc.test.pwd", "s3cret")
    val cfg = parse(sqlServerBase +
      """
        |auth {
        |  user = "svc_ingest"
        |  password = { provider = "sysprop", key = "jdbc.test.pwd" }
        |}
      """.stripMargin)
    assert(cfg.user.contains("svc_ingest"))
    assert(cfg.password.contains("s3cret"))
  }

  test("auth.user resolves through secret providers too (vault-served user ids)") {
    System.setProperty("jdbc.test.user", "svc_from_vault")
    System.setProperty("jdbc.test.pwd2", "p2")
    val cfg = parse(sqlServerBase +
      """
        |auth {
        |  user = { provider = "sysprop", key = "jdbc.test.user" }
        |  password = { provider = "sysprop", key = "jdbc.test.pwd2" }
        |}
      """.stripMargin)
    assert(cfg.user.contains("svc_from_vault"))
    assert(cfg.password.contains("p2"))
  }

  test("explicit connection_properties override dialect defaults") {
    // encrypt=false is a TLS downgrade and now requires the explicit opt-in
    // (ConnectionHardeningTest covers the rejection path).
    val cfg = parse(sqlServerBase + "allow_insecure_tls = true\n" +
      """connection_properties { encrypt = "false", applicationName = "ingest" }""")
    assert(cfg.connectionProperties("encrypt") == "false")
    assert(cfg.connectionProperties("applicationName") == "ingest")
  }
}
