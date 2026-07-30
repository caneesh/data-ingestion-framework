package com.hcsc.generic.ingest.jdbc

import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite

import java.io.StringWriter
import java.sql.DriverManager

/** Captures JdbcSource's log output and proves that sensitive query content
  * (business predicates, watermark literals) never reaches the logs unless
  * diagnostics.log_sql is explicitly enabled. */
class JdbcLogSanitizationTest extends AnyFunSuite with SharedSparkSession {

  private val url = "jdbc:h2:mem:logsan_tests;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false"
  private val SensitiveMarker = "SENSITIVE_SSN_FILTER_VALUE"

  private def seed(): Unit = {
    Class.forName("org.h2.Driver")
    val conn = DriverManager.getConnection(url, "sa", "")
    try {
      val stmt = conn.createStatement()
      try {
        stmt.execute("DROP TABLE IF EXISTS people")
        stmt.execute("CREATE TABLE people (id INT, ssn VARCHAR(64))")
        stmt.execute(s"INSERT INTO people VALUES (1, '$SensitiveMarker')")
      } finally stmt.close()
    } finally conn.close()
  }

  private def confHocon(diagnostics: String) =
    ConfigFactory.parseString(
      s"""
         |type = "jdbc"
         |url = "$url"
         |dialect = "generic"
         |driver = "org.h2.Driver"
         |auth { user = "sa", password = { provider = "inline", value = "" } }
         |health_check { enabled = false }
         |mode = "SELECT_QUERY"
         |table = "people"
         |where = "ssn = '$SensitiveMarker'"
         |$diagnostics
      """.stripMargin)

  /** Spark 3.5 routes org.apache.log4j calls through the log4j2 bridge, so
    * the capture must attach a log4j2 appender (1.x appenders are no-ops). */
  private def captureLogs(body: => Unit): String = {
    import org.apache.logging.log4j.{Level, LogManager}
    import org.apache.logging.log4j.core.LoggerContext
    import org.apache.logging.log4j.core.appender.WriterAppender
    import org.apache.logging.log4j.core.layout.PatternLayout

    val writer = new StringWriter()
    val context = LogManager.getContext(false).asInstanceOf[LoggerContext]
    val config = context.getConfiguration
    val layout = PatternLayout.newBuilder().withPattern("%m%n").withConfiguration(config).build()
    val appender = WriterAppender.createAppender(layout, null, writer, "sanitization-capture", false, true)
    appender.start()
    config.getRootLogger.addAppender(appender, Level.INFO, null)
    context.updateLoggers()
    try body
    finally {
      config.getRootLogger.removeAppender("sanitization-capture")
      context.updateLoggers()
      appender.stop()
    }
    writer.toString
  }

  test("default logging emits query hash but never the predicate content") {
    seed()
    val logs = captureLogs {
      JdbcSource.read(spark, confHocon("")).count()
    }
    assert(logs.contains("queryHash="), "sanitized log must carry a query hash")
    assert(!logs.contains(SensitiveMarker),
      "business predicate content must not appear in default logs")
    assert(!logs.contains("DIAGNOSTIC"))
  }

  test("diagnostics.log_sql=true opts in to full SQL logging") {
    seed()
    val logs = captureLogs {
      JdbcSource.read(spark, confHocon("diagnostics { log_sql = true }")).count()
    }
    assert(logs.contains("DIAGNOSTIC"))
    assert(logs.contains(SensitiveMarker), "diagnostic mode logs the full dbtable")
  }

  test("URL masking covers brace-escaped passwords (SQL Server convention)") {
    import com.hcsc.generic.ingest.jdbc.health.JdbcHealthCheck.sanitized
    // Fix: [^;&]* stopped at the first embedded ';' and leaked the tail.
    val braced = "jdbc:sqlserver://host;databaseName=db;password={my;secret&tail};encrypt=true"
    val masked = sanitized(braced)
    assert(!masked.contains("my;secret"), masked)
    assert(!masked.contains("secret&tail"), masked)
    assert(masked.contains("password=***"))
    assert(masked.contains("encrypt=true"), "non-secret properties stay readable")

    val plain = sanitized("jdbc:mysql://host/db?user=x&pwd=topsecret&ssl=true")
    assert(!plain.contains("topsecret"), plain)
    assert(plain.contains("ssl=true"))
  }

  test("driver exception messages are masked before logging") {
    import com.hcsc.generic.ingest.jdbc.health.JdbcHealthCheck.sanitizedMessage
    // Fix: the JDK's 'No suitable driver found for <url>' echoes the raw URL,
    // credentials included, and previously reached logs unsanitized.
    val e = new java.sql.SQLException(
      "No suitable driver found for jdbc:weird://h;password=hunter2;x=1")
    val masked = sanitizedMessage(e)
    assert(!masked.contains("hunter2"), masked)
    assert(masked.contains("password=***"))

    assert(sanitizedMessage(new RuntimeException(null: String)) == "RuntimeException",
      "null messages must not NPE")
  }
}
