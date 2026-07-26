package com.hcsc.generic.ingest.jdbc.read

import com.hcsc.generic.ingest.jdbc.JdbcSourceConfig
import com.typesafe.config.ConfigFactory
import org.apache.log4j.Logger
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.sql.DriverManager

class DriverQueriesTest extends AnyFunSuite with BeforeAndAfterAll {

  private val logger = Logger.getLogger(getClass.getName)
  private val url = "jdbc:h2:mem:driverq_tests;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false"

  override def beforeAll(): Unit = {
    super.beforeAll()
    Class.forName("org.h2.Driver")
    val conn = DriverManager.getConnection(url, "sa", "")
    try {
      val stmt = conn.createStatement()
      try {
        stmt.execute("DROP TABLE IF EXISTS t")
        stmt.execute("CREATE TABLE t (id INT, ts TIMESTAMP)")
        stmt.execute("INSERT INTO t VALUES (7, '2026-05-01 08:30:00')")
        stmt.execute("INSERT INTO t VALUES (3, '2026-04-01 08:30:00')")
        stmt.execute("DROP TABLE IF EXISTS empty_t")
        stmt.execute("CREATE TABLE empty_t (id INT)")
      } finally stmt.close()
    } finally conn.close()
  }

  private val cfg = JdbcSourceConfig.parse(ConfigFactory.parseString(
    s"""
       |url = "$url"
       |dialect = "generic"
       |driver = "org.h2.Driver"
       |auth { user = "sa", password = { provider = "inline", value = "" } }
       |table = "t"
       |retry { max_attempts = 1, backoff_ms = 1 }
    """.stripMargin))

  test("scalar MAX query returns canonical string forms") {
    val row = DriverQueries.firstRow(cfg, "SELECT MAX(id) FROM t", logger).get
    assert(row == Seq(Some("7")))

    val ts = DriverQueries.firstRow(cfg, "SELECT MAX(ts) FROM t", logger).get
    assert(ts.head.get.startsWith("2026-05-01 08:30:00"))
  }

  test("multi-column top-one query returns all values in order") {
    val sql = cfg.dialect.selectTopOne("ts, id", "t", "ts DESC, id DESC")
    val row = DriverQueries.firstRow(cfg, sql, logger).get
    assert(row.size == 2)
    assert(row.head.get.startsWith("2026-05-01"))
    assert(row(1).contains("7"))
  }

  test("empty tables and all-null aggregates return None") {
    assert(DriverQueries.firstRow(cfg, "SELECT MAX(id) FROM empty_t", logger).isEmpty)
    assert(DriverQueries.firstRow(cfg, "SELECT id FROM empty_t", logger).isEmpty)
  }

  test("invalid SQL fails fast without retry (classified permanent)") {
    val start = System.currentTimeMillis()
    intercept[java.sql.SQLException] {
      DriverQueries.firstRow(cfg, "SELECT nope FROM missing_table", logger)
    }
    // Permanent classification means no backoff sleeps happened
    assert(System.currentTimeMillis() - start < 5000)
  }
}
