package com.hcsc.generic.ingest.jdbc.read

import com.hcsc.generic.ingest.jdbc.JdbcSourceConfig
import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite

class QueryBuilderTest extends AnyFunSuite {

  private def cfg(hocon: String) = JdbcSourceConfig.parse(ConfigFactory.parseString(
    s"""url = "jdbc:sqlserver://h;databaseName=d"
       |$hocon""".stripMargin))

  test("FULL_TABLE passes the bare table for fast metadata paths") {
    assert(QueryBuilder.dbtable(cfg("""table = "dbo.claims""""), None) == "dbo.claims")
  }

  test("SELECT_QUERY builds projection and where") {
    val c = cfg(
      """mode = "SELECT_QUERY"
        |table = "dbo.claims"
        |columns = ["claim_id", "amount"]
        |where = "state = 'IL'"
      """.stripMargin)
    assert(QueryBuilder.dbtable(c, None) ==
      "(SELECT claim_id, amount FROM dbo.claims WHERE state = 'IL') src")
  }

  test("SELECT_QUERY defaults to * without configured columns") {
    val c = cfg("""mode = "SELECT_QUERY", table = "dbo.claims"""")
    assert(QueryBuilder.dbtable(c, None) == "(SELECT * FROM dbo.claims) src")
  }

  test("CUSTOM_SQL wraps the statement as a subquery") {
    val c = cfg("""mode = "CUSTOM_SQL", sql = "SELECT a, b FROM t JOIN u ON t.id = u.id"""")
    assert(QueryBuilder.dbtable(c, None) ==
      "(SELECT a, b FROM t JOIN u ON t.id = u.id) src")
  }

  test("INCREMENTAL over a table combines where and watermark predicates") {
    val c = cfg(
      """mode = "INCREMENTAL"
        |table = "dbo.claims"
        |where = "state = 'IL'"
        |incremental {
        |  watermark_type = "TIMESTAMP"
        |  watermark_columns = ["modified_ts"]
        |  initial_value = "1900-01-01 00:00:00"
        |}
      """.stripMargin)
    val out = QueryBuilder.dbtable(c, Some("[modified_ts] > '2026-01-01 00:00:00.0'"))
    assert(out == "(SELECT * FROM dbo.claims WHERE (state = 'IL') AND ([modified_ts] > '2026-01-01 00:00:00.0')) src")
  }

  test("INCREMENTAL over custom sql wraps the base query") {
    val c = cfg(
      """mode = "INCREMENTAL"
        |sql = "SELECT * FROM a JOIN b ON a.id = b.id"
        |incremental {
        |  watermark_type = "NUMERIC"
        |  watermark_columns = ["id"]
        |  initial_value = "0"
        |}
      """.stripMargin)
    val out = QueryBuilder.dbtable(c, Some("[id] > 0"))
    assert(out == "(SELECT * FROM (SELECT * FROM a JOIN b ON a.id = b.id) base WHERE ([id] > 0)) src")
  }

  test("unsafe table names are rejected (JDBC_003)") {
    val c = cfg("""table = "claims; DROP TABLE x"""")
    val ex = intercept[IllegalArgumentException] { QueryBuilder.dbtable(c, None) }
    assert(ex.getMessage.contains("JDBC_003"))
  }
}
