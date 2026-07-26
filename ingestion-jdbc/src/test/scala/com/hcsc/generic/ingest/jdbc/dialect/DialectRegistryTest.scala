package com.hcsc.generic.ingest.jdbc.dialect

import org.scalatest.funsuite.AnyFunSuite

class DialectRegistryTest extends AnyFunSuite {

  test("all shipped dialects are registered with drivers and prefixes") {
    val expected = Map(
      "sqlserver" -> ("com.microsoft.sqlserver.jdbc.SQLServerDriver", "jdbc:sqlserver://"),
      "postgresql" -> ("org.postgresql.Driver", "jdbc:postgresql://"),
      "oracle" -> ("oracle.jdbc.OracleDriver", "jdbc:oracle:"),
      "db2" -> ("com.ibm.db2.jcc.DB2Driver", "jdbc:db2://"),
      "mysql" -> ("com.mysql.cj.jdbc.Driver", "jdbc:mysql://")
    )
    expected.foreach { case (name, (driver, prefix)) =>
      val d = DialectRegistry.resolve(name)
      assert(d.defaultDriver == driver)
      assert(d.urlPrefix == prefix)
    }
  }

  test("resolution is case-insensitive and unknown dialects fail with JDBC_003") {
    assert(DialectRegistry.resolve("SQLServer") == SqlServerDialect)
    val ex = intercept[IllegalArgumentException] { DialectRegistry.resolve("sybase") }
    assert(ex.getMessage.contains("JDBC_003"))
  }

  test("sqlserver enforces encryption defaults and bracket quoting") {
    assert(SqlServerDialect.defaultConnectionProperties("encrypt") == "true")
    assert(SqlServerDialect.quoteIdentifier("weird]name") == "[weird]]name]")
  }

  test("dialect-specific validation queries") {
    assert(SqlServerDialect.validationQuery == "SELECT 1")
    assert(OracleDialect.validationQuery == "SELECT 1 FROM DUAL")
    assert(Db2Dialect.validationQuery == "SELECT 1 FROM SYSIBM.SYSDUMMY1")
  }

  test("custom dialects can be registered (extension point)") {
    object TestDialect extends JdbcDialect {
      val name = "testdb"; val defaultDriver = "x.Driver"; val urlPrefix = "jdbc:testdb://"
    }
    DialectRegistry.register(TestDialect)
    assert(DialectRegistry.resolve("testdb") == TestDialect)
  }
}
