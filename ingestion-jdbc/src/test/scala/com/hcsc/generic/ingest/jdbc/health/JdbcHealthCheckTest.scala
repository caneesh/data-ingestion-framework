package com.hcsc.generic.ingest.jdbc.health

import com.hcsc.generic.ingest.jdbc.{H2TestDatabase, JdbcSourceConfig}
import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite

class JdbcHealthCheckTest extends AnyFunSuite {

  test("healthy H2 connection passes") {
    val cfg = JdbcSourceConfig.parse(ConfigFactory.parseString(
      H2TestDatabase.sourceHocon("""table = "dual_x"""")))
    assert(JdbcHealthCheck.check(cfg).isRight)
  }

  test("unreachable database fails with JDBC_001") {
    val cfg = JdbcSourceConfig.parse(ConfigFactory.parseString(
      """
        |url = "jdbc:h2:tcp://localhost:19999/nope"
        |dialect = "generic"
        |driver = "org.h2.Driver"
        |table = "t"
      """.stripMargin))
    val result = JdbcHealthCheck.check(cfg)
    assert(result.isLeft)
    assert(result.left.get.contains("JDBC_001"))
  }

  test("missing driver class fails with JDBC_001") {
    val cfg = JdbcSourceConfig.parse(ConfigFactory.parseString(
      """
        |url = "jdbc:h2:mem:x"
        |dialect = "generic"
        |driver = "com.example.MissingDriver"
        |table = "t"
      """.stripMargin))
    val result = JdbcHealthCheck.check(cfg)
    assert(result.isLeft)
    assert(result.left.get.contains("Driver class"))
  }

  test("sanitized urls mask embedded credentials") {
    assert(JdbcHealthCheck.sanitized("jdbc:sqlserver://h;user=u;password=hunter2;encrypt=true") ==
      "jdbc:sqlserver://h;user=u;password=***;encrypt=true")
    assert(JdbcHealthCheck.sanitized("jdbc:mysql://h/db?user=u&pwd=secret&x=1") ==
      "jdbc:mysql://h/db?user=u&pwd=***&x=1")
  }
}
