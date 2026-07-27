package com.hcsc.generic.ingest.jdbc

import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite

/** TLS-downgrade guard on connection_properties and the log_sql /
  * SECRET_PROVIDER diagnostic suppression. */
class ConnectionHardeningTest extends AnyFunSuite {

  private def parse(extra: String) = JdbcSourceConfig.parse(ConfigFactory.parseString(
    s"""url = "jdbc:sqlserver://myserver:1433;databaseName=db"
       |table = "dbo.t"
       |auth { user = "u", password = { provider = "inline", value = "p" } }
       |$extra""".stripMargin))

  // ---------------------------------------------------------------------------
  // TLS downgrade guard
  // ---------------------------------------------------------------------------

  test("trustServerCertificate=true is rejected without explicit approval") {
    val ex = intercept[IllegalArgumentException] {
      parse("""connection_properties { trustServerCertificate = "true" }""")
    }
    assert(ex.getMessage.contains("JDBC_003"))
    assert(ex.getMessage.contains("trustServerCertificate"))
    assert(ex.getMessage.contains("allow_insecure_tls"))
  }

  test("encrypt=false is rejected; keys and values match case-insensitively") {
    assert(intercept[IllegalArgumentException] {
      parse("""connection_properties { encrypt = "false" }""")
    }.getMessage.contains("encrypt=false"))

    assert(intercept[IllegalArgumentException] {
      parse("""connection_properties { TRUSTSERVERCERTIFICATE = "TRUE" }""")
    }.getMessage.contains("allow_insecure_tls"))
  }

  test("the explicit opt-in permits the downgrade (loudly)") {
    val cfg = parse(
      """allow_insecure_tls = true
        |connection_properties { trustServerCertificate = "true" }""".stripMargin)
    assert(cfg.connectionProperties("trustServerCertificate") == "true")
  }

  test("benign overrides pass and the secure dialect defaults stay intact") {
    val cfg = parse("""connection_properties { applicationName = "ingest" }""")
    assert(cfg.connectionProperties("applicationName") == "ingest")
    assert(cfg.connectionProperties("encrypt") == "true")
    assert(cfg.connectionProperties("trustServerCertificate") == "false")

    // Restating the secure values is not a downgrade and needs no approval.
    val explicit = parse("""connection_properties { encrypt = "true", trustServerCertificate = "false" }""")
    assert(explicit.connectionProperties("encrypt") == "true")
  }

  // ---------------------------------------------------------------------------
  // log_sql vs SECRET_PROVIDER parameters
  // ---------------------------------------------------------------------------

  test("log_sql diagnostics are unsafe when any parameter resolves from a secret provider") {
    System.setProperty("hardening.test.secret", "tok-value")
    val secretCfg = parse(
      """mode = "SQL_TEMPLATE"
        |sql = "SELECT c FROM dbo.t WHERE token = :tok"
        |parameters = [
        |  { name = "tok", from = { provider = "sysprop", key = "hardening.test.secret" } }
        |]
        |diagnostics { log_sql = true }""".stripMargin)
    assert(secretCfg.logSql)
    assert(!JdbcSource.sqlDiagnosticsSafe(secretCfg),
      "SECRET_PROVIDER parameter must suppress full-SQL diagnostics")

    val plainCfg = parse(
      """mode = "SQL_TEMPLATE"
        |sql = "SELECT c FROM dbo.t WHERE state = :st"
        |parameters = [ { name = "st", value = "IL" } ]
        |diagnostics { log_sql = true }""".stripMargin)
    assert(JdbcSource.sqlDiagnosticsSafe(plainCfg),
      "non-secret parameters keep the opt-in diagnostic available")
  }
}
