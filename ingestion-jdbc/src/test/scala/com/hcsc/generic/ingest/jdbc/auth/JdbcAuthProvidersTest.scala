package com.hcsc.generic.ingest.jdbc.auth

import com.hcsc.generic.ingest.jdbc.JdbcSourceConfig
import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite

/** Pluggable authentication provider framework: new modes, aliases,
  * incompatible-field rejection and empty-secret rejection. */
class JdbcAuthProvidersTest extends AnyFunSuite {

  private def parse(hocon: String) = JdbcSourceConfig.parse(ConfigFactory.parseString(
    s"""url = "jdbc:sqlserver://myserver.database.windows.net:1433;databaseName=db"
       |table = "dbo.t"
       |$hocon""".stripMargin))

  test("ENTRA_DEFAULT maps to ActiveDirectoryDefault with no credentials") {
    val cfg = parse("""auth { type = "ENTRA_DEFAULT" }""")
    assert(cfg.authType == "ENTRA_DEFAULT")
    assert(cfg.connectionProperties("authentication") == "ActiveDirectoryDefault")
    assert(cfg.user.isEmpty && cfg.password.isEmpty)
  }

  test("ENTRA_INTEGRATED maps to ActiveDirectoryIntegrated") {
    val cfg = parse("""auth { type = "ENTRA_INTEGRATED" }""")
    assert(cfg.connectionProperties("authentication") == "ActiveDirectoryIntegrated")
  }

  test("legacy alias names resolve to their canonical providers") {
    assert(parse("""auth { type = "AZURE_MANAGED_IDENTITY" }""").authType == "MANAGED_IDENTITY")
    assert(parse("""auth { type = "MANAGED_IDENTITY" }""").authType == "MANAGED_IDENTITY")
    assert(parse(
      """auth { type = "ENTRA_ID_PASSWORD", user = "u@t.com",
        |  password = { provider = "inline", value = "p" } }""".stripMargin)
      .authType == "ENTRA_PASSWORD")
    assert(parse("""auth { type = "AZURE_DEFAULT_CREDENTIAL" }""").authType == "ENTRA_DEFAULT")
  }

  test("incompatible fields are rejected per mode") {
    assert(intercept[IllegalArgumentException] {
      parse("""auth { type = "MANAGED_IDENTITY", password = { provider = "inline", value = "p" } }""")
    }.getMessage.contains("does not accept"))

    assert(intercept[IllegalArgumentException] {
      parse("""auth { type = "ENTRA_INTEGRATED", user = "someone" }""")
    }.getMessage.contains("does not accept"))

    assert(intercept[IllegalArgumentException] {
      parse("""auth { type = "ACCESS_TOKEN", user = "u", token = { provider = "inline", value = "t" } }""")
    }.getMessage.contains("does not accept"))
  }

  test("secrets that resolve to blank values are rejected (JDBC_002)") {
    System.setProperty("blank.secret.test", "   ")
    val ex = intercept[IllegalArgumentException] {
      parse(
        """auth {
          |  type = "ENTRA_SERVICE_PRINCIPAL"
          |  client_id = "cid"
          |  client_secret = { provider = "sysprop", key = "blank.secret.test" }
          |}""".stripMargin)
    }
    assert(ex.getMessage.contains("JDBC_002"))
    assert(ex.getMessage.contains("empty"))
  }

  test("registry lists canonical names and accepts custom providers") {
    val names = JdbcAuthenticationProviders.availableNames
    assert(names.contains("ENTRA_DEFAULT"))
    assert(names.contains("ENTRA_INTEGRATED"))
    assert(names.contains("SQL_PASSWORD"))

    object TestAuth extends JdbcAuthenticationProvider {
      val authenticationType = "TEST_MODE"
      def resolve(auth: Option[com.typesafe.config.Config], source: com.typesafe.config.Config) =
        ResolvedAuth(None, None, Map("authentication" -> "Test"))
    }
    JdbcAuthenticationProviders.register(TestAuth)
    assert(JdbcAuthenticationProviders.resolve("test_mode") == TestAuth)
  }

  test("unknown auth types list the available modes") {
    val ex = intercept[IllegalArgumentException] { parse("""auth { type = "KERBEROS" }""") }
    assert(ex.getMessage.contains("JDBC_003"))
    assert(ex.getMessage.contains("MANAGED_IDENTITY"))
  }
}
