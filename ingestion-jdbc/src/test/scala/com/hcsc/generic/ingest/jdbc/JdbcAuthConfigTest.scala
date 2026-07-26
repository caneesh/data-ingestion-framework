package com.hcsc.generic.ingest.jdbc

import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite

/** Azure-native authentication mapped onto Microsoft driver properties. */
class JdbcAuthConfigTest extends AnyFunSuite {

  private def parse(hocon: String) = JdbcSourceConfig.parse(ConfigFactory.parseString(
    s"""url = "jdbc:sqlserver://myserver.database.windows.net:1433;databaseName=db"
       |table = "dbo.t"
       |$hocon""".stripMargin))

  test("default SQL_PASSWORD keeps user/password semantics") {
    val cfg = parse("""auth { user = "u", password = { provider = "inline", value = "p" } }""")
    assert(cfg.authType == AuthType.SqlPassword)
    assert(cfg.user.contains("u"))
    assert(cfg.password.contains("p"))
    assert(!cfg.connectionProperties.contains("authentication"))
  }

  test("AZURE_MANAGED_IDENTITY maps to ActiveDirectoryMSI with optional client id") {
    val cfg = parse("""auth { type = "AZURE_MANAGED_IDENTITY", client_id = "mi-client" }""")
    assert(cfg.connectionProperties("authentication") == "ActiveDirectoryMSI")
    assert(cfg.connectionProperties("msiClientId") == "mi-client")
    assert(cfg.user.isEmpty && cfg.password.isEmpty)

    val noClient = parse("""auth { type = "AZURE_MANAGED_IDENTITY" }""")
    assert(!noClient.connectionProperties.contains("msiClientId"))
  }

  test("AZURE_SERVICE_PRINCIPAL uses client id/secret as credentials") {
    System.setProperty("sp.secret.test", "sp-secret-value")
    val cfg = parse(
      """auth {
        |  type = "AZURE_SERVICE_PRINCIPAL"
        |  client_id = "app-client-id"
        |  client_secret = { provider = "sysprop", key = "sp.secret.test" }
        |}""".stripMargin)
    assert(cfg.connectionProperties("authentication") == "ActiveDirectoryServicePrincipal")
    assert(cfg.user.contains("app-client-id"))
    assert(cfg.password.contains("sp-secret-value"))
  }

  test("ENTRA_ID_PASSWORD maps to ActiveDirectoryPassword") {
    val cfg = parse(
      """auth {
        |  type = "ENTRA_ID_PASSWORD"
        |  user = "user@tenant.com"
        |  password = { provider = "inline", value = "pw" }
        |}""".stripMargin)
    assert(cfg.connectionProperties("authentication") == "ActiveDirectoryPassword")
    assert(cfg.user.contains("user@tenant.com"))
  }

  test("ACCESS_TOKEN carries a secret-backed token property") {
    System.setProperty("token.test", "eyJ-token")
    val cfg = parse(
      """auth { type = "ACCESS_TOKEN", token = { provider = "sysprop", key = "token.test" } }""")
    assert(cfg.connectionProperties("accessToken") == "eyJ-token")
    assert(cfg.user.isEmpty && cfg.password.isEmpty)
  }

  test("Azure auth types are sqlserver-only; missing secrets fail clearly") {
    val ex = intercept[IllegalArgumentException] {
      JdbcSourceConfig.parse(ConfigFactory.parseString(
        """url = "jdbc:postgresql://h/db"
          |table = "t"
          |auth { type = "AZURE_MANAGED_IDENTITY" }""".stripMargin))
    }
    assert(ex.getMessage.contains("only supported for the sqlserver dialect"))

    assert(intercept[IllegalArgumentException] {
      parse("""auth { type = "AZURE_SERVICE_PRINCIPAL", client_id = "x" }""")
    }.getMessage.contains("client_secret"))

    assert(intercept[IllegalArgumentException] {
      parse("""auth { type = "KERBEROS" }""")
    }.getMessage.contains("auth.type"))
  }
}
