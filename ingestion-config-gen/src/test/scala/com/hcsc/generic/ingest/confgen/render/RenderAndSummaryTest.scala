package com.hcsc.generic.ingest.confgen.render

import com.hcsc.generic.ingest.confgen.summary.ConfigSummary
import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite

/** Output renderers (HOCON/JSON/YAML) and the masked summary. */
class RenderAndSummaryTest extends AnyFunSuite {

  private val feed = ConfigFactory.parseString(
    """feeds.claims {
      |  entity = "claims"
      |  source {
      |    type = "jdbc"
      |    url = "jdbc:sqlserver://host;databaseName=db"
      |    fetchsize = 1000
      |    auth {
      |      type = "SQL_PASSWORD"
      |      user = "svc"
      |      password = { provider = "env", key = "PWD_VAR" }
      |    }
      |    where = "state = 'IL' AND type = 'A'"
      |  }
      |  raw { database = "raw_db", table = "claims", path = "/tmp/x", format = "orc" }
      |}""".stripMargin)

  test("HOCON output re-parses to an identical config") {
    val rendered = ConfigRenderers.render(feed, "hocon")
    assert(ConfigFactory.parseString(rendered) == feed)
  }

  test("JSON output is valid JSON and re-parses to an identical config") {
    val rendered = ConfigRenderers.render(feed, "json")
    assert(rendered.trim.startsWith("{"))
    assert(ConfigFactory.parseString(rendered) == feed)
  }

  test("YAML output quotes strings, keeps native numbers and nests correctly") {
    val yaml = ConfigRenderers.render(feed, "yaml")
    assert(yaml.contains("fetchsize: 1000"))            // number unquoted
    assert(yaml.contains("user: 'svc'"))                // string quoted
    assert(yaml.contains("where: 'state = ''IL'' AND type = ''A'''")) // escaping
    val lines = yaml.split("\n")
    val authLine = lines.indexWhere(_.trim == "auth:")
    assert(authLine > 0)
    assert(lines(authLine + 1).startsWith("        ")) // nested one level deeper
  }

  test("unknown format fails fast") {
    val ex = intercept[IllegalArgumentException] { ConfigRenderers.render(feed, "toml") }
    assert(ex.getMessage.contains("toml"))
  }

  test("summary masks inline secrets but keeps secret-reference metadata visible") {
    val config = ConfigFactory.parseString(
      """source {
        |  auth {
        |    user = "svc"
        |    password = { provider = "inline", value = "super-secret" }
        |    client_secret = { provider = "env", key = "SP_SECRET" }
        |  }
        |  kafka.sasl.jaas.config = "PlainLoginModule password=hunter2"
        |}""".stripMargin)
    val masked = ConfigRenderers.render(ConfigSummary.masked(config), "hocon")

    assert(!masked.contains("super-secret"))
    assert(!masked.contains("hunter2"))
    assert(masked.contains("********"))
    assert(masked.contains("svc"))          // non-secret values stay readable
    assert(masked.contains("SP_SECRET"))    // reference metadata stays reviewable
    assert(masked.contains("env"))
  }
}
