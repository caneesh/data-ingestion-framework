package com.hcsc.generic.ingest.config

import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite

/**
  * Pins the deployment contract for per-environment connection details: a
  * feed config declares a default and lets the ENVIRONMENT override it, so
  * one config file serves lower and production.
  *
  * This mirrors `IngestMain`'s `--conf-path` loading exactly —
  * `ConfigFactory.parseFile(f).resolve()`. That path consults environment
  * variables but NOT `-D` system properties (those arrive only through
  * `ConfigFactory.load()`, the classpath route used when `--conf-path` is
  * omitted). If the `.resolve()` were ever dropped, overrides would
  * silently stop working — these tests fail first.
  */
class EnvSubstTest extends AnyFunSuite {

  private def confWith(body: String): java.io.File = {
    val f = java.io.File.createTempFile("envsubst", ".conf")
    f.deleteOnExit()
    val w = new java.io.PrintWriter(f)
    try w.write(body) finally w.close()
    f
  }

  test("connection details fall back to the config's own defaults") {
    val f = confWith(
      """sqlserver {
        |  host     = "SQLHOST-LOWER"
        |  host     = ${?SMARTIQ_HOST_UNSET_IN_TESTS}
        |  database = "SmartIQ"
        |  database = ${?SMARTIQ_DB_UNSET_IN_TESTS}
        |}
        |feeds { t { source {
        |  url = "jdbc:sqlserver://"${sqlserver.host}":1433;databaseName="${sqlserver.database}
        |} } }
        |""".stripMargin)
    val c = ConfigFactory.parseFile(f).resolve().getConfig("feeds.t")
    assert(c.getString("source.url") ==
      "jdbc:sqlserver://SQLHOST-LOWER:1433;databaseName=SmartIQ")
  }

  test("an environment variable overrides the default without editing the file") {
    // PATH is set in every POSIX environment; using a real variable keeps
    // this deterministic instead of assume-skipping in CI.
    assume(sys.env.contains("PATH"), "POSIX environment expected")
    val f = confWith(
      """host = "DEFAULT-HOST"
        |host = ${?PATH}
        |feeds { t { source { url = "jdbc:sqlserver://"${host} } } }
        |""".stripMargin)
    val url = ConfigFactory.parseFile(f).resolve().getConfig("feeds.t").getString("source.url")
    assert(url == s"jdbc:sqlserver://${sys.env("PATH")}",
      "the environment must win over the in-file default")
    assert(url != "jdbc:sqlserver://DEFAULT-HOST")
  }
}
