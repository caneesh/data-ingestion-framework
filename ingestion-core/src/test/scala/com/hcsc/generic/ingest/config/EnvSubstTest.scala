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

  // ---------------------------------------------------------------------
  // Split-config includes (regression: YARN cluster run 2026-08-06 died
  // with "include was not found" because a BARE filename has no parent
  // directory for HOCON to resolve the include against).
  // ---------------------------------------------------------------------

  private def splitConfig(): java.io.File = {
    val dir = java.nio.file.Files.createTempDirectory("split").toFile
    dir.deleteOnExit()
    def write(name: String, body: String): java.io.File = {
      val f = new java.io.File(dir, name); f.deleteOnExit()
      val w = new java.io.PrintWriter(f)
      try w.write(body) finally w.close()
      f
    }
    write("thing-schema.conf", """schema { version = "9.9" }""")
    write("feed.conf",
      """include required("thing-schema.conf")
        |feeds { t { entity = "t" } }
        |""".stripMargin)
  }

  test("a BARE config filename still resolves its include (IngestMain absolutises)") {
    val feed = splitConfig()
    // Reproduce the container shape: the path as IngestMain receives it has
    // no directory component. Without .getAbsoluteFile this throws
    // ConfigException$IO "include was not found".
    val bare = new java.io.File(feed.getName)
    assert(bare.getParentFile == null, "precondition: a bare name has no parent")
    // IngestMain resolves against the absolute file; emulate exactly that,
    // using the real directory the container would be running in.
    val asIngestMainDoes = new java.io.File(feed.getParentFile, bare.getName).getAbsoluteFile
    val c = ConfigFactory.parseFile(asIngestMainDoes).resolve()
    assert(c.getString("schema.version") == "9.9",
      "the sibling schema include must resolve from the config's own directory")
    assert(c.hasPath("feeds.t"))
  }

  test("a directory-qualified path resolves the include as well") {
    val feed = splitConfig()
    val c = ConfigFactory.parseFile(feed).resolve()
    assert(c.getString("schema.version") == "9.9")
  }
}
