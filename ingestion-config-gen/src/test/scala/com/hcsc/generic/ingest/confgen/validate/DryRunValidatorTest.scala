package com.hcsc.generic.ingest.confgen.validate

import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite

/** Dry-run validation must reuse the framework's own parsers and stay
  * independent of this machine's secrets. */
class DryRunValidatorTest extends AnyFunSuite {

  private def jdbcFeed(extraSource: String = "", auth: String =
      """auth { user = "sa", password = { provider = "inline", value = "pw" } }""") =
    ConfigFactory.parseString(
      s"""entity = "claims"
         |source {
         |  type = "jdbc"
         |  url = "jdbc:h2:mem:dryrun"
         |  dialect = "generic"
         |  driver = "org.h2.Driver"
         |  $auth
         |  mode = "FULL_TABLE"
         |  table = "d_claims"
         |  $extraSource
         |}
         |raw { database = "raw_db", table = "claims", path = "/tmp/raw" }
         |""".stripMargin)

  test("a valid JDBC feed passes and previews its SQL") {
    val report = DryRunValidator.validate(jdbcFeed())
    assert(report.ok, report.errors.mkString("; "))
    assert(report.sqlPreview.contains("d_claims"))
  }

  test("framework parse errors surface verbatim (JDBC_003)") {
    val report = DryRunValidator.validate(jdbcFeed(extraSource =
      """partition_strategy = "MIN_MAX_QUERY""""))
    assert(!report.ok)
    assert(report.errors.exists(e =>
      e.contains("JDBC_003") && e.contains("MIN_MAX_QUERY")))
  }

  test("missing common fields are reported") {
    val report = DryRunValidator.validate(
      ConfigFactory.parseString("""entity = "x", source { type = "jdbc" }"""))
    assert(report.errors.exists(_.contains("raw.database")))
    // Each missing field must be reported INDEPENDENTLY — a disjunction with
    // !report.ok was unfalsifiable once any other error existed.
    assert(report.errors.exists(_.contains("url")),
      s"missing source.url must be reported by name, got: ${report.errors.mkString("; ")}")
    assert(!report.ok)
  }

  test("unresolvable secret references validate structurally but warn") {
    val feed = jdbcFeed(auth =
      """auth { user = "sa", password = { provider = "env", key = "CONFGEN_NO_SUCH_VAR_9Z" } }""")
    val report = DryRunValidator.validate(feed)
    assert(report.ok, report.errors.mkString("; "))
    assert(report.warnings.exists(w =>
      w.contains("password") && w.contains("did not resolve")))
  }

  test("connectivity test reuses JdbcHealthCheck against a live database") {
    val ok = DryRunValidator.connectivityTest(jdbcFeed(extraSource =
      """url = "jdbc:h2:mem:dryrun_live;DB_CLOSE_DELAY=-1""""))
    // parseString: later keys override; ensure our live-url feed connected
    assert(ok.isRight, ok)

    val bad = DryRunValidator.connectivityTest(
      ConfigFactory.parseString(
        """entity = "x"
          |source {
          |  type = "jdbc"
          |  url = "jdbc:h2:tcp://localhost:1/nope"
          |  dialect = "generic"
          |  driver = "org.h2.Driver"
          |  auth { user = "sa", password = { provider = "inline", value = "pw" } }
          |  table = "t"
          |}""".stripMargin))
    assert(bad.isLeft)
    assert(bad.left.get.contains("JDBC_001"))
  }

  test("file feed without path or folders fails; contract errors surface") {
    val report = DryRunValidator.validate(ConfigFactory.parseString(
      """entity = "m"
        |source { type = "file" }
        |raw { database = "d", table = "t", path = "/tmp/r" }
        |schema { columns = [] }
        |""".stripMargin))
    assert(report.errors.exists(_.contains("source.path or source.folders")))
    assert(report.errors.exists(e =>
      e.toLowerCase.contains("schema") || e.toLowerCase.contains("column")))
  }

  test("file feed without a contract passes with a warning") {
    val report = DryRunValidator.validate(ConfigFactory.parseString(
      """entity = "m"
        |source { type = "file", path = "/data/in/*.csv" }
        |raw { database = "d", table = "t", path = "/tmp/r" }
        |""".stripMargin))
    assert(report.ok, report.errors.mkString("; "))
    assert(report.warnings.exists(_.contains("no schema contract")))
  }

  test("incremental preview renders the first watermark window") {
    val feed = ConfigFactory.parseString(
      """entity = "claims"
        |source {
        |  type = "jdbc"
        |  url = "jdbc:h2:mem:dryrun_incr"
        |  dialect = "generic"
        |  driver = "org.h2.Driver"
        |  auth { user = "sa", password = { provider = "inline", value = "pw" } }
        |  mode = "INCREMENTAL"
        |  table = "d_claims"
        |  incremental {
        |    watermark_type = "NUMERIC"
        |    watermark_columns = ["claim_id"]
        |    initial_value = "100"
        |    watermark_store { type = "memory" }
        |  }
        |}
        |raw { database = "raw_db", table = "claims", path = "/tmp/raw" }
        |""".stripMargin)
    val report = DryRunValidator.validate(feed)
    assert(report.ok, report.errors.mkString("; "))
    val sql = report.sqlPreview.get
    assert(sql.contains("\"claim_id\" > 100"), sql)
    assert(sql.contains("first window"), sql)
  }

  test("cross-section compatibility rules (CFG) surface in the dry run") {
    // A JDBC incremental block on a FILE source is silently ignored at
    // runtime — the generator must reject it (CFG_008).
    val feed = com.typesafe.config.ConfigFactory.parseString(
      """
        |entity = "member"
        |source {
        |  type = "file"
        |  path = "/data/in/*.csv"
        |  incremental { watermark_type = "TIMESTAMP" }
        |}
        |raw { database = "raw_db", table = "member", path = "/tmp/raw" }
        |""".stripMargin)
    val report = DryRunValidator.validate(feed)
    assert(!report.ok)
    assert(report.errors.exists(_.contains("CFG_008")), report.errors.mkString("; "))
  }
}
