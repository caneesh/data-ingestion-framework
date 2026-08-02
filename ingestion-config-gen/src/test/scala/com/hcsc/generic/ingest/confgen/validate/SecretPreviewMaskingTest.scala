package com.hcsc.generic.ingest.confgen.validate

import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite

/** The SQL preview must never fetch-and-render a SECRET_PROVIDER parameter:
  * secret literals are masked while ordinary parameters render normally. */
class SecretPreviewMaskingTest extends AnyFunSuite {

  test("secret-sourced parameters are masked in the extraction SQL preview") {
    System.setProperty("confgen.preview.secret", "super-secret-token")
    val feed = ConfigFactory.parseString(
      """entity = "claims"
        |source {
        |  type = "jdbc"
        |  url = "jdbc:h2:mem:preview_mask"
        |  dialect = "generic"
        |  driver = "org.h2.Driver"
        |  auth { user = "sa", password = { provider = "inline", value = "pw" } }
        |  mode = "SQL_TEMPLATE"
        |  sql = "SELECT c FROM t WHERE token = :tok AND state = :st"
        |  parameters = [
        |    { name = "tok", from = { provider = "sysprop", key = "confgen.preview.secret" } },
        |    { name = "st", value = "IL" }
        |  ]
        |}
        |raw { database = "raw_db", table = "claims", path = "/tmp/raw" }
        |""".stripMargin)

    val report = DryRunValidator.validate(feed)
    assert(report.ok, report.errors.mkString("; "))

    val sql = report.sqlPreview.get
    assert(sql.contains("'********'"), sql)
    assert(!sql.contains("super-secret-token"), "secret value must never reach the preview")
    assert(sql.contains("'IL'"), "non-secret parameters still render normally")
  }

  test("every secret parameter is masked independently, including substrings") {
    // Two secrets in one query, one being a substring of a rendered literal
    // elsewhere would be the leak modes an exact-whole-value masker misses.
    System.setProperty("confgen.preview.secret_a", "tokenAAA")
    System.setProperty("confgen.preview.secret_b", "tokenBBB")
    try {
      val feed = ConfigFactory.parseString(
        """entity = "claims2"
          |source {
          |  type = "jdbc"
          |  url = "jdbc:h2:mem:preview_mask2"
          |  dialect = "generic"
          |  driver = "org.h2.Driver"
          |  auth { user = "sa", password = { provider = "inline", value = "pw" } }
          |  mode = "SQL_TEMPLATE"
          |  sql = "SELECT c FROM t WHERE a = :a AND b = :b AND s = :plain"
          |  parameters = [
          |    { name = "a", from = { provider = "sysprop", key = "confgen.preview.secret_a" } },
          |    { name = "b", from = { provider = "sysprop", key = "confgen.preview.secret_b" } },
          |    { name = "plain", value = "tokenAAA-adjacent" }
          |  ]
          |}
          |raw { database = "raw_db", table = "claims2", path = "/tmp/raw" }
          |""".stripMargin)
      val report = DryRunValidator.validate(feed)
      assert(report.ok, report.errors.mkString("; "))
      val sql = report.sqlPreview.get
      assert(!sql.contains("tokenAAA'"), s"secret A leaked: $sql")
      assert(!sql.contains("tokenBBB"), s"secret B leaked: $sql")
      assert(sql.contains("'tokenAAA-adjacent'"),
        "a non-secret literal must render even when it shares a prefix with a secret")
    } finally {
      System.clearProperty("confgen.preview.secret_a")
      System.clearProperty("confgen.preview.secret_b")
    }
  }
}
