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
}
