package com.hcsc.generic.ingest.jdbc.query

import com.hcsc.generic.ingest.jdbc.dialect.{GenericDialect, SqlServerDialect}
import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite

/** Dynamic parameter sources, new literal types and SQL governance. */
class DynamicParametersTest extends AnyFunSuite {

  private def defs(hocon: String): Seq[QueryParameterDef] =
    QueryParameter.parseAll(ConfigFactory.parseString(hocon))

  test("explicit sources parse and static ones resolve without context") {
    System.setProperty("dyn.param.test", "sp-value")
    val parsed = defs(
      """parameters = [
        |  { name = "cfg", value = "v" },
        |  { name = "sp", source = "SYSTEM_PROPERTY", key = "dyn.param.test" },
        |  { name = "env_home", source = "ENVIRONMENT", key = "HOME" }
        |]""".stripMargin)
    assert(parsed.map(_.source) == Seq(
      ParameterSource.Config, ParameterSource.SystemProperty, ParameterSource.Environment))
    assert(parsed(0).resolveStatic().value == "v")
    assert(parsed(1).resolveStatic().value == "sp-value")
    assert(parsed(2).resolveStatic().value.nonEmpty) // HOME is always set
  }

  test("dynamic sources resolve from the read-time context") {
    val parsed = defs(
      """parameters = [
        |  { name = "rid", source = "RUN_ID" },
        |  { name = "pd", source = "PROCESSING_DATE", type = "DATE" },
        |  { name = "low", source = "LAST_WATERMARK", type = "TIMESTAMP" },
        |  { name = "high", source = "UPPER_WATERMARK", type = "TIMESTAMP" },
        |  { name = "region", source = "PIPELINE_PARAMETER", key = "region" }
        |]""".stripMargin)
    val ctx = ParameterContext(
      runId = Some("run-77"),
      processingDate = Some("2026-07-26"),
      lastWatermark = Some("2026-01-01 00:00:00"),
      upperWatermark = Some("2026-02-01 00:00:00"),
      pipelineParameters = Map("region" -> "WEST"))
    assert(parsed.map(_.resolve(ctx).value) ==
      Seq("run-77", "2026-07-26", "2026-01-01 00:00:00", "2026-02-01 00:00:00", "WEST"))
  }

  test("dynamic sources fail clearly without their context") {
    val wm = defs("""parameters = [{ name = "low", source = "LAST_WATERMARK" }]""").head
    val ex = intercept[IllegalArgumentException] { wm.resolveStatic() }
    assert(ex.getMessage.contains("INCREMENTAL"))

    val rid = defs("""parameters = [{ name = "r", source = "RUN_ID" }]""").head
    assert(intercept[IllegalArgumentException] { rid.resolveStatic() }
      .getMessage.contains("pipeline"))

    val pp = defs("""parameters = [{ name = "p", source = "PIPELINE_PARAMETER", key = "absent" }]""").head
    assert(intercept[IllegalArgumentException] { pp.resolve(ParameterContext()) }
      .getMessage.contains("pipeline_parameters"))
  }

  test("parse validation: unknown source, missing key, missing value") {
    assert(intercept[IllegalArgumentException] {
      defs("""parameters = [{ name = "x", source = "MAGIC" }]""")
    }.getMessage.contains("JDBC_003"))
    assert(intercept[IllegalArgumentException] {
      defs("""parameters = [{ name = "x", source = "ENVIRONMENT" }]""")
    }.getMessage.contains("requires key"))
    assert(intercept[IllegalArgumentException] {
      defs("""parameters = [{ name = "x" }]""")
    }.getMessage.contains("needs a value"))
  }

  test("DATETIMEOFFSET and NULL literal types render correctly") {
    assert(GenericDialect.renderLiteral("DATETIMEOFFSET", "2026-03-15T10:00:00+05:00") ==
      "'2026-03-15 10:00:00.000 +05:00'")
    assert(SqlServerDialect.renderLiteral("DATETIMEOFFSET", "2026-03-15 10:00:00.000 +05:00") ==
      "'2026-03-15 10:00:00.000 +05:00'")
    assert(GenericDialect.renderLiteral("NULL", "anything") == "NULL")
    intercept[IllegalArgumentException] {
      GenericDialect.renderLiteral("DATETIMEOFFSET", "2026-03-15 10:00:00") // no offset
    }
  }

  test("top-level ORDER BY without TOP/OFFSET is rejected; subquery ORDER BY is fine") {
    assert(intercept[IllegalArgumentException] {
      SqlStatementValidator.validate("SELECT a FROM t ORDER BY a")
    }.getMessage.contains("ORDER BY"))

    // TOP / OFFSET make top-level ORDER BY legal on SQL Server
    SqlStatementValidator.validate("SELECT TOP 10 a FROM t ORDER BY a")
    SqlStatementValidator.validate("SELECT a FROM t ORDER BY a OFFSET 5 ROWS")

    // ORDER BY inside a subquery does not trip the guard
    SqlStatementValidator.validate(
      "SELECT * FROM (SELECT TOP 5 a FROM t ORDER BY a) x")

    // 'ORDER BY' inside a string literal does not trip the guard
    SqlStatementValidator.validate("SELECT 'use ORDER BY carefully' AS note FROM t")
  }
}
