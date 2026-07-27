package com.hcsc.generic.ingest.jdbc.query

import com.hcsc.generic.ingest.jdbc.dialect.{GenericDialect, SqlServerDialect}
import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite

class QueryModelTest extends AnyFunSuite {

  // ---------------------------------------------------------------------------
  // Dialect rendering primitives
  // ---------------------------------------------------------------------------

  test("segment-aware quoting splits qualified identifiers") {
    assert(SqlServerDialect.quoteQualified("dbo.member.modified_ts") == "[dbo].[member].[modified_ts]")
    assert(GenericDialect.quoteQualified("member") == "\"member\"")
    intercept[IllegalArgumentException] { SqlServerDialect.quoteQualified("dbo.mem;ber") }
    intercept[IllegalArgumentException] { SqlServerDialect.quoteQualified("dbo..member") }
  }

  test("typed literal rendering validates through a typed round-trip") {
    assert(GenericDialect.renderLiteral("STRING", "O'Brien") == "'O''Brien'")
    assert(GenericDialect.renderLiteral("NUMBER", " 42.5 ") == "42.5")
    assert(GenericDialect.renderLiteral("TIMESTAMP", "2026-01-01 10:00:00") == "'2026-01-01 10:00:00.0'")
    assert(GenericDialect.renderLiteral("DATE", "2026-01-01") == "'2026-01-01'")
    assert(GenericDialect.renderLiteral("BOOLEAN", "true") == "TRUE")
    assert(SqlServerDialect.renderLiteral("BOOLEAN", "true") == "1")
    assert(SqlServerDialect.renderLiteral("BOOLEAN", "false") == "0")

    intercept[IllegalArgumentException] { GenericDialect.renderLiteral("NUMBER", "42; DROP TABLE x") }
    intercept[IllegalArgumentException] { GenericDialect.renderLiteral("TIMESTAMP", "not-a-time") }
    intercept[IllegalArgumentException] { GenericDialect.renderLiteral("GEOMETRY", "x") }
  }

  // ---------------------------------------------------------------------------
  // Projections
  // ---------------------------------------------------------------------------

  test("structured projections quote source and alias target") {
    val p = QueryProjection(Some("Member ID"), None, "member_id")
    assert(p.render(SqlServerDialect) == "[Member ID] AS [member_id]")
  }

  test("expression projections pass through trusted SQL with a quoted alias") {
    val p = QueryProjection(None, Some("CONVERT(varchar(10), [effective_dt], 23)"), "effective_date")
    assert(p.render(SqlServerDialect) == "CONVERT(varchar(10), [effective_dt], 23) AS [effective_date]")
  }

  test("projection parse accepts mixed strings and objects, rejects ambiguous ones") {
    val conf = ConfigFactory.parseString(
      """columns = [
        |  "plain_col",
        |  { source = "Member ID", target = "member_id" },
        |  { expression = "amount * 2", target = "doubled" }
        |]""".stripMargin)
    val projections = QueryProjection.parseAll(conf)
    assert(projections.size == 3)
    assert(projections.head == QueryProjection(Some("plain_col"), None, "plain_col"))

    intercept[IllegalArgumentException] {
      QueryProjection.parseAll(ConfigFactory.parseString(
        """columns = [{ source = "a", expression = "b", target = "t" }]"""))
    }
    intercept[IllegalArgumentException] {
      QueryProjection.parseAll(ConfigFactory.parseString(
        """columns = [{ source = "a", target = "bad name" }]"""))
    }
  }

  // ---------------------------------------------------------------------------
  // Filters
  // ---------------------------------------------------------------------------

  test("structured filters render quoted columns, whitelisted operators and typed literals") {
    val conf = ConfigFactory.parseString(
      """filters = [
        |  { column = "state", operator = "=", value = "IL" },
        |  { column = "amount", operator = ">=", value = "100", type = "NUMBER" },
        |  { column = "plan", operator = "IN", values = ["A", "B"] },
        |  { column = "name", operator = "LIKE", value = "Sm%" }
        |]""".stripMargin)
    val rendered = QueryFilter.parseAll(conf).map(_.render(SqlServerDialect))
    assert(rendered == Seq(
      "[state] = 'IL'",
      "[amount] >= 100",
      "[plan] IN ('A', 'B')",
      "[name] LIKE 'Sm%'"))
  }

  test("filter operators outside the whitelist are rejected") {
    intercept[IllegalArgumentException] {
      QueryFilter.parseAll(ConfigFactory.parseString(
        """filters = [{ column = "a", operator = "BETWEEN", value = "1" }]"""))
    }
  }

  test("legacy where strings become trusted expression filters") {
    val filters = QueryFilter.parseAll(ConfigFactory.parseString("""where = "a = 1""""))
    assert(filters.size == 1)
    assert(filters.head.expression.contains("a = 1"))
    assert(filters.head.render(GenericDialect) == "(a = 1)")
  }

  // ---------------------------------------------------------------------------
  // Templates and parameters
  // ---------------------------------------------------------------------------

  test("template parameters render typed literals into :name placeholders") {
    val params = Seq(
      QueryParameter("state", "STRING", "IL"),
      QueryParameter("min_amount", "NUMBER", "100"))
    val rendered = QueryTemplate.render(
      "SELECT * FROM claims WHERE state = :state AND amount > :min_amount",
      params, SqlServerDialect)
    assert(rendered == "SELECT * FROM claims WHERE state = 'IL' AND amount > 100")
  }

  test("undefined and unused parameters both fail at startup") {
    intercept[IllegalArgumentException] {
      QueryTemplate.render("SELECT * FROM t WHERE x = :missing", Seq.empty, GenericDialect)
    }
    intercept[IllegalArgumentException] {
      QueryTemplate.render("SELECT * FROM t",
        Seq(QueryParameter("unused", "STRING", "v")), GenericDialect)
    }
  }

  test("parameters resolve from secret providers (sysprop) and inline values") {
    System.setProperty("query.param.test", "resolved-secret")
    val params = QueryParameter.parseAll(ConfigFactory.parseString(
      """parameters = [
        |  { name = "inline_p", type = "STRING", value = "v1" },
        |  { name = "secret_p", type = "STRING", from = { provider = "sysprop", key = "query.param.test" } }
        |]""".stripMargin))
    assert(params.map(_.resolveStatic().value) == Seq("v1", "resolved-secret"))

    intercept[IllegalArgumentException] {
      QueryParameter.parseAll(ConfigFactory.parseString(
        """parameters = [{ name = "bad name", value = "x" }]"""))
    }
  }

  // ---------------------------------------------------------------------------
  // Statement guardrails
  // ---------------------------------------------------------------------------

  test("custom SQL guardrails: single SELECT/WITH only, no semicolons") {
    assert(SqlStatementValidator.validate("SELECT 1") == "SELECT 1")
    assert(SqlStatementValidator.validate("WITH c AS (SELECT 1) SELECT * FROM c")
      .startsWith("WITH"))

    intercept[IllegalArgumentException] { SqlStatementValidator.validate("SELECT 1;") }
    intercept[IllegalArgumentException] { SqlStatementValidator.validate("SELECT 1; DROP TABLE x") }
    intercept[IllegalArgumentException] { SqlStatementValidator.validate("DELETE FROM t") }
    intercept[IllegalArgumentException] { SqlStatementValidator.validate("EXEC sp_who") }
    intercept[IllegalArgumentException] { SqlStatementValidator.validate("  ") }
  }
}
