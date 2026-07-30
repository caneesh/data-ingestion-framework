package com.hcsc.generic.ingest.jdbc.read

import com.hcsc.generic.ingest.jdbc.{JdbcMode, JdbcSourceConfig}
import com.hcsc.generic.ingest.jdbc.query.{QueryParameter, QueryTemplate, SqlStatementValidator}

/**
  * Builds the Spark `dbtable` expression for each read mode from the
  * contract-based query model. Filters expressed here (structured filters,
  * legacy WHERE clauses, watermark predicates) execute inside the database —
  * guaranteed predicate pushdown.
  *
  * Legacy configuration (plain string columns, free-form where) renders
  * byte-for-byte as before; structured projections/filters render through
  * the dialect (segment-aware quoting, typed literals). Custom and
  * templated SQL pass SqlStatementValidator (single SELECT, no ';').
  */
object QueryBuilder {

  private val TablePattern = "[A-Za-z0-9_.$#]+"

  def dbtable(
    cfg: JdbcSourceConfig,
    watermarkPredicate: Option[String],
    resolvedParameters: Seq[QueryParameter] = Seq.empty
  ): String = {
    cfg.table.foreach(validateTableName)

    // Callers resolve dynamic parameter sources (RUN_ID, watermarks, ...)
    // and pass them in; direct/standalone use resolves static sources only.
    def parameters: Seq[QueryParameter] =
      if (resolvedParameters.nonEmpty) resolvedParameters
      else cfg.parameters.map(_.resolveStatic())

    def renderTemplated(sql: String): String =
      if (cfg.parameters.isEmpty) sql
      else QueryTemplate.render(sql, parameters, cfg.dialect)

    cfg.mode match {
      case JdbcMode.FullTable =>
        cfg.table.get

      case JdbcMode.SelectQuery =>
        s"(SELECT ${projection(cfg)} FROM ${cfg.table.get}${whereClause(cfg, None)}) src"

      case JdbcMode.CustomSql =>
        s"(${SqlStatementValidator.validate(cfg.sql.get)}) src"

      case JdbcMode.SqlTemplate =>
        val rendered = QueryTemplate.render(cfg.sql.get, parameters, cfg.dialect)
        s"(${SqlStatementValidator.validate(rendered)}) src"

      case JdbcMode.Incremental =>
        val base = (cfg.table, cfg.sql) match {
          case (Some(t), _)    => s"SELECT ${projection(cfg)} FROM $t"
          case (None, Some(s)) =>
            // Incremental custom bases support :name parameters too (incl.
            // LAST_WATERMARK / UPPER_WATERMARK from the read context)
            s"SELECT * FROM (${SqlStatementValidator.validate(renderTemplated(s))}) base"
          case _ =>
            throw new IllegalArgumentException("JDBC_003 INCREMENTAL requires table or sql")
        }
        s"($base${whereClause(cfg, watermarkPredicate, parenthesized = true)}) src"
    }
  }

  private def projection(cfg: JdbcSourceConfig): String =
    if (cfg.structuredColumns) cfg.projections.map(_.render(cfg.dialect)).mkString(", ")
    else if (cfg.columns.nonEmpty) cfg.columns.mkString(", ")
    else "*"

  /**
    * WHERE clause from every configured filter (structured filters plus the
    * legacy `where`, which parses into an expression filter) — EXACTLY as the
    * extraction query applies them. Driver-side companion queries (upper
    * watermark capture, MIN/MAX bound discovery) must use this so they scope
    * to the same row set the read extracts; scoping only to `cfg.where`
    * captures bounds across rows that are never extracted.
    */
  def filterWhereClause(cfg: JdbcSourceConfig): String = whereClause(cfg, None)

  /**
    * Validated (and, for sql bases, parameter-rendered) base relation for
    * driver-side companion queries — upper watermark capture, NULL-watermark
    * probes, MIN/MAX bound discovery. Companion queries MUST pass through
    * the same guardrails as the extraction query itself: table-name
    * validation, single-statement SQL validation, and :name template
    * rendering — a raw base with unrendered :parameters is not executable
    * SQL, and an unvalidated one bypasses the module's injection guards.
    */
  def validatedBase(cfg: JdbcSourceConfig, resolvedParameters: Seq[QueryParameter]): Option[String] =
    (cfg.table, cfg.sql) match {
      case (Some(t), _) =>
        validateTableName(t)
        Some(t)
      case (None, Some(s)) =>
        val params =
          if (resolvedParameters.nonEmpty) resolvedParameters
          else cfg.parameters.map(_.resolveStatic())
        val rendered =
          if (cfg.parameters.isEmpty) s
          else QueryTemplate.render(s, params, cfg.dialect)
        Some(s"(${SqlStatementValidator.validate(rendered)}) base")
      case _ => None
    }

  /** SELECT_QUERY keeps the historical plain rendering for a lone legacy
    * where; anything structured (or multiple predicates) renders each
    * predicate parenthesized and AND-combined. */
  private def whereClause(
    cfg: JdbcSourceConfig,
    watermarkPredicate: Option[String],
    parenthesized: Boolean = false
  ): String = {
    val structural = cfg.filters.filter(_.expression.isEmpty).map(_.render(cfg.dialect))
    val legacy = cfg.filters.filter(f => f.expression.isDefined).map(_.expression.get)

    val all = structural ++ legacy ++ watermarkPredicate.toSeq
    if (all.isEmpty) ""
    else if (!parenthesized && structural.isEmpty && all.size == 1) s" WHERE ${all.head}"
    else all.map(p => s"($p)").mkString(" WHERE ", " AND ", "")
  }

  private def validateTableName(table: String): Unit =
    require(
      table.matches(TablePattern),
      s"JDBC_003 table name '$table' contains characters outside the safe identifier pattern $TablePattern"
    )
}
