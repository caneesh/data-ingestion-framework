package com.hcsc.generic.ingest.jdbc.read

import com.hcsc.generic.ingest.jdbc.{JdbcMode, JdbcSourceConfig}

/**
  * Builds the Spark `dbtable` expression for each read mode. Filters
  * expressed here (WHERE clauses, watermark predicates) execute inside the
  * database — guaranteed predicate pushdown, independent of Spark's own
  * pushdown heuristics.
  *
  * Trust boundary: `where` and `sql` are operator-authored SQL from feed
  * configuration (same trust level as reject rules); table names are
  * validated against a strict identifier pattern.
  */
object QueryBuilder {

  private val TablePattern = "[A-Za-z0-9_.$#]+"

  def dbtable(cfg: JdbcSourceConfig, watermarkPredicate: Option[String]): String = {
    cfg.table.foreach(validateTableName)

    cfg.mode match {
      case JdbcMode.FullTable =>
        cfg.table.get

      case JdbcMode.SelectQuery =>
        val projection = if (cfg.columns.nonEmpty) cfg.columns.mkString(", ") else "*"
        val whereClause = cfg.where.map(w => s" WHERE $w").getOrElse("")
        s"(SELECT $projection FROM ${cfg.table.get}$whereClause) src"

      case JdbcMode.CustomSql =>
        s"(${cfg.sql.get}) src"

      case JdbcMode.Incremental =>
        val base = (cfg.table, cfg.sql) match {
          case (Some(t), _) =>
            val projection = if (cfg.columns.nonEmpty) cfg.columns.mkString(", ") else "*"
            s"SELECT $projection FROM $t"
          case (None, Some(s)) =>
            s"SELECT * FROM ($s) base"
          case _ =>
            throw new IllegalArgumentException("JDBC_003 INCREMENTAL requires table or sql")
        }
        val predicates = cfg.where.toSeq ++ watermarkPredicate.toSeq
        val whereClause =
          if (predicates.isEmpty) ""
          else predicates.map(p => s"($p)").mkString(" WHERE ", " AND ", "")
        s"($base$whereClause) src"
    }
  }

  private def validateTableName(table: String): Unit =
    require(
      table.matches(TablePattern),
      s"JDBC_003 table name '$table' contains characters outside the safe identifier pattern $TablePattern"
    )
}
