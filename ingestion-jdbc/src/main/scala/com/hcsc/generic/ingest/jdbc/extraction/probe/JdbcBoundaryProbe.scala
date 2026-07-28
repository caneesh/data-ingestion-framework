package com.hcsc.generic.ingest.jdbc.extraction.probe

import com.hcsc.generic.ingest.jdbc.dialect.JdbcDialect
import com.hcsc.generic.ingest.jdbc.extraction._

import java.sql.{DriverManager, ResultSet}
import java.util.Properties

/**
  * Driver-side boundary lookups over plain JDBC. Queries are scoped by the
  * same filters as the extraction so captured bounds always describe rows
  * the read can see. One short-lived connection per probe call — these run
  * once per planning, on the driver.
  */
final class JdbcBoundaryProbe(
  url: String,
  connectionProperties: Map[String, String],
  dialect: JdbcDialect,
  queryTimeoutSeconds: Int = 30
) extends SourceBoundaryProbe {

  override def max(
    columns: Seq[BoundaryColumn],
    relation: Relation,
    filters: Seq[String]
  ): Option[BoundaryValue] = {
    require(columns.nonEmpty, "EXT_002 boundary probe requires at least one column")
    val projection = columns.map(c => dialect.quoteQualified(c.name)).mkString(", ")
    val orderBy = columns.map(c => s"${dialect.quoteQualified(c.name)} DESC").mkString(", ")
    val sql = dialect.selectTopOne(projection, from(relation, filters, columns), orderBy)
    query(sql) { rs =>
      if (!rs.next()) None
      else {
        val values = columns.indices.map(i => canonical(rs, i + 1))
        if (values.exists(_.isEmpty)) None else Some(BoundaryValue(values.map(_.get)))
      }
    }
  }

  override def minMax(column: String, relation: Relation, filters: Seq[String]): Option[(Long, Long)] = {
    val q = dialect.quoteQualified(column)
    val sql = s"SELECT MIN($q), MAX($q) FROM ${from(relation, filters, Seq.empty)}"
    query(sql) { rs =>
      if (!rs.next()) None
      else {
        val lo = rs.getLong(1); val loNull = rs.wasNull()
        val hi = rs.getLong(2); val hiNull = rs.wasNull()
        if (loNull || hiNull) None else Some((lo, hi))
      }
    }
  }

  /** Boundary probes must ignore NULL boundary rows (they cannot advance a
    * window), so the driving column is additionally filtered NOT NULL. */
  private def from(relation: Relation, filters: Seq[String], columns: Seq[BoundaryColumn]): String = {
    val table = relation match { case Relation.Table(name) => name }
    val notNull = columns.headOption.map(c => s"${dialect.quoteQualified(c.name)} IS NOT NULL").toSeq
    val predicates = filters.map(f => s"($f)") ++ notNull
    if (predicates.isEmpty) table
    else s"(SELECT * FROM $table WHERE ${predicates.mkString(" AND ")}) probe_src"
  }

  private def query[A](sql: String)(read: ResultSet => A): A = {
    val props = new Properties()
    connectionProperties.foreach { case (k, v) => props.setProperty(k, v) }
    val connection = DriverManager.getConnection(url, props)
    try {
      val statement = connection.createStatement()
      try {
        statement.setQueryTimeout(queryTimeoutSeconds)
        val rs = statement.executeQuery(sql)
        try read(rs) finally rs.close()
      } finally statement.close()
    } finally connection.close()
  }

  /** Canonical string forms so probed values round-trip through the typed
    * boundary parsers. */
  private def canonical(rs: ResultSet, index: Int): Option[String] =
    Option(rs.getObject(index)).map {
      case ts: java.sql.Timestamp => ts.toString
      case other                  => String.valueOf(other)
    }
}
