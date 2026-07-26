package com.hcsc.generic.ingest.jdbc.query

import com.hcsc.generic.ingest.config.ConfigUtils
import com.hcsc.generic.ingest.jdbc.auth.SecretProviders
import com.hcsc.generic.ingest.jdbc.dialect.JdbcDialect
import com.typesafe.config.Config
import scala.collection.JavaConverters._

/**
  * Contract-based query model. Configuration is validated into typed
  * structures at startup and rendered through the dialect — string
  * concatenation of unvalidated config never reaches SQL. Expressions
  * (projection/filter `expression` fields, custom SQL) are the one
  * explicitly-trusted escape hatch: administrator configuration, surfaced
  * in the query hash for auditing.
  */

/** Typed named parameter for SQL_TEMPLATE mode (:name placeholders).
  * Values come inline or through the secret-provider abstraction
  * (env / sysprop / file / inline), so pipeline- and secret-backed
  * parameters share one mechanism. */
final case class QueryParameter(name: String, paramType: String, value: String)

object QueryParameter {
  def parseAll(source: Config): Seq[QueryParameter] = {
    if (!source.hasPath("parameters")) return Seq.empty
    source.getConfigList("parameters").asScala.map { p =>
      val name = p.getString("name")
      require(name.matches("[A-Za-z_][A-Za-z0-9_]*"),
        s"JDBC_003 parameter name '$name' must be a plain identifier")
      val value = ConfigUtils.optString(p, "value")
        .orElse(SecretProviders.resolveAt(p, "from"))
        .getOrElse(throw new IllegalArgumentException(
          s"JDBC_003 parameter '$name' needs a value or a from = {provider=...} reference"))
      QueryParameter(name, ConfigUtils.optString(p, "type").getOrElse("STRING").toUpperCase, value)
    }.toList
  }
}

/** Structured projection: quoted source column or trusted expression, both
  * aliased to a validated target name. */
final case class QueryProjection(
  source: Option[String],
  expression: Option[String],
  target: String
) {
  def render(dialect: JdbcDialect): String = (source, expression) match {
    case (Some(s), None) => s"${dialect.quoteQualified(s)} AS ${dialect.quoteIdentifier(target)}"
    case (None, Some(e)) => s"$e AS ${dialect.quoteIdentifier(target)}"
    case _ => throw new IllegalArgumentException(
      s"JDBC_003 projection '$target' must define exactly one of source or expression")
  }
}

object QueryProjection {
  /** Accepts the structured object list; plain-string entries stay
    * supported as bare validated identifiers (legacy). */
  def parseAll(source: Config): Seq[QueryProjection] = {
    if (!source.hasPath("columns")) return Seq.empty
    source.getList("columns").asScala.map { v =>
      v.valueType() match {
        case com.typesafe.config.ConfigValueType.STRING =>
          val name = v.unwrapped().toString
          QueryProjection(Some(name), None, name.split("\\.").last)
        case com.typesafe.config.ConfigValueType.OBJECT =>
          val c = v.asInstanceOf[com.typesafe.config.ConfigObject].toConfig
          val target = c.getString("target")
          require(target.matches("[A-Za-z_][A-Za-z0-9_]*"),
            s"JDBC_003 projection target '$target' must be a plain identifier")
          val projection = QueryProjection(
            ConfigUtils.optString(c, "source"),
            ConfigUtils.optString(c, "expression"),
            target)
          require(projection.source.isDefined ^ projection.expression.isDefined,
            s"JDBC_003 projection '$target' must define exactly one of source or expression")
          projection
        case other =>
          throw new IllegalArgumentException(s"JDBC_003 columns entries must be strings or objects, found $other")
      }
    }.toList
  }
}

/** Structured filter: quoted column, whitelisted operator, typed literal.
  * `expression` filters are the trusted escape hatch (legacy `where`
  * strings parse into one). */
final case class QueryFilter(
  column: Option[String],
  operator: Option[String],
  values: Seq[String],
  valueType: String,
  expression: Option[String]
) {
  def render(dialect: JdbcDialect): String = expression match {
    case Some(e) => s"($e)"
    case None =>
      val col = dialect.quoteQualified(column.get)
      operator.get match {
        case "IN" =>
          values.map(v => dialect.renderLiteral(valueType, v)).mkString(s"$col IN (", ", ", ")")
        case op =>
          s"$col $op ${dialect.renderLiteral(valueType, values.head)}"
      }
  }
}

object QueryFilter {
  private val Operators = Set("=", "!=", "<>", "<", "<=", ">", ">=", "LIKE", "IN")

  def parseAll(source: Config): Seq[QueryFilter] = {
    val structured =
      if (!source.hasPath("filters")) Seq.empty
      else source.getConfigList("filters").asScala.map { f =>
        ConfigUtils.optString(f, "expression") match {
          case Some(e) => QueryFilter(None, None, Seq.empty, "STRING", Some(e))
          case None =>
            val op = ConfigUtils.optString(f, "operator").getOrElse("=").toUpperCase
            require(Operators.contains(op), s"JDBC_003 filter operator '$op' not in ${Operators.mkString(",")}")
            val values =
              if (f.hasPath("values")) ConfigUtils.stringList(f, "values")
              else Seq(f.getString("value"))
            require(values.nonEmpty, "JDBC_003 filter needs value or values")
            QueryFilter(Some(f.getString("column")), Some(op), values,
              ConfigUtils.optString(f, "type").getOrElse("STRING").toUpperCase, None)
        }
      }.toList

    // Legacy free-form where clause: trusted expression filter
    val legacyWhere = ConfigUtils.optString(source, "where")
      .map(w => QueryFilter(None, None, Seq.empty, "STRING", Some(w)))

    structured ++ legacyWhere.toSeq
  }
}

/** Named-parameter template rendering for SQL_TEMPLATE mode. */
object QueryTemplate {
  private val Placeholder = ":([A-Za-z_][A-Za-z0-9_]*)".r

  def render(template: String, parameters: Seq[QueryParameter], dialect: JdbcDialect): String = {
    val byName = parameters.map(p => p.name -> p).toMap
    val used = scala.collection.mutable.Set.empty[String]
    val rendered = Placeholder.replaceAllIn(template, m => {
      val name = m.group(1)
      val p = byName.getOrElse(name, throw new IllegalArgumentException(
        s"JDBC_003 template references undefined parameter ':$name'"))
      used += name
      java.util.regex.Matcher.quoteReplacement(dialect.renderLiteral(p.paramType, p.value))
    })
    val unused = byName.keySet -- used
    require(unused.isEmpty, s"JDBC_003 parameters defined but not used in template: ${unused.mkString(",")}")
    rendered
  }
}

/** Custom / templated SQL guardrails: one SELECT statement, no statement
  * separators, no DDL/DML. */
object SqlStatementValidator {
  def validate(sql: String): String = {
    val trimmed = sql.trim
    require(trimmed.nonEmpty, "JDBC_003 sql must not be empty")
    require(!trimmed.contains(";"),
      "JDBC_003 sql must be a single statement without ';' (multi-statements, trailing semicolons and DDL/DML are rejected)")
    val head = trimmed.split("\\s+", 2).head.toUpperCase
    require(head == "SELECT" || head == "WITH",
      s"JDBC_003 sql must start with SELECT or WITH, found '$head' (DDL/DML/procedures are not allowed)")
    trimmed
  }
}
