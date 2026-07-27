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

/** A RESOLVED typed named parameter, ready for template rendering. */
final case class QueryParameter(name: String, paramType: String, value: String)

/** Runtime values for dynamic parameter sources, supplied by the source at
  * read time (the pipeline injects run_id; watermark values exist only for
  * incremental reads). */
final case class ParameterContext(
  runId: Option[String] = None,
  processingDate: Option[String] = None,
  lastWatermark: Option[String] = None,
  upperWatermark: Option[String] = None,
  pipelineParameters: Map[String, String] = Map.empty
)

object ParameterSource {
  val Config = "CONFIG"
  val Environment = "ENVIRONMENT"
  val SystemProperty = "SYSTEM_PROPERTY"
  val SecretProvider = "SECRET_PROVIDER"
  val PipelineParameter = "PIPELINE_PARAMETER"
  val ProcessingDate = "PROCESSING_DATE"
  val RunId = "RUN_ID"
  val LastWatermark = "LAST_WATERMARK"
  val UpperWatermark = "UPPER_WATERMARK"
  val all = Seq(Config, Environment, SystemProperty, SecretProvider, PipelineParameter,
    ProcessingDate, RunId, LastWatermark, UpperWatermark)
  val dynamic = Set(PipelineParameter, ProcessingDate, RunId, LastWatermark, UpperWatermark)
}

/**
  * Parameter DEFINITION for SQL_TEMPLATE / templated INCREMENTAL bases
  * (:name placeholders). Sources:
  *   CONFIG (value = ...), ENVIRONMENT / SYSTEM_PROPERTY (key = ...),
  *   SECRET_PROVIDER (from = { provider = ... }), PIPELINE_PARAMETER
  *   (key into source.pipeline_parameters), PROCESSING_DATE, RUN_ID,
  *   LAST_WATERMARK, UPPER_WATERMARK.
  * Static sources resolve at parse; dynamic sources resolve at read time
  * against a ParameterContext.
  */
final case class QueryParameterDef(
  name: String,
  paramType: String,
  source: String,
  staticValue: Option[String],
  key: Option[String],
  secretRef: Option[Config]
) {
  def resolve(context: ParameterContext): QueryParameter = {
    val value = source match {
      case ParameterSource.Config => staticValue.get
      case ParameterSource.Environment =>
        sys.env.getOrElse(key.get, fail(s"environment variable '${key.get}' is not set"))
      case ParameterSource.SystemProperty =>
        Option(System.getProperty(key.get)).getOrElse(fail(s"system property '${key.get}' is not set"))
      case ParameterSource.SecretProvider =>
        SecretProviders.resolveRef(secretRef.get)
      case ParameterSource.PipelineParameter =>
        context.pipelineParameters.getOrElse(key.get,
          fail(s"pipeline parameter '${key.get}' is not defined in source.pipeline_parameters"))
      case ParameterSource.ProcessingDate =>
        context.processingDate.getOrElse(fail("processing date not available"))
      case ParameterSource.RunId =>
        context.runId.getOrElse(fail("run_id not available; RUN_ID parameters require pipeline execution"))
      case ParameterSource.LastWatermark =>
        context.lastWatermark.getOrElse(fail("LAST_WATERMARK parameters require an INCREMENTAL read"))
      case ParameterSource.UpperWatermark =>
        context.upperWatermark.getOrElse(fail("UPPER_WATERMARK parameters require an INCREMENTAL read"))
      case other => fail(s"unknown parameter source '$other'")
    }
    QueryParameter(name, paramType, value)
  }

  /** Context-free resolution for direct QueryBuilder use: static sources
    * only; dynamic sources fail with a clear message. */
  def resolveStatic(): QueryParameter = resolve(ParameterContext())

  private def fail(message: String): Nothing =
    throw new IllegalArgumentException(s"JDBC_003 parameter '$name': $message")
}

object QueryParameter {
  def parseAll(source: Config): Seq[QueryParameterDef] = {
    if (!source.hasPath("parameters")) return Seq.empty
    source.getConfigList("parameters").asScala.map { p =>
      val name = p.getString("name")
      require(name.matches("[A-Za-z_][A-Za-z0-9_]*"),
        s"JDBC_003 parameter name '$name' must be a plain identifier")
      val paramType = ConfigUtils.optString(p, "type").getOrElse("STRING").toUpperCase
      val explicitSource = ConfigUtils.optString(p, "source").map(_.toUpperCase)
      explicitSource.foreach(s => require(ParameterSource.all.contains(s),
        s"JDBC_003 parameter '$name' source '$s' must be one of ${ParameterSource.all.mkString(", ")}"))

      val staticValue = ConfigUtils.optString(p, "value")
      val secretRef = ConfigUtils.optConfig(p, "from")
      val key = ConfigUtils.optString(p, "key")

      val source0 = explicitSource.getOrElse {
        if (staticValue.isDefined) ParameterSource.Config
        else if (secretRef.isDefined) ParameterSource.SecretProvider
        else throw new IllegalArgumentException(
          s"JDBC_003 parameter '$name' needs a value, a from = {provider=...} reference, or an explicit source")
      }

      source0 match {
        case ParameterSource.Config =>
          require(staticValue.isDefined, s"JDBC_003 parameter '$name' with source CONFIG requires value")
        case ParameterSource.SecretProvider =>
          require(secretRef.isDefined, s"JDBC_003 parameter '$name' with source SECRET_PROVIDER requires from = {...}")
        case ParameterSource.Environment | ParameterSource.SystemProperty | ParameterSource.PipelineParameter =>
          require(key.isDefined, s"JDBC_003 parameter '$name' with source $source0 requires key")
        case _ => () // dynamic context sources need nothing at parse time
      }

      QueryParameterDef(name, paramType, source0, staticValue, key, secretRef)
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
  * separators, no DDL/DML, and no top-level ORDER BY that would break
  * subquery wrapping. */
object SqlStatementValidator {
  def validate(sql: String): String = {
    val trimmed = sql.trim
    require(trimmed.nonEmpty, "JDBC_003 sql must not be empty")
    require(!trimmed.contains(";"),
      "JDBC_003 sql must be a single statement without ';' (multi-statements, trailing semicolons and DDL/DML are rejected)")
    val head = trimmed.split("\\s+", 2).head.toUpperCase
    require(head == "SELECT" || head == "WITH",
      s"JDBC_003 sql must start with SELECT or WITH, found '$head' (DDL/DML/procedures are not allowed)")

    // Top-level ORDER BY is invalid inside the (query) src subquery wrapping
    // on SQL Server unless TOP/OFFSET/FETCH is present. ORDER BY inside
    // parenthesized subqueries is fine.
    if (hasTopLevelOrderBy(trimmed)) {
      val upper = trimmed.toUpperCase
      require(upper.contains(" TOP ") || upper.contains(" OFFSET ") || upper.contains(" FETCH "),
        "JDBC_003 top-level ORDER BY is not valid inside the framework's subquery wrapping " +
          "unless combined with TOP/OFFSET/FETCH; remove the ORDER BY (ingestion does not depend on source order)")
    }
    trimmed
  }

  /** Detects ORDER BY at paren depth 0, outside string literals. */
  private[query] def hasTopLevelOrderBy(sql: String): Boolean = {
    val upper = sql.toUpperCase
    var depth = 0
    var inString = false
    var i = 0
    while (i < upper.length - 7) {
      upper.charAt(i) match {
        case '\'' => inString = !inString
        case '(' if !inString => depth += 1
        case ')' if !inString => depth -= 1
        case 'O' if !inString && depth == 0 &&
          upper.regionMatches(i, "ORDER", 0, 5) &&
          upper.substring(i + 5).dropWhile(_.isWhitespace).startsWith("BY") &&
          (i == 0 || !upper.charAt(i - 1).isLetterOrDigit) =>
          return true
        case _ => ()
      }
      i += 1
    }
    false
  }
}
