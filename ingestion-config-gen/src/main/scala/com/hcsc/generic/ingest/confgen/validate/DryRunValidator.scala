package com.hcsc.generic.ingest.confgen.validate

import com.hcsc.generic.ingest.jdbc.{JdbcMode, JdbcSourceConfig}
import com.hcsc.generic.ingest.jdbc.health.JdbcHealthCheck
import com.hcsc.generic.ingest.jdbc.read.QueryBuilder
import com.hcsc.generic.ingest.jdbc.watermark.{WatermarkValue, Watermarks}
import com.hcsc.generic.ingest.schema.SchemaContract
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
  * Dry-run validation of an assembled feed configuration through the
  * framework's OWN parsers — the generator never re-implements validation
  * rules, so a feed that passes here is a feed the pipeline will accept.
  *
  * Secret references are replaced with inline placeholders before parsing:
  * structural validation must succeed on a workstation where the production
  * environment variables / vaults are unreachable. Whether the real secrets
  * resolve is reported separately (and probed by the optional connectivity
  * test).
  */
object DryRunValidator {

  case class Report(
    errors: Seq[String],
    warnings: Seq[String],
    sqlPreview: Option[String]
  ) {
    def ok: Boolean = errors.isEmpty
  }

  private val SecretPlaceholder = "__DRY_RUN_PLACEHOLDER__"
  private val SecretFields = Seq("password", "client_secret", "token", "user")

  def validate(feed: Config): Report = {
    val common = commonErrors(feed)
    val sourceType = Try(feed.getString("source.type")).getOrElse("")

    sourceType match {
      case "jdbc" => jdbc(feed, common)
      case "file" => file(feed, common)
      case "kafka" => kafka(feed, common)
      case other => Report(common :+ s"Unknown source.type '$other'", Seq.empty, None)
    }
  }

  private def commonErrors(feed: Config): Seq[String] = {
    def missing(path: String) =
      if (feed.hasPath(path)) None else Some(s"$path is required")
    Seq(missing("entity"), missing("source.type"), missing("raw.database"),
      missing("raw.table"), missing("raw.path")).flatten
  }

  // ---------------------------------------------------------------------------
  // JDBC: full parse + SQL preview
  // ---------------------------------------------------------------------------

  private def jdbc(feed: Config, common: Seq[String]): Report = {
    val source = feed.getConfig("source")
    val placeholdered = withSecretPlaceholders(source)

    Try(JdbcSourceConfig.parse(placeholdered)) match {
      case Failure(e) =>
        Report(common :+ e.getMessage, Seq.empty, None)
      case Success(cfg) =>
        val warnings = secretResolutionWarnings(source)
        Report(common, warnings, Some(sqlPreview(cfg)))
    }
  }

  /** The extraction SQL exactly as QueryBuilder will emit it, with the first
    * incremental window rendered from the configured initial value.
    * Parameters sourced from a secret provider are masked — a preview is
    * display output and must never fetch or render the actual secret. */
  private def sqlPreview(cfg: JdbcSourceConfig): String =
    Try {
      val predicate = cfg.watermark.map { wm =>
        Watermarks.predicate(wm, cfg.dialect,
          WatermarkValue(wm.initialValue.split("\\|").toSeq))
      }
      val parameters = cfg.parameters.map { p =>
        if (p.source == com.hcsc.generic.ingest.jdbc.query.ParameterSource.SecretProvider)
          com.hcsc.generic.ingest.jdbc.query.QueryParameter(p.name, p.paramType, "********")
        else p.resolveStatic()
      }
      QueryBuilder.dbtable(cfg, predicate, parameters)
    } match {
      case Success(dbtable) =>
        val body = stripAlias(dbtable)
        if (cfg.mode == JdbcMode.Incremental)
          s"$body\n-- first window shown from initial_value; later runs bound by the stored watermark"
        else body
      case Failure(e) => s"-- preview unavailable: ${e.getMessage}"
    }

  /** QueryBuilder returns Spark's `(query) alias` dbtable form; unwrap the
    * subquery for display. Plain table names pass through. */
  private def stripAlias(dbtable: String): String = {
    val trimmed = dbtable.trim
    if (!trimmed.startsWith("(")) trimmed
    else {
      val close = trimmed.lastIndexOf(')')
      if (close > 0) trimmed.substring(1, close).trim else trimmed
    }
  }

  /** Replaces non-inline secret references so parsing cannot depend on this
    * machine's environment. `user` may itself be a secret reference. */
  private[validate] def withSecretPlaceholders(source: Config): Config = {
    if (!source.hasPath("auth")) return source
    SecretFields.foldLeft(source) { (acc, field) =>
      val path = s"auth.$field"
      if (acc.hasPath(path) && Try(acc.getConfig(path)).isSuccess &&
          !Try(acc.getConfig(path).getString("provider")).toOption.contains("inline")) {
        acc.withValue(path, ConfigValueFactory.fromMap(
          Map[String, Object]("provider" -> "inline", "value" -> SecretPlaceholder).asJava))
      } else acc
    }
  }

  /** Best-effort real resolution: failures are warnings, not errors — the
    * production environment usually differs from the workstation. */
  private def secretResolutionWarnings(source: Config): Seq[String] =
    SecretFields.flatMap { field =>
      val path = s"auth.$field"
      if (source.hasPath(path) && Try(source.getConfig(path)).isSuccess) {
        val ref = source.getConfig(path)
        val provider = Try(ref.getString("provider")).getOrElse("inline")
        if (provider == "inline") None
        else Try(com.hcsc.generic.ingest.jdbc.auth.SecretProviders.resolveRef(ref)) match {
          case Success(_) => None
          case Failure(e) => Some(
            s"auth.$field ($provider) did not resolve in this environment — " +
              s"expected on a workstation, verify at deploy time: ${e.getMessage}")
        }
      } else None
    }

  /** Optional connectivity probe (reuses the pipeline's health check, which
    * needs the real secrets and the JDBC driver on the classpath). */
  def connectivityTest(feed: Config): Either[String, String] =
    Try(JdbcSourceConfig.parse(feed.getConfig("source"))) match {
      case Failure(e) => Left(s"Cannot resolve source for connectivity test: ${e.getMessage}")
      case Success(cfg) => JdbcHealthCheck.check(cfg)
    }

  // ---------------------------------------------------------------------------
  // File: schema contract parse + folder/path coherence
  // ---------------------------------------------------------------------------

  private def file(feed: Config, common: Seq[String]): Report = {
    val source = Try(feed.getConfig("source")).getOrElse(ConfigFactory.empty())
    val errors = scala.collection.mutable.ArrayBuffer(common: _*)
    val warnings = scala.collection.mutable.ArrayBuffer[String]()

    if (!source.hasPath("path") && !source.hasPath("folders"))
      errors += "source.path or source.folders is required for file feeds"
    if (source.hasPath("folders"))
      Seq("landing", "inprogress", "processed", "quarantine").foreach { f =>
        if (!source.hasPath(s"folders.$f")) errors += s"source.folders.$f is required"
      }

    // The contract lives under the feed root (schema { ... }); FileSource
    // reads it from the source config the pipeline passes through, which
    // includes the feed-level schema block.
    val contractCarrier =
      if (feed.hasPath("schema")) source.withValue("schema", feed.getValue("schema")) else source
    try {
      Try(SchemaContract.parse(contractCarrier)) match {
        case Failure(e) => errors += s"schema contract invalid: ${e.getMessage}"
        case Success(None) =>
          warnings += "no schema contract defined: headers will not be validated against a contract"
        case Success(Some(contract)) =>
          if (contract.columns.isEmpty) errors += "schema.columns must not be empty"
      }
    } catch {
      // Contract column types resolve through Spark's DataType; on a plain
      // JVM (no spark jars) we can only skip this check, not fail the feed.
      case _: LinkageError =>
        warnings += "schema contract validation skipped: Spark classes not on the classpath " +
          "(re-run via spark-submit or with spark jars to validate the contract)"
    }

    Report(errors.toSeq, warnings.toSeq, None)
  }

  // ---------------------------------------------------------------------------
  // Kafka
  // ---------------------------------------------------------------------------

  private def kafka(feed: Config, common: Seq[String]): Report = {
    val source = Try(feed.getConfig("source")).getOrElse(ConfigFactory.empty())
    val errors = scala.collection.mutable.ArrayBuffer(common: _*)

    if (!source.hasPath("bootstrap.servers")) errors += "source.bootstrap.servers is required"
    if (!source.hasPath("topic")) errors += "source.topic is required"
    val format = Try(source.getString("value.format")).getOrElse("string")
    if (format == "json" && !source.hasPath("value.schema"))
      errors += "source.value.schema is required when value.format = json"
    if (format == "json" && source.hasPath("value.schema") &&
        !source.getString("value.schema").trim.startsWith("{"))
      errors += "source.value.schema must be a Spark StructType JSON document"

    Report(errors.toSeq, Seq.empty, None)
  }
}
