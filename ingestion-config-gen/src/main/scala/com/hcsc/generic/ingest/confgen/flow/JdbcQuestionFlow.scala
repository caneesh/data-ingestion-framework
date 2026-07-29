package com.hcsc.generic.ingest.confgen.flow

import com.hcsc.generic.ingest.confgen.model.{Answers, Question, QuestionGroups => G, QuestionKind => K, Validators => V}
import com.hcsc.generic.ingest.jdbc.query.SqlStatementValidator

import scala.util.{Failure, Success, Try}

/**
  * JDBC feed questionnaire. Choices and validation rules come from the
  * framework's own vocabularies (auth provider registry, JdbcMode,
  * WatermarkType, SqlStatementValidator) so the wizard can never offer an
  * option the parser would reject.
  */
object JdbcQuestionFlow extends SourceQuestionFlow {

  override val sourceType: String = "jdbc"

  private val Modes = Seq("FULL_TABLE", "SELECT_QUERY", "CUSTOM_SQL", "SQL_TEMPLATE", "INCREMENTAL")
  private val Dialects = Seq("", "sqlserver", "postgresql", "oracle", "db2", "mysql", "generic")
  private val WatermarkTypes = Seq("TIMESTAMP", "NUMERIC", "DATE", "DATETIMEOFFSET", "ROWVERSION", "STRING", "COMPOSITE")
  private val SecretProviders = Seq("env", "sysprop", "file", "inline", "cyberark", "azure_keyvault", "conjur")

  private def authTypes: Seq[String] =
    com.hcsc.generic.ingest.jdbc.auth.JdbcAuthenticationProviders.availableNames.sorted

  private def needsUser(a: Answers) =
    a.is("source.auth.type", "SQL_PASSWORD") || a.is("source.auth.type", "ENTRA_PASSWORD")

  /** Which auth modes need a secret, and which config field receives it. */
  private[confgen] def secretField(a: Answers): Option[String] =
    a.scalar("source.auth.type").map(_.toUpperCase) match {
      case Some("SQL_PASSWORD") | Some("ENTRA_PASSWORD") => Some("password")
      case Some("ENTRA_SERVICE_PRINCIPAL") => Some("client_secret")
      case Some("ACCESS_TOKEN") => Some("token")
      case _ => None
    }

  private def needsSecret(a: Answers) = secretField(a).isDefined
  private def secretProviderIs(a: Answers, p: String) =
    needsSecret(a) && a.is("_auth.secret.provider", p)

  private def modeIs(a: Answers, m: String) = a.is("source.mode", m)
  private def sqlBased(a: Answers) = modeIs(a, "CUSTOM_SQL") || modeIs(a, "SQL_TEMPLATE")
  private def incremental(a: Answers) = modeIs(a, "INCREMENTAL")
  private def scalarWatermark(a: Answers) =
    incremental(a) && !a.is("source.incremental.watermark_type", "COMPOSITE")

  private val validSql: String => Either[String, String] = sql =>
    Try(SqlStatementValidator.validate(sql)) match {
      case Success(_) => Right(sql)
      case Failure(e) => Left(e.getMessage)
    }

  override def questions: Seq[Question] =
    CommonQuestions.general ++ source ++ authentication ++ extraction ++
      CommonQuestions.schemaContract(
        gateDefault = "false",
        gateHelp = "Recommended for wide/long-lived feeds: validates source columns by " +
          "name/alias and applies drift policies (missing/extra/type changes) before any " +
          "RAW write. The mapping can be introspected from the database or supplied " +
          "via @/path/columns.json.",
        introspectable = true) ++
      watermark ++ retry ++ CommonQuestions.audit ++ performance ++
      CommonQuestions.destination

  private def source = Seq(
    Question("source.url", G.Source, "JDBC connection URL",
      help = "e.g. jdbc:sqlserver://myserver.database.windows.net:1433;databaseName=ClaimsDB",
      validate = V.jdbcUrl),
    Question("source.dialect", G.Source, "SQL dialect (blank = infer from URL)",
      kind = K.Choice(Dialects), default = Some(""), required = false,
      validate = v => if (v.isEmpty) Right(v) else V.oneOf(Dialects.filter(_.nonEmpty))(v)),
    Question("source.driver", G.Source, "JDBC driver class (blank = dialect default)",
      required = false, default = Some(""))
  )

  private def authentication = Seq(
    Question("source.auth.type", G.Authentication, "Authentication mode",
      help = "Entra/Azure modes require the sqlserver dialect and the Microsoft JDBC driver.",
      kind = K.Choice(authTypes), default = Some("SQL_PASSWORD"),
      validate = v => Try(com.hcsc.generic.ingest.jdbc.auth.JdbcAuthenticationProviders.resolve(v)) match {
        case Success(p) => Right(p.authenticationType)
        case Failure(e) => Left(e.getMessage)
      }),
    Question("source.auth.user", G.Authentication, "Database user / login",
      validate = V.nonEmpty, appliesWhen = needsUser),
    Question("source.auth.client_id", G.Authentication, "Service principal client id",
      validate = V.nonEmpty, appliesWhen = _.is("source.auth.type", "ENTRA_SERVICE_PRINCIPAL")),

    Question("_auth.secret.provider", G.Authentication, "Secret provider for the credential",
      help = "Where the password/secret/token is stored. 'inline' is for local testing only.",
      kind = K.Choice(SecretProviders), default = Some("env"),
      validate = V.oneOf(SecretProviders), appliesWhen = needsSecret),
    Question("_auth.secret.key", G.Authentication, "Secret key / variable name",
      help = "Environment variable or system property holding the secret.",
      validate = V.nonEmpty,
      appliesWhen = a => secretProviderIs(a, "env") || secretProviderIs(a, "sysprop")),
    Question("_auth.secret.path", G.Authentication, "Secret file path",
      validate = V.nonEmpty, appliesWhen = a => secretProviderIs(a, "file")),
    Question("_auth.secret.value", G.Authentication, "Secret value (stored inline — testing only)",
      secret = true, validate = V.nonEmpty, appliesWhen = a => secretProviderIs(a, "inline")),
    Question("_auth.secret.url", G.Authentication, "CyberArk CCP base URL",
      validate = V.nonEmpty, appliesWhen = a => secretProviderIs(a, "cyberark")),
    Question("_auth.secret.app_id", G.Authentication, "CyberArk application id",
      validate = V.nonEmpty, appliesWhen = a => secretProviderIs(a, "cyberark")),
    Question("_auth.secret.safe", G.Authentication, "CyberArk safe",
      validate = V.nonEmpty, appliesWhen = a => secretProviderIs(a, "cyberark")),
    Question("_auth.secret.object", G.Authentication, "CyberArk object name",
      validate = V.nonEmpty, appliesWhen = a => secretProviderIs(a, "cyberark")),
    Question("_auth.secret.vault_url", G.Authentication, "Azure Key Vault URL",
      help = "e.g. https://myvault.vault.azure.net",
      validate = V.nonEmpty, appliesWhen = a => secretProviderIs(a, "azure_keyvault")),
    Question("_auth.secret.secret_name", G.Authentication, "Key Vault secret name",
      validate = V.nonEmpty, appliesWhen = a => secretProviderIs(a, "azure_keyvault")),
    Question("_auth.secret.conjur_url", G.Authentication, "Conjur base URL",
      help = "e.g. https://conjur.example.com",
      validate = v => if (v.startsWith("https://") || v.startsWith("http://")) Right(v)
        else Left(s"'$v' must be an http(s) URL"),
      appliesWhen = a => secretProviderIs(a, "conjur")),
    Question("_auth.secret.account", G.Authentication, "Conjur account",
      validate = V.nonEmpty, appliesWhen = a => secretProviderIs(a, "conjur")),
    Question("_auth.secret.host_id", G.Authentication, "Conjur host identity",
      help = "e.g. data/ingestion/spark ('host/' prefix optional)",
      validate = V.nonEmpty, appliesWhen = a => secretProviderIs(a, "conjur")),
    Question("_auth.secret.api_key_env", G.Authentication,
      "Environment variable holding the Conjur host API key",
      help = "The API key is referenced, never stored in the config.",
      validate = V.nonEmpty, appliesWhen = a => secretProviderIs(a, "conjur")),
    Question("_auth.secret.variable", G.Authentication, "Conjur variable id",
      help = "e.g. applications/ingestion/database/password",
      validate = V.nonEmpty, appliesWhen = a => secretProviderIs(a, "conjur"))
  )

  private def extraction = Seq(
    Question("source.mode", G.Extraction, "Extraction mode",
      help = "FULL_TABLE: whole table. SELECT_QUERY: table + columns/filters. " +
        "CUSTOM_SQL/SQL_TEMPLATE: hand-written SELECT. INCREMENTAL: watermark-bounded deltas.",
      kind = K.Choice(Modes), default = Some("FULL_TABLE"), validate = V.oneOf(Modes)),
    Question("source.table", G.Extraction, "Source table (schema-qualified)",
      help = "e.g. dbo.claims",
      validate = v => if (v.matches("[A-Za-z0-9_.$#]+")) Right(v)
        else Left(s"'$v' is not a valid table name"),
      appliesWhen = a => !sqlBased(a)),
    Question("source.sql", G.Extraction, "Extraction SQL (single SELECT, no semicolons)",
      help = "SQL_TEMPLATE supports ${parameter} placeholders resolved at run time.",
      validate = validSql, appliesWhen = sqlBased),
    Question("source.columns", G.Extraction, "Columns to extract (blank = all)",
      help = "@file, CSV or JSON accepted; JSON objects may set alias/cast per column.",
      kind = K.ListKind, required = false,
      appliesWhen = a => modeIs(a, "SELECT_QUERY") || incremental(a)),
    Question("source.where", G.Extraction, "WHERE filter pushed into the database (blank = none)",
      required = false, default = Some(""),
      appliesWhen = a => modeIs(a, "SELECT_QUERY") || incremental(a))
  )

  private def watermark = Seq(
    Question("source.incremental.watermark_type", G.Watermark, "Watermark type",
      help = "COMPOSITE combines several columns lexicographically (e.g. modified_ts + id tie-breaker).",
      kind = K.Choice(WatermarkTypes), default = Some("TIMESTAMP"),
      validate = V.oneOf(WatermarkTypes), appliesWhen = incremental),
    Question("_watermark.string_ack", G.Watermark,
      "STRING watermarks compare lexicographically; only zero-padded/fixed-format keys are safe. Proceed?",
      kind = K.BoolKind, default = Some("false"),
      validate = v =>
        if (v.equalsIgnoreCase("true") || v.equalsIgnoreCase("y") || v.equalsIgnoreCase("yes")) Right("true")
        else Left("STRING watermark not approved; choose another watermark type (restart the Watermark group)"),
      appliesWhen = a => incremental(a) && a.is("source.incremental.watermark_type", "STRING")),
    Question("source.incremental.watermark_columns", G.Watermark, "Watermark column(s)",
      help = "One column, or 2+ for COMPOSITE (most significant first). @file, CSV or JSON accepted.",
      kind = K.ListKind, appliesWhen = incremental,
      validate = V.nonEmpty),
    Question("source.incremental.column_types", G.Watermark, "Per-column types for COMPOSITE",
      help = "Same order as the columns, e.g. TIMESTAMP,NUMERIC",
      kind = K.ListKind,
      appliesWhen = a => incremental(a) && a.is("source.incremental.watermark_type", "COMPOSITE")),
    Question("source.incremental.initial_value", G.Watermark, "Initial watermark value",
      help = "First run extracts rows after this value. COMPOSITE: join components with '|'.",
      validate = V.nonEmpty, appliesWhen = incremental),
    Question("source.incremental.overlap", G.Watermark,
      "Overlap window re-read behind the watermark (blank = none)",
      help = "Guards against late/equal-timestamp rows. TIMESTAMP: seconds; NUMERIC: units.",
      required = false, default = Some(""),
      validate = v => if (v.isEmpty) Right(v) else V.nonNegativeNumber(v),
      appliesWhen = a => scalarWatermark(a) &&
        !a.is("source.incremental.watermark_type", "ROWVERSION") &&
        !a.is("source.incremental.watermark_type", "STRING")),
    Question("source.incremental.watermark_store.type", G.Watermark, "Watermark store",
      kind = K.Choice(Seq("hive", "memory")), default = Some("hive"),
      validate = V.oneOf(Seq("hive", "memory")), appliesWhen = incremental),
    Question("source.incremental.watermark_store.database", G.Watermark, "Watermark store database",
      default = Some("ingest_audit"), validate = V.identifier,
      appliesWhen = a => incremental(a) && a.is("source.incremental.watermark_store.type", "hive"))
  )

  private def retry = Seq(
    Question("source.retry.max_attempts", G.Retry, "Retry attempts for transient failures",
      help = "Applies only to failures classified transient (deadlocks, throttling, timeouts).",
      kind = K.IntKind, default = Some("3"), validate = V.positiveInt),
    Question("source.retry.backoff_ms", G.Retry, "Initial retry backoff (ms)",
      help = "Exponential backoff with jitter, starting here.",
      kind = K.IntKind, default = Some("2000"), validate = V.positiveInt)
  )

  private def performance = Seq(
    Question("source.fetchsize", G.Performance, "JDBC fetch size",
      kind = K.IntKind, default = Some("1000"), validate = V.positiveInt),
    Question("_perf.partitioning", G.Performance, "Parallel read partitioning",
      help = "NONE: single connection. STATIC_RANGE: fixed numeric bounds. " +
        "MIN_MAX_QUERY: bounds discovered per run. PREDICATES: explicit predicate list.",
      kind = K.Choice(Seq("NONE", "STATIC_RANGE", "MIN_MAX_QUERY", "PREDICATES")),
      default = Some("NONE"),
      validate = V.oneOf(Seq("NONE", "STATIC_RANGE", "MIN_MAX_QUERY", "PREDICATES"))),
    Question("source.partitionColumn", G.Performance, "Partition column (numeric)",
      validate = V.nonEmpty,
      appliesWhen = a => a.is("_perf.partitioning", "STATIC_RANGE") || a.is("_perf.partitioning", "MIN_MAX_QUERY")),
    Question("source.lowerBound", G.Performance, "Partition lower bound",
      kind = K.IntKind, validate = V.integer,
      appliesWhen = _.is("_perf.partitioning", "STATIC_RANGE")),
    Question("source.upperBound", G.Performance, "Partition upper bound",
      kind = K.IntKind,
      validate = V.integer,
      appliesWhen = _.is("_perf.partitioning", "STATIC_RANGE")),
    Question("source.numPartitions", G.Performance, "Number of partitions",
      kind = K.IntKind, default = Some("8"), validate = V.positiveInt,
      appliesWhen = a => a.is("_perf.partitioning", "STATIC_RANGE") || a.is("_perf.partitioning", "MIN_MAX_QUERY")),
    Question("source.partition_predicates", G.Performance, "Partition predicates",
      help = "One WHERE fragment per partition, e.g. state = 'IL'. @file, CSV or JSON accepted.",
      kind = K.ListKind, appliesWhen = _.is("_perf.partitioning", "PREDICATES"))
  )
}
