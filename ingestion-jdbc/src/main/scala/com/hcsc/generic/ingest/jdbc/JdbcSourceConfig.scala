package com.hcsc.generic.ingest.jdbc

import com.hcsc.generic.ingest.config.ConfigUtils
import com.hcsc.generic.ingest.jdbc.auth.SecretProviders
import com.hcsc.generic.ingest.jdbc.dialect.{DialectRegistry, GenericDialect, JdbcDialect}
import com.typesafe.config.Config
import scala.collection.JavaConverters._

object JdbcMode {
  val FullTable = "FULL_TABLE"
  val SelectQuery = "SELECT_QUERY"
  val CustomSql = "CUSTOM_SQL"
  val SqlTemplate = "SQL_TEMPLATE"
  val Incremental = "INCREMENTAL"
  val all = Seq(FullTable, SelectQuery, CustomSql, SqlTemplate, Incremental)
}

object WatermarkType {
  val Timestamp = "TIMESTAMP"
  val Numeric = "NUMERIC"
  val Date = "DATE"
  val DatetimeOffset = "DATETIMEOFFSET"
  val RowVersion = "ROWVERSION"
  val StringType = "STRING" // approval-gated: lexicographic ordering only
  val Composite = "COMPOSITE"
  val scalar = Seq(Timestamp, Numeric, Date, DatetimeOffset, RowVersion, StringType)
  val all = scalar :+ Composite
}

object AuthType {
  val SqlPassword = "SQL_PASSWORD"
  val AzureManagedIdentity = "AZURE_MANAGED_IDENTITY"
  val AzureServicePrincipal = "AZURE_SERVICE_PRINCIPAL"
  val EntraIdPassword = "ENTRA_ID_PASSWORD"
  val AccessToken = "ACCESS_TOKEN"
  val all = Seq(SqlPassword, AzureManagedIdentity, AzureServicePrincipal, EntraIdPassword, AccessToken)
}

object PartitionStrategy {
  val StaticRange = "STATIC_RANGE"
  val MinMaxQuery = "MIN_MAX_QUERY"
  val Predicates = "PREDICATES"
  val all = Seq(StaticRange, MinMaxQuery, Predicates)
}

object WatermarkUpperBound {
  /** Upper bound = MAX(watermark_column) observed before the read. */
  val MaxValue = "MAX_VALUE"
  /** Upper bound = the source database's clock (SYSUTCDATETIME() on SQL
    * Server), captured before the read. Idle sources still advance. */
  val SourceClock = "SOURCE_CLOCK"
  val all = Seq(MaxValue, SourceClock)
}

final case class WatermarkConfig(
  watermarkType: String,
  columns: Seq[String],
  columnTypes: Seq[String], // per column: TIMESTAMP | NUMERIC (composite)
  initialValue: String,     // composite: values joined with '|'
  overlap: Option[BigDecimal], // seconds for TIMESTAMP, amount for NUMERIC
  storeType: String,        // hive | memory
  storeDatabase: Option[String],
  storeTable: String,
  upperBound: String = WatermarkUpperBound.MaxValue,
  allowNullWatermark: Boolean = false
)

final case class JdbcPartitioning(
  numPartitions: Option[Int],
  partitionColumn: Option[String],
  lowerBound: Option[Long],
  upperBound: Option[Long],
  strategy: String = PartitionStrategy.StaticRange,
  predicates: Seq[String] = Seq.empty,
  skewMetrics: Boolean = false
)

final case class RetryConfig(maxAttempts: Int, backoffMs: Long)

final case class JdbcSourceConfig(
  url: String,
  dialect: JdbcDialect,
  driver: String,
  mode: String,
  table: Option[String],
  columns: Seq[String],
  where: Option[String],
  sql: Option[String],
  user: Option[String],
  password: Option[String],
  connectionProperties: Map[String, String],
  fetchSize: Int,
  partitioning: JdbcPartitioning,
  retry: RetryConfig,
  watermark: Option[WatermarkConfig],
  healthCheckEnabled: Boolean,
  logSql: Boolean = false,
  authType: String = AuthType.SqlPassword,
  projections: Seq[com.hcsc.generic.ingest.jdbc.query.QueryProjection] = Seq.empty,
  structuredColumns: Boolean = false,
  filters: Seq[com.hcsc.generic.ingest.jdbc.query.QueryFilter] = Seq.empty,
  parameters: Seq[com.hcsc.generic.ingest.jdbc.query.QueryParameterDef] = Seq.empty,
  pipelineParameters: Map[String, String] = Map.empty,
  executorProbePartitions: Option[Int] = None
)

object JdbcSourceConfig {

  def parse(source: Config): JdbcSourceConfig = {
    val (url, dialect, driver) = parseConnection(source)
    val (mode, table, sql) = parseMode(source)
    val (structuredColumns, columns, projections, filters, parameters, where) = parseQueryComponents(source)
    validateModeRequirements(mode, table, sql, where, source)
    val partitioning = parsePartitioning(source)
    val retry = parseRetry(source)
    // INCREMENTAL requires the watermark config; FULL_TABLE may carry one to
    // capture an extraction cutoff and SEED the incremental watermark on
    // success (the supported FULL -> INCREMENTAL handoff).
    val watermark =
      if (mode == JdbcMode.Incremental) Some(parseWatermark(source))
      else if (mode == JdbcMode.FullTable && source.hasPath("incremental")) Some(parseWatermark(source))
      else None
    val (authType, user, password, authProps) = resolveAuth(source, dialect)

    JdbcSourceConfig(
      url = url,
      dialect = dialect,
      driver = driver,
      mode = mode,
      table = table,
      columns = columns,
      where = where,
      sql = sql,
      user = user,
      password = password,
      connectionProperties = dialect.defaultConnectionProperties ++ authProps ++
        userConnectionProperties(source),
      fetchSize = ConfigUtils.optInt(source, "fetchsize").getOrElse(1000),
      partitioning = partitioning,
      retry = retry,
      watermark = watermark,
      healthCheckEnabled = ConfigUtils.optConfig(source, "health_check")
        .flatMap(h => ConfigUtils.optBoolean(h, "enabled")).getOrElse(true),
      logSql = ConfigUtils.optConfig(source, "diagnostics")
        .flatMap(d => ConfigUtils.optBoolean(d, "log_sql")).getOrElse(false),
      authType = authType,
      projections = projections,
      structuredColumns = structuredColumns,
      filters = filters,
      parameters = parameters,
      pipelineParameters = ConfigUtils.optConfig(source, "pipeline_parameters")
        .map(c => ConfigUtils.stringMap(c.atKey("p"), "p")).getOrElse(Map.empty),
      executorProbePartitions = ConfigUtils.optConfig(source, "health_check")
        .filter(h => ConfigUtils.optBoolean(h, "executor_probe").getOrElse(false))
        .map(h => ConfigUtils.optInt(h, "probe_partitions").getOrElse(2))
    )
  }

  private def parseConnection(source: Config): (String, JdbcDialect, String) = {
    val url = ConfigUtils.optString(source, "url").getOrElse(
      fail("source.url is required for jdbc sources"))

    val dialect = ConfigUtils.optString(source, "dialect") match {
      case Some(name) => DialectRegistry.resolve(name)
      case None       => inferDialect(url)
    }
    if (!url.startsWith(dialect.urlPrefix))
      fail(s"source.url '$url' does not match dialect '${dialect.name}' prefix '${dialect.urlPrefix}'")

    val driver = ConfigUtils.optString(source, "driver").getOrElse(dialect.defaultDriver)
    if (driver.isEmpty)
      fail(s"source.driver is required for dialect '${dialect.name}'")

    (url, dialect, driver)
  }

  private def parseMode(source: Config): (String, Option[String], Option[String]) = {
    val mode = ConfigUtils.optString(source, "mode").getOrElse(JdbcMode.FullTable).toUpperCase
    if (!JdbcMode.all.contains(mode))
      fail(s"source.mode '$mode' must be one of ${JdbcMode.all.mkString(", ")}")

    val table = ConfigUtils.optString(source, "table")
    val sql = ConfigUtils.optString(source, "sql").orElse(ConfigUtils.optString(source, "query"))
    (mode, table, sql)
  }

  private def parseQueryComponents(source: Config): (Boolean, Seq[String], Seq[com.hcsc.generic.ingest.jdbc.query.QueryProjection], Seq[com.hcsc.generic.ingest.jdbc.query.QueryFilter], Seq[com.hcsc.generic.ingest.jdbc.query.QueryParameterDef], Option[String]) = {
    val structuredColumns = source.hasPath("columns") &&
      source.getList("columns").asScala.exists(_.valueType() == com.typesafe.config.ConfigValueType.OBJECT)
    val columns = if (structuredColumns) Seq.empty else ConfigUtils.stringList(source, "columns")
    val projections = com.hcsc.generic.ingest.jdbc.query.QueryProjection.parseAll(source)
    val filters = com.hcsc.generic.ingest.jdbc.query.QueryFilter.parseAll(source)
    val parameters = com.hcsc.generic.ingest.jdbc.query.QueryParameter.parseAll(source)
    val where = ConfigUtils.optString(source, "where")
    (structuredColumns, columns, projections, filters, parameters, where)
  }

  private def validateModeRequirements(mode: String, table: Option[String], sql: Option[String], where: Option[String], source: Config): Unit = {
    mode match {
      case JdbcMode.FullTable =>
        if (table.isEmpty) fail(s"source.table is required for mode $mode")
        if (source.hasPath("columns") || where.isDefined || source.hasPath("filters"))
          fail("FULL_TABLE mode does not apply columns/where/filters; use SELECT_QUERY or INCREMENTAL")
      case JdbcMode.SelectQuery =>
        if (table.isEmpty) fail(s"source.table is required for mode $mode")
      case JdbcMode.CustomSql =>
        if (sql.isEmpty) fail("source.sql is required for mode CUSTOM_SQL")
      case JdbcMode.SqlTemplate =>
        if (sql.isEmpty) fail("source.sql (the template) is required for mode SQL_TEMPLATE")
      case JdbcMode.Incremental =>
        if (table.isEmpty && sql.isEmpty) fail("source.table or source.sql is required for mode INCREMENTAL")
    }
  }

  private def parsePartitioning(source: Config): JdbcPartitioning = {
    val strategy = ConfigUtils.optString(source, "partition_strategy").map(_.toUpperCase)
      .getOrElse(if (source.hasPath("partition_predicates")) PartitionStrategy.Predicates
                 else PartitionStrategy.StaticRange)
    if (!PartitionStrategy.all.contains(strategy))
      fail(s"partition_strategy '$strategy' must be one of ${PartitionStrategy.all.mkString(", ")}")

    val partitioning = JdbcPartitioning(
      numPartitions = ConfigUtils.optInt(source, "numPartitions"),
      partitionColumn = ConfigUtils.optString(source, "partitionColumn"),
      lowerBound = ConfigUtils.optLong(source, "lowerBound"),
      upperBound = ConfigUtils.optLong(source, "upperBound"),
      strategy = strategy,
      predicates = ConfigUtils.stringList(source, "partition_predicates"),
      skewMetrics = ConfigUtils.optBoolean(source, "skew_metrics").getOrElse(false)
    )

    validatePartitioning(partitioning, source)
    partitioning
  }

  private def validatePartitioning(partitioning: JdbcPartitioning, source: Config): Unit = {
    val maxPartitions = ConfigUtils.optInt(source, "max_partitions").getOrElse(64)
    partitioning.numPartitions.foreach { n =>
      if (n <= 0) fail(s"numPartitions must be greater than zero, found $n")
      if (n > maxPartitions)
        fail(s"numPartitions $n exceeds the operational maximum $maxPartitions (raise max_partitions deliberately)")
    }

    partitioning.strategy match {
      case PartitionStrategy.StaticRange =>
        val partitionFields = Seq(
          partitioning.numPartitions.isDefined,
          partitioning.partitionColumn.isDefined,
          partitioning.lowerBound.isDefined,
          partitioning.upperBound.isDefined)
        if (partitionFields.exists(identity) && !partitionFields.forall(identity))
          fail("numPartitions, partitionColumn, lowerBound and upperBound must all be configured together (or none)")
        for (lo <- partitioning.lowerBound; hi <- partitioning.upperBound)
          if (lo >= hi) fail(s"lowerBound $lo must be less than upperBound $hi")
        if (partitioning.predicates.nonEmpty)
          fail("partition_predicates requires partition_strategy = PREDICATES")
      case PartitionStrategy.MinMaxQuery =>
        if (partitioning.partitionColumn.isEmpty || partitioning.numPartitions.isEmpty)
          fail("MIN_MAX_QUERY partitioning requires partitionColumn and numPartitions")
        if (partitioning.lowerBound.isDefined || partitioning.upperBound.isDefined)
          fail("MIN_MAX_QUERY discovers bounds; lowerBound/upperBound must not be configured")
      case PartitionStrategy.Predicates =>
        if (partitioning.predicates.isEmpty)
          fail("PREDICATES partitioning requires a non-empty partition_predicates list")
        if (partitioning.partitionColumn.isDefined || partitioning.lowerBound.isDefined ||
            partitioning.upperBound.isDefined || partitioning.numPartitions.isDefined)
          fail("PREDICATES partitioning is exclusive with range partitioning options")
        if (partitioning.predicates.size > maxPartitions)
          fail(s"partition_predicates count ${partitioning.predicates.size} exceeds max_partitions $maxPartitions")
    }
  }

  private def parseRetry(source: Config): RetryConfig = {
    val retryConf = ConfigUtils.optConfig(source, "retry")
    RetryConfig(
      maxAttempts = retryConf.flatMap(c => ConfigUtils.optInt(c, "max_attempts")).getOrElse(3),
      backoffMs = retryConf.flatMap(c => ConfigUtils.optLong(c, "backoff_ms")).getOrElse(2000L)
    )
  }

  /**
    * connection_properties overlay the dialect defaults (rightmost wins), so
    * a feed could silently disable the TLS protections the sqlserver dialect
    * ships with (encrypt=true, trustServerCertificate=false). Downgrades are
    * rejected unless the feed explicitly opts in with
    * `allow_insecure_tls = true` — and even then they are logged loudly.
    */
  private def userConnectionProperties(source: Config): Map[String, String] = {
    val props = ConfigUtils.optConfig(source, "connection_properties")
      .map(c => ConfigUtils.stringMap(c.atKey("p"), "p")).getOrElse(Map.empty)

    def truthy(v: String) = {
      val t = v.trim.toLowerCase
      t == "true" || t == "yes" || t == "1"
    }
    val insecure = props.collect {
      case (k, v) if k.equalsIgnoreCase("trustServerCertificate") && truthy(v) => s"$k=$v"
      case (k, v) if k.equalsIgnoreCase("encrypt") && !truthy(v) => s"$k=$v"
    }.toSeq.sorted

    if (insecure.nonEmpty) {
      if (!ConfigUtils.optBoolean(source, "allow_insecure_tls").getOrElse(false))
        fail(s"connection_properties weaken TLS (${insecure.mkString(", ")}); encryption and " +
          "certificate validation protect credentials in transit. Set " +
          "source.allow_insecure_tls = true only for isolated non-production databases")
      org.apache.log4j.Logger.getLogger(getClass.getName).warn(
        s"[JdbcSourceConfig] INSECURE TLS override approved (allow_insecure_tls=true): " +
          s"${insecure.mkString(", ")} — never use this against production data")
    }
    props
  }

  /**
    * Authentication resolves through the pluggable provider registry
    * (auth.JdbcAuthenticationProviders): SQL_PASSWORD, MANAGED_IDENTITY,
    * ENTRA_SERVICE_PRINCIPAL, ENTRA_PASSWORD, ENTRA_DEFAULT,
    * ENTRA_INTEGRATED, ACCESS_TOKEN (plus legacy aliases). Azure/Entra modes
    * map onto Microsoft JDBC driver properties; the driver performs the
    * token flows on driver AND executors.
    */
  private def resolveAuth(source: Config, dialect: JdbcDialect): (String, Option[String], Option[String], Map[String, String]) = {
    val auth = ConfigUtils.optConfig(source, "auth")
    val requested = auth.flatMap(a => ConfigUtils.optString(a, "type")).getOrElse(AuthType.SqlPassword)
    val provider = com.hcsc.generic.ingest.jdbc.auth.JdbcAuthenticationProviders.resolve(requested)

    if (provider.requiresSqlServerDialect && dialect.name != "sqlserver")
      fail(s"auth.type ${provider.authenticationType} is only supported for the sqlserver dialect")

    val resolved = provider.resolve(auth, source)
    (provider.authenticationType, resolved.user, resolved.password, resolved.properties)
  }

  private def parseWatermark(source: Config): WatermarkConfig = {
    val inc = ConfigUtils.optConfig(source, "incremental").getOrElse(
      fail("source.incremental block is required for mode INCREMENTAL"))

    val watermarkType = ConfigUtils.optString(inc, "watermark_type").getOrElse(
      fail("incremental.watermark_type is required")).toUpperCase
    if (!WatermarkType.all.contains(watermarkType))
      fail(s"incremental.watermark_type '$watermarkType' must be one of ${WatermarkType.all.mkString(", ")}")

    val columns = ConfigUtils.stringList(inc, "watermark_columns")
    if (columns.isEmpty) fail("incremental.watermark_columns must not be empty")
    if (watermarkType != WatermarkType.Composite && columns.size != 1)
      fail(s"watermark_type $watermarkType requires exactly one watermark column")
    if (watermarkType == WatermarkType.Composite && columns.size < 2)
      fail("watermark_type COMPOSITE requires at least two watermark columns")

    val columnTypes = watermarkType match {
      case WatermarkType.Composite =>
        val types = ConfigUtils.stringList(inc, "column_types").map(_.toUpperCase)
        if (types.size != columns.size)
          fail("incremental.column_types must match watermark_columns length for COMPOSITE watermarks")
        types.foreach(t => if (!WatermarkType.scalar.contains(t))
          fail(s"incremental.column_types entry '$t' must be one of ${WatermarkType.scalar.mkString(", ")}"))
        types
      case t => Seq(t)
    }

    // STRING watermarks (lexicographic ordering only) are easy to misuse —
    // approve explicitly per feed.
    if (columnTypes.contains(WatermarkType.StringType) &&
        !ConfigUtils.optBoolean(inc, "allow_string_watermark").getOrElse(false))
      fail("STRING watermarks compare lexicographically and are only safe for zero-padded / " +
        "fixed-format keys; set incremental.allow_string_watermark = true to approve")

    // A timestamp-only watermark with no overlap can miss rows sharing the
    // boundary timestamp (equal-timestamp inserts after commit). Require a
    // composite tie-breaker or an overlap window; the policy decides whether
    // an unprotected config warns or fails.
    val overlapValue = ConfigUtils.optString(inc, "overlap").map(BigDecimal(_))
    if (watermarkType == WatermarkType.Timestamp && overlapValue.isEmpty) {
      val policy = ConfigUtils.optString(inc, "on_unprotected_watermark").getOrElse("WARN").toUpperCase
      policy match {
        case "FAIL" =>
          fail("TIMESTAMP watermark without overlap can miss equal-timestamp rows; " +
            "configure incremental.overlap, use a COMPOSITE watermark with a tie-breaker key, " +
            "or set on_unprotected_watermark = WARN to accept the risk")
        case "WARN" =>
          org.apache.log4j.Logger.getLogger(getClass.getName).warn(
            "[JdbcSourceConfig] TIMESTAMP watermark configured without overlap or tie-breaker: " +
              "rows inserted later with the boundary timestamp can be missed. " +
              "Prefer a COMPOSITE watermark or an overlap window.")
        case other =>
          fail(s"incremental.on_unprotected_watermark '$other' must be WARN or FAIL")
      }
    }

    val upperBound = ConfigUtils.optString(inc, "upper_bound").map(_.toUpperCase)
      .getOrElse(WatermarkUpperBound.MaxValue)
    if (!WatermarkUpperBound.all.contains(upperBound))
      fail(s"incremental.upper_bound '$upperBound' must be one of ${WatermarkUpperBound.all.mkString(", ")}")
    if (upperBound == WatermarkUpperBound.SourceClock &&
        !Seq(WatermarkType.Timestamp, WatermarkType.DatetimeOffset).contains(watermarkType))
      fail("incremental.upper_bound = SOURCE_CLOCK requires a single TIMESTAMP or DATETIMEOFFSET " +
        "watermark column (the source clock is a timestamp)")

    val store = ConfigUtils.optConfig(inc, "watermark_store")
    WatermarkConfig(
      watermarkType = watermarkType,
      columns = columns,
      columnTypes = columnTypes,
      initialValue = ConfigUtils.optString(inc, "initial_value").getOrElse(
        fail("incremental.initial_value is required")),
      overlap = overlapValue,
      storeType = store.flatMap(s => ConfigUtils.optString(s, "type")).getOrElse("hive").toLowerCase,
      storeDatabase = store.flatMap(s => ConfigUtils.optString(s, "database")),
      storeTable = store.flatMap(s => ConfigUtils.optString(s, "table")).getOrElse("ingest_watermarks"),
      upperBound = upperBound,
      allowNullWatermark = ConfigUtils.optBoolean(inc, "allow_null_watermark").getOrElse(false)
    )
  }

  private def fail(message: String): Nothing =
    throw new IllegalArgumentException(s"JDBC_003 $message")

  private def inferDialect(url: String): JdbcDialect =
    DialectRegistry.availableNames
      .map(DialectRegistry.resolve)
      .filterNot(_ == GenericDialect)
      .find(d => url.startsWith(d.urlPrefix))
      .getOrElse(fail(s"Cannot infer dialect from url '$url'; set source.dialect explicitly " +
        s"(available: ${DialectRegistry.availableNames.mkString(", ")})"))
}
