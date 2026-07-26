package com.hcsc.generic.ingest.jdbc

import com.hcsc.generic.ingest.config.ConfigUtils
import com.hcsc.generic.ingest.jdbc.auth.SecretProviders
import com.hcsc.generic.ingest.jdbc.dialect.{DialectRegistry, GenericDialect, JdbcDialect}
import com.typesafe.config.Config

object JdbcMode {
  val FullTable = "FULL_TABLE"
  val SelectQuery = "SELECT_QUERY"
  val CustomSql = "CUSTOM_SQL"
  val Incremental = "INCREMENTAL"
  val all = Seq(FullTable, SelectQuery, CustomSql, Incremental)
}

object WatermarkType {
  val Timestamp = "TIMESTAMP"
  val Numeric = "NUMERIC"
  val Composite = "COMPOSITE"
  val all = Seq(Timestamp, Numeric, Composite)
}

final case class WatermarkConfig(
  watermarkType: String,
  columns: Seq[String],
  columnTypes: Seq[String], // per column: TIMESTAMP | NUMERIC (composite)
  initialValue: String,     // composite: values joined with '|'
  overlap: Option[BigDecimal], // seconds for TIMESTAMP, amount for NUMERIC
  storeType: String,        // hive | memory
  storeDatabase: Option[String],
  storeTable: String
)

final case class JdbcPartitioning(
  numPartitions: Option[Int],
  partitionColumn: Option[String],
  lowerBound: Option[Long],
  upperBound: Option[Long]
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
  logSql: Boolean = false
)

object JdbcSourceConfig {

  def parse(source: Config): JdbcSourceConfig = {
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

    val mode = ConfigUtils.optString(source, "mode").getOrElse(JdbcMode.FullTable).toUpperCase
    if (!JdbcMode.all.contains(mode))
      fail(s"source.mode '$mode' must be one of ${JdbcMode.all.mkString(", ")}")

    val table = ConfigUtils.optString(source, "table")
    val sql = ConfigUtils.optString(source, "sql").orElse(ConfigUtils.optString(source, "query"))

    mode match {
      case JdbcMode.FullTable | JdbcMode.SelectQuery =>
        if (table.isEmpty) fail(s"source.table is required for mode $mode")
      case JdbcMode.CustomSql =>
        if (sql.isEmpty) fail("source.sql is required for mode CUSTOM_SQL")
      case JdbcMode.Incremental =>
        if (table.isEmpty && sql.isEmpty) fail("source.table or source.sql is required for mode INCREMENTAL")
    }

    val partitioning = JdbcPartitioning(
      numPartitions = ConfigUtils.optInt(source, "numPartitions"),
      partitionColumn = ConfigUtils.optString(source, "partitionColumn"),
      lowerBound = ConfigUtils.optLong(source, "lowerBound"),
      upperBound = ConfigUtils.optLong(source, "upperBound")
    )
    // Range partitioning is atomic: all four options or none of them.
    val partitionFields = Seq(
      partitioning.numPartitions.isDefined,
      partitioning.partitionColumn.isDefined,
      partitioning.lowerBound.isDefined,
      partitioning.upperBound.isDefined)
    if (partitionFields.exists(identity) && !partitionFields.forall(identity))
      fail("numPartitions, partitionColumn, lowerBound and upperBound must all be configured together (or none)")
    partitioning.numPartitions.foreach { n =>
      if (n <= 0) fail(s"numPartitions must be greater than zero, found $n")
      val maxPartitions = ConfigUtils.optInt(source, "max_partitions").getOrElse(64)
      if (n > maxPartitions)
        fail(s"numPartitions $n exceeds the operational maximum $maxPartitions (raise max_partitions deliberately)")
    }
    for (lo <- partitioning.lowerBound; hi <- partitioning.upperBound)
      if (lo >= hi) fail(s"lowerBound $lo must be less than upperBound $hi")

    val retryConf = ConfigUtils.optConfig(source, "retry")
    val retry = RetryConfig(
      maxAttempts = retryConf.flatMap(c => ConfigUtils.optInt(c, "max_attempts")).getOrElse(3),
      backoffMs = retryConf.flatMap(c => ConfigUtils.optLong(c, "backoff_ms")).getOrElse(2000L)
    )

    val watermark =
      if (mode == JdbcMode.Incremental) Some(parseWatermark(source))
      else None

    JdbcSourceConfig(
      url = url,
      dialect = dialect,
      driver = driver,
      mode = mode,
      table = table,
      columns = ConfigUtils.stringList(source, "columns"),
      where = ConfigUtils.optString(source, "where"),
      sql = sql,
      user = ConfigUtils.optConfig(source, "auth").flatMap(a => ConfigUtils.optString(a, "user"))
        .orElse(ConfigUtils.optString(source, "user")),
      password = ConfigUtils.optConfig(source, "auth").flatMap(a => SecretProviders.resolveAt(a, "password"))
        .orElse(SecretProviders.resolveAt(source, "password")),
      connectionProperties = dialect.defaultConnectionProperties ++
        ConfigUtils.optConfig(source, "connection_properties").map(c => ConfigUtils.stringMap(c.atKey("p"), "p")).getOrElse(Map.empty),
      fetchSize = ConfigUtils.optInt(source, "fetchsize").getOrElse(1000),
      partitioning = partitioning,
      retry = retry,
      watermark = watermark,
      healthCheckEnabled = ConfigUtils.optConfig(source, "health_check")
        .flatMap(h => ConfigUtils.optBoolean(h, "enabled")).getOrElse(true),
      logSql = ConfigUtils.optConfig(source, "diagnostics")
        .flatMap(d => ConfigUtils.optBoolean(d, "log_sql")).getOrElse(false)
    )
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
        types.foreach(t => if (!Seq(WatermarkType.Timestamp, WatermarkType.Numeric).contains(t))
          fail(s"incremental.column_types entry '$t' must be TIMESTAMP or NUMERIC"))
        types
      case t => Seq(t)
    }

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
      storeTable = store.flatMap(s => ConfigUtils.optString(s, "table")).getOrElse("ingest_watermarks")
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
