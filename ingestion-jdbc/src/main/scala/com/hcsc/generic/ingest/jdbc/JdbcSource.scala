package com.hcsc.generic.ingest.jdbc

import com.hcsc.generic.ingest.config.ConfigUtils
import com.hcsc.generic.ingest.jdbc.health.JdbcHealthCheck
import com.hcsc.generic.ingest.jdbc.read.{DriverQueries, QueryBuilder, RetryPolicy}
import com.hcsc.generic.ingest.jdbc.watermark.{WatermarkStores, WatermarkValue, Watermarks}
import com.hcsc.generic.ingest.schema.{SchemaContract, SchemaValidator}
import com.hcsc.generic.ingest.source.{Source, SourceRegistry, WatermarkAdvancing}
import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.lit

/**
  * Pluggable JDBC source (Azure SQL Server first; dialects for PostgreSQL,
  * Oracle, DB2, MySQL and a generic fallback). Modes: FULL_TABLE,
  * SELECT_QUERY, CUSTOM_SQL, INCREMENTAL.
  *
  * Incremental extraction is BOUNDED: the source's upper watermark is
  * captured on the driver before the read, the predicate becomes
  * `> lower AND <= captured upper`, and on success the CAPTURED upper (not
  * the extracted max) is committed with an optimistic version check —
  * a reproducible window that concurrent runs cannot double-advance.
  *
  * Reuses the framework's schema-contract system for JDBC schema drift.
  * Watermarks advance only after successful publish (WatermarkAdvancing,
  * invoked by IngestPipeline at the end of a fully successful run).
  */
object JdbcSource extends Source with WatermarkAdvancing {
  private val logger = Logger.getLogger(getClass.getName)

  /** Extraction window observed at read time, keyed by entity, consumed by
    * advanceWatermark after a successful publish in the same driver JVM. */
  private final case class ReadWindow(lower: WatermarkValue, capturedUpper: Option[WatermarkValue], version: Long)
  private val readWindows = new java.util.concurrent.ConcurrentHashMap[String, ReadWindow]()

  override def sourceType: String = "jdbc"

  override def read(spark: SparkSession, sourceConf: Config): DataFrame = {
    val cfg = JdbcSourceConfig.parse(sourceConf)

    if (cfg.healthCheckEnabled) {
      JdbcHealthCheck.check(cfg) match {
        case Left(error)   => throw new IllegalStateException(error)
        case Right(status) => logger.info(s"[JdbcSource] $status")
      }
    }

    val watermarkPredicate = cfg.watermark.map { wm =>
      val entity = entityKey(sourceConf, cfg)
      val store = WatermarkStores.from(wm, spark)
      val versioned = store.latestVersioned(entity)
      val lower = versioned.map(_.value).getOrElse(WatermarkValue.deserialize(wm.initialValue))
      val version = versioned.map(_.version).getOrElse(0L)

      // Bounded window: capture the source's upper watermark BEFORE the read
      val upper = captureUpper(cfg, wm)
      readWindows.put(entity, ReadWindow(lower, upper, version))

      val predicate = upper match {
        case Some(u) => Watermarks.boundedPredicate(wm, cfg.dialect, lower, u)
        case None    => Watermarks.predicate(wm, cfg.dialect, lower) // empty source; extract is empty anyway
      }
      logger.info(s"[JdbcSource] entity=$entity incremental window " +
        s"columns=[${wm.columns.mkString(",")}] version=$version bounded=${upper.isDefined}")
      predicate
    }

    val dbtable = QueryBuilder.dbtable(cfg, watermarkPredicate)
    logQuery(cfg, dbtable)

    var reader = spark.read
      .format("jdbc")
      .option("url", cfg.url)
      .option("driver", cfg.driver)
      .option("dbtable", dbtable)
      .option("fetchsize", cfg.fetchSize.toString)

    cfg.user.foreach(u => reader = reader.option("user", u))
    cfg.password.foreach(p => reader = reader.option("password", p))
    cfg.connectionProperties.foreach { case (k, v) => reader = reader.option(k, v) }

    cfg.partitioning.partitionColumn.foreach { c =>
      reader = reader
        .option("numPartitions", cfg.partitioning.numPartitions.get.toString)
        .option("partitionColumn", c)
        .option("lowerBound", cfg.partitioning.lowerBound.get.toString)
        .option("upperBound", cfg.partitioning.upperBound.get.toString)
    }

    // Retry layering: this wrapper protects DRIVER-side plan construction and
    // schema retrieval only. Executor partition reads are retried by Spark
    // task retry; whole-run failures are handled by pipeline restart (the
    // watermark never advanced, so replay re-extracts the same window).
    val df = RetryPolicy.withRetries(s"JDBC schema fetch (${cfg.dialect.name})", cfg.retry, logger) {
      reader.load()
    }

    applyContract(df, sourceConf)
  }

  /** Driver-side capture of the source's current maximum watermark, so the
    * extraction window has a stable, reproducible upper edge. */
  private def captureUpper(cfg: JdbcSourceConfig, wm: WatermarkConfig): Option[WatermarkValue] = {
    val baseFrom = (cfg.table, cfg.sql) match {
      case (Some(t), _)   => t
      case (None, Some(s)) => s"($s) base"
      case _ => throw new IllegalArgumentException("JDBC_003 INCREMENTAL requires table or sql")
    }
    val whereClause = cfg.where.map(w => s" WHERE $w").getOrElse("")

    val sql =
      if (wm.columns.size == 1)
        s"SELECT MAX(${cfg.dialect.quoteIdentifier(wm.columns.head)}) FROM $baseFrom$whereClause"
      else
        cfg.dialect.selectTopOne(
          wm.columns.map(cfg.dialect.quoteIdentifier).mkString(", "),
          s"$baseFrom$whereClause",
          wm.columns.map(c => s"${cfg.dialect.quoteIdentifier(c)} DESC").mkString(", ")
        )

    DriverQueries.firstRow(cfg, sql, logger).flatMap { row =>
      if (row.exists(_.isEmpty)) None // any null component -> no usable upper
      else Some(WatermarkValue(row.map(_.get)))
    }
  }

  /** Sanitized by default: query hash + structure, never literals (watermark
    * values and business predicates can carry PII). Full SQL only under
    * diagnostics.log_sql = true. */
  private def logQuery(cfg: JdbcSourceConfig, dbtable: String): Unit = {
    val hash = java.security.MessageDigest.getInstance("SHA-256")
      .digest(dbtable.getBytes("UTF-8")).map("%02x".format(_)).mkString.take(12)
    logger.info(s"[JdbcSource] dialect=${cfg.dialect.name} mode=${cfg.mode} " +
      s"url=${JdbcHealthCheck.sanitized(cfg.url)} table=${cfg.table.getOrElse("<sql>")} " +
      s"queryHash=$hash fetchsize=${cfg.fetchSize} " +
      s"partitions=${cfg.partitioning.numPartitions.getOrElse(1)}")
    if (cfg.logSql)
      logger.info(s"[JdbcSource] DIAGNOSTIC (diagnostics.log_sql=true) dbtable=$dbtable")
  }

  /** JDBC schema drift handling via the shared contract system: canonical
    * name/alias resolution, policy-driven missing/extra handling, optional
    * defaults. Feeds without a contract pass through unchanged. */
  private def applyContract(df: DataFrame, sourceConf: Config): DataFrame =
    SchemaContract.parse(sourceConf) match {
      case None => df
      case Some(contract) =>
        val resolution = SchemaValidator.validateHeaders(df.columns.toSeq, contract, logger)
        logger.info(s"[JdbcSource] Schema drift check: columns=[${df.columns.mkString(",")}] " +
          s"missingRequired=[${resolution.missingRequired.mkString(",")}]")
        SchemaValidator.enforce(resolution.violations, contract.policies, logger, Some(resolution))

        val renamed = resolution.renames.foldLeft(df) {
          case (acc, (from, to)) => acc.withColumnRenamed(from, to)
        }
        contract.optionalColumns
          .filterNot(c => renamed.columns.exists(_.equalsIgnoreCase(c.name)))
          .foldLeft(renamed) { (acc, c) =>
            logger.info(s"[JdbcSource] Adding missing optional column '${c.name}' with default=${c.default.getOrElse("null")}")
            acc.withColumn(c.name, lit(c.default.orNull).cast(c.dataType))
          }
    }

  /**
    * Commits the extraction window after a fully successful publish:
    * the CAPTURED upper watermark (exactly what the bounded read extracted),
    * with an optimistic version check — if a concurrent run advanced the
    * same entity since our read, this fails with JDBC_005 instead of
    * silently overwriting.
    */
  override def advanceWatermark(
    spark: SparkSession,
    sourceConf: Config,
    entity: String,
    runId: String,
    accepted: DataFrame
  ): Unit = {
    val cfg = JdbcSourceConfig.parse(sourceConf)
    cfg.watermark.foreach { wm =>
      val store = WatermarkStores.from(wm, spark)
      val window = Option(readWindows.remove(entity))

      val (lower, version) = window match {
        case Some(w) => (w.lower, w.version)
        case None => // e.g. resume replay where the read stage was skipped
          val v = store.latestVersioned(entity)
          (v.map(_.value).getOrElse(WatermarkValue.deserialize(wm.initialValue)),
            v.map(_.version).getOrElse(0L))
      }

      val candidate = window.flatMap(_.capturedUpper)
        .orElse(Watermarks.computeNext(accepted, wm))
        .filter(next => Watermarks.compare(wm, next, lower) > 0) // advance-only

      candidate match {
        case None =>
          logger.info(s"[JdbcSource] entity=$entity nothing beyond current watermark; not advanced")
        case Some(next) =>
          store.recordIfVersion(entity, next, runId, version)
          logger.info(s"[JdbcSource] entity=$entity watermark committed " +
            s"version=${version + 1} columns=[${wm.columns.mkString(",")}] (runId=$runId)")
      }
    }
  }

  def register(): Unit = SourceRegistry.register(this)

  /** Watermark identity: the pipeline injects the feed entity; direct users
    * may set incremental.watermark_name; last resort is the table name. */
  private def entityKey(sourceConf: Config, cfg: JdbcSourceConfig): String =
    ConfigUtils.optString(sourceConf, "entity")
      .orElse(ConfigUtils.optConfig(sourceConf, "incremental")
        .flatMap(i => ConfigUtils.optString(i, "watermark_name")))
      .orElse(cfg.table)
      .getOrElse(throw new IllegalArgumentException(
        "JDBC_003 Cannot determine watermark identity; set incremental.watermark_name"))
}

object JdbcDrivers {
  val SqlServer = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
  val Db2 = "com.ibm.db2.jcc.DB2Driver"
  val Oracle = "oracle.jdbc.OracleDriver"
  val PostgreSQL = "org.postgresql.Driver"
  val MySQL = "com.mysql.cj.jdbc.Driver"
}
