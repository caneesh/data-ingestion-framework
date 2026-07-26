package com.hcsc.generic.ingest.jdbc

import com.hcsc.generic.ingest.config.ConfigUtils
import com.hcsc.generic.ingest.jdbc.health.JdbcHealthCheck
import com.hcsc.generic.ingest.jdbc.read.{QueryBuilder, RetryPolicy}
import com.hcsc.generic.ingest.jdbc.watermark.{WatermarkStores, Watermarks}
import com.hcsc.generic.ingest.schema.{SchemaContract, SchemaValidator}
import com.hcsc.generic.ingest.source.{Source, SourceRegistry, WatermarkAdvancing}
import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.lit

/**
  * Pluggable JDBC source (Azure SQL Server first; dialects for PostgreSQL,
  * Oracle, DB2, MySQL and a generic fallback). Modes: FULL_TABLE,
  * SELECT_QUERY, CUSTOM_SQL, INCREMENTAL (watermark-driven with overlap).
  *
  * Reuses the framework's schema-contract system for JDBC schema drift:
  * source columns are resolved by canonical name/alias, unexpected and
  * missing columns follow the feed's policies, and missing optional columns
  * receive configured defaults — the same guarantees files get.
  *
  * Watermarks advance only after successful publish (WatermarkAdvancing,
  * invoked by IngestPipeline at the end of a fully successful run).
  */
object JdbcSource extends Source with WatermarkAdvancing {
  private val logger = Logger.getLogger(getClass.getName)

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
      val latest = store.latest(entity)
        .getOrElse(com.hcsc.generic.ingest.jdbc.watermark.WatermarkValue.deserialize(wm.initialValue))
      val predicate = Watermarks.predicate(wm, cfg.dialect, latest)
      logger.info(s"[JdbcSource] entity=$entity incremental watermark=${latest.serialized} predicate=$predicate")
      predicate
    }

    val dbtable = QueryBuilder.dbtable(cfg, watermarkPredicate)
    logger.info(s"[JdbcSource] dialect=${cfg.dialect.name} mode=${cfg.mode} " +
      s"url=${JdbcHealthCheck.sanitized(cfg.url)} dbtable=$dbtable fetchsize=${cfg.fetchSize}")

    var reader = spark.read
      .format("jdbc")
      .option("url", cfg.url)
      .option("driver", cfg.driver)
      .option("dbtable", dbtable)
      .option("fetchsize", cfg.fetchSize.toString)

    cfg.user.foreach(u => reader = reader.option("user", u))
    cfg.password.foreach(p => reader = reader.option("password", p))
    cfg.connectionProperties.foreach { case (k, v) => reader = reader.option(k, v) }

    cfg.partitioning.numPartitions.foreach(n => reader = reader.option("numPartitions", n.toString))
    cfg.partitioning.partitionColumn.foreach { c =>
      reader = reader.option("partitionColumn", c)
        .option("lowerBound", cfg.partitioning.lowerBound.get.toString)
        .option("upperBound", cfg.partitioning.upperBound.get.toString)
    }

    val df = RetryPolicy.withRetries(s"JDBC read (${cfg.dialect.name})", cfg.retry, logger) {
      reader.load()
    }

    applyContract(df, sourceConf)
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

  /** Advances the watermark from the accepted data — called by the pipeline
    * only after a fully successful publish. Advance-only: the position never
    * moves backwards (overlap re-reads cannot regress it). */
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
      Watermarks.computeNext(accepted, wm) match {
        case None =>
          logger.info(s"[JdbcSource] entity=$entity no rows extracted; watermark unchanged")
        case Some(next) =>
          val advanced = store.latest(entity) match {
            case Some(current) => Watermarks.max(wm, current, next)
            case None          => next
          }
          store.record(entity, advanced, runId)
          logger.info(s"[JdbcSource] entity=$entity watermark advanced to ${advanced.serialized} (runId=$runId)")
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
