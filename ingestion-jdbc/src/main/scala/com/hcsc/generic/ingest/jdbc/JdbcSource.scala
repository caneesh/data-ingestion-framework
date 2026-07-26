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
    cfg.executorProbePartitions.foreach { n =>
      logger.info(s"[JdbcSource] Running executor connectivity probe across $n partition(s)")
      JdbcHealthCheck.executorProbe(spark, cfg, n)
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

    // Retry layering: this wrapper protects DRIVER-side plan construction and
    // schema retrieval only. Executor partition reads are retried by Spark
    // task retry; whole-run failures are handled by pipeline restart (the
    // watermark never advanced, so replay re-extracts the same window).
    val df = RetryPolicy.withRetries(s"JDBC schema fetch (${cfg.dialect.name})", cfg.retry, logger) {
      buildReader(spark, cfg, dbtable)
    }

    JdbcMetrics.increment("jdbc_partitions_total", df.rdd.getNumPartitions)
    if (cfg.partitioning.skewMetrics) logSkew(df)

    applyContract(df, sourceConf)
  }

  private def buildReader(spark: SparkSession, cfg: JdbcSourceConfig, dbtable: String): DataFrame = {
    cfg.partitioning.strategy match {
      case PartitionStrategy.Predicates =>
        // Explicit per-partition predicates via the spark.read.jdbc API —
        // for keys where range striding is unsuitable or badly skewed.
        val props = new java.util.Properties()
        props.setProperty("driver", cfg.driver)
        props.setProperty("fetchsize", cfg.fetchSize.toString)
        cfg.user.foreach(props.setProperty("user", _))
        cfg.password.foreach(props.setProperty("password", _))
        cfg.connectionProperties.foreach { case (k, v) => props.setProperty(k, v) }
        spark.read.jdbc(cfg.url, dbtable, cfg.partitioning.predicates.toArray, props)

      case strategy =>
        var reader = spark.read
          .format("jdbc")
          .option("url", cfg.url)
          .option("driver", cfg.driver)
          .option("dbtable", dbtable)
          .option("fetchsize", cfg.fetchSize.toString)
        cfg.user.foreach(u => reader = reader.option("user", u))
        cfg.password.foreach(p => reader = reader.option("password", p))
        cfg.connectionProperties.foreach { case (k, v) => reader = reader.option(k, v) }

        val bounds: Option[(Long, Long)] = strategy match {
          case PartitionStrategy.MinMaxQuery =>
            discoverBounds(cfg) // stale static bounds problem: discover per run
          case _ =>
            for (lo <- cfg.partitioning.lowerBound; hi <- cfg.partitioning.upperBound) yield (lo, hi)
        }

        (cfg.partitioning.partitionColumn, bounds) match {
          case (Some(column), Some((lo, hi))) =>
            reader = reader
              .option("numPartitions", cfg.partitioning.numPartitions.get.toString)
              .option("partitionColumn", column)
              .option("lowerBound", lo.toString)
              .option("upperBound", hi.toString)
          case _ => () // unpartitioned read
        }
        reader.load()
    }
  }

  /** MIN_MAX_QUERY strategy: driver-side bound discovery so partition
    * strides track the actual key range instead of stale static config. */
  private def discoverBounds(cfg: JdbcSourceConfig): Option[(Long, Long)] = {
    val column = cfg.partitioning.partitionColumn.get
    val baseFrom = (cfg.table, cfg.sql) match {
      case (Some(t), _)    => t
      case (None, Some(s)) => s"($s) base"
      case _ => return None
    }
    val whereClause = cfg.where.map(w => s" WHERE $w").getOrElse("")
    val quoted = cfg.dialect.quoteQualified(column)
    val sql = s"SELECT MIN($quoted), MAX($quoted) FROM $baseFrom$whereClause"

    DriverQueries.firstRow(cfg, sql, logger) match {
      case Some(Seq(Some(lo), Some(hi))) =>
        val (l, h) = (BigDecimal(lo).toLong, BigDecimal(hi).toLong)
        if (l < h) {
          logger.info(s"[JdbcSource] MIN_MAX_QUERY discovered bounds [$l, $h] for $column")
          Some((l, h))
        } else {
          logger.info(s"[JdbcSource] MIN_MAX_QUERY bounds degenerate [$l, $h]; reading unpartitioned")
          None
        }
      case _ =>
        logger.info("[JdbcSource] MIN_MAX_QUERY found no rows; reading unpartitioned")
        None
    }
  }

  /** Rows per Spark partition + skew ratio (gated by skew_metrics: this
    * costs one extra pass over the extracted data). */
  private def logSkew(df: DataFrame): Unit = {
    val counts = df.rdd.mapPartitionsWithIndex { case (i, it) => Iterator((i, it.size)) }.collect()
    if (counts.nonEmpty) {
      val sizes = counts.map(_._2.toLong)
      val max = sizes.max
      val avg = sizes.sum.toDouble / sizes.length
      val ratio = if (avg > 0) max / avg else 0.0
      logger.info(f"[JdbcSource] partition skew: partitions=${sizes.length} " +
        f"min=${sizes.min} max=$max avg=$avg%.1f skewRatio=$ratio%.2f " +
        s"counts=${counts.sortBy(_._1).map(_._2).mkString(",")}")
      if (ratio > 3.0)
        logger.warn(f"[JdbcSource] significant partition skew (ratio $ratio%.2f); " +
          "consider PREDICATES partitioning or a different partition column")
    }
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
        if (resolution.violations.nonEmpty) JdbcMetrics.increment("jdbc_schema_drift_total")
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
          try {
            store.recordIfVersion(entity, next, runId, version)
            JdbcMetrics.increment("jdbc_watermark_commit_total")
          } catch {
            case e: com.hcsc.generic.ingest.jdbc.watermark.WatermarkConflictException =>
              JdbcMetrics.increment("jdbc_watermark_conflict_total")
              throw e
          }
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
