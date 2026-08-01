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

  /** Extraction window observed at read time, keyed by (entity, run id) via
    * windowKey so concurrent runs of one entity cannot overwrite each other;
    * consumed by advanceWatermark after a successful publish in the same
    * driver JVM. */
  private final case class ReadWindow(
    lower: WatermarkValue,
    capturedUpper: Option[WatermarkValue],
    version: Long,
    queryHash: Option[String] = None,
    // Read-time parsed config, so the commit step never re-parses (and
    // re-resolves vault secrets) after RAW and curated already committed.
    cfg: Option[JdbcSourceConfig] = None)
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

    var contextLower: Option[String] = None
    var contextUpper: Option[String] = None

    val runId = ConfigUtils.optString(sourceConf, "run_id")

    val watermarkPredicate = cfg.watermark.flatMap { wm =>
      val entity = entityKey(sourceConf, cfg)
      val store = WatermarkStores.from(wm, spark)
      val versioned = store.latestVersioned(entity)
      val lower = versioned.map(_.value).getOrElse(WatermarkValue.deserialize(wm.initialValue))
      val version = versioned.map(_.version).getOrElse(0L)

      // Companion queries against a sql base need :name parameters rendered.
      // The lower bound is known here; UPPER_WATERMARK cannot be (it is what
      // capture computes) — a base parameterized on it is self-referential
      // and fails resolution with the parameter's own error.
      val companionParams =
        if (cfg.parameters.isEmpty) Seq.empty
        else cfg.parameters.map(_.resolve(com.hcsc.generic.ingest.jdbc.query.ParameterContext(
          runId = runId,
          processingDate = Some(java.time.LocalDate.now.toString),
          lastWatermark = Some(lower.serialized),
          upperWatermark = None,
          pipelineParameters = cfg.pipelineParameters)))

      if (cfg.mode == JdbcMode.Incremental) {
        // First run for this entity: verify no rows carry NULL watermark
        // values — such rows are invisible to `col > lower` forever.
        if (versioned.isEmpty && !wm.allowNullWatermark) validateNoNullWatermarks(cfg, wm, companionParams)

        // Bounded window: capture the source's upper bound BEFORE the read
        // (MAX of the watermark column, or the source clock for SOURCE_CLOCK).
        val upper = captureUpper(cfg, wm, companionParams)
        readWindows.put(windowKey(entity, runId), ReadWindow(lower, upper, version, cfg = Some(cfg)))
        contextLower = Some(lower.serialized)
        contextUpper = upper.map(_.serialized)

        val predicate = upper match {
          case Some(u) => Watermarks.boundedPredicate(wm, cfg.dialect, lower, u)
          // captureUpper only returns None for a verified-empty source (or an
          // explicit allow_null_watermark override); the unbounded predicate
          // then extracts nothing / only non-null-watermark rows.
          case None => Watermarks.predicate(wm, cfg.dialect, lower)
        }
        logger.info(s"[JdbcSource] entity=$entity incremental window " +
          s"columns=[${wm.columns.mkString(",")}] version=$version bounded=${upper.isDefined} " +
          s"upperBound=${wm.upperBound}")
        Some(predicate)
      } else if (versioned.isDefined) {
        // FULL_TABLE reseed guard: seeding is a one-time bootstrap. A rerun
        // against an already-populated store must NOT jump an actively
        // advancing incremental watermark — no window is recorded, so the
        // commit step is a no-op for this run.
        logger.warn(s"[JdbcSource] entity=$entity already has a watermark (version $version); " +
          "FULL_TABLE run will NOT reseed it. Delete the watermark history first if a reseed " +
          "is genuinely intended.")
        None
      } else {
        // FULL_TABLE with an incremental block and an EMPTY store: unfiltered
        // read, but the captured cutoff SEEDS the watermark on success (the
        // one-time FULL -> INCR handoff).
        if (!wm.allowNullWatermark) validateNoNullWatermarks(cfg, wm, companionParams)
        val upper = captureUpper(cfg, wm, companionParams)
        readWindows.put(windowKey(entity, runId), ReadWindow(lower, upper, version, cfg = Some(cfg)))
        contextLower = Some(lower.serialized)
        contextUpper = upper.map(_.serialized)
        logger.info(s"[JdbcSource] entity=$entity FULL_TABLE run captured watermark seed " +
          s"cutoff=${upper.map(_.serialized).getOrElse("<none>")} (committed only after success)")
        // Bound the seed pull at the cutoff: rows modified during the long
        // full load belong to the FIRST incremental window (which starts
        // exactly at the committed seed) — loading them here too would
        // duplicate them across the handoff. FULL_TABLE is the only mode
        // that parses with an incremental block (rejectStrayIncremental),
        // so the guard is spelled out rather than assumed.
        // allow_null_watermark feeds stay unbounded: `NOT (col > x)` is
        // NULL (excluded) for NULL watermark rows, and those rows were
        // explicitly admitted. Opt out with incremental.bound_full_load = false.
        upper
          .filter(_ => wm.boundFullLoad && !wm.allowNullWatermark &&
            cfg.mode == JdbcMode.FullTable)
          .map { u =>
            logger.info(s"[JdbcSource] entity=$entity FULL pull bounded at the seed cutoff")
            Watermarks.upperBoundPredicate(wm, cfg.dialect, u)
          }
      }
    }

    // Dynamic parameter sources resolve against the read-time context (the
    // pipeline injects run_id; watermark values exist only for incremental)
    val parameterContext = com.hcsc.generic.ingest.jdbc.query.ParameterContext(
      runId = runId,
      processingDate = Some(java.time.LocalDate.now.toString),
      lastWatermark = contextLower,
      upperWatermark = contextUpper,
      pipelineParameters = cfg.pipelineParameters
    )
    val resolvedParameters = cfg.parameters.map(_.resolve(parameterContext))

    val dbtable = QueryBuilder.dbtable(cfg, watermarkPredicate, resolvedParameters)
    val queryHash = logQuery(cfg, dbtable)
    // Attach the executed-query hash to the window so the watermark commit
    // can persist it (full run reconstruction from the history table)
    cfg.watermark.foreach { _ =>
      readWindows.computeIfPresent(windowKey(entityKey(sourceConf, cfg), runId),
        (_, w) => w.copy(queryHash = Some(queryHash)))
    }

    // Retry layering: this wrapper protects DRIVER-side plan construction and
    // schema retrieval only. Executor partition reads are retried by Spark
    // task retry; whole-run failures are handled by pipeline restart (the
    // watermark never advanced, so replay re-extracts the same window).
    val df = RetryPolicy.withRetries(s"JDBC schema fetch (${cfg.dialect.name})", cfg.retry, logger) {
      buildReader(spark, cfg, dbtable, resolvedParameters)
    }

    JdbcMetrics.increment("jdbc_partitions_total", df.rdd.getNumPartitions)
    if (cfg.partitioning.skewMetrics) logSkew(df)

    applyContract(df, sourceConf)
  }

  private def buildReader(
    spark: SparkSession,
    cfg: JdbcSourceConfig,
    dbtable: String,
    resolvedParameters: Seq[com.hcsc.generic.ingest.jdbc.query.QueryParameter]
  ): DataFrame = {
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
            discoverBounds(cfg, resolvedParameters) // stale static bounds problem: discover per run
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
  private def discoverBounds(
    cfg: JdbcSourceConfig,
    resolvedParameters: Seq[com.hcsc.generic.ingest.jdbc.query.QueryParameter]
  ): Option[(Long, Long)] = {
    val column = cfg.partitioning.partitionColumn.get
    val baseFrom = QueryBuilder.validatedBase(cfg, resolvedParameters) match {
      case Some(base) => base
      case None => return None
    }
    // Same filter scope as the extraction query, so discovered strides track
    // the rows that will actually be read (not the whole table).
    val whereClause = QueryBuilder.filterWhereClause(cfg)
    val quoted = cfg.dialect.quoteQualified(column)
    val sql = s"SELECT MIN($quoted), MAX($quoted) FROM $baseFrom$whereClause"

    // withRetry=false: this runs inside buildReader's outer retry wrapper —
    // its own retry loop would multiply attempts to maxAttempts squared.
    DriverQueries.firstRow(cfg, sql, logger, withRetry = false) match {
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

  /** First-run guard (JDBC_006): rows whose watermark column is NULL can
    * never be selected by the incremental predicate — they would be lost
    * silently and permanently. Verified once per entity, before the first
    * window is established, via COUNT(1) vs COUNT(column) (COUNT(col)
    * excludes NULLs), scoped to the same filters as the extraction query. */
  private def validateNoNullWatermarks(
    cfg: JdbcSourceConfig,
    wm: WatermarkConfig,
    companionParams: Seq[com.hcsc.generic.ingest.jdbc.query.QueryParameter]
  ): Unit = {
    val baseFrom = QueryBuilder.validatedBase(cfg, companionParams) match {
      case Some(base) => base
      case None => return
    }
    val whereClause = QueryBuilder.filterWhereClause(cfg)
    val perColumn = wm.columns.map(c => s"COUNT(${cfg.dialect.quoteIdentifier(c)})").mkString(", ")
    val sql = s"SELECT COUNT(1), $perColumn FROM $baseFrom$whereClause"

    DriverQueries.firstRow(cfg, sql, logger).foreach { row =>
      val counts = row.map(_.map(v => BigDecimal(v).toLong).getOrElse(0L))
      val total = counts.head
      // NULL in ANY watermark column is fatal, trailing tie-breakers
      // included: at an exact leading-column tie, SQL three-valued logic
      // turns `(c1 > v1) OR (c1 = v1 AND c2 > v2)` into NULL for a row with
      // NULL c2 — the row is excluded from that window, and once the
      // watermark advances past the tied value it becomes unreachable
      // FOREVER. The tie case is precisely what a tie-breaker exists for,
      // so "extractable via the leading column" does not hold there.
      val leadingNulls = total - counts(1)
      val trailingNulls = if (counts.size > 2) counts.drop(2).map(total - _).max else 0L
      if (leadingNulls > 0)
        throw new IllegalStateException(
          s"JDBC_006 $leadingNulls row(s) in scope have NULL in the leading watermark column " +
            s"'${wm.columns.head}'; the incremental predicate can never extract them " +
            "(silent permanent data loss). Fix the source data, filter NULL rows explicitly, " +
            "or set incremental.allow_null_watermark = true to accept the loss.")
      if (trailingNulls > 0)
        throw new IllegalStateException(
          s"JDBC_006 $trailingNulls row(s) in scope have NULL in trailing watermark tie-breaker " +
            s"column(s) [${wm.columns.tail.mkString(",")}]; at an exact leading-column tie such " +
            "rows are silently and PERMANENTLY skipped by the composite predicate. Fix the " +
            "source data, filter NULL rows explicitly, or set " +
            "incremental.allow_null_watermark = true to accept the loss.")
    }
  }

  /** Driver-side capture of the window's upper edge BEFORE the read, so the
    * extraction window is stable and reproducible.
    *
    * SOURCE_CLOCK: the database server's current timestamp — idle sources
    * still advance and zero-row windows have a real boundary.
    *
    * MAX_VALUE: MAX(watermark_column) scoped to the extraction filters. A
    * NULL result is only accepted for a VERIFIED-EMPTY source; NULL with
    * rows present means the watermark column carries NULLs — those rows can
    * never be extracted by `col > lower`, so this fails loudly (JDBC_006)
    * instead of silently degrading to an unbounded, non-reproducible window. */
  private def captureUpper(
    cfg: JdbcSourceConfig,
    wm: WatermarkConfig,
    companionParams: Seq[com.hcsc.generic.ingest.jdbc.query.QueryParameter]
  ): Option[WatermarkValue] = {
    val baseFrom = QueryBuilder.validatedBase(cfg, companionParams).getOrElse(
      throw new IllegalArgumentException("JDBC_003 INCREMENTAL requires table or sql"))
    // Must match the extraction query's filters (structured + legacy where):
    // capturing MAX over unfiltered rows advances the watermark past rows the
    // filtered read never extracts — permanent silent data loss.
    val whereClause = QueryBuilder.filterWhereClause(cfg)

    if (wm.upperBound == WatermarkUpperBound.SourceClock) {
      val clockSql = cfg.dialect.currentTimestampSql(wm.watermarkType, wm.clockZone)
      val clock = DriverQueries.firstRow(cfg, clockSql, logger)
        .flatMap(_.headOption.flatten)
        .getOrElse(throw new IllegalStateException(
          s"JDBC_006 source clock capture returned no value ($clockSql); cannot form a bounded window"))
      return Some(WatermarkValue(Seq(clock)))
    }

    if (wm.columns.size == 1) {
      // Single statement so MAX and the row count come from ONE consistent
      // snapshot — separate queries can race a concurrent insert into a
      // spurious "NULL watermark with rows present" failure.
      val quoted = cfg.dialect.quoteIdentifier(wm.columns.head)
      val sql = s"SELECT MAX($quoted), COUNT(1) FROM $baseFrom$whereClause"
      DriverQueries.firstRow(cfg, sql, logger) match {
        case Some(Seq(Some(max), _)) => Some(WatermarkValue(Seq(max)))
        case Some(Seq(None, count)) =>
          val rows = count.map(BigDecimal(_).toLong).getOrElse(0L)
          nullCaptureOutcome(wm, rows)
        case _ => None // no row at all: verified empty
      }
    } else {
      val sql = cfg.dialect.selectTopOne(
        wm.columns.map(cfg.dialect.quoteIdentifier).mkString(", "),
        s"$baseFrom$whereClause",
        wm.columns.map(c => s"${cfg.dialect.quoteIdentifier(c)} DESC").mkString(", ")
      )
      DriverQueries.firstRow(cfg, sql, logger) match {
        case Some(row) if !row.exists(_.isEmpty) => Some(WatermarkValue(row.map(_.get)))
        case _ => // no row, or a NULL component: empty source or NULL watermark data
          val countSql = s"SELECT COUNT(1) FROM $baseFrom$whereClause"
          val rows = DriverQueries.firstRow(cfg, countSql, logger)
            .flatMap(_.headOption.flatten).map(BigDecimal(_).toLong).getOrElse(0L)
          nullCaptureOutcome(wm, rows)
      }
    }
  }

  private def nullCaptureOutcome(wm: WatermarkConfig, rowsInScope: Long): Option[WatermarkValue] =
    if (rowsInScope == 0L) None // verified empty; the unbounded predicate extracts nothing
    else if (wm.allowNullWatermark) {
      logger.warn("[JdbcSource] watermark capture returned NULL and allow_null_watermark=true: " +
        "rows with NULL watermark values are NEVER extracted by the incremental predicate")
      None
    } else throw new IllegalStateException(
      s"JDBC_006 watermark capture returned NULL but the source has $rowsInScope row(s) in scope: " +
        s"column(s) [${wm.columns.mkString(",")}] contain NULLs, and rows with NULL watermark " +
        "values can never be extracted incrementally (silent permanent data loss). Fix the " +
        "source data, filter NULL rows explicitly, or set incremental.allow_null_watermark = true " +
        "to accept the loss.")

  /** Sanitized by default: query hash + structure, never literals (watermark
    * values and business predicates can carry PII). Full SQL only under
    * diagnostics.log_sql = true. */
  private def logQuery(cfg: JdbcSourceConfig, dbtable: String): String = {
    val hash = java.security.MessageDigest.getInstance("SHA-256")
      .digest(dbtable.getBytes("UTF-8")).map("%02x".format(_)).mkString.take(12)
    logger.info(s"[JdbcSource] dialect=${cfg.dialect.name} mode=${cfg.mode} " +
      s"url=${JdbcHealthCheck.sanitized(cfg.url)} table=${cfg.table.getOrElse("<sql>")} " +
      s"queryHash=$hash fetchsize=${cfg.fetchSize} " +
      s"partitions=${cfg.partitioning.numPartitions.getOrElse(1)}")
    if (cfg.logSql) {
      if (sqlDiagnosticsSafe(cfg))
        logger.info(s"[JdbcSource] DIAGNOSTIC (diagnostics.log_sql=true) dbtable=$dbtable")
      else
        logger.warn("[JdbcSource] diagnostics.log_sql=true SUPPRESSED: query parameters " +
          "include a SECRET_PROVIDER source and the rendered SQL would leak the secret")
    }
    hash
  }

  /** log_sql prints the full rendered SQL, parameter literals included —
    * refuse whenever any parameter resolves from a secret provider. */
  private[jdbc] def sqlDiagnosticsSafe(cfg: JdbcSourceConfig): Boolean =
    !cfg.parameters.exists(_.source ==
      com.hcsc.generic.ingest.jdbc.query.ParameterSource.SecretProvider)

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
    // Windows are keyed per (entity, run) so concurrent runs of the same
    // entity in one JVM cannot commit each other's captured upper. The
    // bare-entity fallback covers standalone reads whose config carried no
    // run_id (the pipeline always injects one).
    val window = Option(readWindows.remove(windowKey(entity, Some(runId))))
      .orElse(Option(readWindows.remove(windowKey(entity, None))))
    // Reuse the read-time parsed config: re-parsing at the LAST pipeline
    // step re-resolves vault secrets, and a vault hiccup here would fail
    // the run AFTER raw and curated already committed (forcing a duplicate
    // re-extract). Only the resume path (no window) still parses.
    val cfg = window.flatMap(_.cfg).getOrElse(JdbcSourceConfig.parse(sourceConf))
    cfg.watermark.foreach { wm =>
      val store = WatermarkStores.from(wm, spark)

      // Seeding is only ever committed from a recorded seed window: a
      // FULL_TABLE run whose read declined to seed (store already
      // populated, or resume replay) must not fall back to computeNext and
      // jump an actively advancing incremental watermark.
      if (cfg.mode != JdbcMode.Incremental && window.isEmpty) {
        logger.info(s"[JdbcSource] entity=$entity ${cfg.mode} run recorded no seed window; " +
          "watermark left untouched")
        return
      }

      val (lower, version) = window match {
        case Some(w) => (w.lower, w.version)
        case None => // e.g. resume replay where the read stage was skipped
          val v = store.latestVersioned(entity)
          (v.map(_.value).getOrElse(WatermarkValue.deserialize(wm.initialValue)),
            v.map(_.version).getOrElse(0L))
      }

      // Windowed reads commit ONLY the captured upper: falling back to the
      // extracted max after an unbounded read would commit a boundary that
      // depends on when each partition happened to run. computeNext remains
      // solely for the resume path, where no window exists and the accepted
      // DataFrame is the replayed RAW slice of this run.
      val candidate = (window match {
        case Some(w) => w.capturedUpper
        case None    => Watermarks.computeNext(accepted, wm)
      }).filter(next => Watermarks.compare(wm, next, lower) > 0) // advance-only

      candidate match {
        case None =>
          logger.info(s"[JdbcSource] entity=$entity nothing beyond current watermark; not advanced")
        case Some(next) =>
          val detail = com.hcsc.generic.ingest.jdbc.watermark.WatermarkCommitDetail(
            lower = Some(lower.serialized),
            queryHash = window.flatMap(_.queryHash))
          try {
            store.recordIfVersion(entity, next, runId, version, detail)
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

  /** Peek (not consume) at the window recorded by this run's read so the
    * pipeline can stamp it onto RAW records; advanceWatermark still removes
    * the entry when committing. */
  override def lastWindow(entity: String, runId: Option[String]): Option[(String, Option[String])] =
    Option(readWindows.get(windowKey(entity, runId)))
      .orElse(Option(readWindows.get(windowKey(entity, None))))
      .map(w => (w.lower.serialized, w.capturedUpper.map(_.serialized)))

  /** Drops a failed run's recorded window so a long-lived driver JVM does
    * not accumulate abandoned entries (and a later same-JVM attempt cannot
    * commit a stale captured upper). Invoked by the pipeline on failure. */
  override def discardWindow(entity: String, runId: Option[String]): Unit = {
    readWindows.remove(windowKey(entity, runId))
    readWindows.remove(windowKey(entity, None))
  }

  def register(): Unit = SourceRegistry.register(this)

  /** Watermark identity: the pipeline injects the feed entity; direct users
    * may set incremental.watermark_name; last resort is the table name. */
  /** readWindows key: entity + run id (empty for standalone reads). */
  private def windowKey(entity: String, runId: Option[String]): String =
    s"$entity|${runId.getOrElse("")}"

  private def entityKey(sourceConf: Config, cfg: JdbcSourceConfig): String =
    ConfigUtils.optString(sourceConf, "entity")
      .orElse(ConfigUtils.optConfig(sourceConf, "incremental")
        .flatMap(i => ConfigUtils.optString(i, "watermark_name")))
      .orElse(cfg.table)
      .getOrElse(throw new IllegalArgumentException(
        "JDBC_003 Cannot determine watermark identity; set incremental.watermark_name"))
}

/** Driver class-name constants, kept for external callers. The dialects are
  * the single source of truth; these are aliases, not a second copy. */
object JdbcDrivers {
  val SqlServer: String = dialect.SqlServerDialect.defaultDriver
  val Db2: String = dialect.Db2Dialect.defaultDriver
  val Oracle: String = dialect.OracleDialect.defaultDriver
  val PostgreSQL: String = dialect.PostgresDialect.defaultDriver
  val MySQL: String = dialect.MySqlDialect.defaultDriver
}
