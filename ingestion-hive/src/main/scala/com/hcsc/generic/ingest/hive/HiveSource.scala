package com.hcsc.generic.ingest.hive

import com.hcsc.generic.ingest.schema.{SchemaContract, SchemaValidator}
import com.hcsc.generic.ingest.source.{Source, SourceRegistry, WatermarkAdvancing, WindowReplayable}
import com.hcsc.generic.ingest.watermark.{HiveWatermarkStore, InMemoryWatermarkStore, VersionedWatermark, WatermarkCommitDetail, WatermarkStore, WatermarkValue}
import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTablePartition
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.util.control.NonFatal

/**
  * Reads an existing partitioned Hive table incrementally: each run selects
  * the partitions whose watermark-column tuple is lexicographically greater
  * than the stored watermark (rewound by the configured lookback), prunes
  * them at the catalog, and commits the max tuple it enumerated after the
  * pipeline publishes.
  *
  * The committed upper comes from the ENUMERATED partitions, never from
  * the rows that came back: a partition whose rows are all rejected still
  * advances the watermark (subject to rejects.on_reject_watermark) instead
  * of being re-read forever. The recorded window lower is the stored
  * watermark, never the rewound bound, so watermark_continuity is
  * unaffected by lookback — exactly how JDBC records its overlap.
  */
object HiveSource extends Source with WatermarkAdvancing with WindowReplayable {
  private val logger = Logger.getLogger(getClass.getName)

  override def sourceType: String = "hive"

  def register(): Unit = SourceRegistry.register(this)

  private final case class ReadWindow(
    lower: WatermarkValue,
    upper: WatermarkValue,
    version: Long,
    fingerprint: String,
    cfg: HiveSourceConfig)
  private val readWindows = new java.util.concurrent.ConcurrentHashMap[String, ReadWindow]()

  private def windowKey(entity: String, runId: Option[String]): String = s"$entity|${runId.getOrElse("")}"

  /** Test seam: drops every captured read window. */
  private[hive] def clearReadWindows(): Unit = readWindows.clear()

  /** Test seam: replaces the catalog's listPartitionsByFilter so the
    * fallback path can be exercised deterministically. */
  @volatile private[hive] var listByFilterOverride:
    Option[(TableIdentifier, Seq[Expression]) => Seq[CatalogTablePartition]] = None

  /** Partition-key-set fingerprint is persisted in WatermarkCommitDetail.queryHash
    * under this prefix so it can never be mistaken for a JDBC query hash. */
  private val FingerprintPrefix = "pk:"

  override def read(spark: SparkSession, sourceConf: Config): DataFrame = {
    val cfg = HiveSourceConfig.parse(sourceConf)
    if (cfg.runId.isDefined && cfg.entity.isEmpty)
      throw new IllegalArgumentException(
        "HIVE_001 run_id is present but entity is not; the pipeline injects both — a hive source " +
          "cannot track a watermark without an entity")
    val validateMode = cfg.entity.isEmpty && cfg.runId.isEmpty
    val ident = TableIdentifier(cfg.table, Some(cfg.database))
    val partCols = partitionColumns(spark, cfg)
    val fingerprint = partCols.map(_._1).mkString("|")

    val selectedFrame: DataFrame = cfg.watermark match {
      case None =>
        cfg.where.foreach(w => checkWhere(w, partCols))
        val pred = PartitionPredicate(partCols.map(_._1), None, None, cfg.where)
        logger.info(s"[HiveSource] ${cfg.fullTable}: no incremental block; reading every partition" +
          cfg.where.map(w => s" where $w").getOrElse(""))
        if (validateMode) spark.table(cfg.fullTable).filter(lit(false))
        else { applyMissingFiles(spark, cfg); spark.table(cfg.fullTable).filter(pred.column) }

      case Some(wm) =>
        checkWatermarkColumns(wm, partCols)
        cfg.where.foreach(w => checkWhere(w, partCols))
        val store = makeStore(spark, wm)
        val stored: Option[VersionedWatermark] =
          if (validateMode) None else store.latestVersioned(cfg.entity.get)
        if (!validateMode && cfg.runMode.contains("FULL") && stored.isDefined)
          throw new IllegalArgumentException(
            s"HIVE_007 run_mode = FULL refused for entity '${cfg.entity.get}': the watermark store " +
              s"already holds version ${stored.get.version} (${stored.get.value.serialized}). A FULL " +
              "publish would overwrite curated with only the watermark delta. FULL is allowed only at " +
              "first light (empty store); reset the watermark deliberately if a full rebuild is intended")
        val lower = stored.map(_.value).getOrElse(wm.initialValue)
        if (lower.values.size != wm.columns.size)
          throw new IllegalArgumentException(
            s"HIVE_005 stored watermark '${lower.serialized}' has ${lower.values.size} component(s) " +
              s"but watermark_columns has ${wm.columns.size}; changing watermark_columns requires a " +
              "watermark reset")
        val version = stored.map(_.version).getOrElse(0L)
        if (!validateMode) warnOnFingerprintChange(spark, store, wm, cfg.entity.get, fingerprint)

        val rewound = rewind(spark, ident, partCols, wm, lower, cfg.where)
        if (rewound != lower)
          logger.info(s"[HiveSource] lookback: selecting partitions newer than ${rewound.serialized} " +
            s"(stored watermark ${lower.serialized})")
        val pred = PartitionPredicate(wm.columns, Some(rewound.values), None, cfg.where)
        val selected = enumerate(spark, ident, partCols, pred)
        val key = cfg.entity.map(e => windowKey(e, cfg.runId))

        if (selected.isEmpty) {
          logger.info(s"[HiveSource] ${cfg.fullTable}: no new partitions beyond ${rewound.serialized}" +
            cfg.where.map(w => s" (where $w)").getOrElse(""))
          key.foreach(k => readWindows.put(k, ReadWindow(lower, lower, version, fingerprint, cfg)))
          spark.table(cfg.fullTable).filter(lit(false))
        } else {
          guardFormats(wm, selected)
          val tuples = selected.map(spec => wm.columns.map(c => specValue(spec, c)))
          val upper = WatermarkValue(tuples.max(PartitionPredicate.tupleOrdering))
          key.foreach(k => readWindows.put(k, ReadWindow(lower, upper, version, fingerprint, cfg)))
          logger.info(s"[HiveSource] ${cfg.fullTable}: ${selected.size} partition(s) selected, " +
            s"upper=${upper.serialized}: ${selected.map(specText).mkString(", ")}")
          if (validateMode) spark.table(cfg.fullTable).filter(lit(false))
          else {
            applyMissingFiles(spark, cfg)
            val df = spark.table(cfg.fullTable).filter(pred.column)
            if (cfg.logPartitionCounts) logZeroRowPartitions(df, partCols.map(_._1), selected)
            df
          }
        }
    }
    applyContract(selectedFrame, sourceConf)
  }

  /**
    * The same pruned read for an explicit `(lower, upper]` window — used by
    * decoupled replay (a run's recorded window re-read from the source) and
    * by tests. Touches neither the watermark store nor the read-window map.
    */
  def readWindow(spark: SparkSession, sourceConf: Config, lower: WatermarkValue, upper: WatermarkValue): DataFrame = {
    val cfg = HiveSourceConfig.parse(sourceConf)
    val wm = cfg.watermark.getOrElse(throw new IllegalArgumentException(
      "HIVE_008 readWindow requires source.incremental.watermark_columns"))
    val ident = TableIdentifier(cfg.table, Some(cfg.database))
    val partCols = partitionColumns(spark, cfg)
    checkWatermarkColumns(wm, partCols)
    cfg.where.foreach(w => checkWhere(w, partCols))
    if (lower.values.size != wm.columns.size || upper.values.size != wm.columns.size)
      throw new IllegalArgumentException(
        s"HIVE_005 window bounds (${lower.serialized}, ${upper.serialized}] do not match " +
          s"watermark_columns arity ${wm.columns.size}")
    val pred = PartitionPredicate(wm.columns, Some(lower.values), Some(upper.values), cfg.where)
    val selected = enumerate(spark, ident, partCols, pred)
    if (selected.isEmpty && PartitionPredicate.compare(upper.values, lower.values) != 0)
      throw new IllegalArgumentException(
        s"HIVE_008 no partition of ${cfg.fullTable} exists in the window (${lower.serialized}, " +
          s"${upper.serialized}]; the partitions that run read have since been removed from the " +
          "source, so this window cannot be replayed from it")
    if (selected.nonEmpty) guardFormats(wm, selected)
    logger.info(s"[HiveSource] ${cfg.fullTable}: window (${lower.serialized}, ${upper.serialized}] " +
      s"-> ${selected.size} partition(s): ${selected.map(specText).mkString(", ")}")
    applyMissingFiles(spark, cfg)
    applyContract(spark.table(cfg.fullTable).filter(pred.column), sourceConf)
  }

  /** The pipeline's `WindowReplayable` entry point (`raw.mode = SOURCE`):
    * bounds arrive serialized exactly as the run ledger recorded them. */
  override def readWindow(spark: SparkSession, sourceConf: Config, lower: String, upper: String): DataFrame =
    readWindow(spark, sourceConf, WatermarkValue.deserialize(lower), WatermarkValue.deserialize(upper))

  /**
    * The whole table under the feed's `where` — no watermark, no read
    * window, no store access. Reconciliation needs the same scoping the
    * extraction uses (`where`) but never its window: key existence is
    * timestamp-independent, which is what makes the comparison trustworthy.
    */
  def readAll(spark: SparkSession, sourceConf: Config): DataFrame = {
    val cfg = HiveSourceConfig.parse(sourceConf)
    val partCols = partitionColumns(spark, cfg)
    cfg.where.foreach(w => checkWhere(w, partCols))
    val pred = PartitionPredicate(partCols.map(_._1), None, None, cfg.where)
    logger.info(s"[HiveSource] ${cfg.fullTable}: reading every partition" +
      cfg.where.map(w => s" where $w").getOrElse("") + " (no watermark — reconciliation read)")
    applyMissingFiles(spark, cfg)
    applyContract(spark.table(cfg.fullTable).filter(pred.column), sourceConf)
  }

  override def advanceWatermark(
    spark: SparkSession,
    sourceConf: Config,
    entity: String,
    runId: String,
    accepted: DataFrame
  ): Unit = {
    // The bare-entity key covers a standalone read whose config carried no
    // run_id (the pipeline always injects one), as JdbcSource does.
    val window = Option(readWindows.remove(windowKey(entity, Some(runId))))
      .orElse(Option(readWindows.remove(windowKey(entity, None))))
    val cfg = window.map(_.cfg).getOrElse(HiveSourceConfig.parse(sourceConf))
    cfg.watermark match {
      case None =>
        logger.info(s"[HiveSource] entity=$entity has no incremental block; nothing to commit")
      case Some(wm) =>
        val store = makeStore(spark, wm)
        val resolved = window match {
          case Some(w) => Some((w.lower, w.upper, w.version, w.fingerprint))
          case None => recoverFromLedger(spark, cfg, store, entity, runId)
        }
        resolved.foreach { case (lower, upper, version, fingerprint) =>
          if (PartitionPredicate.compare(upper.values, lower.values) <= 0)
            logger.info(s"[HiveSource] entity=$entity nothing beyond current watermark " +
              s"${lower.serialized}; not advanced")
          else {
            store.recordIfVersion(entity, upper, runId, version,
              WatermarkCommitDetail(lower = Some(lower.serialized), queryHash = Some(FingerprintPrefix + fingerprint)))
            logger.info(s"[HiveSource] entity=$entity committed watermark ${upper.serialized} " +
              s"(version ${version + 1}, window from ${lower.serialized}, run $runId)")
          }
        }
    }
  }

  override def lastWindow(entity: String, runId: Option[String]): Option[(String, Option[String])] =
    Option(readWindows.get(windowKey(entity, runId))).map(w => (w.lower.serialized, Some(w.upper.serialized)))

  override def discardWindow(entity: String, runId: Option[String]): Unit =
    runId.foreach(r => readWindows.remove(windowKey(entity, Some(r))))

  // ---------------------------------------------------------------------------

  /** A fresh JVM (`--resume`, `--pending`) has no in-memory window; the raw
    * SUCCESS ledger row for the run persists exactly the window it read. */
  private def recoverFromLedger(
    spark: SparkSession,
    cfg: HiveSourceConfig,
    store: WatermarkStore,
    entity: String,
    runId: String
  ): Option[(WatermarkValue, WatermarkValue, Long, String)] =
    (cfg.auditDatabase, cfg.auditRunTable) match {
      case (Some(db), Some(t)) if spark.catalog.tableExists(db, t) =>
        val row = spark.table(s"$db.$t")
          .filter(col("entity") === entity && col("run_id") === runId &&
            col("stage") === "raw" && col("status") === "SUCCESS")
          .orderBy(col("event_ts").desc)
          .select("window_start", "window_end")
          .limit(1).collect().headOption
        val bounds = row.flatMap { r =>
          for {
            l <- Option(r.getString(0)).filter(_.nonEmpty)
            u <- Option(r.getString(1)).filter(_.nonEmpty)
          } yield (WatermarkValue.deserialize(l), WatermarkValue.deserialize(u))
        }
        bounds match {
          case None =>
            logger.warn(s"[HiveSource] entity=$entity run=$runId: no in-memory read window and no raw " +
              s"SUCCESS row with a window in $db.$t; nothing to commit")
            None
          case Some((lower, upper)) =>
            val current = store.latestVersioned(entity)
            if (current.exists(c => PartitionPredicate.compare(c.value.values, upper.values) >= 0)) {
              logger.info(s"[HiveSource] entity=$entity: store already at or beyond ${upper.serialized} " +
                s"(${current.get.value.serialized}); ledger window for run $runId not re-committed")
              None
            } else {
              logger.info(s"[HiveSource] entity=$entity run=$runId: recovered window " +
                s"(${lower.serialized}, ${upper.serialized}] from the run ledger $db.$t")
              Some((lower, upper, current.map(_.version).getOrElse(0L),
                partitionColumns(spark, cfg).map(_._1).mkString("|")))
            }
        }
      case (Some(db), Some(t)) =>
        logger.warn(s"[HiveSource] entity=$entity run=$runId: no in-memory read window and the audit " +
          s"table $db.$t does not exist; nothing to commit")
        None
      case _ =>
        logger.warn(s"[HiveSource] entity=$entity run=$runId: no in-memory read window and no " +
          "audit_database/audit_run_table to recover one from; nothing to commit")
        None
    }

  private def partitionColumns(spark: SparkSession, cfg: HiveSourceConfig): Seq[(String, String)] = {
    if (!spark.catalog.tableExists(cfg.database, cfg.table))
      throw new IllegalArgumentException(
        s"HIVE_002 source table ${cfg.fullTable} does not exist (or is not visible to this session)")
    spark.catalog.listColumns(cfg.database, cfg.table).collect()
      .filter(_.isPartition).map(c => (c.name, c.dataType)).toSeq
  }

  private def checkWatermarkColumns(wm: HiveWatermarkConfig, partCols: Seq[(String, String)]): Unit =
    wm.columns.foreach { c =>
      partCols.find(_._1.equalsIgnoreCase(c)) match {
        case None =>
          throw new IllegalArgumentException(
            s"HIVE_003 watermark column '$c' is not a partition column " +
              s"(partition columns: ${partCols.map(_._1).mkString(", ")})")
        case Some((_, dt)) if !dt.equalsIgnoreCase("string") =>
          throw new IllegalArgumentException(
            s"HIVE_003 watermark column '$c' is a $dt partition column; watermark columns must be " +
              "STRING so the lexicographic comparison is well defined")
        case _ => ()
      }
    }

  private def checkWhere(where: String, partCols: Seq[(String, String)]): Unit = {
    val names =
      try PartitionPredicate.attributeNames(where)
      catch { case NonFatal(e) =>
        throw new IllegalArgumentException(s"HIVE_004 source.where '$where' could not be parsed: ${e.getMessage}", e) }
    val bad = names.filterNot(n => partCols.exists(_._1.equalsIgnoreCase(n)))
    if (bad.nonEmpty)
      throw new IllegalArgumentException(
        s"HIVE_004 source.where may reference partition columns only; [${bad.mkString(", ")}] are not " +
          "partition columns of this table. Filtering data columns after the read would let the " +
          "committed watermark skip real data")
  }

  private def makeStore(spark: SparkSession, wm: HiveWatermarkConfig): WatermarkStore = wm.storeType match {
    case "memory" => InMemoryWatermarkStore
    case _ => new HiveWatermarkStore(spark, wm.storeDatabase.get, wm.storeTable)
  }

  private def rewind(
    spark: SparkSession,
    ident: TableIdentifier,
    partCols: Seq[(String, String)],
    wm: HiveWatermarkConfig,
    lower: WatermarkValue,
    where: Option[String]
  ): WatermarkValue = wm.lookback match {
    case None => lower
    case Some(Lookback(Some(days), _)) =>
      try WatermarkValue(PartitionPredicate.rewindDays(lower.values, days, wm.formats.headOption))
      catch { case e: java.time.format.DateTimeParseException =>
        throw new IllegalArgumentException(
          s"CFG_027 lookback.days requires the first watermark column to be a date " +
            s"(${wm.formats.headOption.getOrElse("ISO yyyy-MM-dd")}); watermark component " +
            s"'${lower.values.head}' does not parse", e) }
    case Some(Lookback(_, Some(n))) =>
      val pred = PartitionPredicate(wm.columns, None, Some(lower.values), where)
      val tuples = enumerate(spark, ident, partCols, pred)
        .map(spec => wm.columns.map(c => specValue(spec, c)))
        .sorted(PartitionPredicate.tupleOrdering.reverse)
      if (tuples.size > n) WatermarkValue(tuples(n)) else wm.initialValue
    case Some(_) => lower
  }

  /** Partition specs matching `pred`: catalog-side pruning first, a full
    * enumeration with a driver-side filter if the catalog cannot push the
    * predicate. */
  private def enumerate(
    spark: SparkSession,
    ident: TableIdentifier,
    partCols: Seq[(String, String)],
    pred: PartitionPredicate
  ): Seq[Map[String, String]] = {
    val catalog = spark.sessionState.catalog
    try {
      val meta = catalog.getTableMetadata(ident)
      val attrs = DataTypeUtils.toAttributes(meta.partitionSchema)
      val e = pred.expression(spark.sessionState.sqlParser, attrs)
      listByFilterOverride.map(f => f(ident, Seq(e)))
        .getOrElse(catalog.listPartitionsByFilter(ident, Seq(e)))
        .map(_.spec)
    } catch {
      case NonFatal(e) =>
        logger.warn(s"[HiveSource] listPartitionsByFilter failed for $ident " +
          s"(${e.getClass.getSimpleName}: ${e.getMessage}); falling back to a full partition " +
          "enumeration with a driver-side filter — on a large table check " +
          "hive.metastore.limit.partition.request")
        val all = catalog.listPartitions(ident, None).map(_.spec)
        if (all.isEmpty) Seq.empty
        else {
          val names = partCols.map(_._1)
          val schema = StructType(names.map(n => StructField(n, StringType)))
          val rows = all.map(spec => Row.fromSeq(names.map(n => specValue(spec, n))))
          spark.createDataFrame(spark.sparkContext.parallelize(rows, 1), schema)
            .filter(pred.column).collect()
            .map(r => names.zip(r.toSeq.map(v => if (v == null) null else v.toString)).toMap).toSeq
        }
    }
  }

  private def specValue(spec: Map[String, String], column: String): String =
    spec.getOrElse(column, spec.collectFirst { case (k, v) if k.equalsIgnoreCase(column) => v }.orNull)

  private def specText(spec: Map[String, String]): String =
    spec.map { case (k, v) => s"$k=$v" }.mkString("/")

  private def guardFormats(wm: HiveWatermarkConfig, selected: Seq[Map[String, String]]): Unit = {
    val perColumn = wm.columns.map(c => selected.map(s => specValue(s, c)).distinct)
    PartitionPredicate.formatGuard(wm.columns, perColumn, wm.formats)
      .foreach(message => throw new IllegalArgumentException(message))
  }

  private def applyMissingFiles(spark: SparkSession, cfg: HiveSourceConfig): Unit = {
    val ignore = cfg.missingFiles == "IGNORE"
    spark.conf.set("spark.sql.files.ignoreMissingFiles", ignore.toString)
    val message = s"[HiveSource] spark.sql.files.ignoreMissingFiles=$ignore set SESSION-wide from " +
      s"source.missing_files = ${cfg.missingFiles}; it applies to every read in this Spark session"
    // FAIL is Spark's own default — only the deviation deserves a warning.
    if (ignore) logger.warn(message) else logger.info(message)
  }

  private def logZeroRowPartitions(df: DataFrame, partNames: Seq[String], selected: Seq[Map[String, String]]): Unit = {
    val counts: Map[List[String], Long] = df.groupBy(partNames.map(col): _*).count().collect()
      .map(r => partNames.indices.map(i => String.valueOf(r.get(i))).toList -> r.getLong(partNames.size)).toMap
    selected.foreach { spec =>
      val key = partNames.map(n => specValue(spec, n)).toList
      if (counts.getOrElse(key, 0L) == 0L)
        logger.warn(s"[HiveSource] selected partition ${specText(spec)} returned ZERO rows — registered " +
          "in the catalog but no readable files; the watermark still advances past it")
    }
  }

  private def warnOnFingerprintChange(
    spark: SparkSession,
    store: WatermarkStore,
    wm: HiveWatermarkConfig,
    entity: String,
    fingerprint: String
  ): Unit = {
    val previous: Option[String] = store match {
      case InMemoryWatermarkStore => InMemoryWatermarkStore.latestDetail(entity).flatMap(_.queryHash)
      case _: HiveWatermarkStore =>
        val db = wm.storeDatabase.get
        if (!spark.catalog.tableExists(db, wm.storeTable)) None
        else spark.table(s"$db.${wm.storeTable}")
          .filter(col("entity") === entity)
          .orderBy(col("watermark_version").desc, col("updated_ts").desc)
          .select("query_hash").limit(1).collect().headOption.flatMap(r => Option(r.getString(0)))
      case _ => None
    }
    previous.filter(_.startsWith(FingerprintPrefix)).map(_.stripPrefix(FingerprintPrefix)).foreach { p =>
      if (p != fingerprint)
        logger.warn(s"[HiveSource] entity=$entity: the source table's partition key set changed since " +
          s"the last commit — was [$p], now [$fingerprint]. Selection still prunes on the watermark " +
          "columns; every value of a NEW partition column is read. Confirm this is intended")
    }
  }

  /** Schema drift handling via the shared contract system, the same
    * sequence JdbcSource.applyContract runs: canonical name/alias
    * resolution, policy-driven missing/extra handling, optional defaults. */
  private def applyContract(df: DataFrame, sourceConf: Config): DataFrame =
    SchemaContract.parse(sourceConf) match {
      case None => df
      case Some(contract) =>
        val resolution = SchemaValidator.validateHeaders(df.columns.toSeq, contract, logger)
        logger.info(s"[HiveSource] Schema drift check: columns=[${df.columns.mkString(",")}] " +
          s"missingRequired=[${resolution.missingRequired.mkString(",")}]")
        SchemaValidator.enforce(resolution.violations, contract.policies, logger, Some(resolution))
        val renamed = resolution.renames.foldLeft(df) {
          case (acc, (from, to)) => acc.withColumnRenamed(from, to)
        }
        contract.optionalColumns
          .filterNot(c => renamed.columns.exists(_.equalsIgnoreCase(c.name)))
          .foldLeft(renamed) { (acc, c) =>
            logger.info(s"[HiveSource] Adding missing optional column '${c.name}' with default=${c.default.getOrElse("null")}")
            acc.withColumn(c.name, lit(c.default.orNull).cast(c.dataType))
          }
    }
}
