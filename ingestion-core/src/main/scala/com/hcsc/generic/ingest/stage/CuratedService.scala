package com.hcsc.generic.ingest.stage

import com.hcsc.generic.ingest.config.ConfigUtils
import com.hcsc.generic.ingest.publish.{PublishRequest, PublishService}
import com.hcsc.generic.ingest.reject.RejectService
import com.hcsc.generic.ingest.runtime.RunContext
import com.hcsc.generic.ingest.schema.{CuratedContractValidator, SchemaContract, SchemaContractViolationException}
import com.hcsc.generic.ingest.transform.CuratedTransform
import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lit, row_number}


final case class CuratedResult(
  publishedCount: Long,
  insertCount: Long,
  updateCount: Long,
  deleteCount: Long,
  ignoredCount: Long = 0L,
  nullKeyCount: Long = 0L,
  dedupedCount: Long = 0L,
  passthroughCount: Long = 0L
)

/** Source-freshness contract for the incremental merge: the column whose
  * highest value wins per business key, plus deterministic tie-breakers
  * (all compared descending, nulls last; remaining ties keep the target). */
final case class FreshnessSpec(column: String, tieBreakers: Seq[String])

final class CuratedService(spark: SparkSession, conf: Config) {
  private val logger = Logger.getLogger(getClass.getName)
  private val transform = new CuratedTransform(spark)
  private val publisher = new PublishService(spark, logger)

  val enabled: Boolean =
    ConfigUtils.optBoolean(conf, "enabled").getOrElse(true)

  def process(rawDf: DataFrame, runMode: String, ctx: RunContext, contract: Option[SchemaContract]): Option[CuratedResult] =
    process(rawDf, runMode, ctx, contract, None)

  def process(
    rawDf: DataFrame,
    runMode: String,
    ctx: RunContext,
    contract: Option[SchemaContract],
    rejects: Option[RejectService]
  ): Option[CuratedResult] = {
    if (!enabled) return None

    val database = ConfigUtils.sqlIdentifier(conf, "database")
    val table = ConfigUtils.sqlIdentifier(conf, "table")
    val fullTable = s"$database.$table"
    val path = ConfigUtils.optString(conf, "path")
    val format = ConfigUtils.optString(conf, "format").getOrElse("orc")

    // Drift guard: configured column_types casts must not silently null out
    // values (Spark cast never fails); checked BEFORE the cast is applied.
    guardCasts(rawDf,
      ConfigUtils.stringMap(conf, "column_types").toSeq.flatMap { case (name, ddl) =>
        rawDf.columns.find(_.equalsIgnoreCase(name))
          .map(actual => (actual, org.apache.spark.sql.types.DataType.fromDDL(ddl)))
      },
      where = "curated.column_types")

    val prepared0 = transform.castConfigured(rawDf, conf)
    val prepared1 = transform.applyTransforms(prepared0, conf)
    val prepared2 = transform.ensureAudit(prepared1)
    val prepared = transform.normalizeKeys(prepared2, conf)

    val mergeConf = ConfigUtils.optConfig(conf, "merge")
    val keys = mergeConf.map(m => ConfigUtils.stringList(m, "keys")).getOrElse(Seq.empty)
    val freshness = parseFreshness(mergeConf)
    val ordering = effectiveOrdering(freshness)

    // Second validation gate (HDR_018): the CURATED contract is validated
    // independently of RAW, immediately before publication.
    contract.foreach { c =>
      val violations = CuratedContractValidator.validate(prepared, c, keys, ordering, logger)
      if (violations.nonEmpty)
        throw new SchemaContractViolationException(violations)
    }

    val isIncremental =
      runMode.equalsIgnoreCase("INCR") && spark.catalog.tableExists(fullTable)

    val publishConf = ConfigUtils.optConfig(conf, "publish")
    val enforceUnique = publishConf
      .flatMap(p => ConfigUtils.optBoolean(p, "enforce_unique_keys"))
      .getOrElse(keys.nonEmpty)
    val request = PublishRequest(
      database = database,
      table = table,
      format = format,
      path = path,
      allowEmpty = publishConf.flatMap(p => ConfigUtils.optBoolean(p, "allow_empty")).getOrElse(false),
      validationQuery = publishConf.flatMap(p => ConfigUtils.optString(p, "validation_query")),
      enforceUniqueKeys = if (enforceUnique) keys else Seq.empty,
      // Incremental merges preserve all untouched target rows, so a large
      // shrink can only mean a defect; FULL replaces are opt-in.
      maxShrinkPercent = publishConf.flatMap(p => ConfigUtils.optDouble(p, "max_shrink_percent"))
        .orElse(if (isIncremental) Some(CuratedService.DefaultIncrementalMaxShrinkPercent) else None)
    )

    // Key hygiene applies in EVERY mode with configured keys — FULL loads
    // and the first run of an incremental feed must not seed curated with
    // duplicate or null keys. Null-key rows are ALWAYS separated first:
    // quarantined when drop_null_keys=true, or passed through UNMERGED when
    // false — they must never enter the dedup/merge windows, where
    // PARTITION BY treats NULL = NULL as a match and would collapse
    // distinct keyless records into one arbitrary survivor.
    val persistedFrames = scala.collection.mutable.ArrayBuffer.empty[DataFrame]
    try {
      val (hygienic, nullKeyCount, passthrough, passthroughCount, inputCount) =
        if (keys.isEmpty) (prepared, 0L, None: Option[DataFrame], 0L, -1L)
        else {
          val missingKeys = keys.filterNot(k => prepared.columns.exists(_.equalsIgnoreCase(k)))
          require(missingKeys.isEmpty,
            s"curated.merge.keys missing from incoming data: ${missingKeys.mkString(",")}")
          val dropNull = mergeConf.flatMap(m => ConfigUtils.optBoolean(m, "null_handling.drop_null_keys")).getOrElse(true)
          val blanksAsNull = mergeConf.flatMap(m => ConfigUtils.optBoolean(m, "null_handling.treat_blank_as_null")).getOrElse(true)
          val (valid, nullKeyed) = transform.splitNullKeys(prepared, keys, drop = true, blanksAsNull)
          val (nullCount, pass, passCount) =
            if (dropNull) (quarantineNullKeys(nullKeyed, ctx, rejects), None, 0L)
            else {
              val p = nullKeyed.persist()
              persistedFrames += p
              val c = p.count()
              if (c > 0)
                logger.info(s"[CuratedService] $c null-key row(s) pass through unmerged " +
                  "(drop_null_keys=false); they are appended, never deduplicated")
              (0L, if (c == 0) None else Some(p), c)
            }
          val persisted = valid.persist()
          persistedFrames += persisted
          val count = persisted.count()
          (transform.deduplicate(persisted, keys, ordering), nullCount, pass, passCount, count)
        }

      val result =
        if (!isIncremental)
          publishFull(hygienic, passthrough, nullKeyCount, passthroughCount, inputCount,
            fullTable, request, ctx, contract)
        else
          publishIncremental(hygienic, passthrough, nullKeyCount, passthroughCount, inputCount,
            keys, freshness, fullTable, request, ctx, contract)

      logger.info(
        s"[CuratedService] published=${result.publishedCount} inserts=${result.insertCount} " +
          s"updates=${result.updateCount} deletes=${result.deleteCount} " +
          s"ignoredStale=${result.ignoredCount} nullKeys=${result.nullKeyCount} " +
          s"deduped=${result.dedupedCount} passthrough=${result.passthroughCount}"
      )
      Some(result)
    } finally {
      persistedFrames.foreach(df => try df.unpersist(false) catch { case _: Exception => () })
    }
  }

  /** merge.freshness { column, tie_breakers } — the source-provided
    * last-modified column that decides merge winners. Framework audit
    * columns are rejected: they are stamped at ingestion time and would
    * make run order, not source freshness, decide the winner. */
  private def parseFreshness(mergeConf: Option[Config]): Option[FreshnessSpec] =
    mergeConf.flatMap(m => ConfigUtils.optConfig(m, "freshness")).map { f =>
      val column = f.getString("column")
      require(!CuratedService.FrameworkAuditColumns.contains(column.toLowerCase),
        s"curated.merge.freshness.column '$column' collides with a framework audit column; " +
          "designate the source-provided freshness column instead")
      FreshnessSpec(column, ConfigUtils.stringList(f, "tie_breakers"))
    }

  /** Dedup ordering: freshness spec first (it also decides the cross-run
    * merge), then any additionally configured dedup.order_by columns. */
  private def effectiveOrdering(freshness: Option[FreshnessSpec]): Seq[String] = {
    val configured = ConfigUtils.stringList(conf, "dedup.order_by")
    freshness match {
      case Some(f) =>
        val primary = f.column +: f.tieBreakers
        primary ++ configured.filterNot(o => primary.exists(_.equalsIgnoreCase(o)))
      case None => configured
    }
  }

  /** The (actual column, target type) pairs align() will cast where the
    * incoming type differs — same-type casts can never null a value and are
    * excluded to keep the guard's aggregate narrow. */
  private def alignmentCasts(
    df: DataFrame,
    targetSchema: org.apache.spark.sql.types.StructType
  ): Seq[(String, org.apache.spark.sql.types.DataType)] =
    targetSchema.fields.toSeq.flatMap { field =>
      df.schema.fields.find(_.name.equalsIgnoreCase(field.name))
        .filter(_.dataType != field.dataType)
        .map(actual => (actual.name, field.dataType))
    }

  /** curated.on_cast_error = FAIL (default) | WARN. A cast that nulls real
    * values is silent data corruption — at 350 columns a single type drift
    * can null a column table-wide with no error. */
  private def guardCasts(
    df: DataFrame,
    casts: Seq[(String, org.apache.spark.sql.types.DataType)],
    where: String
  ): Unit = {
    if (casts.isEmpty) return
    val losses = transform.detectCastLoss(df, casts)
    if (losses.nonEmpty) {
      val detail = losses.map { case (c, n) => s"$c ($n value(s))" }.mkString(", ")
      val policy = ConfigUtils.optString(conf, "on_cast_error").getOrElse("FAIL").toUpperCase
      if (policy == "WARN")
        logger.warn(s"[CuratedService] CUR_002 cast would NULL non-null values in $where: $detail " +
          "(on_cast_error=WARN — values are being lost)")
      else
        throw new IllegalStateException(
          s"CUR_002 Cast would silently turn non-null values into NULL in $where: $detail. " +
            "Fix the declared type or the source data, or set curated.on_cast_error = WARN " +
            "to accept the loss.")
    }
  }

  /** Incoming columns absent from the target schema would be silently
    * discarded by align(); surface them through the contract's extra-column
    * policy (default WARN) so schema evolution is never invisible. */
  private def guardDroppedColumns(
    df: DataFrame,
    targetSchema: org.apache.spark.sql.types.StructType,
    contract: Option[SchemaContract],
    fullTable: String
  ): Unit = {
    val targetCols = targetSchema.fields.map(_.name.toLowerCase).toSet
    val dropped = df.columns.filterNot(c =>
      targetCols.contains(c.toLowerCase) ||
        com.hcsc.generic.ingest.transform.RawMetadata.ColumnNames.contains(c.toLowerCase) ||
        c.startsWith("__"))
    if (dropped.nonEmpty) {
      val violations = dropped.toSeq.map(c =>
        com.hcsc.generic.ingest.schema.SchemaViolation(
          com.hcsc.generic.ingest.schema.ViolationKind.ExtraColumn,
          s"Incoming column '$c' is not in curated target $fullTable and will be dropped by alignment"))
      contract match {
        case Some(c) => com.hcsc.generic.ingest.schema.SchemaValidator
          .enforce(violations, c.policies, logger)
        case None => violations.foreach(v => logger.warn(s"[CuratedService] $v"))
      }
    }
  }

  private def quarantineNullKeys(dropped: DataFrame, ctx: RunContext, rejects: Option[RejectService]): Long =
    rejects match {
      case Some(rs) =>
        val count = rs.persistRows(dropped, ctx, CuratedService.NullKeyErrorCode,
          "One or more business-key columns are null or blank", "NULL_BUSINESS_KEY")
        if (count > 0)
          logger.warn(s"[CuratedService] $count row(s) with null/blank business keys quarantined " +
            s"(${CuratedService.NullKeyErrorCode})")
        count
      case None =>
        val count = dropped.count()
        if (count > 0)
          logger.warn(s"[CuratedService] $count row(s) with null/blank business keys DROPPED " +
            "(no reject service wired; configure rejects{} and run via the pipeline to quarantine them)")
        count
    }

  private def publishFull(
    df: DataFrame,
    passthrough: Option[DataFrame],
    nullKeyCount: Long,
    passthroughCount: Long,
    inputCount: Long,
    fullTable: String,
    request: PublishRequest,
    ctx: RunContext,
    contract: Option[SchemaContract]
  ): CuratedResult = {
    val withPassthrough = passthrough.fold(df)(p => df.unionByName(p))
    val publishDf =
      if (spark.catalog.tableExists(fullTable)) {
        val schema = spark.table(fullTable).schema
        guardDroppedColumns(withPassthrough, schema, contract, fullTable)
        guardCasts(withPassthrough, alignmentCasts(withPassthrough, schema), s"alignment to $fullTable")
        transform.align(withPassthrough, schema, contract)
      } else withPassthrough

    val published = publisher.publish(publishDf, request, ctx)
    val keyedPublished = published.publishedCount - passthroughCount
    val dedupedCount = if (inputCount >= 0) math.max(inputCount - keyedPublished, 0L) else 0L
    CuratedResult(published.publishedCount, insertCount = keyedPublished,
      updateCount = 0L, deleteCount = 0L,
      ignoredCount = 0L, nullKeyCount = nullKeyCount, dedupedCount = dedupedCount,
      passthroughCount = passthroughCount)
  }

  /**
    * Preserves update-vs-insert audit semantics on the merge path: incoming
    * rows whose key exists in the target inherit the target's
    * create_timestamp and are marked last_modified_op = 'U'
    * (last_modified_ts stays this run's timestamp from ensureAudit). Feeds
    * without the audit columns pass through unchanged.
    */
  private[stage] def applyUpdateAudit(incoming: DataFrame, target: DataFrame, keys: Seq[String]): DataFrame = {
    import org.apache.spark.sql.functions.{coalesce, when}
    def actual(df: DataFrame, name: String): Option[String] =
      df.columns.find(_.equalsIgnoreCase(name))

    (actual(incoming, "create_timestamp"), actual(target, "create_timestamp"),
      actual(incoming, "last_modified_op")) match {
      case (Some(incCreate), Some(tgtCreate), Some(incOp)) =>
        val original = target
          .select((keys.map(col) :+ col(tgtCreate).cast("timestamp").as("_orig_create_ts")): _*)
          .dropDuplicates(keys)
        incoming.join(original, keys, "left")
          .withColumn(incCreate, coalesce(col("_orig_create_ts"), col(incCreate)))
          .withColumn(incOp, when(col("_orig_create_ts").isNotNull, lit("U")).otherwise(col(incOp)))
          .drop("_orig_create_ts")
      case _ => incoming
    }
  }

  /**
    * Freshness-compared merge: for every business key contested by this
    * batch, the row with the highest freshness value (then tie-breakers)
    * wins — whether it comes from the incoming batch or the existing
    * target. A late-arriving older record therefore never overwrites a
    * newer curated row; remaining exact ties keep the target row. Without a
    * configured (and target-present) freshness column the merge falls back
    * to the legacy last-write-wins replacement, loudly.
    */
  private def publishIncremental(
    incoming: DataFrame,
    passthrough: Option[DataFrame],
    nullKeyCount: Long,
    passthroughCount: Long,
    inputCount: Long,
    keys: Seq[String],
    freshness: Option[FreshnessSpec],
    fullTable: String,
    request: PublishRequest,
    ctx: RunContext,
    contract: Option[SchemaContract]
  ): CuratedResult = {
    require(keys.nonEmpty, "INCR mode requires curated.merge.keys")
    val target = spark.table(fullTable)

    val missingKeys = keys.filterNot(key => target.columns.exists(_.equalsIgnoreCase(key)))
    require(missingKeys.isEmpty, s"Merge keys missing from target: ${missingKeys.mkString(",")}")

    guardDroppedColumns(incoming, target.schema, contract, fullTable)
    guardCasts(incoming, alignmentCasts(incoming, target.schema), s"alignment to $fullTable")
    val aligned0 = transform.align(incoming, target.schema, contract)
    // Update audit semantics: rows replacing an existing key keep the
    // ORIGINAL create_timestamp and are stamped operation 'U'; only truly
    // new keys carry 'I' and their fresh creation time.
    val alignedIncoming = applyUpdateAudit(aligned0, target, keys).persist()

    // Distinct incoming keys under prefixed names: prefixing breaks the
    // shared-lineage ambiguity (alignedIncoming embeds target attributes
    // via applyUpdateAudit). Incoming rows are guaranteed non-null-keyed
    // here (null-key rows were quarantined or diverted to the passthrough
    // upstream), so null-keyed TARGET rows never match the join and are
    // retained verbatim in `unchanged` — keyless history is append-only,
    // never merged or collapsed.
    val keyCols = keys.map(k => target.columns.find(_.equalsIgnoreCase(k)).getOrElse(k))
    val ik = alignedIncoming.select(keyCols.map(col): _*).distinct()
      .toDF(keyCols.map(k => s"__ik_$k"): _*).persist()
    var winnersPersisted: Option[DataFrame] = None
    try {
      val totalIncoming = ik.count()
      val dedupedCount = if (inputCount >= 0) math.max(inputCount - totalIncoming, 0L) else 0L
      val alignedPassthrough = passthrough.map(p => transform.align(p, target.schema, contract))

      if (totalIncoming == 0 && alignedPassthrough.isEmpty) {
        logger.info(s"[CuratedService] Zero incoming rows after key hygiene; $fullTable left untouched")
        return CuratedResult(0L, 0L, 0L, 0L,
          ignoredCount = 0L, nullKeyCount = nullKeyCount, dedupedCount = dedupedCount)
      }

      val joinCond = keyCols.map(k => col(k) <=> col(s"__ik_$k")).reduce(_ && _)
      val contested = target.join(ik, joinCond, "left_semi")
      val unchanged = target.join(ik, joinCond, "left_anti")
      val contestedKeyCount = contested.select(keyCols.map(col): _*).distinct().count()
      val insertCount = totalIncoming - contestedKeyCount

      val freshnessUsable = freshness.filter { f =>
        val present = target.columns.exists(_.equalsIgnoreCase(f.column))
        if (!present)
          logger.warn(s"[CuratedService] merge.freshness.column '${f.column}' is not a column of " +
            s"$fullTable; falling back to last-write-wins replacement until the target carries it")
        present
      }

      val (replacement, updateCount, ignoredCount) = freshnessUsable match {
        case Some(f) =>
          val src = CuratedService.MergeProvenanceColumn
          require(!target.columns.exists(_.equalsIgnoreCase(src)),
            s"Target $fullTable contains reserved column '$src'")
          val union = contested.withColumn(src, lit("T"))
            .unionByName(alignedIncoming.withColumn(src, lit("I")))
          val orderCols = (f.column +: f.tieBreakers)
            .flatMap(o => union.columns.find(_.equalsIgnoreCase(o)))
            .map(c => col(c).desc_nulls_last) :+ col(src).desc // 'T' > 'I': exact ties keep the target
          val w = Window.partitionBy(keyCols.map(col): _*).orderBy(orderCols: _*)
          val winners = union.withColumn("_rn", row_number().over(w))
            .filter(col("_rn") === 1).drop("_rn").persist()
          winnersPersisted = Some(winners)
          val incomingWinners = winners.filter(col(src) === "I").count()
          val updates = incomingWinners - insertCount
          val ignored = totalIncoming - incomingWinners
          (winners.drop(src), updates, ignored)
        case None =>
          if (freshness.isEmpty)
            logger.warn("[CuratedService] No curated.merge.freshness configured: the incremental " +
              "merge is last-write-wins by run order — a late-arriving OLDER record will overwrite " +
              "a newer curated row. Configure merge.freshness.column to enable freshness comparison.")
          (alignedIncoming, contestedKeyCount, 0L)
      }

      val merged0 = unchanged.unionByName(replacement)
      val merged = alignedPassthrough.fold(merged0)(p => merged0.unionByName(p))
      val published = publisher.publish(merged, request, ctx)
      CuratedResult(published.publishedCount, insertCount, updateCount, deleteCount = 0L,
        ignoredCount = ignoredCount, nullKeyCount = nullKeyCount, dedupedCount = dedupedCount,
        passthroughCount = passthroughCount)
    } finally {
      winnersPersisted.foreach(df => try df.unpersist(false) catch { case _: Exception => () })
      ik.unpersist(false)
      alignedIncoming.unpersist(false)
    }
  }
}

object CuratedService {
  /** Reject error code for rows dropped for null/blank business keys. */
  val NullKeyErrorCode = "CUR_001"

  /** Provenance marker used internally by the freshness merge. */
  val MergeProvenanceColumn = "__merge_src"

  /** Default publish shrink guard for incremental merges (percent). */
  val DefaultIncrementalMaxShrinkPercent = 20.0

  /** Columns stamped by ensureAudit; not usable as a freshness column. */
  val FrameworkAuditColumns: Set[String] =
    Set("create_timestamp", "last_modified_ts", "last_modified_op")
}
