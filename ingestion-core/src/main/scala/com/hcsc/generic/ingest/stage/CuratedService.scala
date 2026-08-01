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
import org.apache.spark.sql.functions.{coalesce, col, lit, lower, row_number, trim, when}


final case class CuratedResult(
  publishedCount: Long,
  insertCount: Long,
  updateCount: Long,
  deleteCount: Long,
  ignoredCount: Long = 0L,
  nullKeyCount: Long = 0L,
  dedupedCount: Long = 0L,
  passthroughCount: Long = 0L,
  /** Target keys newly tombstoned because they were ABSENT from a complete
    * incoming snapshot (FULL_SNAPSHOT_ABSENCE). Counted separately: these
    * rows come from the TARGET, not the accepted batch, so they are outside
    * the curated_accounts_for_accepted_rows identity; the ledger's
    * delete_count reports deleteCount + absenceDeleteCount. */
  absenceDeleteCount: Long = 0L
)

/** Source-freshness contract for the incremental merge: the column whose
  * highest value wins per business key (always descending, nulls last),
  * plus deterministic tie-breakers — each optionally carrying direction and
  * null ordering ("col asc nulls_first"), defaulting to descending nulls
  * last. Remaining exact ties keep the target. */
final case class FreshnessSpec(column: String, tieBreakers: Seq[String])

/** Explicit delete strategy (spec §8: delete handling cannot be inferred). */
sealed trait DeleteSpec
object DeleteSpec {
  /** Source deletions are intentionally not reflected in curated. */
  case object Ignore extends DeleteSpec
  /** Rows whose indicator matches are tombstones: merged like any record
    * (freshness decides), stamped last_modified_op='D' and is_deleted=true
    * when the curated schema carries those columns. */
  final case class Soft(indicatorColumn: String, indicatorValues: Seq[String]) extends DeleteSpec
  /** FULL snapshot publishes tombstone target keys ABSENT from the incoming
    * complete snapshot. Gated on confirm_complete_extract and statically
    * incompatible with source-side filtering (CFG_014) — absence over a
    * partial extract would tombstone every excluded row. Reactivation is
    * natural: a key present again simply publishes as a live row. */
  case object SnapshotAbsence extends DeleteSpec
}

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

    // Explicit "none": the staged INSERT OVERWRITE publish never enables
    // dynamic partitioning, so a partitioned curated target would break the
    // swap. Reject rather than silently ignoring the intent.
    require(!conf.hasPath("partitioning"),
      "curated.partitioning is not supported: the staged publish replaces the whole table " +
        "and never enables dynamic partitioning. Remove the block (RAW partitioning lives " +
        "under raw.partitioning); curated partitioning arrives with the Delta/Iceberg publish.")

    // Contract-declared per-column transforms run FIRST (trim/case/date
    // rules from the mapping spec), then the curated-block config transforms.
    val transformed = transform.applyContractTransforms(rawDf, contract)

    // Drift guard: configured column_types casts must not silently null out
    // values (Spark cast never fails); checked on the SAME frame the cast
    // runs against — after contract transforms, immediately before the cast.
    guardCasts(transformed,
      ConfigUtils.stringMap(conf, "column_types").toSeq.flatMap { case (name, ddl) =>
        transformed.columns.find(_.equalsIgnoreCase(name))
          .map(actual => (actual, org.apache.spark.sql.types.DataType.fromDDL(ddl)))
      },
      where = "curated.column_types")

    val prepared0 = transform.castConfigured(transformed, conf)
    val prepared1 = transform.applyTransforms(prepared0, conf)
    val prepared2 = transform.ensureAudit(prepared1)
    val prepared = transform.normalizeKeys(prepared2, conf)

    val mergeConf = ConfigUtils.optConfig(conf, "merge")
    val keys = resolveMergeKeys(mergeConf, contract)
    val freshness = parseFreshness(mergeConf)
    val ordering = effectiveOrdering(freshness)
    val deletes = parseDeletes(mergeConf)

    // merge.require_ordering = true: refuse the nondeterministic
    // dropDuplicates fallback outright — a keyed feed must declare
    // freshness or dedup.order_by. Default false (legacy fallback + warn).
    if (keys.nonEmpty && ordering.isEmpty &&
        mergeConf.flatMap(m => ConfigUtils.optBoolean(m, "require_ordering")).getOrElse(false))
      throw new IllegalStateException(
        "CUR_004 merge.require_ordering = true but no ordering is configured: declare " +
          "curated.merge.freshness.column (preferred) or curated.dedup.order_by so the " +
          "surviving row per key is deterministic")

    // Second validation gate (HDR_018): the CURATED contract is validated
    // independently of RAW, immediately before publication. Ordering
    // entries may carry direction tokens ("col desc nulls_first") — the
    // validator sees column names only.
    contract.foreach { c =>
      val violations = CuratedContractValidator.validate(
        prepared, c, keys,
        ordering.map(o => com.hcsc.generic.ingest.transform.OrderSpec.parse(o).column), logger)
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
          // merge.null_handling.policy = QUARANTINE | ALLOW | FAIL_RUN wins
          // over the legacy drop_null_keys boolean when declared:
          // QUARANTINE = drop_null_keys=true, ALLOW = false, FAIL_RUN fails
          // the run outright on any null/blank business key.
          val nullPolicy = mergeConf
            .flatMap(m => ConfigUtils.optString(m, "null_handling.policy")).map(_.toUpperCase)
          nullPolicy.foreach(p => require(Seq("QUARANTINE", "ALLOW", "FAIL_RUN").contains(p),
            s"curated.merge.null_handling.policy '$p' must be QUARANTINE, ALLOW or FAIL_RUN"))
          val dropNull = nullPolicy match {
            case Some("ALLOW") => false
            case Some(_)       => true
            case None => mergeConf
              .flatMap(m => ConfigUtils.optBoolean(m, "null_handling.drop_null_keys")).getOrElse(true)
          }
          val blanksAsNull = mergeConf.flatMap(m => ConfigUtils.optBoolean(m, "null_handling.treat_blank_as_null")).getOrElse(true)
          val (valid, nullKeyed) = transform.splitNullKeys(prepared, keys, drop = true, blanksAsNull)
          if (nullPolicy.contains("FAIL_RUN")) {
            val invalid = nullKeyed.count()
            if (invalid > 0)
              throw new IllegalStateException(
                s"CUR_001 $invalid row(s) carry null/blank business key(s) and " +
                  "merge.null_handling.policy = FAIL_RUN; fix the source or switch the policy " +
                  "to QUARANTINE to divert them instead")
          }
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

      if (keys.nonEmpty && deletes.isEmpty)
        logger.info("[CuratedService] Delete strategy undeclared for a keyed feed: defaulting to " +
          "IGNORE (source deletions are not reflected in curated). Declare " +
          "curated.merge.deletes { mode = IGNORE | SOFT } to make the decision explicit.")
      if (deletes.contains(DeleteSpec.Ignore))
        logger.info("[CuratedService] deletes.mode = IGNORE declared: source deletions are " +
          "intentionally not reflected in curated (audited decision)")
      if (deletes.contains(DeleteSpec.SnapshotAbsence) && isIncremental)
        logger.warn("[CuratedService] deletes.mode = FULL_SNAPSHOT_ABSENCE applies to FULL " +
          "snapshot publishes only; this incremental run performs no absence deletion")

      val result =
        if (!isIncremental)
          publishFull(hygienic, passthrough, nullKeyCount, passthroughCount, inputCount,
            keys, deletes, fullTable, request, ctx, contract)
        else
          publishIncremental(hygienic, passthrough, nullKeyCount, passthroughCount, inputCount,
            keys, freshness, deletes, fullTable, request, ctx, contract)

      // unchanged/rewrite metrics (§11): ignoredStale counts stale + hash-
      // unchanged skips; rewritten is the real mutation volume of the run.
      logger.info(
        s"[CuratedService] published=${result.publishedCount} inserts=${result.insertCount} " +
          s"updates=${result.updateCount} deletes=${result.deleteCount} " +
          s"absenceDeletes=${result.absenceDeleteCount} " +
          s"ignoredStale=${result.ignoredCount} nullKeys=${result.nullKeyCount} " +
          s"deduped=${result.dedupedCount} passthrough=${result.passthroughCount} " +
          s"rewritten=${result.insertCount + result.updateCount + result.deleteCount}"
      )
      Some(result)
    } finally {
      persistedFrames.foreach(df => try df.unpersist(false) catch { case _: Exception => () })
    }
  }

  /** Merge keys: explicit curated.merge.keys wins; otherwise the contract's
    * business_key columns derive them. A configured set that CONTRADICTS the
    * contract's declaration is an HDR_017 configuration collision — two
    * sources of truth silently disagreeing about the business key is exactly
    * the ambiguity the mapping spec forbids. */
  private def resolveMergeKeys(mergeConf: Option[Config], contract: Option[SchemaContract]): Seq[String] = {
    // An EXPLICIT empty list is an opt-out, not an omission: `merge.keys = []`
    // says "publish this feed keyless even though the contract declares a
    // business_key" (e.g. an append-only FULL snapshot of a keyed source).
    val configured: Option[Seq[String]] =
      mergeConf.filter(_.hasPath("keys")).map(m => ConfigUtils.stringList(m, "keys"))
    val declared = contract.map(_.businessKeyColumns).getOrElse(Seq.empty)
    (configured, declared) match {
      case (Some(Nil), d) =>
        if (d.nonEmpty)
          logger.info(s"[CuratedService] merge.keys = [] explicitly disables merge semantics; " +
            s"contract business_key [${d.mkString(",")}] is NOT applied to this feed")
        Seq.empty
      case (Some(c), d) if d.nonEmpty =>
        if (c.map(_.toLowerCase).toSet != d.map(_.toLowerCase).toSet)
          throw new IllegalArgumentException(
            s"HDR_017 curated.merge.keys [${c.mkString(",")}] contradicts the contract's " +
              s"business_key declaration [${d.mkString(",")}]; align them or remove one")
        c
      case (Some(c), _) => c
      case (None, d) if d.nonEmpty =>
        logger.info(s"[CuratedService] merge keys derived from contract business_key columns: ${d.mkString(",")}")
        d
      case _ => Seq.empty
    }
  }

  /** merge.deletes { mode = IGNORE | SOFT, indicator_column,
    * indicator_values } — the delete strategy is an explicit decision.
    * SOFT treats matching incoming rows as tombstones: they compete through
    * the freshness merge like any record and, when they win, are stamped
    * last_modified_op='D' (and is_deleted=true when the target carries the
    * column). IGNORE is the default and is logged once for keyed feeds. */
  private def parseDeletes(mergeConf: Option[Config]): Option[DeleteSpec] =
    mergeConf.flatMap(m => ConfigUtils.optConfig(m, "deletes")).map { d =>
      ConfigUtils.optString(d, "mode").getOrElse("IGNORE").toUpperCase match {
        case "IGNORE" => DeleteSpec.Ignore
        case "SOFT" =>
          val indicator = ConfigUtils.optString(d, "indicator_column").getOrElse(
            throw new IllegalArgumentException(
              "curated.merge.deletes.mode = SOFT requires indicator_column (the source's soft-delete flag)"))
          DeleteSpec.Soft(indicator,
            ConfigUtils.stringList(d, "indicator_values") match {
              case Nil => Seq("true", "1", "y", "d")
              case vs => vs.map(_.toLowerCase)
            })
        case "FULL_SNAPSHOT_ABSENCE" =>
          require(ConfigUtils.optBoolean(d, "confirm_complete_extract").getOrElse(false),
            "CUR_005 deletes.mode = FULL_SNAPSHOT_ABSENCE requires confirm_complete_extract = true: " +
              "absence-based deletion is only sound over a COMPLETE source extract")
          DeleteSpec.SnapshotAbsence
        case m @ ("CHANGE_TRACKING" | "CDC") =>
          throw new IllegalArgumentException(
            s"CUR_005 deletes.mode = $m is a declared capability that is NOT implemented (no " +
              "Change Tracking / CDC extraction exists); use SOFT with a source indicator, " +
              "FULL_SNAPSHOT_ABSENCE for complete snapshots, or IGNORE")
        case m @ ("RECONCILE" | "PERIODIC_KEY_RECONCILIATION") =>
          throw new IllegalArgumentException(
            s"CUR_005 deletes.mode = $m (periodic source-key reconciliation) is future work; " +
              "it needs a source-key snapshot channel, a reviewable candidate table and a " +
              "governed apply step. Use SOFT, FULL_SNAPSHOT_ABSENCE or IGNORE")
        case other =>
          throw new IllegalArgumentException(
            s"curated.merge.deletes.mode '$other' must be IGNORE, SOFT or FULL_SNAPSHOT_ABSENCE")
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
    * merge), then any additionally configured dedup.order_by columns.
    * Entries may carry direction tokens; duplicate suppression compares the
    * COLUMN, so "seq" configured in both places orders once (the freshness
    * spec's direction wins by position). */
  private def effectiveOrdering(freshness: Option[FreshnessSpec]): Seq[String] = {
    val configured = ConfigUtils.stringList(conf, "dedup.order_by")
    def columnOf(spec: String) = com.hcsc.generic.ingest.transform.OrderSpec.parse(spec).column
    freshness match {
      case Some(f) =>
        val primary = f.column +: f.tieBreakers
        val primaryColumns = primary.map(columnOf)
        primary ++ configured.filterNot(o => primaryColumns.exists(_.equalsIgnoreCase(columnOf(o))))
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

  /** Stamps soft-delete tombstones: last_modified_op='D' plus
    * is_deleted=true when the frame carries the column. The indicator must
    * survive into the curated schema — a source-only flag dropped by
    * alignment could not mark anything. */
  private def applyDeleteMarking(df: DataFrame, deletes: Option[DeleteSpec], fullTable: String): DataFrame =
    deletes match {
      case Some(DeleteSpec.Soft(indicator, _)) =>
        df.columns.find(_.equalsIgnoreCase(indicator)).getOrElse(
          throw new IllegalArgumentException(
            s"curated.merge.deletes.indicator_column '$indicator' is not present in the curated " +
              s"frame for $fullTable; soft deletes require the indicator in the curated schema"))
        val isDelete = softDeletePredicate(df, deletes).get
        val withOp = df.columns.find(_.equalsIgnoreCase("last_modified_op"))
          .fold(df)(op => df.withColumn(op, when(isDelete, lit("D")).otherwise(col(op))))
        df.columns.find(_.equalsIgnoreCase("is_deleted"))
          .fold(withOp)(flag => withOp.withColumn(flag,
            when(isDelete, lit(true)).otherwise(col(flag).cast("boolean"))))
      case _ => df
    }

  /** The row-level "this is a tombstone" predicate, shared by the marking
    * step and the record_hash pre-filter exemption. None when deletes are
    * not SOFT or the indicator is absent from the frame. */
  private def softDeletePredicate(df: DataFrame, deletes: Option[DeleteSpec]): Option[org.apache.spark.sql.Column] =
    deletes match {
      case Some(DeleteSpec.Soft(indicator, values)) =>
        df.columns.find(_.equalsIgnoreCase(indicator))
          .map(actual => lower(trim(col(actual).cast("string"))).isin(values: _*))
      case _ => None
    }

  private def deleteMarkCount(df: DataFrame, deletes: Option[DeleteSpec]): Long =
    deletes match {
      case Some(_: DeleteSpec.Soft) =>
        df.columns.find(_.equalsIgnoreCase("last_modified_op"))
          .map(op => df.filter(col(op) === "D").count()).getOrElse(0L)
      case _ => 0L
    }

  private def publishFull(
    df: DataFrame,
    passthrough: Option[DataFrame],
    nullKeyCount: Long,
    passthroughCount: Long,
    inputCount: Long,
    keys: Seq[String],
    deletes: Option[DeleteSpec],
    fullTable: String,
    request: PublishRequest,
    ctx: RunContext,
    contract: Option[SchemaContract]
  ): CuratedResult = {
    val schemaOpt =
      if (spark.catalog.tableExists(fullTable)) Some(spark.table(fullTable).schema) else None
    schemaOpt.foreach { schema =>
      val withPassthrough = passthrough.fold(df)(p => df.unionByName(p))
      guardDroppedColumns(withPassthrough, schema, contract, fullTable)
      guardCasts(withPassthrough, alignmentCasts(withPassthrough, schema), s"alignment to $fullTable")
    }
    def alignTo(d: DataFrame): DataFrame = schemaOpt.fold(d)(s => transform.align(d, s, contract))
    // Keyed rows and passthrough rows are marked SEPARATELY so deleteCount
    // stays disjoint from passthroughCount: a keyed tombstone lands in
    // deleteCount, a keyless one stays a passthrough row (marked, but
    // counted once). Marking is column-wise, so marking the parts equals
    // marking the union.
    val keyedMarked = applyDeleteMarking(alignTo(df), deletes, fullTable)
    val incomingDf = passthrough
      .map(p => applyDeleteMarking(alignTo(p), deletes, fullTable))
      .fold(keyedMarked)(p => keyedMarked.unionByName(p))

    // FULL_SNAPSHOT_ABSENCE: target keys missing from the complete incoming
    // snapshot become retained tombstones (op='D', is_deleted=true when the
    // column exists). Already-tombstoned rows are retained but only NEWLY
    // absent rows count as this run's deletes. Reactivation needs no code:
    // a key present again publishes as a live incoming row.
    val (absentRows, absenceDeletes) = (deletes, schemaOpt) match {
      case (Some(DeleteSpec.SnapshotAbsence), Some(_)) if keys.nonEmpty =>
        val target = spark.table(fullTable)
        val missingKeys = keys.filterNot(k => target.columns.exists(_.equalsIgnoreCase(k)))
        require(missingKeys.isEmpty,
          s"CUR_005 FULL_SNAPSHOT_ABSENCE requires business keys in $fullTable; " +
            s"missing: ${missingKeys.mkString(", ")}")
        val incomingKeys = incomingDf
          .select(keys.map(k => col(incomingDf.columns.find(_.equalsIgnoreCase(k)).get).as(k)): _*)
          .distinct().alias("k")
        val t = target.alias("t")
        val condition = keys.map { k =>
          col(s"t.${target.columns.find(_.equalsIgnoreCase(k)).get}") <=> col(s"k.$k")
        }.reduce(_ && _)
        val absent = t.join(incomingKeys, condition, "left_anti").persist()
        val newlyDeleted = target.columns.find(_.equalsIgnoreCase("is_deleted")) match {
          case Some(flag) => absent.filter(!coalesce(col(flag).cast("boolean"), lit(false))).count()
          case None       => absent.count()
        }
        val retained = absent.count()
        if (newlyDeleted > 0)
          logger.info(s"[CuratedService] FULL_SNAPSHOT_ABSENCE: $newlyDeleted key(s) absent " +
            s"from the complete snapshot tombstoned (retained rows: $retained)")
        (Some((markAllDeleted(absent), retained)), newlyDeleted)
      case _ => (None, 0L)
    }
    val publishDf = absentRows.map(_._1).fold(incomingDf)(a => incomingDf.unionByName(a))

    val published = try publisher.publish(publishDf, request, ctx)
    finally absentRows.foreach { case (m, _) => try m.unpersist(false) catch { case _: Exception => () } }
    val absentTotal = absentRows.map(_._2).getOrElse(0L)
    val keyedPublished = published.publishedCount - passthroughCount - absentTotal
    val deleteCount = deleteMarkCount(keyedMarked, deletes)
    val dedupedCount = if (inputCount >= 0) math.max(inputCount - keyedPublished, 0L) else 0L
    // Disjoint accounting: a published keyed row is EITHER an insert or a
    // tombstone, never both — insert+update+delete+ignored+nullKey+deduped+
    // passthrough must total the accepted rows. Absence tombstones come
    // from the TARGET and stay outside this identity (absenceDeleteCount).
    CuratedResult(published.publishedCount, insertCount = keyedPublished - deleteCount,
      updateCount = 0L, deleteCount = deleteCount,
      ignoredCount = 0L, nullKeyCount = nullKeyCount, dedupedCount = dedupedCount,
      passthroughCount = passthroughCount, absenceDeleteCount = absenceDeletes)
  }

  /** Unconditional tombstone stamping for absence rows: op='D' plus
    * is_deleted=true when the schema carries the columns. */
  private def markAllDeleted(df: DataFrame): DataFrame = {
    val withOp = df.columns.find(_.equalsIgnoreCase("last_modified_op"))
      .fold(df)(op => df.withColumn(op, lit("D")))
    df.columns.find(_.equalsIgnoreCase("is_deleted"))
      .fold(withOp)(flag => withOp.withColumn(flag, lit(true)))
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
    deletes: Option[DeleteSpec],
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
    // new keys carry 'I' and their fresh creation time. Soft-delete
    // tombstones are marked BEFORE the contest so they compete by freshness
    // like any record.
    val alignedIncoming =
      applyDeleteMarking(applyUpdateAudit(aligned0, target, keys), deletes, fullTable).persist()

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
    var persistedContested: Option[DataFrame] = None
    var contestedFramePersisted: Option[DataFrame] = None
    try {
      val totalIncoming = ik.count()
      val dedupedCount = if (inputCount >= 0) math.max(inputCount - totalIncoming, 0L) else 0L
      // Passthrough rows reach the SAME published table: they get the same
      // alignment guards (value-level cast loss is per-row, so a clean keyed
      // frame proves nothing about the keyless one) and the same tombstone
      // marking as the FULL path applies.
      val alignedPassthrough = passthrough.map { p =>
        guardDroppedColumns(p, target.schema, contract, fullTable)
        guardCasts(p, alignmentCasts(p, target.schema), s"alignment to $fullTable")
        applyDeleteMarking(transform.align(p, target.schema, contract), deletes, fullTable)
      }

      if (totalIncoming == 0 && alignedPassthrough.isEmpty) {
        logger.info(s"[CuratedService] Zero incoming rows after key hygiene; $fullTable left untouched")
        return CuratedResult(0L, 0L, 0L, 0L,
          ignoredCount = 0L, nullKeyCount = nullKeyCount, dedupedCount = dedupedCount)
      }

      // Broadcast the small key frames when the batch's key count (already
      // in hand) says they fit comfortably — spares the target-side shuffle.
      // Above the threshold the hint is withheld and AQE decides.
      val hintKeys: DataFrame => DataFrame =
        if (totalIncoming <= CuratedService.BroadcastKeyHintMaxKeys)
          org.apache.spark.sql.functions.broadcast
        else identity

      val joinCond = keyCols.map(k => col(k) <=> col(s"__ik_$k")).reduce(_ && _)
      // contested feeds THREE consumers (key count, hash frame, the merge
      // union): persist it so the target semi-join runs once, not three times.
      val contested = target.join(hintKeys(ik), joinCond, "left_semi").persist()
      contestedFramePersisted = Some(contested)
      val unchanged = target.join(hintKeys(ik), joinCond, "left_anti")
      val contestedKeys = contested.select(keyCols.map(col): _*).distinct()
        .toDF(keyCols.map(k => s"__ck_$k"): _*).persist()
      persistedContested = Some(contestedKeys)
      val ckCond = keyCols.map(k => col(k) <=> col(s"__ck_$k")).reduce(_ && _)
      val contestedKeyCount = contestedKeys.count()
      val insertCount = totalIncoming - contestedKeyCount

      val freshnessUsable = freshness.filter { f =>
        val present = target.columns.exists(_.equalsIgnoreCase(f.column))
        if (!present)
          logger.warn(s"[CuratedService] merge.freshness.column '${f.column}' is not a column of " +
            s"$fullTable; falling back to last-write-wins replacement until the target carries it")
        present
      }

      val (replacement, updateCount, ignoredCount, deletesNewKeys, deletesContested) = freshnessUsable match {
        case Some(f) =>
          val src = CuratedService.MergeProvenanceColumn
          require(!target.columns.exists(_.equalsIgnoreCase(src)),
            s"Target $fullTable contains reserved column '$src'")
          // record_hash no-change pre-filter: identical business content must
          // not churn curated rows (or restamp their audit columns) just
          // because the source touched a timestamp. Filtered challengers
          // leave only the target row in the contest, so the key lands in
          // ignoredCount through the normal winner arithmetic.
          val hashCol = target.columns
            .find(_.equalsIgnoreCase(com.hcsc.generic.ingest.transform.RecordHash.Column))
          val challengers = hashCol match {
            case Some(h) =>
              val kh = keyCols :+ h
              val tgtKeyHash = contested.select(kh.map(col): _*).distinct()
                .toDF(kh.map(k => s"__th_$k"): _*)
              val sameContent = kh.map(k => col(k) <=> col(s"__th_$k")).reduce(_ && _)
              // Tombstones are EXEMPT from the no-change filter: the delete
              // indicator is not a business column, so a tombstone hashes
              // identically to the live target row it deletes — filtering it
              // would silently swallow the delete.
              softDeletePredicate(alignedIncoming, deletes) match {
                case Some(isDel) =>
                  val del = coalesce(isDel, lit(false))
                  alignedIncoming.filter(!del).join(hintKeys(tgtKeyHash), sameContent, "left_anti")
                    .unionByName(alignedIncoming.filter(del))
                case None =>
                  alignedIncoming.join(hintKeys(tgtKeyHash), sameContent, "left_anti")
              }
            case None => alignedIncoming
          }
          val union = contested.withColumn(src, lit("T"))
            .unionByName(challengers.withColumn(src, lit("I")))
          // The freshness column itself is always highest-wins (desc, nulls
          // last); tie-breakers may declare direction and null ordering
          // ("col asc nulls_first") and default to the same desc_nulls_last.
          val freshnessCol = union.columns.find(_.equalsIgnoreCase(f.column))
            .map(c => col(c).desc_nulls_last).toSeq
          val tieBreakCols = f.tieBreakers
            .map(com.hcsc.generic.ingest.transform.OrderSpec.parse)
            .flatMap(s => union.columns.find(_.equalsIgnoreCase(s.column)).map(s.toColumn))
          val orderCols = (freshnessCol ++ tieBreakCols) :+
            col(src).desc // 'T' > 'I': exact ties keep the target
          val w = Window.partitionBy(keyCols.map(col): _*).orderBy(orderCols: _*)
          val winners = union.withColumn("_rn", row_number().over(w))
            .filter(col("_rn") === 1).drop("_rn").persist()
          winnersPersisted = Some(winners)
          val winnersI = winners.filter(col(src) === "I")
          val incomingWinners = winnersI.count()
          val updates = incomingWinners - insertCount
          val ignored = totalIncoming - incomingWinners
          // Soft deletes that WON their key's contest — INCOMING winners
          // only, so a target row tombstoned in an earlier run (or an
          // incoming tombstone outrun by fresher data) is not recounted.
          // Split by contested membership so the counts stay disjoint: a
          // tombstone for a brand-new key comes out of insertCount, one for
          // an existing key out of updateCount.
          val deletesWon = deleteMarkCount(winnersI, deletes)
          val deletesNew = deleteMarkCount(
            winnersI.join(hintKeys(contestedKeys), ckCond, "left_anti"), deletes)
          (winners.drop(src), updates, ignored, deletesNew, deletesWon - deletesNew)
        case None =>
          if (freshness.isEmpty)
            logger.warn("[CuratedService] No curated.merge.freshness configured: the incremental " +
              "merge is last-write-wins by run order — a late-arriving OLDER record will overwrite " +
              "a newer curated row. Configure merge.freshness.column to enable freshness comparison.")
          val deletesAll = deleteMarkCount(alignedIncoming, deletes)
          val deletesNew = deleteMarkCount(
            alignedIncoming.join(hintKeys(contestedKeys), ckCond, "left_anti"), deletes)
          (alignedIncoming, contestedKeyCount, 0L, deletesNew, deletesAll - deletesNew)
      }

      val merged0 = unchanged.unionByName(replacement)
      val merged = alignedPassthrough.fold(merged0)(p => merged0.unionByName(p))
      val published = publisher.publish(merged, request, ctx)
      // Disjoint accounting: every incoming key is EXACTLY one of insert,
      // update, delete or ignored — the four plus nullKey/deduped/passthrough
      // total the accepted rows.
      CuratedResult(published.publishedCount,
        insertCount - deletesNewKeys, updateCount - deletesContested,
        deleteCount = deletesNewKeys + deletesContested,
        ignoredCount = ignoredCount, nullKeyCount = nullKeyCount, dedupedCount = dedupedCount,
        passthroughCount = passthroughCount)
    } finally {
      winnersPersisted.foreach(df => try df.unpersist(false) catch { case _: Exception => () })
      persistedContested.foreach(df => try df.unpersist(false) catch { case _: Exception => () })
      contestedFramePersisted.foreach(df => try df.unpersist(false) catch { case _: Exception => () })
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

  /** Broadcast-hint the merge's key frames only below this many distinct
    * incoming keys — a hint FORCES the broadcast regardless of size, so an
    * unbounded batch must fall back to AQE's own planning. ~1M small key
    * rows is comfortably inside default driver/executor memory. */
  val BroadcastKeyHintMaxKeys = 1000000L

  /** Columns stamped by ensureAudit; not usable as a freshness column. */
  val FrameworkAuditColumns: Set[String] =
    Set("create_timestamp", "last_modified_ts", "last_modified_op")
}
