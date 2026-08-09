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
final case class FreshnessSpec(
  column: String,
  tieBreakers: Seq[String],
  /** Logical comparison type (compare_as): the freshness value is CAST for
    * every comparison — the window contest, the same-hash version-advance
    * check, dedup — while physical storage keeps the user's chosen type
    * (e.g. an all-string layout). */
  compareAs: Option[String] = None,
  /** Datetime parse pattern (compare_format, e.g. "M/d/yyyy"): parses the
    * string via to_timestamp(col, fmt) instead of a bare cast. */
  compareFormat: Option[String] = None
) {
  /** The comparison expression over the ACTUAL column name. */
  def compareExpr(actualColumn: String): org.apache.spark.sql.Column = {
    import org.apache.spark.sql.functions.{col, to_timestamp}
    compareFormat match {
      case Some(fmt) => to_timestamp(col(actualColumn), fmt)
      case None => compareAs.fold(col(actualColumn))(t => col(actualColumn).cast(t))
    }
  }
  def hasLogicalType: Boolean = compareAs.isDefined || compareFormat.isDefined
}

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

    // Physical layout of the curated target + the null-partition policy.
    // Partition keys are layout only — independent of business keys.
    val partitionSpec = CuratedPartitioning.parse(conf)

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
    val prepared2 = stampRunLineage(transform.ensureAudit(prepared1), ctx)
    val prepared3 = transform.normalizeKeys(prepared2, conf)

    // Partition-column validation + null policy run on the fully prepared
    // frame (a partition column may be produced by a transform above).
    if (partitionSpec.isPartitioned) {
      val missing = partitionSpec.keys.filterNot(k => prepared3.columns.exists(_.equalsIgnoreCase(k)))
      require(missing.isEmpty,
        s"CUR_006 curated.partitioning.keys missing from incoming data: ${missing.mkString(", ")}")
    }
    val prepared4 = applyNullPartitionPolicy(prepared3, partitionSpec)

    val mergeConf = ConfigUtils.optConfig(conf, "merge")
    val keys = resolveMergeKeys(mergeConf, contract)
    val freshness = parseFreshness(mergeConf)
    val ordering = effectiveOrdering(freshness)
    val deletes = parseDeletes(mergeConf)

    // Source-specific TECHNICAL metadata (Kafka coordinates, file record
    // numbers) must not become curated business columns by accident: drop
    // them unless the feed opts in — and fail loudly when the merge
    // configuration references one, instead of letting it vanish mid-merge.
    val prepared = {
      val technical = com.hcsc.generic.ingest.transform.SourceMetadata.TechnicalColumns
      val present = prepared4.columns.filter(c => technical.contains(c.toLowerCase))
      val retain = ConfigUtils.optBoolean(conf, "retain_source_metadata").getOrElse(false)
      if (present.isEmpty || retain) prepared4
      else {
        val deleteIndicator = deletes.collect { case DeleteSpec.Soft(ind, _) => ind }.toSeq
        val referenced = (keys ++ partitionSpec.keys ++ deleteIndicator ++
          ordering.map(o => com.hcsc.generic.ingest.transform.OrderSpec.parse(o).column))
          .filter(r => present.exists(_.equalsIgnoreCase(r)))
        require(referenced.isEmpty,
          s"CUR_007 merge keys/ordering/partitioning/deletes reference source metadata " +
            s"column(s) [${referenced.distinct.mkString(", ")}] which are dropped before curated " +
            "by default. Set curated.retain_source_metadata = true to keep them in the " +
            "curated schema (deliberate, audited decision).")
        logger.info(s"[CuratedService] Dropping source metadata column(s) before curated: " +
          s"${present.mkString(", ")} (curated.retain_source_metadata = true keeps them)")
        prepared4.drop(present: _*)
      }
    }

    // Freshness columns must exist in the INCOMING data before any hygiene
    // touches them — named CUR_008 failure instead of the later generic
    // dedup error (fail closed, precisely).
    if (keys.nonEmpty && !mergeConf.flatMap(m => ConfigUtils.optString(m, "strategy"))
        .exists(_.equalsIgnoreCase("LAST_WRITE_WINS")))
      freshness.foreach { f =>
        (f.column +: f.tieBreakers.map(t =>
          com.hcsc.generic.ingest.transform.OrderSpec.parse(t).column)).foreach { c =>
          if (!prepared.columns.exists(_.equalsIgnoreCase(c))) {
            val kind = if (c.equalsIgnoreCase(f.column)) "CURATED_FRESHNESS_COLUMN_MISSING"
              else "CURATED_TIE_BREAKER_COLUMN_MISSING"
            throw new IllegalStateException(
              s"CUR_008 $kind: column '$c' is missing from the incoming data for $fullTable. " +
                "The merge cannot compare versions it cannot see — fix the configuration or " +
                "the incoming data")
          }
        }
      }

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

    val tableExists = spark.catalog.tableExists(fullTable)
    val isIncremental = runMode.equalsIgnoreCase("INCR") && tableExists

    val publishConf = ConfigUtils.optConfig(conf, "publish")
    // Publish mode: explicit config wins; absent keeps the legacy mapping
    // (INCR against an existing table merges, everything else fully
    // replaces). PARTITION_OVERWRITE against a missing table downgrades to
    // the initial full build — there is nothing to scope yet.
    val configuredMode = publishConf
      .flatMap(p => ConfigUtils.optString(p, "mode")).map(CuratedPublishMode.parse)
    val publishMode: CuratedPublishMode = configuredMode match {
      case Some(CuratedPublishMode.PartitionOverwrite) =>
        require(partitionSpec.isPartitioned,
          "CUR_006 curated.publish.mode = PARTITION_OVERWRITE requires curated.partitioning.keys")
        require(keys.nonEmpty,
          "CUR_006 curated.publish.mode = PARTITION_OVERWRITE requires merge keys " +
            "(curated.merge.keys or a contract business_key) — the affected partitions are " +
            "combined and deduplicated by business key")
        require(!deletes.contains(DeleteSpec.SnapshotAbsence),
          "CUR_006 PARTITION_OVERWRITE cannot be combined with deletes.mode = " +
            "FULL_SNAPSHOT_ABSENCE: absence semantics need the COMPLETE snapshot, a " +
            "partition-scoped batch cannot prove a key absent")
        if (tableExists) CuratedPublishMode.PartitionOverwrite
        else {
          logger.info(s"[CuratedService] $fullTable does not exist yet; initial build runs as " +
            "a full publish (PARTITION_OVERWRITE applies from the next run)")
          CuratedPublishMode.FullOverwrite
        }
      case Some(m) => m
      case None =>
        if (isIncremental) CuratedPublishMode.Merge else CuratedPublishMode.FullOverwrite
    }
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
        .orElse(if (publishMode != CuratedPublishMode.FullOverwrite)
          Some(CuratedService.DefaultIncrementalMaxShrinkPercent) else None),
      partitionColumns = partitionSpec.keys,
      partitionOverwrite = publishMode == CuratedPublishMode.PartitionOverwrite
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
          // ALLOW on a current-state table accumulates unmergeable keyless
          // rows forever: require an explicit second acknowledgment so the
          // growth is a decision, not an accident. QUARANTINE stays the
          // safe default.
          if (nullPolicy.contains("ALLOW"))
            require(mergeConf.flatMap(m =>
              ConfigUtils.optBoolean(m, "null_handling.acknowledge_unmerged_growth")).getOrElse(false),
              "CUR_009 null_handling.policy = ALLOW admits keyless rows that can never be " +
                "merged or updated — they accumulate in the current-state table indefinitely. " +
                "Acknowledge this explicitly with " +
                "null_handling.acknowledge_unmerged_growth = true, or use QUARANTINE (default).")
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
              if (c > 0) {
                logger.warn(s"[CuratedService] $c null-key row(s) pass through UNMERGED " +
                  "(keyless rows append forever and can never be updated; QUARANTINE is the " +
                  "safe default)")
                // Keyless duplicate-content signal: identical hashes among
                // keyless rows are pure accumulation with zero information.
                p.columns.find(_.equalsIgnoreCase(
                  com.hcsc.generic.ingest.transform.RecordHash.Column)).foreach { h =>
                  val dupes = p.groupBy(col(h)).count().filter(col("count") > 1).count()
                  if (dupes > 0)
                    logger.warn(s"[CuratedService] $dupes duplicate record_hash value(s) " +
                      "among the keyless passthrough rows — identical content accumulating")
                }
              }
              (0L, if (c == 0) None else Some(p), c)
            }
          val persisted = valid.persist()
          persistedFrames += persisted
          val count = persisted.count()
          // Logical comparison type for the freshness term: the dedup
          // winner must be chosen by the SAME typed comparison as the merge
          // contest, even when storage is all-string.
          val dedupOverrides: Map[String, org.apache.spark.sql.Column] =
            freshness.filter(_.hasLogicalType).flatMap(f =>
              persisted.columns.find(_.equalsIgnoreCase(f.column))
                .map(a => Map(f.column.toLowerCase -> f.compareExpr(a)))).getOrElse(Map.empty)
          (transform.deduplicate(persisted, keys, ordering, dedupOverrides),
            nullCount, pass, passCount, count)
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

      val result = publishMode match {
        case CuratedPublishMode.FullOverwrite =>
          publishFull(hygienic, passthrough, nullKeyCount, passthroughCount, inputCount,
            keys, deletes, fullTable, request, ctx, contract)
        case CuratedPublishMode.Merge if !tableExists =>
          publishFull(hygienic, passthrough, nullKeyCount, passthroughCount, inputCount,
            keys, deletes, fullTable, request, ctx, contract)
        case CuratedPublishMode.Merge =>
          publishIncremental(hygienic, passthrough, nullKeyCount, passthroughCount, inputCount,
            keys, freshness, deletes, fullTable, request, ctx, contract,
            mergeConf, partitionSpec, partitionOverwrite = false)
        case CuratedPublishMode.PartitionOverwrite =>
          publishIncremental(hygienic, passthrough, nullKeyCount, passthroughCount, inputCount,
            keys, freshness, deletes, fullTable, request, ctx, contract,
            mergeConf, partitionSpec, partitionOverwrite = true)
      }

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
      val compareAs = ConfigUtils.optString(f, "compare_as").map(_.toLowerCase)
      compareAs.foreach { t =>
        try org.apache.spark.sql.types.DataType.fromDDL(t)
        catch { case e: Exception => throw new IllegalArgumentException(
          s"CUR_008 CURATED_FRESHNESS_TYPE_INVALID: compare_as '$t' is not a valid Spark type", e) }
      }
      FreshnessSpec(column, ConfigUtils.stringList(f, "tie_breakers"),
        compareAs = compareAs,
        compareFormat = ConfigUtils.optString(f, "compare_format"))
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

  /**
    * Stamps `last_modified_run_id` with the publishing run when — and only
    * when — the curated schema carries that column.
    *
    * Batch lineage: RAW rows carry run_id, curated rows historically did
    * not, so a suspect curated row could not be traced to the batch that
    * last wrote it without inferring from timestamps. Opt-in by DDL,
    * exactly like `is_deleted`: adding the column to a curated table starts
    * populating it, and every existing table keeps working untouched. The
    * incoming value is overwritten rather than preserved — unlike
    * create_timestamp, this records the LAST write, not the first.
    */
  private def stampRunLineage(df: DataFrame, ctx: RunContext): DataFrame =
    df.columns.find(_.equalsIgnoreCase("last_modified_run_id"))
      .fold(df)(c => df.withColumn(c, lit(ctx.runId)))

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

  /** Fail-closed merge-strategy gate (CUR_008): FRESHNESS (default)
    * requires a fully usable freshness configuration — column present in
    * INCOMING and TARGET, tie-breakers present in both, orderable types.
    * Anything less fails BEFORE publishing instead of silently degrading
    * to last-write-wins. Legacy LWW survives only behind
    * merge { strategy = "LAST_WRITE_WINS", allow_unsafe_legacy_merge = true }.
    * Returns Some(freshness) for the freshness contest, None for approved
    * legacy LWW. */
  private def resolveMergeStrategy(
    incoming: DataFrame,
    target: DataFrame,
    freshness: Option[FreshnessSpec],
    mergeConf: Option[Config],
    fullTable: String
  ): Option[FreshnessSpec] = {
    val strategy = mergeConf.flatMap(m => ConfigUtils.optString(m, "strategy"))
      .map(_.toUpperCase).getOrElse("FRESHNESS")
    require(Seq("FRESHNESS", "LAST_WRITE_WINS").contains(strategy),
      s"CUR_008 curated.merge.strategy '$strategy' must be FRESHNESS or LAST_WRITE_WINS")
    if (strategy == "LAST_WRITE_WINS") {
      require(mergeConf.flatMap(m =>
        ConfigUtils.optBoolean(m, "allow_unsafe_legacy_merge")).getOrElse(false),
        "CUR_008 curated.merge.strategy = LAST_WRITE_WINS is UNSAFE for a current-snapshot " +
          "table (a late-arriving OLDER record overwrites newer curated rows) and requires " +
          "merge.allow_unsafe_legacy_merge = true to acknowledge that explicitly")
      logger.warn("[CuratedService] LAST_WRITE_WINS legacy merge explicitly enabled: run " +
        "order, not source freshness, decides winners")
      return None
    }
    val f = freshness.getOrElse(throw new IllegalStateException(
      "CUR_008 CURATED_FRESHNESS_MISSING: the incremental merge requires " +
        "curated.merge.freshness.column (the source-version column that decides winners). " +
        "Configure it, or opt into the legacy behavior explicitly with " +
        "merge { strategy = \"LAST_WRITE_WINS\", allow_unsafe_legacy_merge = true }."))
    def requireIn(df: DataFrame, where: String, name: String, kind: String): String =
      df.columns.find(_.equalsIgnoreCase(name)).getOrElse(throw new IllegalStateException(
        s"CUR_008 $kind: column '$name' is missing from the $where for $fullTable. " +
          "The merge cannot compare versions it cannot see — fix the configuration or the " +
          (if (where == "target") "target schema (backfill the column or run an explicit FULL_OVERWRITE once)"
           else "incoming data")))
    requireIn(incoming, "incoming data", f.column, "CURATED_FRESHNESS_COLUMN_MISSING")
    val fActual = requireIn(target, "target", f.column, "CURATED_FRESHNESS_COLUMN_MISSING")
    f.tieBreakers.map(t => com.hcsc.generic.ingest.transform.OrderSpec.parse(t).column).foreach { t =>
      requireIn(incoming, "incoming data", t, "CURATED_TIE_BREAKER_COLUMN_MISSING")
      requireIn(target, "target", t, "CURATED_TIE_BREAKER_COLUMN_MISSING")
    }
    val fType = target.schema(fActual).dataType
    val orderable = fType match {
      case _: org.apache.spark.sql.types.MapType => false
      case _: org.apache.spark.sql.types.ArrayType => false
      case _: org.apache.spark.sql.types.StructType => false
      case _ => true
    }
    // With a declared LOGICAL type (compare_as / compare_format) the
    // comparison runs over the cast, so the physical type only needs to be
    // castable — the all-string layout is fully supported.
    require(orderable || f.hasLogicalType,
      s"CUR_008 CURATED_FRESHNESS_TYPE_INVALID: freshness column '$fActual' has non-orderable " +
        s"type ${fType.simpleString}; use a timestamp, numeric or string version column, or " +
        "declare freshness.compare_as / compare_format for a logical comparison type")
    // Comparison-cast honesty: a value the logical type cannot parse
    // becomes NULL and would silently lose every contest — fail instead.
    if (f.hasLogicalType) {
      val incomingActual = incoming.columns.find(_.equalsIgnoreCase(f.column)).get
      val broken = incoming.filter(
        col(incomingActual).isNotNull && f.compareExpr(incomingActual).isNull).limit(1).count()
      require(broken == 0,
        s"CUR_008 CURATED_FRESHNESS_TYPE_INVALID: incoming '$incomingActual' values are not " +
          s"parseable as ${f.compareFormat.map(fmt => s"format '$fmt'").getOrElse(f.compareAs.get)} — " +
          "an unparseable version would silently lose every freshness contest; fix the data or " +
          "the declared comparison type")
    }
    Some(f)
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
    contract: Option[SchemaContract],
    mergeConf: Option[Config] = None,
    partitionSpec: CuratedPartitionSpec = CuratedPartitioning.Unpartitioned,
    partitionOverwrite: Boolean = false
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
    var slicePersisted: Option[DataFrame] = None
    var affectedPartitionsPersisted: Option[DataFrame] = None
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

      // ---- partition scoping (PARTITION_OVERWRITE) ------------------------
      // The merge runs against the SLICE of the target the batch can touch:
      // the incoming batch's partitions plus — when a business key can move
      // between partitions (partition columns not a subset of the business
      // keys) — the OLD partitions of contested keys, discovered via a
      // column-pruned key lookup. Skipping that lookup when keys cannot
      // move is the documented trade-off for the extra scan.
      val partCols = partitionSpec.keys.map(k =>
        target.columns.find(_.equalsIgnoreCase(k)).getOrElse(
          throw new IllegalStateException(
            s"CUR_006 curated.partitioning.keys entry '$k' is not a column of $fullTable — " +
              "the configured layout must match the physical table")))
      val (mergeTarget, sliceCountOpt, affectedPartsOpt) =
        if (!partitionOverwrite) (target, None: Option[Long], None: Option[DataFrame])
        else {
          val incomingParts0 = alignedIncoming.select(partCols.map(col): _*)
          val incomingParts = alignedPassthrough
            .fold(incomingParts0)(p => incomingParts0.unionByName(p.select(partCols.map(col): _*)))
            .distinct()
          val keysCanMove =
            !partCols.map(_.toLowerCase).toSet.subsetOf(keyCols.map(_.toLowerCase).toSet)
          val affectedParts = (
            if (!keysCanMove) incomingParts
            else {
              val oldParts = target.join(hintKeys(ik), joinCond, "left_semi")
                .select(partCols.map(col): _*).distinct()
              incomingParts.unionByName(oldParts).distinct()
            }).persist()
          affectedPartitionsPersisted = Some(affectedParts)

          // Literal pruning predicate while the tuple count stays under the
          // configured cap (collect is bounded); beyond it, a distributed
          // broadcast semi-join filters the target instead.
          val tuples = affectedParts.limit(partitionSpec.maxAffectedPartitions + 1).collect()
          val slice =
            if (tuples.length <= partitionSpec.maxAffectedPartitions) {
              logger.info(s"[CuratedService] PARTITION_OVERWRITE: ${tuples.length} affected " +
                s"partition(s) of $fullTable: " +
                tuples.take(20).map(r => partCols.zipWithIndex
                  .map { case (c, i) => s"$c=${r.get(i)}" }.mkString("[", ",", "]")).mkString(" ") +
                (if (tuples.length > 20) " ..." else ""))
              val predicate = tuples.map(r =>
                partCols.zipWithIndex.map { case (c, i) => col(c) <=> lit(r.get(i)) }.reduce(_ && _)
              ).reduce(_ || _)
              target.filter(predicate)
            } else {
              logger.info(s"[CuratedService] PARTITION_OVERWRITE: affected partitions exceed " +
                s"max_affected_partitions=${partitionSpec.maxAffectedPartitions}; filtering the " +
                "target via a distributed semi-join instead of a literal predicate")
              val ap = affectedParts.toDF(partCols.map(c => s"__ap_$c"): _*)
              val apCond = partCols.map(c => col(c) <=> col(s"__ap_$c")).reduce(_ && _)
              target.join(org.apache.spark.sql.functions.broadcast(ap), apCond, "left_semi")
            }
          val persistedSlice = slice.persist()
          slicePersisted = Some(persistedSlice)
          (persistedSlice, Some(persistedSlice.count()), Some(affectedParts))
        }

      // contested feeds THREE consumers (key count, hash frame, the merge
      // union): persist it so the target semi-join runs once, not three times.
      val contested = mergeTarget.join(hintKeys(ik), joinCond, "left_semi").persist()
      contestedFramePersisted = Some(contested)
      val unchanged = mergeTarget.join(hintKeys(ik), joinCond, "left_anti")
      val contestedKeys = contested.select(keyCols.map(col): _*).distinct()
        .toDF(keyCols.map(k => s"__ck_$k"): _*).persist()
      persistedContested = Some(contestedKeys)
      val ckCond = keyCols.map(k => col(k) <=> col(s"__ck_$k")).reduce(_ && _)
      val contestedKeyCount = contestedKeys.count()
      val insertCount = totalIncoming - contestedKeyCount

      // FAIL CLOSED (CUR_008): the incremental merge of a current-snapshot
      // table must never silently degrade to last-write-wins — that would
      // let a late-arriving OLDER record overwrite newer curated rows.
      // Legacy LWW survives only behind an explicit double opt-in.
      val freshnessUsable = resolveMergeStrategy(incoming, target, freshness, mergeConf, fullTable)

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
          val (challengers, versionAdvances) = hashCol match {
            case Some(h) =>
              val kh = keyCols :+ h
              val tgtKeyHash = contested.select(kh.map(col): _*).distinct()
                .toDF(kh.map(k => s"__th_$k"): _*)
              val sameContent = kh.map(k => col(k) <=> col(s"__th_$k")).reduce(_ && _)
              // Tombstones are EXEMPT from the no-change filter: the delete
              // indicator is not a business column, so a tombstone hashes
              // identically to the live target row it deletes — filtering it
              // would silently swallow the delete.
              val live = softDeletePredicate(alignedIncoming, deletes) match {
                case Some(isDel) => alignedIncoming.filter(!coalesce(isDel, lit(false)))
                case None => alignedIncoming
              }
              val tombstones = softDeletePredicate(alignedIncoming, deletes)
                .map(isDel => alignedIncoming.filter(coalesce(isDel, lit(false))))
              val changed = live.join(hintKeys(tgtKeyHash), sameContent, "left_anti")

              // SOURCE-VERSION ADVANCEMENT: a same-hash incoming row with a
              // STRICTLY NEWER freshness value means the source produced a
              // new version of unchanged content. Dropping it would leave
              // the target's version metadata stale, letting a later record
              // with an in-between version (different hash) wrongly win.
              // Publish the TARGET row (business content and audit stamps
              // untouched — no 'U', no restamp) carrying the incoming
              // version columns; it competes tagged 'T' and outranks both
              // the stale target row and any in-between challenger.
              val fActual = target.columns.find(_.equalsIgnoreCase(f.column)).get
              val versionCols = (fActual +: f.tieBreakers
                .map(t => com.hcsc.generic.ingest.transform.OrderSpec.parse(t).column)
                .flatMap(c => target.columns.find(_.equalsIgnoreCase(c)))).distinct
              val sameHash = live.join(hintKeys(tgtKeyHash), sameContent, "left_semi")
              val iVer = sameHash
                .select((keyCols.map(col) ++ versionCols.map(col)): _*)
                .toDF((keyCols.map(k => s"__vk_$k") ++ versionCols.map(v => s"__vv_$v")): _*)
              val vkCond = keyCols.map(k => col(k) <=> col(s"__vk_$k")).reduce(_ && _)
              val advanced = contested.join(hintKeys(iVer), vkCond, "inner")
                // strictly newer source version, under the LOGICAL type
                .filter(f.compareExpr(s"__vv_$fActual") > f.compareExpr(fActual))
                .select(contested.columns.map(c =>
                  if (versionCols.exists(_.equalsIgnoreCase(c))) col(s"__vv_$c").as(c)
                  else col(c)): _*)

              (tombstones.fold(changed)(t => changed.unionByName(t)), Some(advanced))
            case None => (alignedIncoming, None)
          }
          val union = contested.withColumn(src, lit("T"))
            .unionByName(versionAdvances.fold(contested.filter(lit(false)))(identity)
              .withColumn(src, lit("T")))
            .unionByName(challengers.withColumn(src, lit("I")))
          // The freshness column itself is always highest-wins (desc, nulls
          // last); tie-breakers may declare direction and null ordering
          // ("col asc nulls_first") and default to the same desc_nulls_last.
          val freshnessCol = union.columns.find(_.equalsIgnoreCase(f.column))
            .map(c => f.compareExpr(c).desc_nulls_last).toSeq
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
          // Reachable ONLY via merge.strategy = LAST_WRITE_WINS with
          // allow_unsafe_legacy_merge = true (resolveMergeStrategy).
          val deletesAll = deleteMarkCount(alignedIncoming, deletes)
          val deletesNew = deleteMarkCount(
            alignedIncoming.join(hintKeys(contestedKeys), ckCond, "left_anti"), deletes)
          (alignedIncoming, contestedKeyCount, 0L, deletesNew, deletesAll - deletesNew)
      }

      val merged0 = unchanged.unionByName(replacement)
      val merged = alignedPassthrough.fold(merged0)(p => merged0.unionByName(p))
      // Partition-scoped publish compares shrink against the SLICE it
      // replaces, and only ever rewrites the partitions present in merged.
      val published = publisher.publish(
        merged, request.copy(shrinkBaseCount = sliceCountOpt), ctx)
      // A moved key can leave an affected partition with ZERO surviving
      // rows; dynamic overwrite only replaces partitions PRESENT in the
      // written data, so an emptied partition must be dropped explicitly or
      // its stale rows would survive.
      affectedPartsOpt.foreach(ap => dropEmptiedPartitions(fullTable, partCols, ap, merged))
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
      slicePersisted.foreach(df => try df.unpersist(false) catch { case _: Exception => () })
      affectedPartitionsPersisted.foreach(df => try df.unpersist(false) catch { case _: Exception => () })
      ik.unpersist(false)
      alignedIncoming.unpersist(false)
    }
  }

  /** Null-partition policy (curated.partitioning.null_values): REJECT fails
    * fast, DEFAULT substitutes the configured value (cast to the column's
    * type), HIVE passes nulls through to __HIVE_DEFAULT_PARTITION__. */
  private def applyNullPartitionPolicy(df: DataFrame, spec: CuratedPartitionSpec): DataFrame = {
    if (!spec.isPartitioned) return df
    val actuals = spec.keys.map(k => df.columns.find(_.equalsIgnoreCase(k)).get)
    spec.nullPolicy match {
      case NullPartitionPolicy.Hive => df
      case NullPartitionPolicy.Default(value) =>
        df.withColumns(actuals.map(c =>
          c -> coalesce(col(c), lit(value).cast(df.schema(c).dataType))).toMap)
      case NullPartitionPolicy.Reject =>
        val anyNull = actuals.map(col(_).isNull).reduce(_ || _)
        if (df.filter(anyNull).limit(1).count() > 0)
          throw new IllegalStateException(
            s"CUR_006 incoming data carries NULL value(s) in partition column(s) " +
              s"[${actuals.mkString(", ")}] and curated.partitioning.null_values = REJECT " +
              "(the default). Fix the source, derive the column, or configure " +
              "null_values = DEFAULT (with default_value) or HIVE.")
        df
    }
  }

  /** Drops affected partitions that ended with zero surviving rows. Tuples
    * containing NULL partition values cannot be addressed by ALTER TABLE
    * DROP PARTITION and are logged instead (HIVE null policy only). */
  private def dropEmptiedPartitions(
    fullTable: String,
    partCols: Seq[String],
    affectedParts: DataFrame,
    merged: DataFrame
  ): Unit = {
    val written = merged.select(partCols.map(col): _*).distinct()
    val emptied = affectedParts.exceptAll(written).limit(1001).collect()
    if (emptied.length > 1000)
      logger.warn(s"[CuratedService] More than 1000 affected partition(s) of $fullTable were " +
        "emptied in one run — dropping the first 1000; re-run retention or repeat the run to " +
        "clear the remainder (this volume usually indicates a misconfigured batch)")
    emptied.take(1000).foreach { row =>
      val values = partCols.indices.map(i => Option(row.get(i)))
      if (values.exists(_.isEmpty))
        logger.warn(s"[CuratedService] Affected partition with NULL value(s) emptied but not " +
          s"droppable via ALTER TABLE (${partCols.mkString(",")}); rows were overwritten away, " +
          "the empty default partition may linger")
      else {
        val spec = partCols.zip(values.map(_.get)).map { case (c, v) =>
          s"$c='${v.toString.replace("'", "''")}'"
        }.mkString(", ")
        logger.info(s"[CuratedService] Dropping emptied partition ($spec) of $fullTable")
        spark.sql(s"ALTER TABLE $fullTable DROP IF EXISTS PARTITION ($spec)")
      }
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
    Set("create_timestamp", "last_modified_ts", "last_modified_op", "last_modified_run_id")
}
