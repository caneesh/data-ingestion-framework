package com.hcsc.generic.ingest.reject

import com.hcsc.generic.ingest.config.ConfigUtils
import com.hcsc.generic.ingest.runtime.RunContext
import com.hcsc.generic.ingest.schema.SchemaContract
import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import com.hcsc.generic.ingest.schema.ColumnMapping.quotedCol

final case class RejectRule(
  name: String,
  condition: String,
  errorCode: String,
  message: String,
  category: String
)

final case class RejectSplit(accepted: DataFrame, acceptedCount: Long, rejectedCount: Long)

final class RejectThresholdExceededException(message: String) extends RuntimeException(message)

/**
  * Splits records into accepted/rejected using configurable rules, persists
  * rejected rows with full lineage, and enforces reject thresholds.
  *
  * Configuration reference: docs/architecture/CONFIGURATION_MODEL.md.
  */
final class RejectService(
  spark: SparkSession,
  rejectConf: Option[Config],
  contract: Option[SchemaContract],
  logger: Logger
) {

  val enabled: Boolean =
    rejectConf.exists(c => ConfigUtils.optBoolean(c, "enabled").getOrElse(true))

  /** True when this service diverts nullability violations row-by-row, in
    * which case run-level contract nullability enforcement must stand down. */
  val handlesContractNullability: Boolean =
    enabled && rejectConf.exists(c => ConfigUtils.optBoolean(c, "use_contract_nullability").getOrElse(false))

  /** rejects.payload = FULL | MASKED | HASHED | KEYS_ONLY. The reject table
    * can hold PHI: FULL stores the record verbatim (loud warning), MASKED
    * hashes only the columns the contract classifies PII/PHI/CONFIDENTIAL
    * (keeping the rest readable for triage), HASHED SHA-256s every value,
    * KEYS_ONLY stores only rejects.payload_columns. */
  private val payloadMode: String = {
    val mode = rejectConf.flatMap(c => ConfigUtils.optString(c, "payload"))
      .getOrElse("FULL").toUpperCase
    require(Seq("FULL", "MASKED", "HASHED", "KEYS_ONLY").contains(mode),
      s"rejects.payload '$mode' must be FULL, MASKED, HASHED or KEYS_ONLY")
    if (mode == "MASKED")
      require(contract.exists(_.sensitiveColumns.nonEmpty),
        "rejects.payload = MASKED requires a schema contract with sensitivity classifications " +
          "(PII/PHI/CONFIDENTIAL) — without them nothing would be masked; use HASHED instead")
    if (enabled && mode == "FULL")
      logger.warn("[RejectService] rejects.payload=FULL: rejected records are stored UNREDACTED " +
        "in the reject table — for sensitive feeds set payload = MASKED, HASHED or KEYS_ONLY " +
        "and restrict access to the reject table like the RAW layer")
    mode
  }

  private def payloadJson(df: DataFrame, businessCols: Seq[String]): Column = payloadMode match {
    case "HASHED" =>
      to_json(struct(businessCols.map(c => sha2(quotedCol(c).cast("string"), 256).as(c)): _*))
    case "MASKED" =>
      val sensitive = contract.map(_.sensitiveColumns.map(_.toLowerCase).toSet).getOrElse(Set.empty)
      to_json(struct(businessCols.map { c =>
        if (sensitive.contains(c.toLowerCase)) sha2(quotedCol(c).cast("string"), 256).as(c)
        else quotedCol(c)
      }: _*))
    case "KEYS_ONLY" =>
      val keep = rejectConf.map(c => ConfigUtils.stringList(c, "payload_columns")).getOrElse(Seq.empty)
      require(keep.nonEmpty, "rejects.payload = KEYS_ONLY requires rejects.payload_columns")
      val present = keep.flatMap(k => df.columns.find(_.equalsIgnoreCase(k)))
      // Sensitivity classification applies to EVERY payload mode: a business
      // key tagged PII/PHI must not land verbatim just because it is a key.
      val sensitiveKeys = contract.map(_.sensitiveColumns.map(_.toLowerCase).toSet).getOrElse(Set.empty)
      to_json(struct(present.map { c =>
        if (sensitiveKeys.contains(c.toLowerCase)) sha2(com.hcsc.generic.ingest.schema.ColumnMapping.quotedCol(c).cast("string"), 256).as(c)
        else com.hcsc.generic.ingest.schema.ColumnMapping.quotedCol(c)
      }: _*))
    case _ =>
      to_json(struct(businessCols.map(com.hcsc.generic.ingest.schema.ColumnMapping.quotedCol): _*))
  }

  /** rejects.on_reject_watermark = ADVANCE | HOLD. Rows diverted to the
    * reject table leave the committed source window: with ADVANCE (default,
    * the historical behavior) they can never be re-extracted and recovery is
    * manual from the reject payload. HOLD keeps the watermark unchanged
    * whenever this run rejected anything, so the window is re-extracted next
    * run — after the cause is fixed, previously rejected rows are recovered
    * from the source (previously accepted rows re-append into RAW as
    * documented at-least-once duplicates; the curated merge absorbs them). */
  val onRejectWatermark: String = {
    val mode = rejectConf.flatMap(c => ConfigUtils.optString(c, "on_reject_watermark"))
      .getOrElse("ADVANCE").toUpperCase
    require(Seq("ADVANCE", "HOLD").contains(mode),
      s"rejects.on_reject_watermark '$mode' must be ADVANCE or HOLD")
    mode
  }

  private lazy val rejectTable: String = {
    val c = rejectConf.get
    val db = ConfigUtils.sqlIdentifier(c, "database")
    val table = ConfigUtils.optString(c, "table").getOrElse("ingest_rejects")
    ConfigUtils.requireSqlIdentifier(table, "rejects.table")
    s"$db.$table"
  }

  private def rules(df: DataFrame): Seq[RejectRule] = {
    val c = rejectConf.get
    val configured = ConfigUtils.configList(c, "rules").map { r =>
      RejectRule(
        name = r.getString("name"),
        condition = r.getString("condition"),
        errorCode = ConfigUtils.optString(r, "error_code").getOrElse(r.getString("name")),
        message = ConfigUtils.optString(r, "message").getOrElse(s"Rule '${r.getString("name")}' matched"),
        category = ConfigUtils.optString(r, "category").getOrElse("RULE")
      )
    }

    val fromContract =
      if (!ConfigUtils.optBoolean(c, "use_contract_nullability").getOrElse(false)) Seq.empty
      else contract.toSeq.flatMap(_.columns.filter(!_.nullable)).collect {
        case col if df.columns.exists(_.equalsIgnoreCase(col.name)) =>
          // Backtick-quote so unusual column names cannot alter the parsed SQL
          val quoted = s"`${col.name.replace("`", "``")}`"
          RejectRule(
            name = s"nullability_${col.name}",
            condition = s"$quoted IS NULL OR trim(cast($quoted as string)) = ''",
            errorCode = "NULLABILITY",
            message = s"Column '${col.name}' is declared non-nullable",
            category = "NULLABILITY"
          )
      }

    configured ++ fromContract
  }

  /**
    * Splits df into accepted rows (returned) and rejected rows (persisted).
    * The DataFrame must already carry RAW metadata (file_id, source_file,
    * row_idx) so rejected rows keep full lineage.
    */
  def split(df: DataFrame, ctx: RunContext): RejectSplit = {
    if (!enabled) {
      return RejectSplit(df, -1L, 0L)
    }
    val activeRules = rules(df)
    if (activeRules.isEmpty) {
      return RejectSplit(df, -1L, 0L)
    }

    val flags: Seq[(RejectRule, Column)] = activeRules.map(r => r -> expr(r.condition))
    // SQL three-valued logic: a condition evaluating to NULL (e.g. a
    // comparison against a null column) matches neither filter(isRejected)
    // nor filter(!isRejected) and the row would silently vanish. NULL means
    // "rule did not match", so it must resolve to false.
    val isRejected = coalesce(flags.map(_._2).reduce(_ || _), lit(false))

    def firstMatch(f: RejectRule => String): Column =
      coalesce(flags.map { case (r, cond) => when(cond, lit(f(r))) }: _*)

    // Caller is expected to have persisted df (IngestPipeline does); this
    // service does not manage its cache lifecycle.
    val accepted = df.filter(!isRejected)
    val rejected = df.filter(isRejected)

    val businessCols = df.columns.filterNot(c =>
      com.hcsc.generic.ingest.transform.RawMetadata.ColumnNames.contains(c.toLowerCase) &&
        !c.equalsIgnoreCase("source_file") && !c.equalsIgnoreCase("run_id"))

    val rejectRows = rejected.select(
      lit(ctx.runId).as("run_id"),
      lit(ctx.entity).as("entity"),
      col("file_id"),
      col("source_file"),
      col("row_idx"),
      payloadJson(df, businessCols).as("raw_record"),
      firstMatch(_.errorCode).as("error_code"),
      firstMatch(_.message).as("error_message"),
      firstMatch(_.category).as("reject_category"),
      current_timestamp().as("reject_ts")
    )

    val rejectedCount =
      if (ctx.dryRun) rejected.count()
      else persist(rejectRows, ctx)

    val acceptedCount = accepted.count()

    logger.info(s"[RejectService] accepted=$acceptedCount rejected=$rejectedCount rules=${activeRules.map(_.name).mkString(",")}")
    enforceThresholds(acceptedCount, rejectedCount)

    RejectSplit(accepted, acceptedCount, rejectedCount)
  }

  private def persist(rejectRows: DataFrame, ctx: RunContext): Long =
    persistGuarded(rejectRows, ctx, guardCode = None)

  /**
    * Quarantines rows rejected by a later stage (e.g. null business keys
    * dropped before the curated merge) with full lineage and a stable error
    * code. Lineage columns absent from the DataFrame are stored as null.
    * Returns the number of rows quarantined; in dry-run only counts.
    */
  def persistRows(df: DataFrame, ctx: RunContext, errorCode: String, message: String, category: String): Long = {
    if (!enabled) {
      val count = df.count()
      if (count > 0)
        logger.warn(s"[RejectService] $count row(s) rejected with $errorCode but no rejects " +
          "config is present; rows are dropped WITHOUT quarantine")
      return count
    }
    def lineage(name: String, dataType: String): Column =
      df.columns.find(_.equalsIgnoreCase(name)).map(col).getOrElse(lit(null).cast(dataType))

    val businessCols = df.columns
      .filterNot(c => Seq("row_idx", "load_timestamp", "file_type", "file_id").exists(c.equalsIgnoreCase))

    val rejectRows = df.select(
      lit(ctx.runId).as("run_id"),
      lit(ctx.entity).as("entity"),
      lineage("file_id", "string").as("file_id"),
      lineage("source_file", "string").as("source_file"),
      lineage("row_idx", "bigint").as("row_idx"),
      payloadJson(df, businessCols).as("raw_record"),
      lit(errorCode).as("error_code"),
      lit(message).as("error_message"),
      lit(category).as("reject_category"),
      current_timestamp().as("reject_ts")
    )
    if (ctx.dryRun) df.count()
    else persistGuarded(rejectRows, ctx, guardCode = Some(errorCode))
  }

  /** Columns compared to decide whether a replay reproduced the same reject
    * set; reject_ts is excluded (it is stamped per attempt). */
  private val ComparableColumns = Seq("run_id", "entity", "file_id", "source_file",
    "row_idx", "raw_record", "error_code", "error_message", "reject_category")

  private def persistGuarded(rejectRows0: DataFrame, ctx: RunContext, guardCode: Option[String]): Long = {
    // The reject-rule predicates and payload hashing are re-evaluated by
    // every action on this frame (count, replay comparison, write) — persist
    // once so the expressions run once per row, then release.
    val rejectRows = rejectRows0.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
    try persistGuardedInternal(rejectRows, ctx, guardCode)
    finally { try rejectRows.unpersist(false) catch { case _: Exception => () } }
  }

  private def persistGuardedInternal(rejectRows: DataFrame, ctx: RunContext, guardCode: Option[String]): Long = {
    val count = rejectRows.count()
    if (count > 0)
      com.hcsc.generic.ingest.hive.HiveTables.ensure(
        spark, rejectTable.split("\\.")(0), rejectTable,
        "run_id STRING, entity STRING, file_id STRING, source_file STRING, " +
          "row_idx BIGINT, raw_record STRING, error_code STRING, " +
          "error_message STRING, reject_category STRING, reject_ts TIMESTAMP")

    guardCode match {
      case None =>
        // Idempotency guard (mirrors the RAW one): a run that failed after
        // this append and was resumed with the same --run-id must not append
        // the same reject rows a second time.
        if (count > 0) {
          val alreadyPersisted = spark.table(rejectTable)
            .filter(col("run_id") === ctx.runId && col("entity") === ctx.entity)
            .limit(1).count() > 0
          if (alreadyPersisted)
            logger.warn(s"[RejectService] Reject table already holds rows for run ${ctx.runId}; " +
              "skipping append (idempotent replay)")
          else
            rejectRows.write.mode(SaveMode.Append).insertInto(rejectTable)
        }

      case Some(code) =>
        // Stage-specific rejects REPLACE their (run_id, entity, error_code)
        // slice on replay: a resumed run whose reject set changed (config
        // drift, rewritten raw) must not leave the previous attempt's rows
        // standing — the audit trail would silently diverge from the
        // reconciliation counts of the attempt that actually published.
        if (!spark.catalog.tableExists(rejectTable)) return count
        val existing = spark.table(rejectTable).filter(
          col("run_id") === ctx.runId && col("entity") === ctx.entity && col("error_code") === code)
        val hasPrior = existing.limit(1).count() > 0
        if (!hasPrior) {
          if (count > 0) rejectRows.write.mode(SaveMode.Append).insertInto(rejectTable)
        } else {
          val prior = existing.select(ComparableColumns.map(col): _*)
          val fresh = rejectRows.select(ComparableColumns.map(col): _*)
          val unchanged = count > 0 &&
            fresh.exceptAll(prior).limit(1).count() == 0 &&
            prior.exceptAll(fresh).limit(1).count() == 0
          if (unchanged)
            logger.warn(s"[RejectService] Reject table already holds the identical rows for " +
              s"run ${ctx.runId} code=$code; skipping append (idempotent replay)")
          else {
            logger.warn(s"[RejectService] Replay of run ${ctx.runId} produced a different " +
              s"$code reject set; replacing the previous attempt's rows")
            // null-safe negation: a NULL in any guard column must keep the row
            val survivors = spark.table(rejectTable).filter(
              !(col("run_id") <=> ctx.runId && col("entity") <=> ctx.entity && col("error_code") <=> code))
            val staging = s"${rejectTable}_replay_stg"
            survivors.write.mode(SaveMode.Overwrite).format("orc").saveAsTable(staging)
            try {
              spark.sql(s"INSERT OVERWRITE TABLE $rejectTable SELECT * FROM $staging")
              if (count > 0) rejectRows.write.mode(SaveMode.Append).insertInto(rejectTable)
            } finally spark.sql(s"DROP TABLE IF EXISTS $staging")
          }
        }
    }
    count
  }

  private def enforceThresholds(acceptedCount: Long, rejectedCount: Long): Unit = {
    val c = rejectConf.get
    ConfigUtils.optLong(c, "max_reject_count").foreach { max =>
      if (rejectedCount > max)
        throw new RejectThresholdExceededException(
          s"Rejected $rejectedCount records exceeds max_reject_count=$max"
        )
    }
    ConfigUtils.optString(c, "max_reject_percent").map(_.toDouble).foreach { maxPct =>
      val total = acceptedCount + rejectedCount
      if (total > 0) {
        val pct = rejectedCount * 100.0 / total
        if (pct > maxPct)
          throw new RejectThresholdExceededException(
            f"Rejected $pct%.2f%% of records exceeds max_reject_percent=$maxPct%%"
          )
      }
    }
  }
}
