package com.hcsc.generic.ingest.reject

import com.hcsc.generic.ingest.config.ConfigUtils
import com.hcsc.generic.ingest.runtime.RunContext
import com.hcsc.generic.ingest.schema.SchemaContract
import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

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
  * rejects {
  *   enabled = true
  *   database = "ingest_audit"
  *   table = "ingest_rejects"
  *   use_contract_nullability = true   # derive rules from schema contract
  *   rules = [
  *     { name = "missing_subscriber", condition = "subscriber_id IS NULL OR trim(subscriber_id) = ''",
  *       error_code = "R001", category = "MISSING_KEY", message = "subscriber_id is required" }
  *   ]
  *   max_reject_count = 1000
  *   max_reject_percent = 5.0
  * }
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

    val businessCols = df.columns
      .filterNot(c => Seq("row_idx", "load_timestamp", "file_type", "file_id").exists(c.equalsIgnoreCase))

    val rejectRows = rejected.select(
      lit(ctx.runId).as("run_id"),
      lit(ctx.entity).as("entity"),
      col("file_id"),
      col("source_file"),
      col("row_idx"),
      to_json(struct(businessCols.map(col): _*)).as("raw_record"),
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

  private def persist(rejectRows: DataFrame, ctx: RunContext): Long = {
    val count = rejectRows.count()
    if (count > 0) {
      com.hcsc.generic.ingest.hive.HiveTables.ensure(
        spark, rejectTable.split("\\.")(0), rejectTable,
        "run_id STRING, entity STRING, file_id STRING, source_file STRING, " +
          "row_idx BIGINT, raw_record STRING, error_code STRING, " +
          "error_message STRING, reject_category STRING, reject_ts TIMESTAMP")
      // Idempotency guard (mirrors the RAW one): a run that failed after this
      // append and was resumed with the same --run-id must not append the same
      // reject rows a second time.
      val alreadyPersisted = spark.table(rejectTable)
        .filter(col("run_id") === ctx.runId && col("entity") === ctx.entity)
        .limit(1).count() > 0
      if (alreadyPersisted)
        logger.warn(s"[RejectService] Reject table already holds rows for run ${ctx.runId}; " +
          "skipping append (idempotent replay)")
      else
        rejectRows.write.mode(SaveMode.Append).insertInto(rejectTable)
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
