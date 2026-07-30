package com.hcsc.generic.ingest.audit

import com.hcsc.generic.ingest.config.ConfigUtils
import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.sql.Timestamp

/** Per-stage counters captured for the run audit. Use -1 for "not measured". */
final case class StageCounts(
  sourceCount: Long = -1L,
  rawCount: Long = -1L,
  acceptedCount: Long = -1L,
  rejectedCount: Long = -1L,
  insertCount: Long = -1L,
  updateCount: Long = -1L,
  deleteCount: Long = -1L,
  controlTotal: Option[String] = None
)

final case class FileAuditRecord(
  run_id: String,
  entity: String,
  file_name: String,
  file_path: String,
  checksum: String,
  size_bytes: Long,
  status: String,
  reason: String,
  event_ts: Timestamp
)

final case class RunAuditRecord(
  run_id: String,
  entity: String,
  stage: String,
  status: String,
  source_count: Long,
  raw_count: Long,
  accepted_count: Long,
  rejected_count: Long,
  insert_count: Long,
  update_count: Long,
  delete_count: Long,
  control_total: String,
  message: String,
  event_ts: Timestamp,
  // Appended at the END so pre-existing tables migrate via ALTER TABLE
  // ADD COLUMNS without breaking positional insertInto.
  run_mode: String,
  window_start: String,
  window_end: String
)

final case class ReconciliationRecord(
  run_id: String,
  entity: String,
  check_name: String,
  expected: String,
  actual: String,
  passed: Boolean,
  event_ts: Timestamp
)

final case class HeaderAuditRecord(
  run_id: String,
  entity: String,
  source_files: String,
  validation_stage: String,
  validation_status: String,
  expected_canonical_columns: String,
  actual_source_headers: String,
  normalized_source_headers: String,
  resolved_mappings: String,
  missing_required_columns: String,
  missing_optional_columns: String,
  unexpected_columns: String,
  duplicate_columns: String,
  positional_fallback_used: Boolean,
  error_code: String,
  error_message: String,
  quarantine_path: String,
  event_ts: Timestamp
)

/**
  * Persists file-level, stage-level and reconciliation audit records to Hive.
  * Entirely opt-in: when the feed has no `audit` block (or enabled=false)
  * every call is a logged no-op, so feeds keep working without audit
  * infrastructure.
  *
  * Config:
  * audit {
  *   enabled = true
  *   database = "ingest_audit"
  *   file_table = "ingest_file_audit"
  *   run_table = "ingest_run_audit"
  *   reconciliation_table = "ingest_reconciliation"
  * }
  */
final class AuditService(spark: SparkSession, auditConf: Option[Config]) {
  private val logger = Logger.getLogger(getClass.getName)

  val enabled: Boolean =
    auditConf.exists(c => ConfigUtils.optBoolean(c, "enabled").getOrElse(true))

  private lazy val database: String =
    ConfigUtils.sqlIdentifier(auditConf.get, "database")

  private def table(key: String, default: String): String = {
    val name = auditConf.flatMap(c => ConfigUtils.optString(c, key)).getOrElse(default)
    ConfigUtils.requireSqlIdentifier(name, s"audit.$key")
    s"$database.$name"
  }

  private lazy val fileTable = table("file_table", "ingest_file_audit")
  private lazy val runTable = table("run_table", "ingest_run_audit")
  private lazy val reconciliationTable = table("reconciliation_table", "ingest_reconciliation")
  private lazy val headerTable = table("header_table", "ingest_header_audit")

  private def now(): Timestamp = new Timestamp(System.currentTimeMillis())

  private val fileTableDdl =
    "run_id STRING, entity STRING, file_name STRING, file_path STRING, checksum STRING, " +
      "size_bytes BIGINT, status STRING, reason STRING, event_ts TIMESTAMP"

  private val runTableDdl =
    "run_id STRING, entity STRING, stage STRING, status STRING, source_count BIGINT, " +
      "raw_count BIGINT, accepted_count BIGINT, rejected_count BIGINT, insert_count BIGINT, " +
      "update_count BIGINT, delete_count BIGINT, control_total STRING, message STRING, " +
      "event_ts TIMESTAMP, run_mode STRING, window_start STRING, window_end STRING"

  /** Columns added after the original run-audit DDL; pre-existing tables are
    * migrated in place (Hive appends ADD COLUMNS at the end, matching the
    * record's field order). */
  private val runTableAddedColumns = Seq(
    "run_mode" -> "STRING", "window_start" -> "STRING", "window_end" -> "STRING")

  private def ensureRunTableColumns(): Unit =
    if (spark.catalog.tableExists(runTable)) {
      val existing = spark.table(runTable).columns.map(_.toLowerCase).toSet
      val missing = runTableAddedColumns.filterNot { case (n, _) => existing.contains(n) }
      if (missing.nonEmpty) {
        val ddl = missing.map { case (n, t) => s"$n $t" }.mkString(", ")
        logger.warn(s"[Audit] Migrating $runTable: ADD COLUMNS ($ddl)")
        spark.sql(s"ALTER TABLE $runTable ADD COLUMNS ($ddl)")
      }
    }

  /** The extract window observed by this run's source read; stamped onto
    * subsequent run-audit rows so the ledger records the incremental
    * boundaries (spec: start/end watermark per run). */
  @volatile private var extractWindow: (String, String) = ("", "")

  def setExtractWindow(start: Option[String], end: Option[String]): Unit =
    extractWindow = (start.getOrElse(""), end.getOrElse(""))

  private val reconciliationTableDdl =
    "run_id STRING, entity STRING, check_name STRING, expected STRING, actual STRING, " +
      "passed BOOLEAN, event_ts TIMESTAMP"

  private val headerTableDdl =
    "run_id STRING, entity STRING, source_files STRING, validation_stage STRING, " +
      "validation_status STRING, expected_canonical_columns STRING, actual_source_headers STRING, " +
      "normalized_source_headers STRING, resolved_mappings STRING, missing_required_columns STRING, " +
      "missing_optional_columns STRING, unexpected_columns STRING, duplicate_columns STRING, " +
      "positional_fallback_used BOOLEAN, error_code STRING, error_message STRING, " +
      "quarantine_path STRING, event_ts TIMESTAMP"

  private def append(rows: org.apache.spark.sql.DataFrame, fullTable: String, columnsDdl: String): Unit =
    com.hcsc.generic.ingest.hive.HiveTables.appendEnsuringTable(spark, database, fullTable, columnsDdl, rows)

  def recordFile(
    ctx: com.hcsc.generic.ingest.runtime.RunContext,
    fileName: String,
    filePath: String,
    checksum: String,
    sizeBytes: Long,
    status: String,
    reason: String = ""
  ): Unit = {
    if (!enabled) {
      logger.info(s"[Audit] (disabled) file=$fileName status=$status reason=$reason")
      return
    }
    import spark.implicits._
    val record = FileAuditRecord(ctx.runId, ctx.entity, fileName, filePath, checksum, sizeBytes, status, reason, now())
    append(Seq(record).toDF(), fileTable, fileTableDdl)
    logger.info(s"[Audit] file=$fileName status=$status reason=$reason")
  }

  def recordStage(
    ctx: com.hcsc.generic.ingest.runtime.RunContext,
    stage: String,
    status: String,
    counts: StageCounts = StageCounts(),
    message: String = ""
  ): Unit = {
    if (!enabled) {
      logger.info(s"[Audit] (disabled) stage=$stage status=$status")
      return
    }
    import spark.implicits._
    val record = RunAuditRecord(
      ctx.runId, ctx.entity, stage, status,
      counts.sourceCount, counts.rawCount, counts.acceptedCount, counts.rejectedCount,
      counts.insertCount, counts.updateCount, counts.deleteCount,
      counts.controlTotal.orNull, message, now(),
      ctx.mode, extractWindow._1, extractWindow._2
    )
    ensureRunTableColumns()
    append(Seq(record).toDF(), runTable, runTableDdl)
    logger.info(s"[Audit] stage=$stage status=$status counts=$counts")
  }

  def recordReconciliation(
    ctx: com.hcsc.generic.ingest.runtime.RunContext,
    entries: Seq[(String, String, String, Boolean)]
  ): Unit = {
    if (!enabled || entries.isEmpty) return
    import spark.implicits._
    val ts = now()
    val records = entries.map { case (check, expected, actual, passed) =>
      ReconciliationRecord(ctx.runId, ctx.entity, check, expected, actual, passed, ts)
    }
    append(records.toDF(), reconciliationTable, reconciliationTableDdl)
    entries.foreach { case (check, expected, actual, passed) =>
      logger.info(s"[Audit] reconciliation check=$check expected=$expected actual=$actual passed=$passed")
    }
  }

  /** Persists a header validation outcome. Called BEFORE any quarantine file
    * move so validation information is captured safely first. */
  def recordHeaderValidation(ctx: com.hcsc.generic.ingest.runtime.RunContext, record: HeaderAuditRecord): Unit = {
    if (!enabled) {
      logger.info(s"[Audit] (disabled) header validation status=${record.validation_status} code=${record.error_code}")
      return
    }
    import spark.implicits._
    append(Seq(record).toDF(), headerTable, headerTableDdl)
    logger.info(s"[Audit] header validation status=${record.validation_status} code=${record.error_code} " +
      s"missingRequired=[${record.missing_required_columns}]")
  }

  /** Latest recorded status for a stage of a given run, used by --resume.
    * Terminal statuses win over STARTED when timestamps tie. */
  def stageStatus(runId: String, entity: String, stage: String): Option[String] = {
    if (!enabled || !spark.catalog.tableExists(runTable)) return None
    import org.apache.spark.sql.functions._
    spark.table(runTable)
      .filter(col("run_id") === runId && col("entity") === entity && col("stage") === stage)
      .orderBy(col("event_ts").desc, when(col("status") === "STARTED", 1).otherwise(0))
      .select("status")
      .collect()
      .headOption
      .map(_.getString(0))
  }
}

object AuditService {
  def apply(spark: SparkSession, feedConf: Config): AuditService =
    new AuditService(spark, ConfigUtils.optConfig(feedConf, "audit"))
}
