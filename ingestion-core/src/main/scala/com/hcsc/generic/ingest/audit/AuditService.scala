package com.hcsc.generic.ingest.audit

import com.hcsc.generic.ingest.config.ConfigUtils
import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.sql.Timestamp
import com.hcsc.generic.ingest.schema.ColumnMapping.quotedCol

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
  window_end: String,
  // Provenance: WHICH code, WHICH config and WHO produced this batch.
  // Without these a batch found to be wrong cannot be traced to the build
  // or configuration that made it.
  framework_version: String,
  config_fingerprint: String,
  principal: String
)

/** One RAW batch as seen by the decoupled curated driver: identity, the
  * mode it was extracted under, and its extract window (empty strings on
  * pre-migration ledgers). */
final case class PendingBatch(
  runId: String,
  runMode: Option[String],
  windowStart: String,
  windowEnd: String,
  rawSuccessTs: Timestamp
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
  * Configuration reference: docs/architecture/CONFIGURATION_MODEL.md.
  */
final class AuditService(
  spark: SparkSession,
  auditConf: Option[Config],
  /** The whole feed config, for the provenance fingerprint. Optional so the
    * many call sites that construct with just the audit block still compile;
    * they simply record an empty fingerprint. */
  feedConf: Option[Config] = None
) {
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

  /**
    * Stable digest of the AUDIT-visible feed configuration, so two batches
    * can be compared for "was this produced under the same settings?".
    *
    * Computed once per service instance. Values are never included — a
    * config can carry inline endpoints and identifiers, and this string
    * lands in a table many people can read — only the rendered structure is
    * hashed, which is enough to detect that something changed.
    */
  private lazy val configFingerprint: String =
    feedConf.map(AuditService.fingerprint).getOrElse("")

  private val fileTableDdl =
    "run_id STRING, entity STRING, file_name STRING, file_path STRING, checksum STRING, " +
      "size_bytes BIGINT, status STRING, reason STRING, event_ts TIMESTAMP"

  private val runTableDdl =
    "run_id STRING, entity STRING, stage STRING, status STRING, source_count BIGINT, " +
      "raw_count BIGINT, accepted_count BIGINT, rejected_count BIGINT, insert_count BIGINT, " +
      "update_count BIGINT, delete_count BIGINT, control_total STRING, message STRING, " +
      "event_ts TIMESTAMP, run_mode STRING, window_start STRING, window_end STRING, " +
      "framework_version STRING, config_fingerprint STRING, principal STRING"

  /** Columns added after the original run-audit DDL; pre-existing tables are
    * migrated in place (Hive appends ADD COLUMNS at the end, matching the
    * record's field order). */
  private val runTableAddedColumns = Seq(
    "run_mode" -> "STRING", "window_start" -> "STRING", "window_end" -> "STRING",
    "framework_version" -> "STRING", "config_fingerprint" -> "STRING", "principal" -> "STRING")

  /** The migration check runs ONCE per service instance, not on every
    * recordStage — repeated metadata reads and racing ALTERs from concurrent
    * stages were pure overhead. A lost cross-process ALTER race is absorbed
    * by re-checking: if another writer added the columns first, that is
    * success, not failure. */
  @volatile private var runTableColumnsEnsured = false

  private def ensureRunTableColumns(): Unit = {
    if (runTableColumnsEnsured) return
    if (spark.catalog.tableExists(runTable)) {
      val existing = spark.table(runTable).columns.map(_.toLowerCase).toSet
      val missing = runTableAddedColumns.filterNot { case (n, _) => existing.contains(n) }
      if (missing.nonEmpty) {
        val ddl = missing.map { case (n, t) => s"$n $t" }.mkString(", ")
        logger.warn(s"[Audit] Migrating $runTable: ADD COLUMNS ($ddl)")
        try spark.sql(s"ALTER TABLE $runTable ADD COLUMNS ($ddl)")
        catch {
          case e: Exception =>
            spark.catalog.refreshTable(runTable)
            val after = spark.table(runTable).columns.map(_.toLowerCase).toSet
            val stillMissing = runTableAddedColumns.filterNot { case (n, _) => after.contains(n) }
            if (stillMissing.nonEmpty) throw e
            logger.info(s"[Audit] $runTable columns added concurrently by another writer")
        }
      }
    }
    runTableColumnsEnsured = true
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
    val record = FileAuditRecord(ctx.runId, ctx.entity, fileName, filePath, checksum, sizeBytes,
      status, AuditService.sanitizeMessage(reason), now())
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
      counts.controlTotal.orNull, AuditService.sanitizeMessage(message), now(),
      ctx.mode, extractWindow._1, extractWindow._2,
      AuditService.frameworkVersion, configFingerprint, AuditService.principal
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

  /** A dry run records SUCCESS while writing nothing, so it never
    * checkpoints a batch. coalesce is required: `=!=` against a NULL message
    * yields NULL, which filter drops — silently losing real successes. */
  private def notDryRun: org.apache.spark.sql.Column = {
    import org.apache.spark.sql.functions.{coalesce, col, lit}
    coalesce(col("message"), lit("")) =!= "dry-run"
  }

  /** True when some OTHER run's raw stage succeeded over the same extract
    * window with rejected rows — the signal the HOLD recovery path uses to
    * re-append a held window instead of window-skipping it. Requires the
    * migrated ledger (window columns); false otherwise. */
  def windowHadRejects(entity: String, excludeRunId: String, start: String, end: String): Boolean = {
    if (!enabled || !spark.catalog.tableExists(runTable)) return false
    import org.apache.spark.sql.functions.col
    val cols = spark.table(runTable).columns.map(_.toLowerCase).toSet
    if (!cols.contains("window_start") || !cols.contains("window_end")) return false
    spark.table(runTable)
      .filter(col("entity") === entity &&
        col("stage") === com.hcsc.generic.ingest.runtime.Stages.Raw &&
        col("status") === com.hcsc.generic.ingest.runtime.StageStatus.Success &&
        col("run_id") =!= excludeRunId &&
        col("window_start") === start && col("window_end") === end &&
        col("rejected_count") > 0)
      .limit(1).count() > 0
  }

  /** Most recent OTHER run's successful raw-stage extract window for the
    * entity — the previous link the watermark-continuity check validates
    * against. None without a migrated ledger or a prior windowed run. */
  def lastRawWindow(entity: String, excludeRunId: String): Option[(String, String)] = {
    if (!enabled || !spark.catalog.tableExists(runTable)) return None
    import org.apache.spark.sql.functions.col
    val cols = spark.table(runTable).columns.map(_.toLowerCase).toSet
    if (!cols.contains("window_start") || !cols.contains("window_end")) return None
    spark.table(runTable)
      .filter(col("entity") === entity &&
        col("stage") === com.hcsc.generic.ingest.runtime.Stages.Raw &&
        col("status") === com.hcsc.generic.ingest.runtime.StageStatus.Success &&
        col("run_id") =!= excludeRunId &&
        col("window_start") =!= "")
      .orderBy(col("event_ts").desc)
      .select("window_start", "window_end")
      .limit(1)
      .collect()
      .headOption
      .map(r => (r.getString(0), r.getString(1)))
  }

  /** EXISTS-based success check: true iff the ledger holds ANY SUCCESS row
    * for (run, entity, stage). Unlike stageStatus's latest-wins convention,
    * a SUCCESS here is a monotone fact — later SKIPPED rows (resume) or
    * FAILED replay attempts never un-record it. This is the batch
    * CHECKPOINT predicate for decoupled curated processing. */
  def hasStageSuccess(runId: String, entity: String, stage: String): Boolean = {
    if (!enabled || !spark.catalog.tableExists(runTable)) return false
    import org.apache.spark.sql.functions.col
    spark.table(runTable)
      .filter(col("run_id") === runId && col("entity") === entity &&
        col("stage") === stage &&
        col("status") === com.hcsc.generic.ingest.runtime.StageStatus.Success &&
        notDryRun)
      .limit(1).count() > 0
  }

  /** One RAW batch eligible for (or selected into) curated batch
    * processing. runMode/window fields empty on pre-migration ledgers. */
  private def batchFrame(entity: String): Option[org.apache.spark.sql.DataFrame] = {
    if (!enabled || !spark.catalog.tableExists(runTable)) return None
    Some(spark.table(runTable).filter(
      org.apache.spark.sql.functions.col("entity") === entity))
  }

  /** RAW-successful runs (real data — dry-run SUCCESS rows excluded, they
    * write nothing) aggregated one row per run_id, ordered by ascending
    * raw-SUCCESS event_ts. The entity lock serializes raw runs, so this is
    * extraction commit order; window values are opaque serialized strings
    * and are never used for ordering. */
  private def rawSuccessBatches(entity: String): Seq[PendingBatch] = {
    import org.apache.spark.sql.functions._
    batchFrame(entity).fold(Seq.empty[PendingBatch]) { runs =>
      val cols = runs.columns.map(_.toLowerCase).toSet
      def optCol(n: String) = if (cols.contains(n)) quotedCol(n) else lit("")
      runs.filter(col("stage") === com.hcsc.generic.ingest.runtime.Stages.Raw &&
          col("status") === com.hcsc.generic.ingest.runtime.StageStatus.Success &&
          notDryRun)
        .groupBy(col("run_id"))
        // run_mode/window come from the SAME row as the first commit
        // (struct-min keyed on event_ts) — a later re-invocation under a
        // different mode must not swap in its metadata.
        .agg(min(struct(col("event_ts").as("ts"), optCol("run_mode").as("m"),
          optCol("window_start").as("ws"), optCol("window_end").as("we"))).as("s"))
        .select(col("run_id"), col("s.ts").as("raw_success_ts"), col("s.m").as("run_mode"),
          col("s.ws").as("window_start"), col("s.we").as("window_end"))
        .orderBy(col("raw_success_ts").asc)
        .collect()
        .map(r => PendingBatch(
          runId = r.getAs[String]("run_id"),
          runMode = Option(r.getAs[String]("run_mode")).filter(_.nonEmpty),
          windowStart = Option(r.getAs[String]("window_start")).getOrElse(""),
          windowEnd = Option(r.getAs[String]("window_end")).getOrElse(""),
          rawSuccessTs = r.getAs[Timestamp]("raw_success_ts")))
        .toSeq
    }
  }

  private def runIdsWith(entity: String, stage: String, status: String): Set[String] = {
    import org.apache.spark.sql.functions.col
    batchFrame(entity).fold(Set.empty[String]) { runs =>
      // dry-run rows checkpoint nothing (see hasStageSuccess)
      runs.filter(col("stage") === stage && col("status") === status && notDryRun)
        .select("run_id").distinct().collect().map(_.getString(0)).toSet
    }
  }

  /** Checkpoint 2 of decoupled operation: RAW-successful batches with NO
    * curated SUCCESS yet, in extraction commit order. */
  def pendingBatches(entity: String): Seq[PendingBatch] = {
    val curatedDone = runIdsWith(entity, com.hcsc.generic.ingest.runtime.Stages.Curated,
      com.hcsc.generic.ingest.runtime.StageStatus.Success)
    rawSuccessBatches(entity).filterNot(b => curatedDone.contains(b.runId))
  }

  /** Pending batches whose curated stage has FAILED at least once. */
  def failedBatches(entity: String): Seq[PendingBatch] = {
    val curatedFailed = runIdsWith(entity, com.hcsc.generic.ingest.runtime.Stages.Curated,
      com.hcsc.generic.ingest.runtime.StageStatus.Failed)
    pendingBatches(entity).filter(b => curatedFailed.contains(b.runId))
  }

  /** Last N RAW-successful batches regardless of curated state (forced
    * rebuild; content-idempotent under the freshness merge). */
  def lastRawSuccessBatches(entity: String, n: Int): Seq[PendingBatch] =
    rawSuccessBatches(entity).takeRight(n)

  /**
    * RAW-successful batches whose raw SUCCESS date falls in [from, to]
    * inclusive, bucketed on the SESSION-zone calendar.
    *
    * The session zone is what renders event_ts everywhere an operator reads
    * it — Hive queries, the batch control view, the run ledger — so
    * `--replay-from` / `--replay-to` must agree with that calendar.
    * `Timestamp.toLocalDateTime` would instead render in the JVM default
    * zone, and a driver JVM on local time with the framework's default UTC
    * session zone puts every run in the hours around midnight into the
    * wrong day.
    */
  def rawSuccessBatchesBetween(entity: String, from: java.time.LocalDate, to: java.time.LocalDate): Seq[PendingBatch] = {
    val zone = java.time.ZoneId.of(
      spark.conf.get("spark.sql.session.timeZone", java.time.ZoneId.systemDefault().getId))
    rawSuccessBatches(entity).filter { b =>
      val d = b.rawSuccessTs.toInstant.atZone(zone).toLocalDate
      !d.isBefore(from) && !d.isAfter(to)
    }
  }

  /**
    * Rows this run's raw stage actually WROTE, from its own ledger entry.
    *
    * The decoupled curated job re-reads those rows from the RAW table, so
    * this is an INDEPENDENT measure of how many rows it should have
    * accounted for — recorded by a different run, at a different time, from
    * a different frame. That independence is the whole point: deriving the
    * expectation from the curated result itself would make the accounting
    * identity tautological.
    *
    * None when the ledger is disabled, the run never succeeded at raw, or
    * the count was not measured (-1).
    */
  def rawRowCount(runId: String, entity: String): Option[Long] = {
    if (!enabled || !spark.catalog.tableExists(runTable)) return None
    import org.apache.spark.sql.functions.col
    spark.table(runTable)
      .filter(col("run_id") === runId && col("entity") === entity &&
        col("stage") === com.hcsc.generic.ingest.runtime.Stages.Raw &&
        col("status") === com.hcsc.generic.ingest.runtime.StageStatus.Success &&
        notDryRun)
      .orderBy(col("event_ts").asc)
      .select("raw_count")
      .collect()
      .headOption
      .map(_.getLong(0))
      .filter(_ >= 0L)
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
    new AuditService(spark, ConfigUtils.optConfig(feedConf, "audit"), Some(feedConf))

  /** Build identity from the jar manifest; "unknown" when running from
    * classes (tests, IDE) where no manifest is present. */
  private[ingest] lazy val frameworkVersion: String =
    com.hcsc.generic.ingest.BuildInfo.summary

  /** The identity the run executed under. Prefers the Hadoop login user so
    * a Kerberised cluster records the real principal rather than whatever
    * user.name happens to be set to; falls back when Hadoop is unavailable. */
  private[ingest] lazy val principal: String =
    try org.apache.hadoop.security.UserGroupInformation.getCurrentUser.getUserName
    catch { case _: Throwable => Option(System.getProperty("user.name")).getOrElse("unknown") }

  /**
    * SHA-256 over the config's rendered STRUCTURE, values excluded.
    *
    * Deliberately not a hash of the whole rendered config: feed configs
    * carry hostnames, database names and identifiers, and this value is
    * written to a table with a wide readership. Hashing the sorted key
    * paths detects "the shape of the configuration changed" — which is what
    * provenance needs — without republishing any of it.
    */
  private[ingest] def fingerprint(conf: Config): String = {
    import scala.collection.JavaConverters._
    val paths = conf.entrySet().asScala.map(_.getKey).toSeq.sorted.mkString("\n")
    val digest = java.security.MessageDigest.getInstance("SHA-256")
      .digest(paths.getBytes(java.nio.charset.StandardCharsets.UTF_8))
    "v1:" + digest.take(16).map("%02x".format(_)).mkString
  }

  private val CredentialPattern =
    "(?i)(password|passwd|pwd|secret|token|accesskey|access_key|credential)\\s*=\\s*[^;,&\\s]+".r

  /** Raw exception messages flow into the ledger: JDBC drivers embed the
    * connection URL (which can carry credentials) and messages can be
    * arbitrarily long. Redact credential-shaped pairs and bound the size. */
  private[ingest] def sanitizeMessage(message: String): String = {
    if (message == null) return ""
    val redacted = CredentialPattern.replaceAllIn(message, m => s"${m.group(1)}=***")
    if (redacted.length > 4000) redacted.take(4000) + "...(truncated)" else redacted
  }
}
