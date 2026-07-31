package com.hcsc.generic.ingest.files

import com.hcsc.generic.ingest.audit.{AuditService, HeaderAuditRecord}
import com.hcsc.generic.ingest.config.ConfigUtils
import com.hcsc.generic.ingest.runtime.RunContext
import com.hcsc.generic.ingest.schema.{BatchPolicy, HeaderFingerprint, HeaderResolution, PolicyAction, SchemaContract, SchemaContractViolationException, SchemaValidator, SchemaViolation, ViolationKind}
import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import java.sql.Timestamp

/** A file that passed intake and is staged (inprogress) for reading. */
final case class StagedFile(
  name: String,
  landingPath: String,
  stagedPath: String,
  checksum: String,
  sizeBytes: Long,
  headerFingerprint: Option[String] = None
)

object FileStatuses {
  val Validated = "VALIDATED"
  val Quarantined = "QUARANTINED"
  val SkippedDuplicate = "SKIPPED_DUPLICATE"
  val RejectedDuplicate = "REJECTED_DUPLICATE"
  val PendingApproval = "PENDING_APPROVAL"
  val Processed = "PROCESSED"
}

object DuplicatePolicy {
  val Skip = "SKIP"
  val Reject = "REJECT"
  val ReprocessWithApproval = "REPROCESS_WITH_APPROVAL"
}

/**
  * File intake for feeds with a managed folder lifecycle:
  *
  * 1. Lists landing (plus any inprogress leftovers from a crashed run —
  *    restart-safe: those files were never registered as processed).
  * 2. Validates each file (FileValidator); invalid files move to quarantine
  *    with the full reason recorded in the file audit.
  * 3. Computes the SHA-256 content checksum and consults the file registry;
  *    duplicates are handled per idempotency.duplicate_policy:
  *      SKIP                    -> moved to processed, audited, not loaded
  *      REJECT                  -> moved to quarantine, audited
  *      REPROCESS_WITH_APPROVAL -> left in landing awaiting --force-reprocess
  * 4. Valid, non-duplicate files move to inprogress and are returned.
  *
  * After a fully successful run, complete() moves staged files to processed
  * (optionally copying to archive) and registers their checksums; on failure
  * files stay in inprogress and are retried on the next run.
  *
  * Feeds without source.folders return None and keep the legacy static-path
  * behavior.
  */
final class FileIntakeService(
  spark: SparkSession,
  sourceConf: Config,
  idempotencyConf: Option[Config],
  audit: AuditService,
  logger: Logger
) {
  private val layout = FolderLayout.parse(sourceConf)
  private val validator = new FileValidator(ConfigUtils.optConfig(sourceConf, "validation"))
  private val hadoopConf = spark.sparkContext.hadoopConfiguration

  // Per-file header-contract validation (multi-file batch safety)
  private val contract = SchemaContract.parse(sourceConf)
  private val headerEnabled = ConfigUtils.optBoolean(sourceConf, "header").getOrElse(false)
  private val delimiter = ConfigUtils.optString(sourceConf, "delimiter").getOrElse(",").charAt(0)
  private val quoteChar = ConfigUtils.optString(sourceConf, "quote").getOrElse("\"").charAt(0)
  private val escapeChar = ConfigUtils.optString(sourceConf, "escape").getOrElse("\\").charAt(0)
  // Prefer source.encoding (the same level Spark's CSV option lives at) so
  // the physical header reader can never diverge from the data reader.
  private val encoding = ConfigUtils.optString(sourceConf, "encoding")
    .orElse(ConfigUtils.optConfig(sourceConf, "validation")
      .flatMap(v => ConfigUtils.optString(v, "encoding")))
    .getOrElse("UTF-8")

  private val idempotencyEnabled =
    idempotencyConf.exists(c => ConfigUtils.optBoolean(c, "enabled").getOrElse(true))

  private val duplicatePolicy = idempotencyConf
    .flatMap(c => ConfigUtils.optString(c, "duplicate_policy"))
    .getOrElse(DuplicatePolicy.Skip)
    .toUpperCase

  private lazy val registryTable: String = {
    val c = idempotencyConf.get
    val db = ConfigUtils.sqlIdentifier(c, "database")
    val table = ConfigUtils.optString(c, "registry_table").getOrElse("ingest_file_registry")
    ConfigUtils.requireSqlIdentifier(table, "idempotency.registry_table")
    s"$db.$table"
  }

  def managed: Boolean = layout.isDefined

  /** Stages landing files for processing. None = feed uses a static path. */
  def stage(ctx: RunContext): Option[Seq[StagedFile]] = layout.map { l =>
    val fs = FsUtils.fileSystem(l.landing, hadoopConf)
    val candidates = listCandidates(fs, l)
    logger.info(s"[FileIntake] Found ${candidates.size} candidate file(s) in landing/inprogress")

    val knownChecksums = if (idempotencyEnabled) processedChecksums(ctx.entity) else Set.empty[String]
    val stagedChecksums = scala.collection.mutable.Set.empty[String]

    candidates.flatMap { case (status, fromLanding) =>
      processCandidate(ctx, fs, l, status, fromLanding, knownChecksums, stagedChecksums)
    }
  }

  /** Lists candidate files from landing (new) and inprogress (leftover from crash). */
  private def listCandidates(
    fs: org.apache.hadoop.fs.FileSystem,
    l: FolderLayout
  ): Seq[(org.apache.hadoop.fs.FileStatus, Boolean)] =
    FsUtils.listFiles(fs, l.landing).map(_ -> true) ++
      FsUtils.listFiles(fs, l.inprogress).map(_ -> false)

  /** Processes a single candidate file: validate, check headers, handle duplicates, stage. */
  private def processCandidate(
    ctx: RunContext,
    fs: org.apache.hadoop.fs.FileSystem,
    l: FolderLayout,
    status: org.apache.hadoop.fs.FileStatus,
    fromLanding: Boolean,
    knownChecksums: Set[String],
    stagedChecksums: scala.collection.mutable.Set[String]
  ): Option[StagedFile] = {
    val file = status.getPath
    val name = file.getName

    if (isSidecar(name)) return None

    val failures = validator.validate(fs, status)
    if (failures.nonEmpty) {
      quarantineInvalid(ctx, fs, l, status, failures.mkString("; "))
      return None
    }

    headerCheck(ctx, fs, l, status) match {
      case HeaderCheck.Invalid => None
      case HeaderCheck.SkipEmpty => None
      case HeaderCheck.Ok(fingerprint) =>
        val checksum = FsUtils.checksum(fs, file)
        val duplicate = idempotencyEnabled && !ctx.forceReprocess &&
          (knownChecksums.contains(checksum) || stagedChecksums.contains(checksum))
        if (duplicate)
          handleDuplicate(ctx, fs, l, status, checksum, fromLanding)
        else
          stageValidFile(ctx, fs, l, status, checksum, fingerprint, fromLanding, stagedChecksums)
    }
  }

  /** Quarantines a file that failed validation. */
  private def quarantineInvalid(
    ctx: RunContext,
    fs: org.apache.hadoop.fs.FileSystem,
    l: FolderLayout,
    status: org.apache.hadoop.fs.FileStatus,
    reason: String
  ): Unit = {
    val file = status.getPath
    val dest = if (ctx.dryRun) file else FsUtils.moveToDir(fs, file, l.quarantine)
    audit.recordFile(ctx, file.getName, dest.toString, "", status.getLen, FileStatuses.Quarantined, reason)
    logger.warn(s"[FileIntake] Quarantined ${file.getName}: $reason")
  }

  /** Stages a valid, non-duplicate file for processing. */
  private def stageValidFile(
    ctx: RunContext,
    fs: org.apache.hadoop.fs.FileSystem,
    l: FolderLayout,
    status: org.apache.hadoop.fs.FileStatus,
    checksum: String,
    fingerprint: Option[String],
    fromLanding: Boolean,
    stagedChecksums: scala.collection.mutable.Set[String]
  ): Option[StagedFile] = {
    val file = status.getPath
    val name = file.getName

    if (idempotencyEnabled) stagedChecksums += checksum
    val staged = if (ctx.dryRun) file else FsUtils.moveToDir(fs, file, l.inprogress)

    if (fromLanding)
      audit.recordFile(ctx, name, staged.toString, checksum, status.getLen, FileStatuses.Validated)
    else
      logger.info(s"[FileIntake] Re-staged leftover inprogress file $name (restart)")

    Some(StagedFile(name, file.toString, staged.toString, checksum, status.getLen, fingerprint))
  }

  private sealed trait HeaderCheck
  private object HeaderCheck {
    case class Ok(fingerprint: Option[String]) extends HeaderCheck
    case object Invalid extends HeaderCheck
    case object SkipEmpty extends HeaderCheck
  }

  /** Validates one physical file's header against the contract BEFORE it can
    * join a multi-file Spark read. Failing files are audited then quarantined
    * (FILE_ATOMIC) or fail the whole batch (BATCH_ATOMIC). */
  private def headerCheck(
    ctx: RunContext,
    fs: org.apache.hadoop.fs.FileSystem,
    l: FolderLayout,
    status: org.apache.hadoop.fs.FileStatus
  ): HeaderCheck = {
    val c = contract.getOrElse(return HeaderCheck.Ok(None))
    if (!headerEnabled) return HeaderCheck.Ok(None)

    val file = status.getPath
    val name = file.getName
    val physical = PhysicalHeaderReader.read(fs, file, delimiter, quoteChar, escapeChar, encoding)

    // Empty / header-only handling
    if (physical.fields.nonEmpty && !physical.hasDataRows) {
      if (c.headerOnlyPolicy == "FAIL") {
        val v = Seq(SchemaViolation(ViolationKind.HeaderOnlyFile, s"File $name contains a header but no data rows"))
        failFile(ctx, fs, l, status, None, v, v)
        return HeaderCheck.Invalid
      } else {
        logger.warn(s"[FileIntake] $name is header-only; skipping per header_only_policy=WARN_AND_SKIP")
        // Real checksum keeps the audit trail accurate and idempotency-safe
        val checksum = FsUtils.checksum(fs, file)
        val dest = if (ctx.dryRun) file else FsUtils.moveToDir(fs, file, l.processed)
        audit.recordFile(ctx, name, dest.toString, checksum, status.getLen, "SKIPPED_HEADER_ONLY",
          "Header-only file skipped per policy")
        return HeaderCheck.SkipEmpty
      }
    }

    // NOTE: FileSource.read re-validates headers on the combined DataFrame.
    // The intake call validates each PHYSICAL file (multi-file safety); the
    // FileSource call validates what Spark actually assembled. Both are
    // needed; the duplicate work per file is intentional and cheap relative
    // to the read itself.
    val resolution =
      if (physical.fields.isEmpty) None
      else Some(SchemaValidator.validateHeaders(physical.fields.map(_.trim), c, logger))

    val violations = physical.issues ++ resolution.map(_.violations).getOrElse(Seq.empty)
    val failures = violations.filter(v => v.kind.policyOf(c.policies) == PolicyAction.Fail)

    if (failures.isEmpty) {
      HeaderCheck.Ok(Some(HeaderFingerprint.of(physical.fields.map(_.trim), c)))
    } else {
      failFile(ctx, fs, l, status, resolution, violations, failures)
      if (c.batchPolicy == BatchPolicy.BatchAtomic)
        throw new SchemaContractViolationException(failures, resolution)
      HeaderCheck.Invalid
    }
  }

  /** Audit first (validation info captured safely), then quarantine.
    * WARN-policy violations are logged and included in the audit detail
    * fields so the quarantined file's full context is preserved; only
    * FAIL-policy violations drive error_code/error_message. */
  private def failFile(
    ctx: RunContext,
    fs: org.apache.hadoop.fs.FileSystem,
    l: FolderLayout,
    status: org.apache.hadoop.fs.FileStatus,
    resolution: Option[HeaderResolution],
    violations: Seq[SchemaViolation],
    failures: Seq[SchemaViolation]
  ): Unit = {
    val file = status.getPath
    val name = file.getName
    val c = contract.get

    violations.filter(v => v.kind.policyOf(c.policies) == PolicyAction.Warn)
      .foreach(v => logger.warn(s"[FileIntake] WARN $name: $v"))
    audit.recordHeaderValidation(ctx, HeaderAuditRecord(
      run_id = ctx.runId,
      entity = ctx.entity,
      source_files = name,
      validation_stage = "file_header",
      validation_status = "FAILED",
      expected_canonical_columns = c.columnNames.mkString(","),
      actual_source_headers = resolution.map(_.actualHeaders.mkString(",")).getOrElse(""),
      normalized_source_headers = resolution.map(_.normalizedHeaders.mkString(",")).getOrElse(""),
      resolved_mappings = resolution
        .map(_.canonicalToActual.map { case (canonical, actual) => s"$actual->$canonical" }.mkString(","))
        .getOrElse(""),
      missing_required_columns = resolution.map(_.missingRequired.mkString(",")).getOrElse(""),
      missing_optional_columns = resolution.map(_.missingOptional.map(_.name).mkString(",")).getOrElse(""),
      unexpected_columns = violations.filter(_.kind == ViolationKind.ExtraColumn).map(_.message).mkString("; "),
      duplicate_columns = violations.filter(v =>
        v.kind == ViolationKind.DuplicateHeader || v.kind == ViolationKind.DuplicatePhysicalHeader)
        .map(_.message).mkString("; "),
      positional_fallback_used = false,
      error_code = failures.headOption.map(_.kind.code).getOrElse(""),
      error_message = failures.map(_.toString).mkString("; "),
      quarantine_path = l.quarantine.toString,
      event_ts = new Timestamp(System.currentTimeMillis())
    ))
    val reason = failures.map(_.toString).mkString("; ")
    val dest = if (ctx.dryRun) file else FsUtils.moveToDir(fs, file, l.quarantine)
    audit.recordFile(ctx, name, dest.toString, "", status.getLen, FileStatuses.Quarantined, reason)
    logger.warn(s"[FileIntake] Quarantined $name (header contract): $reason")
  }

  /** Read-only inspection of every candidate file's header for
    * validate-only / explain-mapping: no moves, no audit writes. */
  def inspectHeaders(): Seq[(String, PhysicalHeader, Option[HeaderResolution])] =
    layout.toSeq.flatMap { l =>
      val fs = FsUtils.fileSystem(l.landing, hadoopConf)
      (FsUtils.listFiles(fs, l.landing) ++ FsUtils.listFiles(fs, l.inprogress))
        .filterNot(s => isSidecar(s.getPath.getName))
        .map { status =>
          val physical = PhysicalHeaderReader.read(fs, status.getPath, delimiter, quoteChar, escapeChar, encoding)
          val resolution = contract.filter(_ => headerEnabled && physical.fields.nonEmpty)
            .map(c => SchemaValidator.validateHeaders(physical.fields.map(_.trim), c, logger))
          (status.getPath.getName, physical, resolution)
        }
    }

  private def handleDuplicate(
    ctx: RunContext,
    fs: org.apache.hadoop.fs.FileSystem,
    l: FolderLayout,
    status: org.apache.hadoop.fs.FileStatus,
    checksum: String,
    fromLanding: Boolean
  ): Option[StagedFile] = {
    val file = status.getPath
    val name = file.getName
    // An INPROGRESS leftover whose checksum is already registered is OUR OWN
    // interrupted complete() (registry first, moves second): its content was
    // ingested — finish the move to processed. The duplicate policy governs
    // NEW deliveries from landing; applying REJECT here would quarantine a
    // fully-processed file, and REPROCESS_WITH_APPROVAL would strand it in
    // inprogress on every subsequent run.
    if (!fromLanding) {
      val dest =
        if (ctx.dryRun) file
        else {
          l.archive.foreach { a =>
            try FsUtils.copyToDir(fs, file, a)
            catch { case e: Exception =>
              logger.warn(s"[FileIntake] Archive copy of recovered $name failed: ${e.getMessage}") }
          }
          FsUtils.moveToDir(fs, file, l.processed)
        }
      audit.recordFile(ctx, name, dest.toString, checksum, status.getLen,
        FileStatuses.SkippedDuplicate,
        "Crash recovery: checksum already registered as processed; completed the interrupted move")
      logger.warn(s"[FileIntake] Recovered $name from inprogress (already registered as " +
        "processed); moved to processed without re-ingesting")
      return None
    }
    duplicatePolicy match {
      case DuplicatePolicy.Reject =>
        val dest = if (ctx.dryRun) file else FsUtils.moveToDir(fs, file, l.quarantine)
        audit.recordFile(ctx, name, dest.toString, checksum, status.getLen,
          FileStatuses.RejectedDuplicate, s"Content checksum $checksum already processed")
        logger.warn(s"[FileIntake] Rejected duplicate $name (checksum $checksum)")
        None
      case DuplicatePolicy.ReprocessWithApproval =>
        audit.recordFile(ctx, name, file.toString, checksum, status.getLen,
          FileStatuses.PendingApproval, "Duplicate content; re-run with --force-reprocess to load")
        logger.warn(s"[FileIntake] Duplicate $name left in landing pending approval (--force-reprocess)")
        None
      case _ => // SKIP
        val dest = if (ctx.dryRun) file else FsUtils.moveToDir(fs, file, l.processed)
        audit.recordFile(ctx, name, dest.toString, checksum, status.getLen,
          FileStatuses.SkippedDuplicate, s"Content checksum $checksum already processed")
        logger.info(s"[FileIntake] Skipped duplicate $name (checksum $checksum)")
        None
    }
  }

  /** Moves already-staged files to quarantine (post-read validation failures
    * such as header contract or content validation). Returns the quarantine
    * destination paths. Audit records must be written by the caller BEFORE
    * invoking this, so validation information is captured before any move. */
  def quarantineStaged(ctx: RunContext, files: Seq[StagedFile], reason: String): Seq[String] =
    layout.toSeq.flatMap { l =>
      if (ctx.dryRun) files.map(_.stagedPath)
      else {
        val fs = FsUtils.fileSystem(l.quarantine, hadoopConf)
        files.map { f =>
          val dest = FsUtils.moveToDir(fs, new org.apache.hadoop.fs.Path(f.stagedPath), l.quarantine)
          audit.recordFile(ctx, f.name, dest.toString, f.checksum, f.sizeBytes, FileStatuses.Quarantined, reason)
          logger.warn(s"[FileIntake] Quarantined staged file ${f.name}: $reason")
          dest.toString
        }
      }
    }

  /** The configured quarantine directory, if this feed is managed. */
  def quarantineDir: Option[String] = layout.map(_.quarantine.toString)

  /** Moves staged files to processed (+archive copy) and registers checksums.
    * Only called after the whole pipeline succeeded — restart-safe. */
  def complete(ctx: RunContext, files: Seq[StagedFile]): Unit = layout.foreach { l =>
    if (ctx.dryRun || files.isEmpty) return
    // Registry FIRST, moves second: a crash in between leaves the file in
    // inprogress with its checksum registered — the next run's duplicate
    // policy resolves it idempotently (skip/reject). The old order (move
    // first) could leave a processed-but-unregistered file whose content
    // would be re-ingested if delivered again.
    if (idempotencyEnabled) registerProcessed(ctx, files)
    val fs = FsUtils.fileSystem(l.processed, hadoopConf)
    files.foreach { f =>
      val staged = new org.apache.hadoop.fs.Path(f.stagedPath)
      l.archive.foreach(a => FsUtils.copyToDir(fs, staged, a))
      val dest = FsUtils.moveToDir(fs, staged, l.processed)
      audit.recordFile(ctx, f.name, dest.toString, f.checksum, f.sizeBytes, FileStatuses.Processed)
    }
  }

  private def isSidecar(name: String): Boolean = {
    val suffix = ConfigUtils.optConfig(sourceConf, "validation")
      .flatMap(v => ConfigUtils.optConfig(v, "checksum"))
      .flatMap(c => ConfigUtils.optString(c, "sidecar_suffix"))
      .getOrElse(".sha256")
    name.endsWith(suffix)
  }

  private def processedChecksums(entity: String): Set[String] = {
    if (!spark.catalog.tableExists(registryTable)) Set.empty
    else {
      import org.apache.spark.sql.functions.col
      spark.table(registryTable)
        .filter(col("entity") === entity && col("status") === FileStatuses.Processed)
        .select("checksum")
        .collect()
        .map(_.getString(0))
        .toSet
    }
  }

  private def registerProcessed(ctx: RunContext, files: Seq[StagedFile]): Unit = {
    import spark.implicits._
    val c = idempotencyConf.get
    val db = ConfigUtils.sqlIdentifier(c, "database")
    val ts = new Timestamp(System.currentTimeMillis())
    val rows = files.map(f =>
      (f.checksum, ctx.entity, f.name, f.stagedPath, f.sizeBytes, ctx.runId, FileStatuses.Processed, ts)
    ).toDF("checksum", "entity", "file_name", "file_path", "size_bytes", "run_id", "status", "processed_ts")

    com.hcsc.generic.ingest.hive.HiveTables.appendEnsuringTable(
      spark, db, registryTable,
      "checksum STRING, entity STRING, file_name STRING, file_path STRING, " +
        "size_bytes BIGINT, run_id STRING, status STRING, processed_ts TIMESTAMP",
      rows)
  }
}
