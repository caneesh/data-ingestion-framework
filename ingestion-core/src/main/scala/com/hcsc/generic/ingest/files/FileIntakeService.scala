package com.hcsc.generic.ingest.files

import com.hcsc.generic.ingest.audit.AuditService
import com.hcsc.generic.ingest.config.ConfigUtils
import com.hcsc.generic.ingest.runtime.RunContext
import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.sql.Timestamp

/** A file that passed intake and is staged (inprogress) for reading. */
final case class StagedFile(
  name: String,
  landingPath: String,
  stagedPath: String,
  checksum: String,
  sizeBytes: Long
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
    require(table.matches("[a-zA-Z_][a-zA-Z0-9_]*"), s"idempotency.registry_table '$table' is not a safe SQL identifier")
    s"$db.$table"
  }

  def managed: Boolean = layout.isDefined

  /** Stages landing files for processing. None = feed uses a static path. */
  def stage(ctx: RunContext): Option[Seq[StagedFile]] = layout.map { l =>
    val fs = FsUtils.fileSystem(l.landing, hadoopConf)
    // fromLanding=false marks leftovers of a crashed run already in inprogress
    val candidates =
      FsUtils.listFiles(fs, l.landing).map(_ -> true) ++
        FsUtils.listFiles(fs, l.inprogress).map(_ -> false)
    logger.info(s"[FileIntake] Found ${candidates.size} candidate file(s) in landing/inprogress")

    val knownChecksums = if (idempotencyEnabled) processedChecksums(ctx.entity) else Set.empty[String]

    candidates.flatMap { case (status, fromLanding) =>
      val file = status.getPath
      val name = file.getName
      // Skip validator sidecar files themselves
      if (isSidecar(name)) None
      else {
        val failures = validator.validate(fs, status)
        if (failures.nonEmpty) {
          val reason = failures.mkString("; ")
          val dest = if (ctx.dryRun) file else FsUtils.moveToDir(fs, file, l.quarantine)
          audit.recordFile(ctx, name, dest.toString, "", status.getLen, FileStatuses.Quarantined, reason)
          logger.warn(s"[FileIntake] Quarantined $name: $reason")
          None
        } else {
          val checksum = FsUtils.checksum(fs, file)
          if (idempotencyEnabled && knownChecksums.contains(checksum) && !ctx.forceReprocess) {
            handleDuplicate(ctx, fs, l, status, checksum)
          } else {
            val staged = if (ctx.dryRun) file else FsUtils.moveToDir(fs, file, l.inprogress)
            // Re-staged leftovers were audited VALIDATED when first staged;
            // don't write a duplicate audit row on restart.
            if (fromLanding)
              audit.recordFile(ctx, name, staged.toString, checksum, status.getLen, FileStatuses.Validated)
            else
              logger.info(s"[FileIntake] Re-staged leftover inprogress file $name (restart)")
            Some(StagedFile(name, file.toString, staged.toString, checksum, status.getLen))
          }
        }
      }
    }
  }

  private def handleDuplicate(
    ctx: RunContext,
    fs: org.apache.hadoop.fs.FileSystem,
    l: FolderLayout,
    status: org.apache.hadoop.fs.FileStatus,
    checksum: String
  ): Option[StagedFile] = {
    val file = status.getPath
    val name = file.getName
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
    val fs = FsUtils.fileSystem(l.processed, hadoopConf)
    files.foreach { f =>
      val staged = new org.apache.hadoop.fs.Path(f.stagedPath)
      l.archive.foreach(a => FsUtils.copyToDir(fs, staged, a))
      val dest = FsUtils.moveToDir(fs, staged, l.processed)
      audit.recordFile(ctx, f.name, dest.toString, f.checksum, f.sizeBytes, FileStatuses.Processed)
    }
    if (idempotencyEnabled) registerProcessed(ctx, files)
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

    // CREATE IF NOT EXISTS + append avoids the create/overwrite race between
    // concurrent first runs sharing one registry.
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $db")
    spark.sql(
      s"""CREATE TABLE IF NOT EXISTS $registryTable (
         |  checksum STRING, entity STRING, file_name STRING, file_path STRING,
         |  size_bytes BIGINT, run_id STRING, status STRING, processed_ts TIMESTAMP
         |) USING ORC""".stripMargin)
    rows.write.mode(SaveMode.Append).insertInto(registryTable)
  }
}

/** Raised when a managed feed finds no valid files to process; callers treat
  * this as a graceful no-op run rather than a failure. */
final class NoInputFilesException(message: String) extends RuntimeException(message)
