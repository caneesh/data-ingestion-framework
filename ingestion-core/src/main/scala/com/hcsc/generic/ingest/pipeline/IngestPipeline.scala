package com.hcsc.generic.ingest.pipeline

import com.hcsc.generic.ingest.audit.{AuditService, HeaderAuditRecord, StageCounts}
import com.hcsc.generic.ingest.config.ConfigUtils
import com.hcsc.generic.ingest.files.{FileIntakeService, StagedFile}
import com.hcsc.generic.ingest.model.Cli
import com.hcsc.generic.ingest.reject.RejectService
import com.hcsc.generic.ingest.runtime.{RunContext, StageStatus, Stages}
import com.hcsc.generic.ingest.schema.{ContentValidator, SchemaContract, SchemaContractViolationException, SchemaValidator, SchemaVersions, SchemaViolation, ViolationKind}
import com.hcsc.generic.ingest.sink.SinkRegistry
import com.hcsc.generic.ingest.source.SourceRegistry
import com.hcsc.generic.ingest.stage.{CuratedResult, CuratedStageRunner}
import com.hcsc.generic.ingest.transform.RawMetadata
import com.typesafe.config.{Config, ConfigValueFactory}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, expr, lit}
import org.apache.spark.storage.StorageLevel

import java.util.UUID
import scala.collection.JavaConverters._

/**
  * End-to-end run orchestrator: validate -> raw -> curated (publish) with
  * stage-level audit, restart/replay, reject handling, idempotent file intake
  * and reconciliation. Every capability is opt-in via the feed config, so
  * feeds using only the basic blocks behave exactly as before.
  */
final class IngestPipeline(
  spark: SparkSession,
  feedConf: Config,
  cli: Cli,
  logger: Logger
) {
  private val audit = AuditService(spark, feedConf)
  private val contract = SchemaContract.parse(feedConf)
    .orElse(SchemaContract.parse(feedConf.getConfig("source")))

  private val ctx = RunContext(
    runId = cli.runId.getOrElse(UUID.randomUUID().toString),
    entity = cli.entity,
    mode = cli.mode,
    rawFlag = cli.rawFlag.getOrElse(if (cli.mode == "FULL") "F" else "I"),
    dryRun = cli.dryRun,
    forceReprocess = cli.forceReprocess,
    fileIdFilter = cli.fileId,
    resume = cli.resume
  )

  /** DataFrames persisted during the run; released in run()'s finally so a
    * long-lived shared SparkSession does not accumulate executor cache. */
  private val cached = scala.collection.mutable.ArrayBuffer.empty[DataFrame]

  private def track(df: DataFrame): DataFrame = { cached += df; df }

  def run(): Unit =
    try runInternal()
    finally {
      cached.foreach(df => try df.unpersist(false) catch { case _: Exception => () })
      cached.clear()
    }

  private def runInternal(): Unit = {
    logger.info(s"==== Pipeline start entity=${ctx.entity} mode=${ctx.mode} runId=${ctx.runId} " +
      s"dryRun=${ctx.dryRun} resume=${ctx.resume} ====")

    val rawConf = feedConf.getConfig("raw")
    val curatedConf =
      if (feedConf.hasPath("curated")) Some(feedConf.getConfig("curated")) else None

    // ---- Stage: validate (file intake) --------------------------------------
    val sourceConf = feedConf.getConfig("source")
    val intake = new FileIntakeService(
      spark, sourceConf, ConfigUtils.optConfig(feedConf, "idempotency"), audit, logger)

    // Validate is never skipped on resume: intake is idempotent (leftover
    // inprogress files are re-staged) and later stages need the file list.
    val staged: Option[Seq[StagedFile]] = runStage(Stages.Validate, skippable = false) {
      val files = intake.stage(ctx).map(filterByFileId)
      files.foreach { fs =>
        logger.info(s"[Pipeline] ${fs.size} file(s) staged for processing")
      }
      (files, StageCounts(sourceCount = files.map(_.size.toLong).getOrElse(-1L)))
    }.getOrElse(None)

    if (intake.managed && staged.exists(_.isEmpty)) {
      audit.recordStage(ctx, Stages.Raw, StageStatus.Skipped, message = "No valid files to process")
      logger.info("[Pipeline] No valid files to process; run completes as a no-op")
      return
    }

    // ---- Stage: raw ---------------------------------------------------------
    val rejectService = new RejectService(
      spark, ConfigUtils.optConfig(feedConf, "rejects"), contract, logger)

    val rawOutcome: Option[RawOutcome] = runStage(Stages.Raw) {
      val outcome = runRaw(sourceConf, rawConf, staged, rejectService, intake)
      (Some(outcome), outcome.counts)
    }.getOrElse(readRawSlice(rawConf))

    val acceptedDf = rawOutcome.map(_.accepted).getOrElse(
      throw new IllegalStateException("RAW stage produced no data and no prior RAW slice was found for resume")
    )

    // ---- Stage: curated + publish -------------------------------------------
    val curatedResult: Option[CuratedResult] = runStage(Stages.Curated) {
      val result = new CuratedStageRunner(spark, curatedConf, logger).run(acceptedDf, ctx.mode, ctx, contract)
      val counts = result.map(r => StageCounts(
        insertCount = r.insertCount, updateCount = r.updateCount, deleteCount = r.deleteCount
      )).getOrElse(StageCounts())
      (result, counts)
    }.getOrElse(None)

    // ---- Completion: file lifecycle, registry, reconciliation ---------------
    staged.foreach(files => intake.complete(ctx, files))
    reconcile(rawOutcome, curatedResult)

    logger.info(s"==== Pipeline success entity=${ctx.entity} runId=${ctx.runId} ====")
  }

  private final case class RawOutcome(accepted: DataFrame, counts: StageCounts)

  /** Runs a stage unless --resume finds it already SUCCESS; records audit
    * STARTED/SUCCESS/FAILED around the body. Returns None when skipped. */
  private def runStage[T](stage: String, skippable: Boolean = true)(body: => (T, StageCounts)): Option[T] = {
    if (skippable && ctx.resume &&
        audit.stageStatus(ctx.runId, ctx.entity, stage).contains(StageStatus.Success)) {
      logger.info(s"[Pipeline] Stage $stage already SUCCESS for run ${ctx.runId}; skipping (resume)")
      audit.recordStage(ctx, stage, StageStatus.Skipped, message = "resume: already completed")
      return None
    }
    audit.recordStage(ctx, stage, StageStatus.Started)
    try {
      val (result, counts) = body
      audit.recordStage(ctx, stage, StageStatus.Success, counts,
        message = if (ctx.dryRun) "dry-run" else "")
      Some(result)
    } catch {
      case e: Throwable =>
        audit.recordStage(ctx, stage, StageStatus.Failed, message = String.valueOf(e.getMessage))
        throw e
    }
  }

  private def runRaw(
    sourceConf: Config,
    rawConf: Config,
    staged: Option[Seq[StagedFile]],
    rejectService: RejectService,
    intake: FileIntakeService
  ): RawOutcome = {
    // Point the source at the staged files (managed feeds) and attach the
    // schema contract for connector-side header resolution.
    var effectiveSource = sourceConf
    staged.foreach { files =>
      effectiveSource = effectiveSource.withValue(
        "paths", ConfigValueFactory.fromIterable(files.map(_.stagedPath).asJava))
    }
    if (feedConf.hasPath("schema"))
      effectiveSource = effectiveSource.withValue("schema", feedConf.getValue("schema"))

    val sourceType = ConfigUtils.optString(effectiveSource, "type").getOrElse("file")
    val source = SourceRegistry.resolve(sourceType)

    val rawDatabase = ConfigUtils.sqlIdentifier(rawConf, "database")
    val rawTable = ConfigUtils.sqlIdentifier(rawConf, "table")
    val rawFullTable = s"$rawDatabase.$rawTable"

    // Header/content/data validation all run BEFORE any RAW write. On a
    // contract violation the outcome is audited (and staged files
    // quarantined when configured) before the failure propagates.
    val df0 =
      try {
        val df = source.read(spark, effectiveSource)
        contract.foreach { c =>
          // Hard guard: required canonical columns must exist before RAW.
          // Connectors enforce this too; this covers every source type.
          val missingRequired = c.requiredColumns.map(_.name)
            .filterNot(n => df.columns.exists(_.equalsIgnoreCase(n)))
          if (missingRequired.nonEmpty)
            throw new SchemaContractViolationException(missingRequired.map(n =>
              SchemaViolation(ViolationKind.MissingColumn,
                s"Required column '$n' absent before RAW write")))

          val contentViolations = ContentValidator.validate(df, c, logger)
          if (contentViolations.nonEmpty)
            throw new SchemaContractViolationException(contentViolations)

          // Row-level nullability is diverted to the reject stage when
          // configured; failing here would preempt record-level handling.
          val violations = SchemaValidator.validateData(df, c).filterNot(v =>
            rejectService.handlesContractNullability &&
              v.kind == ViolationKind.NullabilityViolation) ++
            SchemaValidator.versionMismatch(SchemaVersions.stored(spark, rawDatabase, rawTable), c)
          SchemaValidator.enforce(violations, c.policies, logger)
        }
        df
      } catch {
        case e: SchemaContractViolationException =>
          handleContractFailure(e, staged, intake)
          throw e
      }

    val withMeta0 = RawMetadata.add(df0, ctx.rawFlag, ctx.runId)
    val withMeta = ctx.fileIdFilter match {
      case Some(fileId) if staged.isEmpty => withMeta0.filter(col("file_id") === lit(fileId))
      case _ => withMeta0
    }

    val sourceCount = track(withMeta.persist(StorageLevel.MEMORY_AND_DISK)).count()

    val split = rejectService.split(withMeta, ctx)
    val accepted = track(split.accepted.persist(StorageLevel.MEMORY_AND_DISK))

    if (!ctx.dryRun) {
      // Idempotency guard: if a prior attempt committed this run's rows to
      // RAW but died before recording stage SUCCESS, do not append twice.
      val alreadyLoaded = spark.catalog.tableExists(rawFullTable) &&
        spark.table(rawFullTable).columns.contains("run_id") &&
        spark.table(rawFullTable).filter(col("run_id") === lit(ctx.runId)).limit(1).count() > 0
      if (alreadyLoaded) {
        logger.warn(s"[Pipeline] RAW already holds rows for run ${ctx.runId}; skipping write (idempotent replay)")
      } else {
        val sinkType = ConfigUtils.optString(rawConf, "type").getOrElse("hive")
        SinkRegistry.resolve(sinkType).write(spark, accepted, rawConf)
      }
      contract.foreach(c => SchemaVersions.record(spark, rawDatabase, rawTable, c.version, logger))
    } else {
      logger.info("[Pipeline] DRY-RUN: skipping RAW write")
    }

    val acceptedCount = if (split.acceptedCount >= 0) split.acceptedCount else sourceCount
    val counts = StageCounts(
      sourceCount = sourceCount,
      rawCount = if (ctx.dryRun) 0L else acceptedCount,
      acceptedCount = acceptedCount,
      rejectedCount = split.rejectedCount,
      controlTotal = controlTotal(accepted)
    )
    RawOutcome(accepted, counts)
  }

  /** Audits a header/content contract failure and quarantines the staged
    * files when configured. The audit record is written BEFORE any file is
    * moved so validation information is captured safely first. */
  private def handleContractFailure(
    e: SchemaContractViolationException,
    staged: Option[Seq[StagedFile]],
    intake: FileIntakeService
  ): Unit = contract.foreach { c =>
    def byKind(kind: ViolationKind): String =
      e.violations.filter(_.kind == kind).map(_.message).mkString("; ")

    val quarantinePath =
      if (c.quarantineOnFailure) intake.quarantineDir.getOrElse("") else ""

    val record = HeaderAuditRecord(
      run_id = ctx.runId,
      entity = ctx.entity,
      source_files = staged.map(_.map(_.name).mkString(",")).getOrElse(""),
      validation_stage = "header_contract",
      validation_status = "FAILED",
      expected_canonical_columns = c.columnNames.mkString(","),
      actual_source_headers = e.resolution.map(_.actualHeaders.mkString(",")).getOrElse(""),
      normalized_source_headers = e.resolution.map(_.normalizedHeaders.mkString(",")).getOrElse(""),
      resolved_mappings = e.resolution
        .map(_.canonicalToActual.map { case (canonical, actual) => s"$actual->$canonical" }.mkString(","))
        .getOrElse(""),
      missing_required_columns = e.resolution.map(_.missingRequired.mkString(","))
        .filter(_.nonEmpty).getOrElse(byKind(ViolationKind.MissingColumn)),
      missing_optional_columns = e.resolution.map(_.missingOptional.map(_.name).mkString(",")).getOrElse(""),
      unexpected_columns = byKind(ViolationKind.ExtraColumn),
      duplicate_columns = byKind(ViolationKind.DuplicateHeader),
      positional_fallback_used = e.resolution.exists(_.positionalFallbackUsed),
      error_code = e.violations.headOption.map(_.kind.code).getOrElse(""),
      error_message = e.violations.map(_.toString).mkString("; "),
      quarantine_path = quarantinePath,
      event_ts = new java.sql.Timestamp(System.currentTimeMillis())
    )
    audit.recordHeaderValidation(ctx, record)

    if (c.quarantineOnFailure)
      staged.filter(_.nonEmpty).foreach { files =>
        intake.quarantineStaged(ctx, files, record.error_message)
      }
  }

  /** For --resume runs whose RAW stage already succeeded: re-read this run's
    * slice of the RAW table so curated can be replayed idempotently. */
  private def readRawSlice(rawConf: Config): Option[RawOutcome] = {
    if (!ctx.resume) return None
    val database = ConfigUtils.sqlIdentifier(rawConf, "database")
    val table = ConfigUtils.sqlIdentifier(rawConf, "table")
    val fullTable = s"$database.$table"
    if (!spark.catalog.tableExists(fullTable)) return None
    val slice = spark.table(fullTable).filter(col("run_id") === lit(ctx.runId))
    logger.info(s"[Pipeline] Resume: replaying curated from RAW slice run_id=${ctx.runId}")
    Some(RawOutcome(slice, StageCounts()))
  }

  private def filterByFileId(files: Seq[StagedFile]): Seq[StagedFile] =
    ctx.fileIdFilter match {
      case Some(id) =>
        // Exact match only: a checksum prefix could select multiple files.
        val matched = files.filter(f => f.checksum == id || f.name == id)
        logger.info(s"[Pipeline] --file-id $id matched ${matched.size} of ${files.size} staged file(s)")
        matched
      case None => files
    }

  private def controlTotal(df: DataFrame): Option[String] =
    ConfigUtils.optConfig(feedConf, "audit")
      .flatMap(a => ConfigUtils.optString(a, "control_total_expr"))
      .map { expression =>
        val value = df.agg(expr(expression)).first().get(0)
        String.valueOf(value)
      }

  /** Cross-stage consistency checks persisted to the reconciliation table. */
  private def reconcile(raw: Option[RawOutcome], curated: Option[CuratedResult]): Unit = {
    val checks = scala.collection.mutable.ArrayBuffer.empty[(String, String, String, Boolean)]

    raw.map(_.counts).foreach { c =>
      if (c.sourceCount >= 0 && c.acceptedCount >= 0 && c.rejectedCount >= 0) {
        val expected = c.sourceCount
        val actual = c.acceptedCount + c.rejectedCount
        checks += (("source_equals_accepted_plus_rejected", expected.toString, actual.toString, expected == actual))
      }
      if (!ctx.dryRun && c.rawCount >= 0 && c.acceptedCount >= 0) {
        checks += (("raw_equals_accepted", c.acceptedCount.toString, c.rawCount.toString, c.rawCount == c.acceptedCount))
      }
    }
    curated.foreach { r =>
      checks += (("curated_inserts_plus_updates_covered",
        s"<= ${r.publishedCount}", (r.insertCount + r.updateCount).toString,
        r.insertCount + r.updateCount <= r.publishedCount || r.publishedCount == 0))
    }

    if (checks.isEmpty) return
    audit.recordReconciliation(ctx, checks.toList)

    val failed = checks.filterNot(_._4)
    if (failed.nonEmpty) {
      val onMismatch = ConfigUtils.optConfig(feedConf, "audit")
        .flatMap(a => ConfigUtils.optString(a, "reconciliation.on_mismatch"))
        .getOrElse("WARN").toUpperCase
      val summary = failed.map(f => s"${f._1}: expected=${f._2} actual=${f._3}").mkString("; ")
      if (onMismatch == "FAIL")
        throw new IllegalStateException(s"Reconciliation failed: $summary")
      else
        logger.warn(s"[Pipeline] Reconciliation mismatches: $summary")
    }
  }
}
