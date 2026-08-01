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

  /** The feed-level schema block attached to a source config so connectors
    * and intake validate against the contract. No-op when absent; idempotent
    * when the config already carries it. */
  private def withSchema(conf: Config): Config =
    if (feedConf.hasPath("schema")) conf.withValue("schema", feedConf.getValue("schema"))
    else conf

  def run(): Unit = {
    // Entity-level run lease: acquired BEFORE any extraction, held through
    // the watermark commit — two concurrent runs of one entity would extract
    // overlapping windows and erase each other's curated writes.
    val lock = com.hcsc.generic.ingest.lock.LockService.fromConfig(spark, feedConf, logger)
    val held = if (ctx.dryRun) None else lock.map { l => l.acquire(ctx.entity, ctx.runId); l }
    try runInternal(held)
    catch {
      case e: Throwable =>
        // A failed run's in-memory read window must not linger: it would
        // accumulate in a long-lived driver and could feed a stale captured
        // upper to a later same-JVM attempt.
        try {
          val sourceType = ConfigUtils.optString(feedConf.getConfig("source"), "type").getOrElse("file")
          SourceRegistry.resolve(sourceType) match {
            case w: com.hcsc.generic.ingest.source.WatermarkAdvancing =>
              w.discardWindow(ctx.entity, Some(ctx.runId))
            case _ => ()
          }
        } catch { case _: Exception => () }
        throw e
    }
    finally {
      held.foreach(l =>
        try l.release(ctx.entity, ctx.runId)
        catch { case e: Exception => logger.warn(s"[Pipeline] Lock release failed: ${e.getMessage}") })
      cached.foreach(df => try df.unpersist(false) catch { case _: Exception => () })
      cached.clear()
    }
  }

  /**
    * --stage curated: rebuilds curated from an existing RAW slice — either
    * one run's rows (--run-id) or a whole ingest_dt partition
    * (--resume-ingest-dt). Runs under the SAME governance as a full run:
    * real run context, entity lock, mandatory ledger, reject handling,
    * contract validation — and NEVER touches the watermark (replay is not
    * new-source progress). Replaces the audit-less legacy curated-only path.
    */
  def curatedReplay(): Unit = {
    val lock = com.hcsc.generic.ingest.lock.LockService.fromConfig(spark, feedConf, logger)
    val held = if (ctx.dryRun) None else lock.map { l => l.acquire(ctx.entity, ctx.runId); l }
    try {
      requireLedger()
      validateFeedCompatibility()
      val rawConf = feedConf.getConfig("raw")
      val database = ConfigUtils.sqlIdentifier(rawConf, "database")
      val table = ConfigUtils.sqlIdentifier(rawConf, "table")
      val fullTable = s"$database.$table"
      require(spark.catalog.tableExists(fullTable),
        s"PIPE_003 curated replay requires the RAW table $fullTable to exist")

      val slice = cli.runId match {
        case Some(rid) =>
          // The replay publishes whatever the slice holds and stamps the run
          // SUCCESS — replaying a run whose raw stage never completed would
          // publish a PARTIAL slice that later --resume trusts as complete.
          if (audit.enabled) {
            val rawStatus = audit.stageStatus(rid, ctx.entity, Stages.Raw)
            require(rawStatus.contains(StageStatus.Success),
              s"PIPE_003 curated replay from run_id=$rid requires that run's raw stage to be " +
                s"SUCCESS in the ledger (found: ${rawStatus.getOrElse("no record")}). A partial " +
                "RAW slice must not be published as a complete curated build; re-run or --resume " +
                "the original run first.")
          } else
            logger.warn(s"[Pipeline] Replaying run_id=$rid WITHOUT a ledger check " +
              "(audit disabled): cannot verify the RAW slice is complete")
          logger.info(s"[Pipeline] Curated replay from RAW slice run_id=$rid")
          spark.table(fullTable).filter(col("run_id") === lit(rid))
        case None =>
          val dt = cli.resumeIngestDt.getOrElse(throw new IllegalArgumentException(
            "PIPE_003 --stage curated requires --run-id (replay one run) or " +
              "--resume-ingest-dt (replay a whole partition)"))
          require(spark.table(fullTable).columns.exists(_.equalsIgnoreCase("ingest_dt")),
            s"PIPE_003 $fullTable has no ingest_dt column; replay by --run-id instead")
          logger.info(s"[Pipeline] Curated replay from RAW partition ingest_dt=$dt flag=${ctx.rawFlag}")
          spark.table(fullTable)
            .filter(col("ingest_dt") === lit(dt) && col("file_type") === lit(ctx.rawFlag))
      }

      val rejectService = new RejectService(
        spark, ConfigUtils.optConfig(feedConf, "rejects"), contract, logger)
      val curatedConf =
        if (feedConf.hasPath("curated")) Some(feedConf.getConfig("curated")) else None

      val result = runStage(Stages.Curated, skippable = false) {
        val r = new CuratedStageRunner(spark, curatedConf, logger)
          .run(slice, ctx.mode, ctx, contract, Some(rejectService))
        val counts = r.map(x => StageCounts(
          insertCount = x.insertCount, updateCount = x.updateCount,
          deleteCount = x.deleteCount + x.absenceDeleteCount
        )).getOrElse(StageCounts())
        (r, counts)
      }.flatten

      reconcile(None, result)
      logger.info(s"==== Curated replay success entity=${ctx.entity} runId=${ctx.runId} " +
        "(watermark intentionally untouched) ====")
    } finally {
      held.foreach(l =>
        try l.release(ctx.entity, ctx.runId)
        catch { case e: Exception => logger.warn(s"[Pipeline] Lock release failed: ${e.getMessage}") })
      cached.foreach(df => try df.unpersist(false) catch { case _: Exception => () })
      cached.clear()
    }
  }

  /**
    * --validate-only: discovers files, extracts and validates physical
    * headers, resolves canonical mappings and (optionally) prints a mapping
    * explanation. Performs NO RAW write, NO CURATED write, NO file moves and
    * NO audit writes — safe for production support.
    */
  def validateOnly(explainMapping: Boolean): Unit = {
    logger.info(s"==== VALIDATE-ONLY entity=${ctx.entity} runId=${ctx.runId} " +
      "(no writes, no file moves) ====")

    val sourceWithSchema = withSchema(feedConf.getConfig("source"))

    // A disabled AuditService makes the "no writes" promise structural: even
    // a future change inside inspectHeaders cannot write audit records here.
    val noOpAudit = new AuditService(spark, None)
    val intake = new FileIntakeService(
      spark, sourceWithSchema, None, noOpAudit, logger)

    if (intake.managed) {
      val inspections = intake.inspectHeaders()
      var passed = 0
      var failed = 0
      inspections.foreach { case (name, physical, resolution) =>
        val violations = physical.issues ++ resolution.map(_.violations).getOrElse(Seq.empty)
        val hardFailures = contract.map(c =>
          violations.filter(_.kind.policyOf(c.policies) == com.hcsc.generic.ingest.schema.PolicyAction.Fail))
          .getOrElse(violations)
        val status = if (hardFailures.isEmpty) { passed += 1; "PASSED" } else { failed += 1; "FAILED" }
        logger.info(s"[ValidateOnly] file=$name status=$status" +
          (if (violations.nonEmpty) s" violations=${violations.mkString("; ")}" else ""))
        if (explainMapping) contract.foreach { c =>
          logger.info("\n" + com.hcsc.generic.ingest.schema.MappingExplanationRenderer
            .render(ctx.entity, name, c, physical.fields.map(_.trim), resolution))
        }
      }
      logger.info(s"[ValidateOnly] Summary: discovered=${inspections.size} passed=$passed failed=$failed")
    } else {
      // Static-path feeds: a read-only source read exercises the same
      // contract validation without writing anything.
      val sourceType = ConfigUtils.optString(sourceWithSchema, "type").getOrElse("file")
      val df = SourceRegistry.resolve(sourceType).read(spark, sourceWithSchema)
      logger.info(s"[ValidateOnly] Static-path feed validated; resolved columns: ${df.columns.mkString(",")}")
    }
  }

  /** The run ledger is mandatory: without it --resume silently degrades to a
    * full re-run, replay guards go blind and the spec's per-run count capture
    * has nowhere to live. Feeds that truly want no ledger must say so. */
  private def requireLedger(): Unit = {
    val optedOut = ConfigUtils.optConfig(feedConf, "audit")
      .flatMap(a => ConfigUtils.optBoolean(a, "enabled")).contains(false)
    if (!audit.enabled && !optedOut)
      throw new IllegalStateException(
        "PIPE_002 The run ledger is required: configure audit { database = ... } or opt out " +
          "explicitly with audit { enabled = false } (resume, replay guards and count capture " +
          "are all degraded without it)")
    if (ctx.resume && !audit.enabled)
      throw new IllegalStateException(
        "PIPE_002 --resume requires a functioning audit ledger: without stage history the " +
          "resume silently degrades into a full re-run")
  }

  /** Startup cross-section validation (CFG_001..CFG_009): configuration
    * combinations that are individually valid but jointly wrong fail here,
    * before any extraction, instead of corrupting data mid-pipeline. */
  private def validateFeedCompatibility(): Unit = {
    val problems = com.hcsc.generic.ingest.config.FeedCompatibilityValidator.validate(feedConf)
    if (problems.nonEmpty)
      throw new IllegalStateException(
        s"Feed configuration for '${ctx.entity}' failed compatibility validation:\n" +
          problems.map(p => s"  - $p").mkString("\n"))
  }

  private def runInternal(heldLock: Option[com.hcsc.generic.ingest.lock.LockService]): Unit = {
    logger.info(s"==== Pipeline start entity=${ctx.entity} mode=${ctx.mode} runId=${ctx.runId} " +
      s"dryRun=${ctx.dryRun} resume=${ctx.resume} ====")
    requireLedger()
    validateFeedCompatibility()

    val rawConf = feedConf.getConfig("raw")
    val curatedConf =
      if (feedConf.hasPath("curated")) Some(feedConf.getConfig("curated")) else None

    // ---- Stage: validate (file intake) --------------------------------------
    // The schema contract is attached before intake so per-file physical
    // header validation can run against it.
    val sourceConf = withSchema(feedConf.getConfig("source"))
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
    }.flatten

    if (intake.managed && staged.exists(_.isEmpty)) {
      audit.recordStage(ctx, Stages.Raw, StageStatus.Skipped, message = "No valid files to process")
      logger.info("[Pipeline] No valid files to process; run completes as a no-op")
      return
    }

    // ---- Stage: raw ---------------------------------------------------------
    val rejectService = new RejectService(
      spark, ConfigUtils.optConfig(feedConf, "rejects"), contract, logger)

    val rawOutcome: RawOutcome = runStage(Stages.Raw) {
      val outcome = runRaw(sourceConf, rawConf, staged, rejectService, intake)
      (outcome, outcome.counts)
    }.orElse(readRawSlice(rawConf)).getOrElse(
      throw new IllegalStateException("RAW stage produced no data and no prior RAW slice was found for resume")
    )

    val acceptedDf = rawOutcome.accepted

    // The lease was sized for a stage, not the whole run: renew between
    // stages so a long extraction cannot silently outlive the lock. A failed
    // renewal (entity claimed by another run after our lease expired) aborts
    // BEFORE the curated write.
    heldLock.foreach(_.renew(ctx.entity, ctx.runId))

    // ---- Stage: curated + publish -------------------------------------------
    // curatedOutcome distinguishes three states the watermark gate needs:
    //   None             -> stage skipped (--stage raw, or resume-skip)
    //   Some(None)       -> stage ran but published nothing (disabled/absent)
    //   Some(Some(r))    -> curated actually published
    val curatedOutcome: Option[Option[CuratedResult]] =
      if (cli.stage.equalsIgnoreCase("raw")) {
        audit.recordStage(ctx, Stages.Curated, StageStatus.Skipped, message = "--stage raw")
        logger.info("[Pipeline] --stage raw: curated stage skipped")
        None
      } else runStage(Stages.Curated) {
        val result = new CuratedStageRunner(spark, curatedConf, logger)
          .run(acceptedDf, ctx.mode, ctx, contract, Some(rejectService))
        val counts = result.map(r => StageCounts(
          insertCount = r.insertCount, updateCount = r.updateCount,
          deleteCount = r.deleteCount + r.absenceDeleteCount
        )).getOrElse(StageCounts())
        (result, counts)
      }
    val curatedResult: Option[CuratedResult] = curatedOutcome.flatten

    // ---- Completion: reconcile FIRST, then file lifecycle -------------------
    // Reconciliation runs before any irreversible file movement so a count
    // mismatch fails the run while the staged files are still in inprogress
    // (normal retry remains possible). complete() itself registers before
    // moving, so every partial-failure state is resolved idempotently by the
    // next run's duplicate policy.
    reconcile(Some(rawOutcome), curatedResult)
    staged.foreach(files => intake.complete(ctx, files))

    // Watermarks advance ONLY here, and only when the curated publish
    // actually happened in (or before, on resume) this run — a raw-only
    // execution must not burn the source window, whatever the route
    // (--stage raw, curated.enabled=false, or a missing curated block).
    // Feeds that are intentionally raw-only declare watermark.advance_after=RAW.
    if (!ctx.dryRun) {
      // Renew once more before the watermark commit — the single most
      // dangerous write to make while another run holds the entity.
      heldLock.foreach(_.renew(ctx.entity, ctx.runId))
      val sourceType = ConfigUtils.optString(sourceConf, "type").getOrElse("file")
      SourceRegistry.resolve(sourceType) match {
        case w: com.hcsc.generic.ingest.source.WatermarkAdvancing =>
          val advanceAfter = ConfigUtils.optConfig(feedConf, "watermark")
            .flatMap(wm => ConfigUtils.optString(wm, "advance_after"))
            .getOrElse("CURATED").toUpperCase
          // A resume-skip only proves a REAL prior publish when curated is
          // actually configured and enabled — runStage records SUCCESS even
          // for the no-op stage of a curated-less feed (vacuous success),
          // which must not unlock the watermark.
          val curatedConfigured = curatedConf.exists(c =>
            ConfigUtils.optBoolean(c, "enabled").getOrElse(true))
          val curatedResumedComplete = curatedConfigured && curatedOutcome.isEmpty &&
            !cli.stage.equalsIgnoreCase("raw") && ctx.resume &&
            audit.stageStatus(ctx.runId, ctx.entity, Stages.Curated).contains(StageStatus.Success)
          // rejects.on_reject_watermark = HOLD: rows diverted to the reject
          // table left the committed window — advancing over them makes them
          // permanently unreachable from the source. Holding keeps the window
          // open; the next run re-extracts it and the window-idempotency
          // guard's held-window exemption re-appends (recovering the rejects
          // once the cause is fixed).
          val holdForRejects = rejectService.onRejectWatermark == "HOLD" &&
            rawOutcome.counts.rejectedCount > 0
          // Intent-override patterns (BACKFILL, RAW_REPLAY) never commit the
          // watermark unless the feed declares ingestion.watermark_commit=true:
          // a backfill re-reads history and must not burn the live window.
          val patternCommitAllowed =
            com.hcsc.generic.ingest.config.IngestionPattern.commitAllowed(feedConf)
          if (!patternCommitAllowed) {
            w.discardWindow(ctx.entity, Some(ctx.runId))
            logger.info("[Pipeline] Watermark NOT advanced: ingestion.pattern is an intent " +
              "override (BACKFILL/RAW_REPLAY) with watermark_commit=false (default). The live " +
              "incremental window is preserved.")
          } else if (holdForRejects) {
            w.discardWindow(ctx.entity, Some(ctx.runId))
            logger.warn(s"[Pipeline] Watermark HELD: ${rawOutcome.counts.rejectedCount} row(s) " +
              "rejected and rejects.on_reject_watermark = HOLD. The source window stays open and " +
              "will be re-extracted next run; previously accepted rows re-append into RAW as " +
              "documented duplicates (the curated merge absorbs them). Fix the reject cause to " +
              "recover the rows, or switch to ADVANCE to accept the loss.")
          } else if (curatedResult.isDefined || curatedResumedComplete || advanceAfter == "RAW")
            w.advanceWatermark(spark, sourceConf, ctx.entity, ctx.runId, rawOutcome.accepted)
          else
            logger.warn(s"[Pipeline] Watermark NOT advanced: no curated publish in this run " +
              s"(stage=${cli.stage}, curated config ${if (curatedConf.isEmpty) "absent" else "present but produced no publish"}). " +
              "The source window will be re-read next run. Declare watermark.advance_after=RAW " +
              "for intentionally raw-only feeds.")
        case _ => ()
      }
    }

    logger.info(s"==== Pipeline success entity=${ctx.entity} runId=${ctx.runId} ====")
  }

  private final case class RawOutcome(
    accepted: DataFrame,
    counts: StageCounts,
    window: Option[(String, Option[String])] = None,
    duplicateVersions: Long = -1L,
    /** Rows the DEDUPLICATED_APPEND delivery mode skipped because the same
      * source-version identity already exists in RAW — the measured overlap
      * reread. 0 under AT_LEAST_ONCE_APPEND. */
    overlapSkipped: Long = 0L)

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
    // Attach the schema contract for connector-side header resolution, plus
    // the entity name and run id so stateful sources (JDBC watermarks,
    // RUN_ID query parameters) see the pipeline's execution context.
    val effectiveSource = withSchema(sourceConf
      .withValue("entity", ConfigValueFactory.fromAnyRef(ctx.entity))
      .withValue("run_id", ConfigValueFactory.fromAnyRef(ctx.runId)))

    val rawDatabase = ConfigUtils.sqlIdentifier(rawConf, "database")
    val rawTable = ConfigUtils.sqlIdentifier(rawConf, "table")
    val rawFullTable = s"$rawDatabase.$rawTable"

    // Header/content/data validation all run BEFORE any RAW write. On a
    // contract violation the outcome is audited (and staged files
    // quarantined when configured) before the failure propagates.
    val df0 =
      try {
        val df = readGroupedSource(effectiveSource, staged)
        validateContractBeforeRaw(df, rawDatabase, rawTable, rejectService)
        df
      } catch {
        case e: SchemaContractViolationException =>
          handleContractFailure(e, staged, intake)
          throw e
      }

    // Lineage: source identity from config, extract window from the source's
    // read (JDBC records the bounded watermark window it actually used).
    val sourceType = ConfigUtils.optString(effectiveSource, "type").getOrElse("file")
    val window = SourceRegistry.resolve(sourceType) match {
      case w: com.hcsc.generic.ingest.source.WatermarkAdvancing =>
        w.lastWindow(ctx.entity, Some(ctx.runId))
      case _ => None
    }
    // The extract window becomes part of the run ledger (spec: start/end
    // watermark recorded per run).
    audit.setExtractWindow(window.map(_._1), window.flatMap(_._2))
    // String-only reads: the pipeline attaches the schema CONTRACT as an
    // OBJECT under source.schema, so the schema NAME lives at
    // source.source_schema (with a string-typed source.schema still honored).
    def lineageStr(path: String): Option[String] =
      if (effectiveSource.hasPath(path) &&
          effectiveSource.getValue(path).valueType() == com.typesafe.config.ConfigValueType.STRING)
        Some(effectiveSource.getString(path))
      else None
    val lineage = com.hcsc.generic.ingest.transform.RawLineage(
      runId = ctx.runId,
      sourceSystem = lineageStr("system"),
      sourceDatabase = lineageStr("database"),
      sourceSchema = lineageStr("source_schema").orElse(lineageStr("schema")),
      sourceTable = lineageStr("table"),
      extractStart = window.map(_._1),
      extractEnd = window.flatMap(_._2)
    )
    val withMetaBase = RawMetadata.add(df0, ctx.rawFlag, lineage)
    // Change-detection fingerprint (opt-in raw.record_hash = true): stamped
    // after the metadata columns so only business content is hashed; the
    // curated merge uses it to skip no-change rewrites.
    val withMeta0 =
      if (ConfigUtils.optBoolean(rawConf, "record_hash").getOrElse(false))
        com.hcsc.generic.ingest.transform.RecordHash.stamp(
          withMetaBase, contract, recordHashOptions(rawConf))
      else withMetaBase
    // Opt-in extended lineage (raw.lineage_extended = true): per-record
    // source_modified_ts / source_operation / source_primary_key, and no
    // constant file_id for file-less sources. Stamped after the record hash
    // so the hash never covers technical columns.
    val withMetaExt =
      if (!ConfigUtils.optBoolean(rawConf, "lineage_extended").getOrElse(false)) withMeta0
      else {
        val modifiedColumn = ConfigUtils.optString(rawConf, "source_modified_column")
          .orElse(contract.flatMap(_.incrementalColumns.headOption))
          .orElse(ConfigUtils.optConfig(effectiveSource, "incremental")
            .flatMap(i => ConfigUtils.stringList(i, "watermark_columns").headOption))
        val softDelete = ConfigUtils.optConfig(feedConf, "curated")
          .flatMap(c => ConfigUtils.optConfig(c, "merge"))
          .flatMap(m => ConfigUtils.optConfig(m, "deletes"))
          .filter(d => ConfigUtils.optString(d, "mode").exists(_.equalsIgnoreCase("SOFT")))
          .flatMap(d => ConfigUtils.optString(d, "indicator_column").map { indicator =>
            val values = ConfigUtils.stringList(d, "indicator_values")
            (indicator, if (values.nonEmpty) values else Seq("true", "1", "y", "d"))
          })
        val primaryKey = contract.map(_.businessKeyColumns).filter(_.nonEmpty)
          .orElse(ConfigUtils.optConfig(feedConf, "curated")
            .flatMap(c => ConfigUtils.optConfig(c, "merge"))
            .map(m => ConfigUtils.stringList(m, "keys")))
          .getOrElse(Seq.empty)
        RawMetadata.addExtended(withMeta0, com.hcsc.generic.ingest.transform.ExtendedLineage(
          sourceModifiedColumn = modifiedColumn,
          softDeleteIndicator = softDelete,
          primaryKeyColumns = primaryKey,
          nullFileId = staged.isEmpty && !sourceType.equalsIgnoreCase("file")))
      }
    val withMeta1 = ctx.fileIdFilter match {
      case Some(fileId) if staged.isEmpty => withMetaExt.filter(col("file_id") === lit(fileId))
      case _ => withMetaExt
    }

    // Cross-run file idempotency (managed feeds): a crash after the RAW
    // append but before completion leaves the file in inprogress, and the
    // NEXT run (new run_id) re-reads it — the run_id guard below cannot see
    // that. Excluding already-loaded file_ids up front keeps every count,
    // reject and reconciliation consistent with what is actually written.
    val withMeta =
      if (ctx.dryRun || ctx.forceReprocess || !staged.exists(_.nonEmpty)) withMeta1
      else RawIdempotency.excludeLoadedFiles(spark, rawFullTable, withMeta1, logger)

    val sourceCount = track(withMeta.persist(StorageLevel.MEMORY_AND_DISK)).count()

    val split = rejectService.split(withMeta, ctx)
    val accepted = track(split.accepted.persist(StorageLevel.MEMORY_AND_DISK))

    val (windowSkipped, overlapSkipped) =
      writeRawIdempotently(accepted, rawConf, rawFullTable, rawDatabase, rawTable, window, rejectService)

    val acceptedCount = if (split.acceptedCount >= 0) split.acceptedCount else sourceCount
    // Measure rawCount from the table itself so the raw_equals_accepted
    // reconciliation check verifies the write instead of restating its input.
    // A window-skip (rerun of a failed run under a NEW run id) attributes by
    // the extract window instead — the rows exist under the failed run's id.
    // Tables without a run_id column (legacy) cannot attribute rows to this
    // run, so the count stays unmeasured there.
    val rawCount =
      if (ctx.dryRun) 0L
      else if (windowSkipped.isDefined)
        spark.table(rawFullTable)
          .filter(col("extract_start_ts") === lit(windowSkipped.get._1) &&
            col("extract_end_ts") === lit(windowSkipped.get._2))
          .count()
      else if (spark.catalog.tableExists(rawFullTable) &&
               spark.table(rawFullTable).columns.contains("run_id"))
        spark.table(rawFullTable).filter(col("run_id") === lit(ctx.runId)).count()
      else -1L

    // Optional raw version-duplicate measurement: with raw.idempotency_key
    // configured (e.g. source PK + freshness column), duplicate key-tuples
    // among THIS run's written rows indicate a source or extraction defect.
    val idempotencyKey = ConfigUtils.stringList(rawConf, "idempotency_key")
    val duplicateVersions =
      if (ctx.dryRun || idempotencyKey.isEmpty || windowSkipped.isDefined ||
          !spark.catalog.tableExists(rawFullTable)) -1L
      else {
        val cols = spark.table(rawFullTable).columns.map(_.toLowerCase).toSet
        if (!cols.contains("run_id") || !idempotencyKey.forall(k => cols.contains(k.toLowerCase))) -1L
        else spark.table(rawFullTable)
          .filter(col("run_id") === lit(ctx.runId))
          .groupBy(idempotencyKey.map(col): _*)
          .count()
          .filter(col("count") > 1)
          .count()
      }

    val counts = StageCounts(
      sourceCount = sourceCount,
      rawCount = rawCount,
      acceptedCount = acceptedCount,
      rejectedCount = split.rejectedCount,
      controlTotal = controlTotal(accepted)
    )
    RawOutcome(accepted, counts, window, duplicateVersions, overlapSkipped)
  }

  /** Multi-file batch safety: staged files were validated per-file at
    * intake; here alias-equivalent files (same canonical fingerprint) are
    * grouped and each group is read separately, then unioned by canonical
    * name so mixed header layouts can never contaminate one Spark read. */
  private def readGroupedSource(effectiveSource: Config, staged: Option[Seq[StagedFile]]): DataFrame = {
    val sourceType = ConfigUtils.optString(effectiveSource, "type").getOrElse("file")
    val source = SourceRegistry.resolve(sourceType)
    staged match {
      case Some(files) if files.nonEmpty =>
        val groups = files.groupBy(_.headerFingerprint).values.toSeq
        if (groups.size > 1)
          logger.info(s"[Pipeline] ${groups.size} header compatibility groups detected; reading separately")
        groups.map { group =>
          val conf = effectiveSource.withValue(
            "paths", ConfigValueFactory.fromIterable(group.map(_.stagedPath).asJava))
          source.read(spark, conf)
        }.reduce((a, b) => a.unionByName(b, allowMissingColumns = true))
      case _ =>
        source.read(spark, effectiveSource)
    }
  }

  /** Contract enforcement gate ahead of the RAW write: required-column hard
    * guard, content validation, and data/version validation (with row-level
    * nullability deferred to the reject stage when it owns that rule). */
  private def validateContractBeforeRaw(
    df: DataFrame,
    rawDatabase: String,
    rawTable: String,
    rejectService: RejectService
  ): Unit = contract.foreach { c =>
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
      SchemaValidator.versionMismatch(
        SchemaVersions.stored(spark, rawDatabase, rawTable),
        SchemaVersions.storedRequired(spark, rawDatabase, rawTable), c)
    SchemaValidator.enforce(violations, c.policies, logger)
  }

  /** RAW write with two idempotency guards, or the dry-run log line; the
    * schema version property is recorded after a real write. Returns the
    * extract window that caused a window-skip, when one did.
    *
    * Guard 1 (same run id): a prior attempt of THIS run committed rows but
    * died before recording SUCCESS.
    * Guard 2 (same extract window, different run id): a failed run's rows
    * are already in RAW and the operator reran WITHOUT --resume — a fresh
    * run id re-extracting the identical bounded window must not append the
    * same rows twice. Best-effort: SOURCE_CLOCK uppers differ per attempt,
    * so only MAX_VALUE windows (or identical clocks) match. */
  private def writeRawIdempotently(
    accepted: DataFrame,
    rawConf: Config,
    rawFullTable: String,
    rawDatabase: String,
    rawTable: String,
    window: Option[(String, Option[String])],
    rejectService: RejectService
  ): (Option[(String, String)], Long) = {
    if (ctx.dryRun) {
      logger.info("[Pipeline] DRY-RUN: skipping RAW write")
      return (None, 0L)
    }
    val runIdLoaded = spark.catalog.tableExists(rawFullTable) &&
      spark.table(rawFullTable).columns.contains("run_id") &&
      spark.table(rawFullTable).filter(col("run_id") === lit(ctx.runId)).limit(1).count() > 0

    val windowMatched: Option[(String, String)] =
      if (runIdLoaded || ctx.forceReprocess) None
      else window.flatMap { case (s, eOpt) =>
        eOpt.filter { e =>
          spark.catalog.tableExists(rawFullTable) && {
            val cols = spark.table(rawFullTable).columns.map(_.toLowerCase)
            cols.contains("extract_start_ts") && cols.contains("extract_end_ts") &&
              spark.table(rawFullTable)
                .filter(col("extract_start_ts") === lit(s) && col("extract_end_ts") === lit(e) &&
                  col("run_id") =!= lit(ctx.runId))
                .limit(1).count() > 0
          }
        }.map(e => (s, e))
      }

    // Held-window recovery (rejects.on_reject_watermark = HOLD): the matched
    // window was written by a run that REJECTED rows and held the watermark —
    // this re-extraction exists precisely to recover them, so it must append
    // rather than window-skip. Previously accepted rows duplicate in RAW
    // (documented at-least-once, same as drifted retries; curated dedups).
    val heldRecovery = windowMatched.exists { case (s, e) =>
      rejectService.onRejectWatermark == "HOLD" &&
        audit.windowHadRejects(ctx.entity, ctx.runId, s, e)
    }
    val windowLoaded = if (heldRecovery) None else windowMatched
    if (heldRecovery)
      logger.warn(s"[Pipeline] Held-window recovery: RAW already holds rows for extract window " +
        s"${windowMatched.get} from a run that rejected rows under HOLD. Appending the " +
        "re-extracted window — previously accepted rows will duplicate in RAW (curated dedup " +
        "absorbs them); rows that now pass the reject rules are recovered.")

    var overlapSkipped = 0L
    if (runIdLoaded) {
      logger.warn(s"[Pipeline] RAW already holds rows for run ${ctx.runId}; skipping write (idempotent replay)")
    } else if (windowLoaded.isDefined) {
      logger.warn(s"[Pipeline] RAW already holds rows for extract window ${windowLoaded.get} " +
        "(written by a previous failed/incomplete run); skipping write (window idempotency). " +
        "Use --force-reprocess to append anyway.")
    } else {
      // Drift detection: a prior failed attempt with the SAME window start
      // but a DIFFERENT end means new source rows arrived between attempts —
      // the exact-window guard cannot match, and this append will duplicate
      // the previously written overlap range in RAW. Curated dedup/merge
      // absorbs the duplicates; raw-level cleanup needs raw.idempotency_key
      // or manual intervention. Detection only — skipping instead would
      // permanently lose the delta between the two captured uppers.
      window.foreach { case (s, eOpt) =>
        if (!ctx.forceReprocess && eOpt.isDefined && spark.catalog.tableExists(rawFullTable)) {
          val cols = spark.table(rawFullTable).columns.map(_.toLowerCase)
          if (cols.contains("extract_start_ts") && cols.contains("extract_end_ts") &&
              spark.table(rawFullTable)
                .filter(col("extract_start_ts") === lit(s) &&
                  col("extract_end_ts") =!= lit(eOpt.get) &&
                  col("run_id") =!= lit(ctx.runId))
                .limit(1).count() > 0)
            logger.warn(s"[Pipeline] A previous failed attempt already wrote RAW rows for window " +
              s"start $s with a different end than ${eOpt.get} (the captured upper drifted between " +
              "attempts). Appending will DUPLICATE the overlapping range in RAW; the curated merge " +
              "deduplicates by key, and raw.idempotency_key monitoring can quantify the overlap.")
        }
      }
      // raw.delivery_mode = AT_LEAST_ONCE_APPEND (default) | DEDUPLICATED_APPEND.
      // Deduplicated mode anti-joins on the SOURCE record-version identity
      // (raw.idempotency_key: PK + version [+ operation] — never run_id), so
      // overlap rereads, drifted retries and held-window recoveries append
      // only genuinely new versions; the skipped count is the measured
      // overlap, reconciled below.
      val deliveryMode = ConfigUtils.optString(rawConf, "delivery_mode")
        .map(_.trim.toUpperCase).getOrElse("AT_LEAST_ONCE_APPEND")
      require(Seq("AT_LEAST_ONCE_APPEND", "DEDUPLICATED_APPEND").contains(deliveryMode),
        s"raw.delivery_mode '$deliveryMode' must be AT_LEAST_ONCE_APPEND or DEDUPLICATED_APPEND")
      val idempotencyKeyCols = ConfigUtils.stringList(rawConf, "idempotency_key")
      val toWrite =
        if (deliveryMode != "DEDUPLICATED_APPEND" || !spark.catalog.tableExists(rawFullTable)) accepted
        else {
          require(idempotencyKeyCols.nonEmpty,
            "RAW_004 raw.delivery_mode = DEDUPLICATED_APPEND requires raw.idempotency_key " +
              "(the source record-version identity: source PK + version column[s])")
          val existingCols = spark.table(rawFullTable).columns
          val missing = idempotencyKeyCols.filterNot(k =>
            existingCols.exists(_.equalsIgnoreCase(k)) && accepted.columns.exists(_.equalsIgnoreCase(k)))
          require(missing.isEmpty,
            s"RAW_004 raw.idempotency_key column(s) [${missing.mkString(", ")}] missing from the " +
              "RAW table or the incoming batch; DEDUPLICATED_APPEND cannot establish identity")
          val incoming = accepted.alias("i")
          val existing = spark.table(rawFullTable)
            .select(idempotencyKeyCols.map(k => col(existingCols.find(_.equalsIgnoreCase(k)).get)): _*)
            .distinct().alias("e")
          val condition = idempotencyKeyCols.map { k =>
            val iCol = accepted.columns.find(_.equalsIgnoreCase(k)).get
            val eCol = existingCols.find(_.equalsIgnoreCase(k)).get
            col(s"i.$iCol") <=> col(s"e.$eCol")
          }.reduce(_ && _)
          val fresh = incoming.join(existing, condition, "left_anti")
          val freshCount = fresh.count()
          overlapSkipped = accepted.count() - freshCount
          if (overlapSkipped > 0)
            logger.info(s"[Pipeline] DEDUPLICATED_APPEND: $overlapSkipped overlap-reread row(s) " +
              s"already in RAW by identity [${idempotencyKeyCols.mkString(", ")}]; " +
              s"appending $freshCount new version(s)")
          fresh
        }
      val sinkType = ConfigUtils.optString(rawConf, "type").getOrElse("hive")
      SinkRegistry.resolve(sinkType).write(spark, toWrite, rawConf)
    }
    contract.foreach(c => SchemaVersions.record(spark, rawDatabase, rawTable, c.version, Some(c), logger))
    // Record-hash recipe versioning (§11): the active recipe (base version +
    // canonicalization options) lives on the table so a recipe change is
    // DETECTED — hashes across two recipes are never comparable silently.
    if (ConfigUtils.optBoolean(rawConf, "record_hash").getOrElse(false) &&
        spark.catalog.tableExists(rawFullTable))
      recordHashVersionProperty(rawFullTable,
        com.hcsc.generic.ingest.transform.RecordHash.recipeVersion(recordHashOptions(rawConf)))
    (windowLoaded, overlapSkipped)
  }

  private def recordHashOptions(rawConf: Config): com.hcsc.generic.ingest.transform.RecordHashOptions = {
    val opts = ConfigUtils.optConfig(rawConf, "record_hash_options")
    com.hcsc.generic.ingest.transform.RecordHashOptions(
      trimValues = opts.flatMap(o => ConfigUtils.optBoolean(o, "trim")).getOrElse(false),
      uppercaseValues = opts.flatMap(o => ConfigUtils.optBoolean(o, "uppercase")).getOrElse(false))
  }

  /** Writes ingest.record_hash.version only when it changed (no per-run
    * ALTER chatter); a change is loudly warned — unchanged-row detection
    * treats every pre-recipe-change row as changed until rewritten. */
  private def recordHashVersionProperty(fullTable: String, version: String): Unit =
    try {
      val stored = spark.sql(s"SHOW TBLPROPERTIES $fullTable('ingest.record_hash.version')")
        .collect().headOption.map(_.getString(1))
        .filterNot(_.toLowerCase.contains("does not have property"))
      if (!stored.contains(version)) {
        stored.foreach(v => logger.warn(
          s"[Pipeline] record_hash recipe changed for $fullTable: stored '$v' -> '$version'. " +
            "Hashes across recipes are NOT comparable; the no-change filter treats every " +
            "pre-change row as changed until it is rewritten under the new recipe."))
        spark.sql(s"ALTER TABLE $fullTable SET TBLPROPERTIES ('ingest.record_hash.version' = '$version')")
      }
    } catch {
      case scala.util.control.NonFatal(e) =>
        logger.warn(s"[Pipeline] could not record the record_hash recipe version: ${e.getMessage}")
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

    raw.foreach { o =>
      val c = o.counts
      if (c.sourceCount >= 0 && c.acceptedCount >= 0 && c.rejectedCount >= 0) {
        val expected = c.sourceCount
        val actual = c.acceptedCount + c.rejectedCount
        checks += (("source_equals_accepted_plus_rejected", expected.toString, actual.toString, expected == actual))
      }
      if (!ctx.dryRun && c.rawCount >= 0 && c.acceptedCount >= 0) {
        // DEDUPLICATED_APPEND deliberately writes fewer rows than accepted:
        // the overlap already present by source-version identity.
        val expectedRaw = c.acceptedCount - o.overlapSkipped
        checks += (("raw_equals_accepted", expectedRaw.toString, c.rawCount.toString, c.rawCount == expectedRaw))
      }
      // Measured overlap reread (DEDUPLICATED_APPEND): informational record
      // so operators can see exactly how much of the window was re-extracted.
      if (o.overlapSkipped > 0)
        checks += (("raw_overlap_reread", o.overlapSkipped.toString, o.overlapSkipped.toString, true))
      // Raw version-duplicate check (raw.idempotency_key configured):
      // duplicate key-tuples inside one run's write indicate a source or
      // extraction defect, not the intentional overlap re-read.
      if (o.duplicateVersions >= 0)
        checks += (("raw_duplicate_versions", "0", o.duplicateVersions.toString, o.duplicateVersions == 0))
      // Watermark continuity (§11): this run's window start must equal the
      // previous run's recorded end (watermark advanced) or the previous
      // run's own start (watermark unchanged: held, empty window, raw-only
      // run, or failed publish). Anything else means the watermark store and
      // the run ledger disagree — history was skipped, reseeded or edited
      // outside the pipeline. The recorded start is the stored watermark
      // itself, never the overlap-widened read bound, so a configured
      // overlap does not affect this check.
      if (!ctx.dryRun) o.window.foreach { case (start, _) =>
        audit.lastRawWindow(ctx.entity, ctx.runId).foreach { case (prevStart, prevEnd) =>
          val passed = start == prevEnd || start == prevStart
          val expected =
            if (passed && start == prevStart && start != prevEnd) prevStart else prevEnd
          checks += (("watermark_continuity", expected, start, passed))
        }
      }
    }
    // Full accounting identity: every accepted RAW row must be explained by
    // the curated outcome — a distinct key that was inserted, updated,
    // tombstoned or ignored as stale, a quarantined null-key row, or an
    // in-batch dedup loser. The counts are DISJOINT (a tombstone is not also
    // an insert/update). Rows silently vanishing between RAW and CURATED
    // fail this.
    curated.foreach { r =>
      val rawAccepted = raw.map(_.counts.acceptedCount).getOrElse(-1L)
      if (!ctx.dryRun && rawAccepted >= 0) {
        val accounted = r.insertCount + r.updateCount + r.deleteCount + r.ignoredCount +
          r.nullKeyCount + r.dedupedCount + r.passthroughCount
        checks += (("curated_accounts_for_accepted_rows",
          rawAccepted.toString, accounted.toString, accounted == rawAccepted))
      }
    }

    if (checks.isEmpty) return
    audit.recordReconciliation(ctx, checks.toList)

    val failed = checks.filterNot(_._4)
    if (failed.nonEmpty) {
      // Default FAIL: the safety net must be ON unless a feed explicitly
      // accepts inconsistent loads with on_mismatch = WARN.
      val onMismatch = ConfigUtils.optConfig(feedConf, "audit")
        .flatMap(a => ConfigUtils.optString(a, "reconciliation.on_mismatch"))
        .getOrElse("FAIL").toUpperCase
      val summary = failed.map(f => s"${f._1}: expected=${f._2} actual=${f._3}").mkString("; ")
      if (onMismatch == "FAIL")
        throw new IllegalStateException(s"Reconciliation failed: $summary")
      else
        logger.warn(s"[Pipeline] Reconciliation mismatches: $summary")
    }
  }
}
