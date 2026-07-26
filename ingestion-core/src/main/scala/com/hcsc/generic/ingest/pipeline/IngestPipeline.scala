package com.hcsc.generic.ingest.pipeline

import com.hcsc.generic.ingest.audit.{AuditService, StageCounts}
import com.hcsc.generic.ingest.config.ConfigUtils
import com.hcsc.generic.ingest.files.{FileIntakeService, StagedFile}
import com.hcsc.generic.ingest.model.Cli
import com.hcsc.generic.ingest.reject.RejectService
import com.hcsc.generic.ingest.runtime.{RunContext, StageStatus, Stages}
import com.hcsc.generic.ingest.schema.{SchemaContract, SchemaValidator}
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

  def run(): Unit = {
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
      val outcome = runRaw(sourceConf, rawConf, staged, rejectService)
      (Some(outcome), outcome.counts)
    }.getOrElse(readRawSlice(rawConf))

    val acceptedDf = rawOutcome.map(_.accepted).getOrElse(
      throw new IllegalStateException("RAW stage produced no data and no prior RAW slice was found for resume")
    )

    // ---- Stage: curated + publish -------------------------------------------
    val curatedResult: Option[CuratedResult] = runStage(Stages.Curated) {
      val result = new CuratedStageRunner(spark, curatedConf, logger).run(acceptedDf, ctx.mode, ctx)
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
    rejectService: RejectService
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
    val df0 = source.read(spark, effectiveSource)

    contract.foreach { c =>
      // Row-level nullability is diverted to the reject stage when configured;
      // failing the whole run here would preempt record-level handling.
      val violations = SchemaValidator.validateData(df0, c).filterNot(v =>
        rejectService.handlesContractNullability &&
          v.kind == com.hcsc.generic.ingest.schema.ViolationKind.NullabilityViolation)
      SchemaValidator.enforce(violations, c.policies, logger)
    }

    val withMeta0 = RawMetadata.add(df0, ctx.rawFlag, ctx.runId)
    val withMeta = ctx.fileIdFilter match {
      case Some(fileId) if staged.isEmpty => withMeta0.filter(col("file_id") === lit(fileId))
      case _ => withMeta0
    }

    val sourceCount = withMeta.persist(StorageLevel.MEMORY_AND_DISK).count()

    val split = rejectService.split(withMeta, ctx)
    val accepted = split.accepted.persist(StorageLevel.MEMORY_AND_DISK)

    if (!ctx.dryRun) {
      val sinkType = ConfigUtils.optString(rawConf, "type").getOrElse("hive")
      SinkRegistry.resolve(sinkType).write(spark, accepted, rawConf)
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
        val matched = files.filter(f => f.checksum.startsWith(id) || f.name == id)
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
