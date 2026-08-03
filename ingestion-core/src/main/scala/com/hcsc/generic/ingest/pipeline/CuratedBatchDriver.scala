package com.hcsc.generic.ingest.pipeline

import com.hcsc.generic.ingest.audit.{AuditService, PendingBatch}
import com.hcsc.generic.ingest.config.ConfigUtils
import com.hcsc.generic.ingest.model.Cli
import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import scala.util.control.NonFatal

/**
  * Decoupled curated batch driver (`--stage curated` + a batch selector):
  * resolves a run_id list from the LEDGER and sequences the existing
  * governed `curatedReplay` once per batch — lock, ledger, raw-SUCCESS
  * guard, rejects and staged publish are all inherited; this class writes
  * nothing itself.
  *
  * Checkpoint semantics: curatedReplay reuses the RAW run's id as its own
  * run context, so each successful replay lands a curated SUCCESS ledger
  * row under that run_id — which is exactly what `pendingBatches` excludes
  * next pass. Batches are processed PER RUN (never unioned): a unioned
  * publish under a fresh run_id would checkpoint nothing.
  *
  * Each batch replays under the MODE the ledger recorded for its raw run
  * (a blanket --mode FULL over an INCR batch could trigger
  * FULL_SNAPSHOT_ABSENCE semantics against a single-batch slice).
  *
  * No driver-level lock: every replay takes the entity lock itself, so a
  * live raw run may interleave between batches — that IS the decoupling.
  *
  * curated.pending {
  *   max_batches = 0          # 0 = unlimited
  *   on_failure  = "STOP"     # STOP (default) | CONTINUE
  * }
  */
final class CuratedBatchDriver(
  spark: SparkSession,
  feedConf: Config,
  cli: Cli,
  logger: Logger
) {

  def run(): Unit = {
    val audit = AuditService(spark, feedConf)
    requirePreconditions(audit)

    val pendingConf = ConfigUtils.optConfig(feedConf, "curated")
      .flatMap(c => ConfigUtils.optConfig(c, "pending"))
    val maxBatches = pendingConf.flatMap(p => ConfigUtils.optInt(p, "max_batches")).getOrElse(0)
    val onFailure = pendingConf.flatMap(p => ConfigUtils.optString(p, "on_failure"))
      .getOrElse("STOP").toUpperCase
    require(Seq("STOP", "CONTINUE").contains(onFailure),
      s"CFG_015 curated.pending.on_failure '$onFailure' must be STOP or CONTINUE")
    require(maxBatches >= 0, "CFG_015 curated.pending.max_batches must be >= 0")

    val selected = resolveSelector(audit)
    val batches = if (maxBatches > 0) selected.take(maxBatches) else selected
    if (selected.size > batches.size)
      logger.info(s"[CuratedBatchDriver] ${selected.size} batch(es) selected; processing the " +
        s"first ${batches.size} (curated.pending.max_batches = $maxBatches)")

    if (batches.isEmpty) {
      logger.info(s"[CuratedBatchDriver] entity=${cli.entity}: no batches match the selector; " +
        "no curated changes required")
      return
    }
    logger.info(s"[CuratedBatchDriver] entity=${cli.entity}: processing ${batches.size} " +
      s"batch(es) in raw-commit order: ${batches.map(_.runId).mkString(", ")}")

    var succeeded = List.empty[String]
    var failed = List.empty[(String, Throwable)]
    batches.foreach { b =>
      val mode = b.runMode.filter(_.nonEmpty).getOrElse(cli.mode)
      try {
        new IngestPipeline(spark, feedConf,
          cli.copy(runId = Some(b.runId), mode = mode, resume = false,
            pending = false, replayLast = None, replayFrom = None, replayTo = None,
            replayFailed = false, replaySourceSystem = None),
          logger).curatedReplay()
        succeeded ::= b.runId
      } catch {
        case NonFatal(e) if onFailure == "CONTINUE" =>
          logger.error(s"[CuratedBatchDriver] batch ${b.runId} FAILED " +
            "(on_failure=CONTINUE; moving on)", e)
          failed ::= (b.runId, e)
        case NonFatal(e) =>
          throw new IllegalStateException(
            s"PIPE_005 curated batch driver stopped at batch ${b.runId}: " +
              s"${succeeded.size} succeeded, 1 failed, " +
              s"${batches.size - succeeded.size - 1} not attempted " +
              "(on_failure=STOP; the failed batch stays pending — rerun after fixing)", e)
      }
    }
    if (failed.nonEmpty)
      throw new IllegalStateException(
        s"PIPE_005 curated batch driver: ${failed.size} of ${batches.size} batch(es) failed " +
          s"(${failed.map(_._1).mkString(", ")}); ${succeeded.size} succeeded and are " +
          "checkpointed — failed batches stay pending for the next pass")
    logger.info(s"[CuratedBatchDriver] entity=${cli.entity}: all ${succeeded.size} batch(es) " +
      "published and checkpointed")
  }

  /** The pending model only works when the ledger records real curated
    * SUCCESS: without a ledger there is no checkpoint, and with curated
    * absent/disabled every "SUCCESS" would be vacuous. */
  private def requirePreconditions(audit: AuditService): Unit = {
    require(audit.enabled,
      "PIPE_006 batch selectors require the run ledger (audit.database); the ledger IS the " +
        "curated checkpoint — without it there is nothing to select from or mark done")
    val curatedEnabled = ConfigUtils.optConfig(feedConf, "curated")
      .exists(c => ConfigUtils.optBoolean(c, "enabled").getOrElse(true))
    require(curatedEnabled,
      "PIPE_006 batch selectors require a configured, enabled curated block — a disabled " +
        "curated stage records vacuous SUCCESS rows that would falsely checkpoint batches")
  }

  private def resolveSelector(audit: AuditService): Seq[PendingBatch] = {
    val base: Seq[PendingBatch] =
      if (cli.replayFailed) audit.failedBatches(cli.entity)
      else if (cli.replayLast.isDefined) audit.lastRawSuccessBatches(cli.entity, cli.replayLast.get)
      else if (cli.replayFrom.isDefined || cli.replayTo.isDefined) {
        val from = cli.replayFrom.map(java.time.LocalDate.parse).getOrElse(java.time.LocalDate.MIN)
        val to = cli.replayTo.map(java.time.LocalDate.parse).getOrElse(java.time.LocalDate.MAX)
        audit.rawSuccessBatchesBetween(cli.entity, from, to)
      }
      else audit.pendingBatches(cli.entity) // --pending, or bare --replay-source-system

    cli.replaySourceSystem.fold(base)(filterBySourceSystem(base, _))
  }

  /** source_system lives on RAW rows, not in the ledger: resolve the run_ids
    * carrying it from the RAW table and intersect. PIPE_007 when the RAW
    * table does not carry the column (pre-lineage feeds). */
  private def filterBySourceSystem(batches: Seq[PendingBatch], system: String): Seq[PendingBatch] = {
    val rawConf = feedConf.getConfig("raw")
    val fullTable = s"${ConfigUtils.sqlIdentifier(rawConf, "database")}." +
      ConfigUtils.sqlIdentifier(rawConf, "table")
    require(spark.catalog.tableExists(fullTable),
      s"PIPE_007 --replay-source-system requires the RAW table $fullTable to exist")
    val raw = spark.table(fullTable)
    require(raw.columns.exists(_.equalsIgnoreCase("source_system")),
      s"PIPE_007 --replay-source-system requires a source_system column on $fullTable " +
        "(stamped by raw lineage); this RAW table does not carry it")
    import org.apache.spark.sql.functions.col
    val matching = raw.filter(col("source_system") === system)
      .select("run_id").distinct().collect().map(_.getString(0)).toSet
    batches.filter(b => matching.contains(b.runId))
  }
}
