package com.hcsc.generic.ingest.audit

import com.hcsc.generic.ingest.runtime.{RunContext, StageStatus, Stages}
import com.hcsc.generic.ingest.transform.SharedSparkSession
import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite

/** Ledger-derived batch checkpoint queries + the batch control projection.
  * Histories are written through the REAL AuditService.recordStage so the
  * queries see genuine ledger shapes (STARTED/terminal event rows). */
class AuditPendingQueriesTest extends AnyFunSuite with SharedSparkSession {

  private val conf = ConfigFactory.parseString(
    """audit { database = "apq_audit", run_table = "run_audit" }""")
  private val feedConf = ConfigFactory.parseString(
    """audit { database = "apq_audit", run_table = "run_audit" }""")
  private val audit = new AuditService(spark,
    Some(conf.getConfig("audit")))
  private def ctx(runId: String, mode: String = "INCR") =
    RunContext(runId, "apq_feed", mode, "I")

  private def raw(runId: String, status: String, mode: String = "INCR", message: String = ""): Unit =
    audit.recordStage(ctx(runId, mode), Stages.Raw, status, message = message)
  private def curated(runId: String, status: String): Unit =
    audit.recordStage(ctx(runId), Stages.Curated, status)

  // Deterministic, order-encoded history (recordStage stamps real now();
  // successive calls are strictly ordered):
  //  A: raw SUCCESS, curated SUCCESS               -> done
  //  B: raw SUCCESS (FULL), curated SKIPPED        -> pending
  //  C: raw SUCCESS, curated FAILED                -> pending + failed
  //  D: raw FAILED only                            -> not eligible
  //  E: raw SUCCESS message=dry-run                -> not eligible (no data)
  //  F: raw SUCCESS then raw SKIPPED (resume),
  //     curated SUCCESS then curated SKIPPED       -> done (EXISTS beats latest)
  //  G: curated SUCCESS then curated FAILED retry  -> done (monotone checkpoint)
  locally {
    // The shared non-Hive session forgets tables between JVMs but their
    // WAREHOUSE LOCATIONS persist — purge the leftover physical dir so
    // CREATE TABLE cannot hit LOCATION_ALREADY_EXISTS on the next run.
    val warehouse = new java.io.File(
      new java.net.URI(spark.conf.get("spark.sql.warehouse.dir")).getPath, "apq_audit.db")
    def purge(f: java.io.File): Unit = {
      if (f.isDirectory) f.listFiles().foreach(purge)
      f.delete()
    }
    if (warehouse.exists()) purge(warehouse)
    spark.sql("DROP TABLE IF EXISTS apq_audit.run_audit")

    raw("A", StageStatus.Started); raw("A", StageStatus.Success); curated("A", StageStatus.Success)
    raw("B", StageStatus.Success, mode = "FULL"); curated("B", StageStatus.Skipped)
    raw("C", StageStatus.Success); curated("C", StageStatus.Failed)
    raw("D", StageStatus.Failed)
    raw("E", StageStatus.Success, message = "dry-run")
    raw("F", StageStatus.Success); curated("F", StageStatus.Success)
    raw("F", StageStatus.Skipped, message = "resume: already completed")
    curated("F", StageStatus.Skipped)
    raw("G", StageStatus.Success); curated("G", StageStatus.Success); curated("G", StageStatus.Failed)
  }

  test("pendingBatches: raw-SUCCESS minus curated-SUCCESS, ordered by raw commit, no dry-runs") {
    val pending = audit.pendingBatches("apq_feed")
    assert(pending.map(_.runId) == Seq("B", "C"),
      s"expected B,C in commit order, got ${pending.map(_.runId)}")
    assert(pending.head.runMode.contains("FULL"), "run_mode must come from the raw ledger row")
  }

  test("hasStageSuccess is EXISTS-based: later SKIPPED/FAILED rows never un-record it") {
    assert(audit.hasStageSuccess("F", "apq_feed", Stages.Raw),
      "resumed run's SKIPPED row must not hide the earlier raw SUCCESS")
    assert(audit.hasStageSuccess("G", "apq_feed", Stages.Curated),
      "a failed replay attempt must not un-checkpoint a curated-done batch")
    assert(!audit.hasStageSuccess("D", "apq_feed", Stages.Raw))
    // dry-run SUCCESS rows published nothing: they neither checkpoint a
    // batch nor prove a raw slice exists
    audit.recordStage(ctx("H"), Stages.Curated, StageStatus.Success, message = "dry-run")
    assert(!audit.hasStageSuccess("H", "apq_feed", Stages.Curated),
      "a dry-run curated SUCCESS must not checkpoint the batch")
    assert(!audit.hasStageSuccess("E", "apq_feed", Stages.Raw),
      "a dry-run raw SUCCESS must not satisfy the replay guard")
    // contrast: latest-wins view differs, deliberately
    assert(audit.stageStatus("G", "apq_feed", Stages.Curated).contains(StageStatus.Failed))
  }

  test("failedBatches = pending with at least one curated FAILED") {
    assert(audit.failedBatches("apq_feed").map(_.runId) == Seq("C"))
  }

  test("lastRawSuccessBatches and date-range select regardless of curated state") {
    val last2 = audit.lastRawSuccessBatches("apq_feed", 2).map(_.runId)
    assert(last2 == Seq("F", "G"), s"got $last2")
    val today = java.time.LocalDate.now(java.time.ZoneId.of(
      spark.conf.get("spark.sql.session.timeZone", java.time.ZoneId.systemDefault().getId)))
    val all = audit.rawSuccessBatchesBetween("apq_feed", today, today).map(_.runId)
    assert(all == Seq("A", "B", "C", "F", "G"))
    assert(audit.rawSuccessBatchesBetween("apq_feed",
      today.plusDays(1), today.plusDays(2)).isEmpty)
  }

  test("batch control projection: latest-terminal statuses, checkpoint truth, retry_count") {
    val bc = new BatchControl(spark, feedConf).batchControl("apq_feed")
    val rows = bc.collect().map(r => r.getAs[String]("batch_id") -> r).toMap

    assert(rows("G").getAs[String]("curated_status") == StageStatus.Failed,
      "display status is latest-terminal")
    assert(rows("G").getAs[Boolean]("curated_done"),
      "checkpoint truth is EXISTS — done despite the later failed retry")
    assert(rows("B").getAs[String]("curated_status") == StageStatus.Skipped)
    assert(!rows("B").getAs[Boolean]("curated_done"))
    assert(rows("C").getAs[Long]("retry_count") == 1L)
    assert(rows("D").getAs[Long]("retry_count") == 1L)
    assert(rows("A").getAs[Long]("retry_count") == 0L)
    assert(rows("B").getAs[String]("run_mode") == "FULL")
  }
}
