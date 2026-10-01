package com.hcsc.generic.ingest.audit

import com.hcsc.generic.ingest.runtime.{RunContext, StageStatus, Stages}
import com.hcsc.generic.ingest.transform.SharedSparkSession
import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite

/** `rawWindow`: the extract window a run's successful raw stage recorded —
  * under `raw.mode = SOURCE` the only description of that run's RAW slice,
  * so the curated replay re-reads exactly it from the source. Histories are
  * written through the REAL AuditService.recordStage so the query sees
  * genuine ledger shapes. */
class AuditRawWindowTest extends AnyFunSuite with SharedSparkSession {

  private val conf = ConfigFactory.parseString(
    """audit { database = "arw_audit", run_table = "run_audit" }""")
  private val audit = new AuditService(spark, Some(conf.getConfig("audit")))
  private val Entity = "arw_feed"
  private def ctx(runId: String, dryRun: Boolean = false) =
    RunContext(runId, Entity, "INCR", "I", dryRun = dryRun)

  private def raw(runId: String, status: String, window: Option[(String, String)],
                  message: String = ""): Unit = {
    audit.setExtractWindow(window.map(_._1), window.map(_._2))
    audit.recordStage(ctx(runId), Stages.Raw, status, message = message)
  }

  //  A: STARTED (no window yet), SUCCESS with window           -> Some
  //  B: SUCCESS without a window (source without tracking)     -> None
  //  C: FAILED with a window                                   -> None
  //  D: SUCCESS message=dry-run with a window                  -> None (no data)
  //  E: SUCCESS with window, then SKIPPED (resume) no window   -> Some (the SUCCESS row's)
  //  F: SUCCESS w1, SUCCESS w2 (--force-reprocess re-run)      -> Some(w2), latest wins
  //  G: curated SUCCESS only                                   -> None
  locally {
    purgeWarehouseDb("arw_audit")
    spark.sql("DROP TABLE IF EXISTS arw_audit.run_audit")

    raw("A", StageStatus.Started, None)
    raw("A", StageStatus.Success, Some(("2026-01-05|04.00.00", "2026-01-06|04.00.00")))
    raw("B", StageStatus.Success, None)
    raw("C", StageStatus.Failed, Some(("2026-01-06|04.00.00", "2026-01-07|04.00.00")))
    raw("D", StageStatus.Success, Some(("2026-01-06|04.00.00", "2026-01-07|04.00.00")), message = "dry-run")
    raw("E", StageStatus.Success, Some(("2026-01-07|04.00.00", "2026-01-08|04.00.00")))
    raw("E", StageStatus.Skipped, None, message = "resume: already completed")
    raw("F", StageStatus.Success, Some(("2026-01-08|04.00.00", "2026-01-09|04.00.00")))
    raw("F", StageStatus.Success, Some(("2026-01-08|04.00.00", "2026-01-10|04.00.00")))
    audit.setExtractWindow(Some("2026-01-10|04.00.00"), Some("2026-01-11|04.00.00"))
    audit.recordStage(ctx("G"), Stages.Curated, StageStatus.Success)
  }

  test("a raw SUCCESS row with a window yields exactly that window") {
    assert(audit.rawWindow("A", Entity).contains(("2026-01-05|04.00.00", "2026-01-06|04.00.00")))
  }

  test("a raw SUCCESS row without a window is not a replayable slice") {
    assert(audit.rawWindow("B", Entity).isEmpty)
  }

  test("a FAILED raw stage and a dry-run never yield a window") {
    assert(audit.rawWindow("C", Entity).isEmpty, "failed")
    assert(audit.rawWindow("D", Entity).isEmpty, "dry-run records SUCCESS while reading nothing to publish")
  }

  test("a later SKIPPED resume row does not hide the SUCCESS row's window") {
    assert(audit.rawWindow("E", Entity).contains(("2026-01-07|04.00.00", "2026-01-08|04.00.00")))
  }

  test("two raw SUCCESS rows: the latest window wins") {
    assert(audit.rawWindow("F", Entity).contains(("2026-01-08|04.00.00", "2026-01-10|04.00.00")))
  }

  test("other stages, other entities, unknown runs and a disabled ledger yield None") {
    assert(audit.rawWindow("G", Entity).isEmpty, "curated row only")
    assert(audit.rawWindow("A", "someone_else").isEmpty, "entity-scoped")
    assert(audit.rawWindow("nope", Entity).isEmpty)
    assert(new AuditService(spark, None).rawWindow("A", Entity).isEmpty, "disabled")
  }
}
