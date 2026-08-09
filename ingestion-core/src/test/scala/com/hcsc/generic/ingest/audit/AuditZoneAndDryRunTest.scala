package com.hcsc.generic.ingest.audit

import com.hcsc.generic.ingest.runtime.{RunContext, StageStatus, Stages}
import com.hcsc.generic.ingest.transform.SharedSparkSession
import com.typesafe.config.ConfigFactory
import java.time.{LocalDate, ZoneId}
import org.scalatest.funsuite.AnyFunSuite

/**
  * Two ledger behaviours that the existing suite cannot see:
  *
  *  1. `rawSuccessBatchesBetween` buckets by DATE. Every other date in the
  *     framework is a SESSION-zone date, and the ledger rows an operator
  *     reads in Hive are rendered in the session zone — so `--replay-from`
  *     / `--replay-to` must select on the same calendar the operator is
  *     reading. AuditPendingQueriesTest derives its expected date from the
  *     session zone, but the session zone and the JVM zone are identical
  *     under a default test session, so a JVM-zone implementation passes it.
  *     Here they are deliberately divergent (precedent: RetentionCutoffTest).
  *
  *  2. `curated_done` in the batch-control projection is documented as "the
  *     checkpoint truth the pending driver uses". A dry run records a
  *     SUCCESS row while writing nothing, and AuditService excludes those
  *     from every checkpoint query, so the projection must exclude them too
  *     or it contradicts the predicate it claims to mirror.
  */
class AuditZoneAndDryRunTest extends AnyFunSuite with SharedSparkSession {

  private val db = "azdr_audit"
  private val conf = ConfigFactory.parseString(
    s"""audit { database = "$db", run_table = "run_audit" }""")
  private val audit = new AuditService(spark, Some(conf.getConfig("audit")))
  private val feedConf = ConfigFactory.parseString(
    s"""audit { database = "$db", run_table = "run_audit" }""")

  private def ctx(runId: String) = RunContext(runId, "azdr_feed", "INCR", "I")

  locally {
    val warehouse = new java.io.File(
      new java.net.URI(spark.conf.get("spark.sql.warehouse.dir")).getPath, s"$db.db")
    def purge(f: java.io.File): Unit = {
      if (f.isDirectory) f.listFiles().foreach(purge)
      f.delete()
    }
    if (warehouse.exists()) purge(warehouse)
    spark.sql(s"DROP TABLE IF EXISTS $db.run_audit")

    // Z: a real batch.  DR: a DRY RUN that recorded SUCCESS but wrote nothing.
    audit.recordStage(ctx("Z"), Stages.Raw, StageStatus.Success)
    audit.recordStage(ctx("DR"), Stages.Raw, StageStatus.Success, message = "dry-run")
    audit.recordStage(ctx("DR"), Stages.Curated, StageStatus.Success, message = "dry-run")
  }

  private def withSessionZone[T](zone: String)(body: => T): T = {
    val prev = spark.conf.get("spark.sql.session.timeZone")
    spark.conf.set("spark.sql.session.timeZone", zone)
    try body finally spark.conf.set("spark.sql.session.timeZone", prev)
  }

  test("date-range replay selects on the SESSION-zone calendar, not the JVM's") {
    // Pick a session zone whose current date differs from the JVM's. Two
    // extremes 26h apart guarantee one of them disagrees with the JVM date
    // at every instant, so the test never silently degrades to a no-op.
    val jvmToday = LocalDate.now(ZoneId.systemDefault())
    val candidates = Seq("Pacific/Kiritimati", "Etc/GMT+12")
    val zone = candidates
      .find(z => LocalDate.now(ZoneId.of(z)) != jvmToday)
      .getOrElse(cancel("no divergent zone available"))

    withSessionZone(zone) {
      val sessionToday = LocalDate.now(ZoneId.of(zone))
      assert(sessionToday != jvmToday, "precondition: the two calendars must differ")

      val selected = audit.rawSuccessBatchesBetween("azdr_feed", sessionToday, sessionToday)
        .map(_.runId)
      assert(selected.contains("Z"),
        s"batch Z was recorded 'today' in session zone $zone ($sessionToday) and must be " +
          s"selected by that date; JVM zone says $jvmToday. Selecting on the JVM calendar " +
          "makes --replay-from/--replay-to disagree with the ledger the operator reads.")
    }
  }

  test("batch control curated_done excludes dry-run, matching the checkpoint predicate") {
    // The driver's view: a dry run checkpoints nothing.
    assert(!audit.hasStageSuccess("DR", "azdr_feed", Stages.Curated),
      "precondition: AuditService already excludes dry-run SUCCESS")
    assert(audit.pendingBatches("azdr_feed").map(_.runId).forall(_ != "DR"),
      "a dry run wrote no RAW data, so it is not a pending batch either")

    // The operator's view must agree with it.
    val rows = new BatchControl(spark, feedConf).batchControl("azdr_feed")
      .collect().map(r => r.getAs[String]("batch_id") -> r).toMap

    rows.get("DR").foreach { r =>
      assert(!r.getAs[Boolean]("curated_done"),
        "curated_done is documented as the checkpoint truth the pending driver uses, " +
          "but a dry-run SUCCESS reports done here while AuditService treats it as nothing. " +
          "An operator would read the batch as curated when it was never written.")
    }
  }

  test("a NULL message does not drop a SUCCESS row from the checkpoint queries") {
    // recordStage never writes NULL (sanitizeMessage maps null to ""), but
    // rows can reach the ledger from an older writer or an external insert.
    // A bare `col("message") =!= "dry-run"` evaluates to NULL for those and
    // filter drops them, silently un-checkpointing a completed batch.
    // Built from the table's CURRENT schema rather than a positional VALUES
    // list: the ledger is designed to gain columns over time (run_mode,
    // window_*, provenance...), and a hard-coded arity would break this test
    // on every such addition while telling us nothing about NULL handling.
    val schema = spark.table(s"$db.run_audit").schema
    val values = schema.map { f =>
      f.name.toLowerCase match {
        case "run_id"   => "NULLMSG"
        case "entity"   => "azdr_feed"
        case "stage"    => Stages.Raw
        case "status"   => StageStatus.Success
        case "message"  => null                       // <- the point of the test
        case "event_ts" => new java.sql.Timestamp(System.currentTimeMillis())
        case _ => f.dataType match {
          case org.apache.spark.sql.types.LongType => 0L
          case _ => null
        }
      }
    }
    spark.createDataFrame(
      java.util.Collections.singletonList(org.apache.spark.sql.Row(values: _*)), schema)
      .write.mode(org.apache.spark.sql.SaveMode.Append).insertInto(s"$db.run_audit")
    assert(audit.hasStageSuccess("NULLMSG", "azdr_feed", Stages.Raw),
      "a SUCCESS row with a NULL message must still count as a success")
    assert(audit.pendingBatches("azdr_feed").map(_.runId).contains("NULLMSG"),
      "it must also be visible as a pending batch")
  }
}
