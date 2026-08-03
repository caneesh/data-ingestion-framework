package com.hcsc.generic.ingest.audit

import com.hcsc.generic.ingest.config.ConfigUtils
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
  * Read-only batch control surface: ONE row per batch (batch_id = run_id)
  * projected from the append-only `ingest_run_audit` ledger — the ledger
  * stays the single source of truth; nothing is ever written here.
  *
  * Two status views per stage, deliberately both exposed:
  *  - raw_status / curated_status: latest-terminal event (stageStatus
  *    ordering — display truth: shows a FAILED retry after a SUCCESS);
  *  - curated_done: EXISTS(curated SUCCESS) — the CHECKPOINT truth the
  *    pending driver uses (a later failed replay never un-checkpoints).
  * retry_count counts FAILED events across all stages of the batch.
  *
  * An equivalent SQL view for external tools ships as
  * ddl/ingest_batch_control_view.sql.
  */
final class BatchControl(spark: SparkSession, feedConf: Config) {

  private val runTable: Option[String] =
    ConfigUtils.optConfig(feedConf, "audit").filter(a =>
      ConfigUtils.optBoolean(a, "enabled").getOrElse(true) && a.hasPath("database")).map { a =>
      val db = ConfigUtils.sqlIdentifier(a, "database")
      val t = ConfigUtils.optString(a, "run_table").getOrElse("ingest_run_audit")
      ConfigUtils.requireSqlIdentifier(t, "audit.run_table")
      s"$db.$t"
    }

  def batchControl(entity: String): DataFrame = {
    val table = runTable.getOrElse(throw new IllegalStateException(
      "PIPE_006 the batch control view requires the run ledger (audit.database)"))
    require(spark.catalog.tableExists(table), s"PIPE_006 ledger table $table does not exist yet")
    val cols = spark.table(table).columns.map(_.toLowerCase).toSet
    def optCol(n: String) = if (cols.contains(n)) col(n) else lit("")

    val ev = spark.table(table)
      .filter(col("entity") === entity)
      .withColumn("terminal", when(col("status") === "STARTED", lit(0)).otherwise(lit(1)))

    // Latest-terminal per stage: max(struct(event_ts, terminal, field))
    // reproduces stageStatus ordering (latest event_ts; terminal beats
    // STARTED on exact ties).
    def latest(stage: String, field: org.apache.spark.sql.Column) =
      max(when(col("stage") === stage,
        struct(col("event_ts").as("ts"), col("terminal").as("t"), field.as("v"))))
        .getField("v")

    // run_mode/window from the FIRST raw SUCCESS row (struct-min on
    // event_ts) — a later re-invocation under a different mode must not
    // swap in its metadata.
    def firstRawSuccess(field: org.apache.spark.sql.Column) =
      min(when(col("stage") === "raw" && col("status") === "SUCCESS",
        struct(col("event_ts").as("ts"), field.as("v")))).getField("v")

    ev.groupBy(col("run_id").as("batch_id"))
      .agg(
        first(col("entity")).as("entity"),
        firstRawSuccess(optCol("run_mode")).as("run_mode"),
        firstRawSuccess(optCol("window_start")).as("extract_window_start"),
        firstRawSuccess(optCol("window_end")).as("extract_window_end"),
        latest("raw", col("status")).as("raw_status"),
        latest("curated", col("status")).as("curated_status"),
        max(when(col("stage") === "curated" && col("status") === "SUCCESS", 1).otherwise(0))
          .cast("boolean").as("curated_done"),
        max(when(col("stage") === "raw", col("raw_count"))).as("raw_count"),
        max(when(col("stage") === "raw", col("accepted_count"))).as("accepted_count"),
        max(when(col("stage") === "raw", col("rejected_count"))).as("rejected_count"),
        latest("curated", col("insert_count")).as("insert_count"),
        latest("curated", col("update_count")).as("update_count"),
        latest("curated", col("delete_count")).as("delete_count"),
        sum(when(col("status") === "FAILED", 1).otherwise(0)).as("retry_count"),
        min(col("event_ts")).as("first_event_ts"),
        max(col("event_ts")).as("last_event_ts"))
      .orderBy(col("first_event_ts").asc)
  }
}
