package com.hcsc.generic.ingest.stage

import com.hcsc.generic.ingest.runtime.RunContext
import com.hcsc.generic.ingest.transform.SharedSparkSession
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions.col
import org.scalatest.funsuite.AnyFunSuite

/**
  * Curated batch lineage. RAW rows carry run_id; curated rows did not, so a
  * suspect curated row could not be traced to the batch that last wrote it
  * except by inferring from timestamps.
  *
  * Opt-in by DDL, exactly like `is_deleted`: declaring
  * `last_modified_run_id` on a curated table starts populating it, and
  * every table that does not declare it keeps working unchanged. That
  * matters here — a forced schema change would break every curated table
  * already deployed.
  */
class CuratedRunLineageTest extends AnyFunSuite with SharedSparkSession {

  import spark.implicits._

  private def conf(table: String) = ConfigFactory.parseString(
    s"""database = "lineage_db"
       |table = "$table"
       |merge { keys = ["id"], freshness { column = "src_ts" } }""".stripMargin)

  private def ctx(runId: String) = RunContext(runId, "lineage_feed", "INCR", "I")

  locally {
    purgeWarehouseDb("lineage_db")
    spark.sql("CREATE DATABASE IF NOT EXISTS lineage_db")
    Seq("with_lineage", "without_lineage").foreach(t =>
      spark.sql(s"DROP TABLE IF EXISTS lineage_db.$t"))
  }

  private def batch(rows: Seq[(Int, String, String)], withLineage: Boolean) = {
    val df = rows.toDF("id", "name", "src_ts")
      .withColumn("src_ts", col("src_ts").cast("timestamp"))
    if (withLineage) df.withColumn("last_modified_run_id", org.apache.spark.sql.functions.lit(""))
    else df
  }

  test("last_modified_run_id records the run that last wrote each key") {
    val svc = new CuratedService(spark, conf("with_lineage"))

    svc.process(batch(Seq(
      (1, "a-v1", "2026-01-01 10:00:00"),
      (2, "b-v1", "2026-01-01 10:00:00")), withLineage = true),
      "INCR", ctx("run-alpha"), None, None)

    def lineageOf(id: Int) = spark.table("lineage_db.with_lineage")
      .filter(col("id") === id).collect().head.getAs[String]("last_modified_run_id")

    assert(lineageOf(1) == "run-alpha")
    assert(lineageOf(2) == "run-alpha")

    // A later run updates only key 1. Key 2 must keep its ORIGINAL batch:
    // lineage records the last run that wrote THAT ROW, not the last run to
    // touch the table — otherwise it would be no better than a timestamp.
    svc.process(batch(Seq((1, "a-v2", "2026-01-01 12:00:00")), withLineage = true),
      "INCR", ctx("run-beta"), None, None)

    assert(lineageOf(1) == "run-beta", "the updated row moves to the new batch")
    assert(lineageOf(2) == "run-alpha",
      "an untouched row must still point at the batch that actually wrote it")
  }

  test("a curated table without the column is completely unaffected") {
    val svc = new CuratedService(spark, conf("without_lineage"))
    svc.process(batch(Seq((1, "x", "2026-01-01 10:00:00")), withLineage = false),
      "INCR", ctx("run-gamma"), None, None)

    val cols = spark.table("lineage_db.without_lineage").columns.map(_.toLowerCase)
    assert(!cols.contains("last_modified_run_id"),
      "the column must never be invented — existing deployed tables keep their schema")
    assert(spark.table("lineage_db.without_lineage").count() == 1)
  }

  test("the lineage column is framework-stamped, so it cannot be a freshness column") {
    assert(CuratedService.FrameworkAuditColumns.contains("last_modified_run_id"),
      "a column the framework overwrites every run would be a meaningless freshness " +
        "comparator — every incoming row would tie")
  }
}
