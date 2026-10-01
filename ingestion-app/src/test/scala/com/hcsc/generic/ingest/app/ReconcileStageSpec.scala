package com.hcsc.generic.ingest.app

import com.hcsc.generic.ingest.model.Cli
import com.hcsc.generic.ingest.pipeline.IngestPipeline
import com.hcsc.generic.ingest.runtime.FailureClass
import com.hcsc.generic.ingest.watermark.InMemoryWatermarkStore
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Path => JPath}
import java.sql.DriverManager

/**
  * Golden for `--stage reconcile` driven through `IngestMain` on a JDBC feed.
  *
  * Nothing exercised this path before the hive dispatch was added: the entity
  * lock and heartbeat, the `reconcile` ledger rows, the REPORT/FAIL policy,
  * the exit code and the lock release in `finally` were all unverified. This
  * spec pins them from the code as it stood BEFORE the dispatch change and
  * must pass unchanged after it.
  */
class ReconcileStageSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private var tempDir: JPath = _
  private val logger = Logger.getLogger(getClass.getName)
  private val h2Url = "jdbc:h2:mem:recon_stage_spec;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false"
  private val Audit = "rs_audit"
  private val Entity = "forms"

  private def h2(statements: String*): Unit = {
    Class.forName("org.h2.Driver")
    val conn = DriverManager.getConnection(h2Url, "sa", "")
    try { val st = conn.createStatement(); statements.foreach(st.execute); st.close() }
    finally conn.close()
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    tempDir = Files.createTempDirectory("recon-stage-spec-")
    System.setProperty("derby.stream.error.file", tempDir.resolve("derby.log").toString)
    spark = SparkSession.builder()
      .master("local[2]").appName("recon-stage-spec")
      .config("spark.sql.warehouse.dir", tempDir.resolve("warehouse").toAbsolutePath.toString)
      .config("javax.jdo.option.ConnectionURL",
        s"jdbc:derby:;databaseName=${tempDir.resolve("metastore_db").toAbsolutePath};create=true")
      .config("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver")
      .config("hive.metastore.schema.verification", "false")
      .config("spark.sql.shuffle.partitions", "2").config("spark.ui.enabled", "false")
      .enableHiveSupport().getOrCreate()

    IngestMain.registerConnectors()
    Seq("rs_raw", "rs_curated", Audit).foreach(db => spark.sql(s"CREATE DATABASE IF NOT EXISTS $db"))

    h2("DROP TABLE IF EXISTS forms",
      """CREATE TABLE forms (
        |  FileName VARCHAR(40), Amount INT, LastModifiedDatetime TIMESTAMP)""".stripMargin,
      "INSERT INTO forms VALUES ('F001', 100, '2026-01-01 09:00:00')",
      "INSERT INTO forms VALUES ('F002', 200, '2026-01-02 09:00:00')",
      "INSERT INTO forms VALUES ('F003', 300, '2026-01-03 09:00:00')")
    InMemoryWatermarkStore.clear()

    // Load once so source and curated agree at the start; this also creates
    // the ledger, reconciliation and lock tables the assertions read.
    new IngestPipeline(spark, feed("REPORT"),
      Cli(entity = Entity, mode = "FULL", runId = Some("rs-load-1")), logger).run()
  }

  override def afterAll(): Unit = {
    try { if (spark != null) spark.stop() } finally {
      import scala.collection.JavaConverters._
      Files.walk(tempDir).iterator().asScala.toSeq.reverse.foreach(Files.deleteIfExists)
    }
    super.afterAll()
  }

  private def feed(onMismatch: String): Config = ConfigFactory.parseString(
    s"""
       |entity = $Entity
       |schema { version = "1", columns = [
       |  { name = "file_name", type = "string", aliases = ["FileName"], business_key = true },
       |  { name = "amount", type = "int", aliases = ["Amount"], required = false },
       |  { name = "last_modified_datetime", type = "timestamp",
       |    aliases = ["LastModifiedDatetime"], required = false, incremental = true }
       |] }
       |source {
       |  type = jdbc
       |  url = "$h2Url"
       |  driver = "org.h2.Driver"
       |  user = "sa"
       |  password = ""
       |  table = "forms"
       |  dialect = "generic"
       |  health_check { enabled = false }
       |  incremental {
       |    watermark_type = "TIMESTAMP"
       |    watermark_columns = ["LastModifiedDatetime"]
       |    initial_value = "1900-01-01 00:00:00"
       |    watermark_store { type = "memory" }
       |  }
       |}
       |audit { enabled = true, database = $Audit }
       |concurrency { lock = "REQUIRED", database = $Audit, table = ingest_run_locks, lease_minutes = 5 }
       |raw { database = rs_raw, table = forms, format = parquet }
       |curated {
       |  enabled = true
       |  database = rs_curated
       |  table = forms
       |  format = parquet
       |  merge { keys = ["file_name"], freshness { column = "last_modified_datetime" } }
       |}
       |reconcile { enabled = true, on_mismatch = "$onMismatch" }
     """.stripMargin)

  /** The stage exactly as Control-M runs it, minus the JVM exit. */
  private def reconcile(onMismatch: String, runId: String): Int =
    IngestMain.execute(spark, ConfigFactory.empty(), feed(onMismatch),
      Cli(entity = Entity, mode = "INCR", stage = "reconcile", runId = Some(runId)))

  private def ledger(runId: String): Seq[(String, String)] =
    spark.table(s"$Audit.ingest_run_audit")
      .filter(col("run_id") === runId && col("stage") === "reconcile")
      .select("stage", "status").collect().map(r => (r.getString(0), r.getString(1))).toSeq

  private def checks(runId: String): Map[String, (String, String, Boolean)] =
    spark.table(s"$Audit.ingest_reconciliation").filter(col("run_id") === runId)
      .select("check_name", "expected", "actual", "passed").collect()
      .map(r => r.getString(0) -> (r.getString(1), r.getString(2), r.getBoolean(3))).toMap

  /** The newest lock-ledger row for the entity: (holder, action). A live
    * lease is a CLAIM newest; a released one ends in RELEASE. */
  private def latestLock(): Option[(String, String)] =
    spark.table(s"$Audit.ingest_run_locks").filter(col("entity") === Entity)
      .select("holder_run_id", "action", "event_ts").collect()
      .sortBy(r => (r.getTimestamp(2).getTime, r.getString(1))).lastOption
      .map(r => (r.getString(0), r.getString(1)))

  // ---------------------------------------------------------------------------

  test("a consistent feed: exit 0, reconcile SUCCESS on the ledger, three checks recorded, lock released") {
    val exit = reconcile("REPORT", "rs-recon-1")
    assert(exit == 0)
    assert(ledger("rs-recon-1") == Seq(("reconcile", "SUCCESS")))
    val c = checks("rs-recon-1")
    assert(c.keySet == Set("source_curated_cardinality", "source_keys_present_in_curated",
      "curated_keys_absent_from_source"), c.toString)
    assert(c("source_keys_present_in_curated") == ("0", "0", true))
    assert(c("curated_keys_absent_from_source")._3, "informational by construction")
    assert(latestLock().contains(("rs-recon-1", "RELEASE")), s"lease must be released: ${latestLock()}")
  }

  test("REPORT: a planted loss is recorded and the stage is marked FAILED, but the exit code is 0") {
    h2("INSERT INTO forms VALUES ('F404', 400, '2026-02-01 09:00:00')")
    val exit = reconcile("REPORT", "rs-recon-2")
    assert(exit == 0, "REPORT never fails the job — there is no batch to stop")
    val loss = checks("rs-recon-2")("source_keys_present_in_curated")
    assert(loss == ("0", "1", false), loss.toString)
    assert(ledger("rs-recon-2") == Seq(("reconcile", "FAILED")),
      "the ledger records the mismatch even under REPORT")
    assert(latestLock().contains(("rs-recon-2", "RELEASE")))
  }

  test("FAIL: the same loss exits DATA_INTEGRITY, records FAILED, and still releases the lock") {
    val exit = reconcile("FAIL", "rs-recon-3")
    assert(exit == FailureClass.DataIntegrity.exitCode, s"exit=$exit")
    assert(checks("rs-recon-3")("source_keys_present_in_curated") == ("0", "1", false))
    val rows = ledger("rs-recon-3")
    assert(rows.nonEmpty && rows.forall(_ == ("reconcile", "FAILED")), rows.toString)
    assert(latestLock().contains(("rs-recon-3", "RELEASE")), s"lease must be released even on FAIL: ${latestLock()}")
  }

  test("an invalid policy is CONFIGURATION, recorded on the ledger, lock released") {
    val exit = reconcile("MAYBE", "rs-recon-4")
    assert(exit == FailureClass.Configuration.exitCode, s"exit=$exit")
    assert(ledger("rs-recon-4").contains(("reconcile", "FAILED")))
    assert(latestLock().contains(("rs-recon-4", "RELEASE")))
  }
}
