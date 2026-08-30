package com.hcsc.generic.ingest.app

import com.hcsc.generic.ingest.jdbc.JdbcSource
import com.hcsc.generic.ingest.jdbc.reconcile.SourceReconciliationService
import com.hcsc.generic.ingest.jdbc.watermark.InMemoryWatermarkStore
import com.hcsc.generic.ingest.model.Cli
import com.hcsc.generic.ingest.pipeline.IngestPipeline
import com.hcsc.generic.ingest.sink.HiveSink
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Path => JPath}
import java.sql.DriverManager

/**
  * Independent SOURCE-vs-CURATED reconciliation.
  *
  * Every other check in the framework is a within-run identity, so a batch
  * lost OUTSIDE the pipeline — a dropped partition, a run that never
  * happened, a manual edit — leaves the ledger perfectly clean. These tests
  * create exactly that situation, which no other suite does, and assert the
  * comparison finds it.
  *
  * The most important case here is the one that would make the feature
  * useless if it were wrong: a row edited at source AFTER the last load must
  * NOT be reported as drift. Comparing "as of the watermark" produces
  * precisely that false alarm, which is why the comparison is key-based.
  */
class SourceReconciliationSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private var tempDir: JPath = _
  private val logger = Logger.getLogger(getClass.getName)
  private val h2Url = "jdbc:h2:mem:recon_spec;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false"

  private def h2(statements: String*): Unit = {
    Class.forName("org.h2.Driver")
    val conn = DriverManager.getConnection(h2Url, "sa", "")
    try { val st = conn.createStatement(); statements.foreach(st.execute); st.close() }
    finally conn.close()
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    tempDir = Files.createTempDirectory("recon-spec-")
    System.setProperty("derby.stream.error.file", tempDir.resolve("derby.log").toString)
    spark = SparkSession.builder()
      .master("local[2]").appName("recon-spec")
      .config("spark.sql.warehouse.dir", tempDir.resolve("warehouse").toAbsolutePath.toString)
      .config("javax.jdo.option.ConnectionURL",
        s"jdbc:derby:;databaseName=${tempDir.resolve("metastore_db").toAbsolutePath};create=true")
      .config("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver")
      .config("hive.metastore.schema.verification", "false")
      .config("spark.sql.shuffle.partitions", "2").config("spark.ui.enabled", "false")
      .enableHiveSupport().getOrCreate()

    JdbcSource.register(); HiveSink.register()
    spark.sql("CREATE DATABASE IF NOT EXISTS rc_raw")
    spark.sql("CREATE DATABASE IF NOT EXISTS rc_curated")

    h2("DROP TABLE IF EXISTS forms",
      """CREATE TABLE forms (
        |  FileName VARCHAR(40), Amount INT, LastModifiedDatetime TIMESTAMP)""".stripMargin,
      "INSERT INTO forms VALUES ('F001', 100, '2026-01-01 09:00:00')",
      "INSERT INTO forms VALUES ('F002', 200, '2026-01-02 09:00:00')",
      "INSERT INTO forms VALUES ('F003', 300, '2026-01-03 09:00:00')")
    InMemoryWatermarkStore.clear()

    // Load everything once so source and curated agree at the start.
    new IngestPipeline(spark, feedConf,
      Cli(entity = "forms", mode = "FULL", runId = Some("rc-load-1")), logger).run()
  }

  override def afterAll(): Unit = {
    try { if (spark != null) spark.stop() } finally {
      import scala.collection.JavaConverters._
      Files.walk(tempDir).iterator().asScala.toSeq.reverse.foreach(Files.deleteIfExists)
    }
    super.afterAll()
  }

  /** The contract carries the canonical <- source alias mapping, which is
    * what lets the comparison resolve `file_name` to `FileName` with no
    * extra configuration. */
  private def feedConf: Config = ConfigFactory.parseString(
    s"""
       |entity = forms
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
       |audit { enabled = false }
       |raw { database = rc_raw, table = forms, format = parquet }
       |curated {
       |  enabled = true
       |  database = rc_curated
       |  table = forms
       |  format = parquet
       |  merge { keys = ["file_name"], freshness { column = "last_modified_datetime" } }
       |}
       |reconcile { enabled = true }
     """.stripMargin)

  private def checks(): Map[String, (String, String, Boolean)] =
    new SourceReconciliationService(spark, feedConf, logger).run()
      .map(c => c.name -> (c.expected, c.actual, c.passed)).toMap

  // ---------------------------------------------------------------------------

  test("a consistent feed reconciles clean") {
    val r = checks()
    assert(r("source_curated_cardinality")._3, r("source_curated_cardinality").toString)
    assert(r("source_keys_present_in_curated")._3)
    assert(r("curated_keys_absent_from_source")._2 == "0")
  }

  test("a row edited at source AFTER the last load is NOT reported as drift") {
    // The false alarm that would sink this feature. Comparing "as of the
    // committed watermark" excludes this row on the source side (its
    // timestamp is now newer) while including the stale curated copy — a
    // permanent phantom mismatch. Key existence is timestamp-independent.
    h2("UPDATE forms SET Amount = 999, LastModifiedDatetime = '2026-06-01 09:00:00' " +
      "WHERE FileName = 'F001'")
    val r = checks()
    assert(r("source_keys_present_in_curated")._3,
      "an un-ingested EDIT is lag, not loss — the key still exists on both sides")
    assert(r("curated_keys_absent_from_source")._2 == "0")
  }

  test("a row that never reached curated is detected as loss") {
    // The failure no within-run check can see: the source has a row the
    // pipeline never carried across, and no run is misbehaving.
    h2("INSERT INTO forms VALUES ('F404', 400, '2026-02-01 09:00:00')")
    val r = checks()
    val loss = r("source_keys_present_in_curated")   // (expected, actual, passed)
    assert(!loss._3, "a source key with no curated row is data loss and must fail")
    assert(loss._1 == "0", "zero missing keys is what a healthy feed expects")
    assert(loss._2 == "1", s"exactly the un-ingested key must be counted: $loss")
    // Cardinality deliberately does NOT alarm here: this feed has business
    // keys, so Tier 2 is the alarm and cardinality is a recorded trend.
    // (Originally asserted the opposite; that design false-alarmed on any
    // keyed feed with in-batch duplicates or quarantined rows — the shipped
    // e2e seed data trips it in its very first scenario.)
    assert(r("source_curated_cardinality")._3,
      "keyed-feed cardinality is informational; Tier 2 carries the alarm")
  }

  test("rows deleted at source are reported as a NUMBER, not a failure") {
    // Under deletes.mode = IGNORE these are deliberately retained. The count
    // is the evidence that source deletes DO happen — the measurement that
    // tests a feed's 'no deletes expected' assumption — but it must not
    // fail a job, or feeds that legitimately retain history go permanently
    // red.
    h2("DELETE FROM forms WHERE FileName = 'F404'")   // undo the loss case
    h2("DELETE FROM forms WHERE FileName = 'F003'")   // a genuine source delete
    val r = checks()
    assert(r("source_keys_present_in_curated")._3, "no loss now that F404 is gone from source")
    assert(r("curated_keys_absent_from_source")._2 == "1",
      s"the retained upstream deletion must be counted: ${r("curated_keys_absent_from_source")}")
    assert(r("curated_keys_absent_from_source")._3,
      "informational by construction — it must never fail the job")
  }

  test("cardinality on a keyed feed records the counts but never alarms") {
    // Keyed curated is legitimately shorter than raw source rows (in-batch
    // dedup, quarantined null keys) and legitimately longer (retained
    // deletes) — either direction is normal, so for keyed feeds the check
    // is informational and Tier 2 carries the alarm.
    val r = checks()
    val card = r("source_curated_cardinality")
    assert(card._3, s"must always pass on a keyed feed: $card")
    assert(card._1.startsWith("source=") && card._2.startsWith("curated="),
      s"both counts must still be recorded for trending: $card")
  }

  test("null-key source rows are never reported as loss — CUR_001 quarantines them by design") {
    // The false alarm the shipped e2e feed would have raised: its seed data
    // deliberately includes a null-business-key row (scenario 4), which the
    // curated merge quarantines. A row that CANNOT be in curated must not be
    // counted as missing from it.
    h2("INSERT INTO forms VALUES (NULL, 900, '2026-03-01 09:00:00')")
    val r = checks()
    assert(r("source_keys_present_in_curated")._3,
      s"a null-key row is accounted for in ingest_rejects, not here: " +
        r("source_keys_present_in_curated").toString)
  }

  test("in-batch duplicate keys at source do not read as drift") {
    // Two source rows, one key: curated keeps one (dedup). Key-based
    // comparison must see one key present, not one row missing.
    h2("INSERT INTO forms VALUES ('F001', 101, '2026-03-02 09:00:00')")
    val r = checks()
    assert(r("source_keys_present_in_curated")._3,
      "a duplicated key still EXISTS in curated; dedup is not loss")
    assert(r("source_curated_cardinality")._3)
  }

  test("REPORT is the default policy; FAIL is opt-in") {
    val svc = new SourceReconciliationService(spark, feedConf, logger)
    assert(svc.onMismatch == "REPORT", "a nightly comparison that goes red gets silenced")
    val failing = ConfigFactory.parseString("""reconcile { on_mismatch = "FAIL" }""")
      .withFallback(feedConf)
    assert(new SourceReconciliationService(spark, failing, logger).onMismatch == "FAIL")
  }

  test("an invalid policy is rejected rather than silently treated as REPORT") {
    val bad = ConfigFactory.parseString("""reconcile { on_mismatch = "MAYBE" }""")
      .withFallback(feedConf)
    val e = intercept[IllegalArgumentException](
      new SourceReconciliationService(spark, bad, logger).run())
    assert(e.getMessage.contains("CFG_022"))
  }
}
