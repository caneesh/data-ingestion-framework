package com.hcsc.generic.ingest.app

import com.hcsc.generic.ingest.model.Cli
import com.hcsc.generic.ingest.runtime.FailureClass
import com.hcsc.generic.ingest.watermark.InMemoryWatermarkStore
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Path => JPath}

/**
  * `--stage reconcile` for a hive source, driven through `IngestMain` on a
  * Hive-metastore fixture shaped like `bstar_raw`: a landing table
  * partitioned `inc_ful_flag / file_date / file_time` with a two-column
  * business key, compared against a curated table. Covers the dispatch, the
  * three checks, `where` scoping, null/blank keys, the keyless cardinality
  * assertion, CFG_022 / CFG_026, the exit codes, the lock, and the invariant
  * that the source table is never written.
  */
class HiveReconcileStageSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private var tempDir: JPath = _
  private val Src = "hr_src"
  private val Cur = "hr_cur"
  private val Audit = "hr_audit"
  private val Entity = "bstar"
  private val SourceTable = s"$Src.bstar"
  private val CuratedTable = s"$Cur.bstar"

  override def beforeAll(): Unit = {
    super.beforeAll()
    tempDir = Files.createTempDirectory("hive-recon-stage-spec-")
    System.setProperty("derby.stream.error.file", tempDir.resolve("derby.log").toString)
    spark = SparkSession.builder()
      .master("local[2]").appName("hive-recon-stage-spec")
      .config("spark.sql.warehouse.dir", tempDir.resolve("warehouse").toAbsolutePath.toString)
      .config("javax.jdo.option.ConnectionURL",
        s"jdbc:derby:;databaseName=${tempDir.resolve("metastore_db").toAbsolutePath};create=true")
      .config("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver")
      .config("hive.metastore.schema.verification", "false")
      .config("spark.sql.shuffle.partitions", "2").config("spark.ui.enabled", "false")
      .enableHiveSupport().getOrCreate()
    spark.sqlContext.setConf("spark.sql.caseSensitive", "false")

    IngestMain.registerConnectors()
    InMemoryWatermarkStore.clear()
    Seq(Src, Cur, Audit).foreach(db => spark.sql(s"CREATE DATABASE IF NOT EXISTS $db"))

    // The landing table: three I partitions and one F snapshot. F1 exists ONLY
    // in the F partition, which is what makes `where` scoping observable.
    spark.sql(s"CREATE TABLE $SourceTable (acct_nbr STRING, sub_seq STRING, val STRING) " +
      "STORED AS ORC PARTITIONED BY (inc_ful_flag STRING, file_date STRING, file_time STRING)")
    insertSource("I", "2025-01-07", "06.24.07", Seq(("A1", "1", "x"), ("A2", "1", "y")))
    insertSource("I", "2025-01-08", "04.48.31", Seq(("A3", "1", "z")))
    insertSource("F", "2025-01-10", "04.52.00",
      Seq(("A1", "1", "x"), ("A2", "1", "y"), ("A3", "1", "z"), ("F1", "1", "f")))

    spark.sql(s"CREATE TABLE $CuratedTable (acct_nbr STRING, sub_seq STRING, val STRING) STORED AS ORC")
    spark.sql(s"INSERT INTO $CuratedTable VALUES ('A1','1','x'), ('A2','1','y'), ('A3','1','z')")

    // A second pair for the contract-alias case: the source column is named
    // differently and the contract maps it to the canonical key name.
    spark.sql(s"CREATE TABLE $Src.bstar_alias (acct_number STRING, sub_seq STRING, val STRING) " +
      "STORED AS ORC PARTITIONED BY (inc_ful_flag STRING, file_date STRING, file_time STRING)")
    spark.sql(s"INSERT INTO $Src.bstar_alias PARTITION (inc_ful_flag='I', file_date='2025-01-07', file_time='06.24.07') " +
      "VALUES ('B1','1','p'), ('B2','1','q')")
    spark.sql(s"CREATE TABLE $Cur.bstar_alias (acct_nbr STRING, sub_seq STRING, val STRING) STORED AS ORC")
    spark.sql(s"INSERT INTO $Cur.bstar_alias VALUES ('B1','1','p'), ('B2','1','q')")
  }

  override def afterAll(): Unit = {
    try { if (spark != null) spark.stop() } finally {
      import scala.collection.JavaConverters._
      Files.walk(tempDir).iterator().asScala.toSeq.reverse.foreach(Files.deleteIfExists)
    }
    super.afterAll()
  }

  private def insertSource(flag: String, date: String, time: String, rows: Seq[(String, String, String)]): Unit =
    spark.sql(s"INSERT INTO $SourceTable PARTITION (inc_ful_flag='$flag', file_date='$date', file_time='$time') VALUES " +
      rows.map { case (a, s, v) =>
        s"(${if (a == null) "NULL" else s"'$a'"}, '$s', '$v')"
      }.mkString(", "))

  // ---- feed builder ---------------------------------------------------------

  private def feed(
    onMismatch: String = "REPORT",
    where: Option[String] = Some("inc_ful_flag = 'I'"),
    keys: Option[Seq[String]] = Some(Seq("acct_nbr", "sub_seq")),
    curated: Boolean = true,
    sourceType: String = "hive",
    table: String = "bstar",
    schema: Option[String] = None
  ): Config = {
    val whereLine = where.map(w => s"""  where = "$w"""").getOrElse("")
    val keysLine = keys.map(k => s"""merge { keys = [${k.map(x => s""""$x"""").mkString(", ")}] }""").getOrElse("")
    val curatedBlock =
      if (curated) s"curated {\n  enabled = true\n  database = $Cur\n  table = $table\n  format = orc\n  $keysLine\n}" else ""
    ConfigFactory.parseString(
      s"""
         |entity = $Entity
         |${schema.getOrElse("")}
         |source {
         |  type = $sourceType
         |  database = $Src
         |  table = $table
         |$whereLine
         |  incremental {
         |    watermark_columns = ["file_date", "file_time"]
         |    initial_value = "1900-01-01|00.00.00"
         |    watermark_store { type = memory }
         |  }
         |}
         |audit { enabled = true, database = $Audit }
         |concurrency { lock = "REQUIRED", database = $Audit, table = ingest_run_locks, lease_minutes = 5 }
         |$curatedBlock
         |reconcile { enabled = true, on_mismatch = "$onMismatch" }
       """.stripMargin)
  }

  /** The stage exactly as Control-M runs it, minus the JVM exit. */
  private def reconcile(conf: Config, runId: String): Int =
    IngestMain.execute(spark, ConfigFactory.empty(), conf,
      Cli(entity = Entity, mode = "INCR", stage = "reconcile", runId = Some(runId)))

  // ---- assertions helpers ---------------------------------------------------

  private def ledger(runId: String): Seq[(String, String)] =
    if (!spark.catalog.tableExists(Audit, "ingest_run_audit")) Seq.empty
    else spark.table(s"$Audit.ingest_run_audit")
      .filter(col("run_id") === runId && col("stage") === "reconcile")
      .select("stage", "status").collect().map(r => (r.getString(0), r.getString(1))).toSeq

  private def checks(runId: String): Map[String, (String, String, Boolean)] =
    spark.table(s"$Audit.ingest_reconciliation").filter(col("run_id") === runId)
      .select("check_name", "expected", "actual", "passed").collect()
      .map(r => r.getString(0) -> (r.getString(1), r.getString(2), r.getBoolean(3))).toMap

  private def lockRows(runId: String): Long =
    if (!spark.catalog.tableExists(Audit, "ingest_run_locks")) 0L
    else spark.table(s"$Audit.ingest_run_locks").filter(col("holder_run_id") === runId).count()

  private def latestLock(): Option[(String, String)] =
    spark.table(s"$Audit.ingest_run_locks").filter(col("entity") === Entity)
      .select("holder_run_id", "action", "event_ts").collect()
      .sortBy(r => (r.getTimestamp(2).getTime, r.getString(1))).lastOption
      .map(r => (r.getString(0), r.getString(1)))

  private def sourceState(table: String): (Seq[String], Long) =
    (spark.sql(s"SHOW PARTITIONS $table").collect().map(_.getString(0)).sorted.toSeq, spark.table(table).count())

  /** The invariant: reconciliation never writes, alters or drops the source. */
  private def untouched[A](body: => A): A = {
    val before = (sourceState(SourceTable), sourceState(s"$Src.bstar_alias"))
    val result = body
    assert((sourceState(SourceTable), sourceState(s"$Src.bstar_alias")) == before,
      "reconciliation modified a source table")
    result
  }

  // ---------------------------------------------------------------------------

  test("clean under where: exit 0, three checks, reconcile SUCCESS, lock released") {
    untouched {
      val exit = reconcile(feed(), "hr-1")
      assert(exit == 0)
      val c = checks("hr-1")
      assert(c.keySet == Set("source_curated_cardinality", "source_keys_present_in_curated",
        "curated_keys_absent_from_source"), c.toString)
      assert(c("source_keys_present_in_curated") == ("0", "0", true))
      assert(c("curated_keys_absent_from_source") == ("0", "0", true))
      // keyed feed: cardinality is the recorded trend, never the alarm
      assert(c("source_curated_cardinality") == ("source=3", "curated=3", true), c.toString)
      assert(ledger("hr-1") == Seq(("reconcile", "SUCCESS")))
      assert(latestLock().contains(("hr-1", "RELEASE")), s"lease must be released: ${latestLock()}")
    }
  }

  test("without where the F-only key reads as loss — the source side is scoped exactly like the extraction") {
    untouched {
      val exit = reconcile(feed(where = None), "hr-2")
      assert(exit == 0, "REPORT never fails the job")
      val c = checks("hr-2")
      assert(c("source_keys_present_in_curated") == ("0", "1", false), c.toString)
      assert(c("source_curated_cardinality") == ("source=7", "curated=3", true))
      assert(ledger("hr-2") == Seq(("reconcile", "FAILED")), "the ledger records the mismatch even under REPORT")
      assert(latestLock().contains(("hr-2", "RELEASE")))
    }
  }

  test("a key that never reached curated: REPORT exits 0 and records it; FAIL exits DATA_INTEGRITY") {
    insertSource("I", "2025-01-11", "07.39.59", Seq(("L1", "1", "q")))
    untouched {
      assert(reconcile(feed(), "hr-3") == 0)
      assert(checks("hr-3")("source_keys_present_in_curated") == ("0", "1", false))
      assert(ledger("hr-3") == Seq(("reconcile", "FAILED")))

      val exit = reconcile(feed(onMismatch = "FAIL"), "hr-4")
      assert(exit == FailureClass.DataIntegrity.exitCode, s"exit=$exit")
      assert(checks("hr-4")("source_keys_present_in_curated") == ("0", "1", false))
      val rows = ledger("hr-4")
      assert(rows.nonEmpty && rows.forall(_ == ("reconcile", "FAILED")), rows.toString)
      assert(latestLock().contains(("hr-4", "RELEASE")), s"lease must be released even on FAIL: ${latestLock()}")
    }
    // catch curated up so the later cases start clean
    spark.sql(s"INSERT INTO $CuratedTable VALUES ('L1','1','q')")
  }

  test("a curated-only key is informational — counted, never failing") {
    spark.sql(s"INSERT INTO $CuratedTable VALUES ('C9','1','c')")
    untouched {
      assert(reconcile(feed(), "hr-5") == 0)
      val c = checks("hr-5")
      assert(c("source_keys_present_in_curated") == ("0", "0", true))
      assert(c("curated_keys_absent_from_source") == ("1", "1", true), c.toString)
      assert(ledger("hr-5") == Seq(("reconcile", "SUCCESS")))
    }
  }

  test("NULL and BLANK keys are excluded on both sides") {
    insertSource("I", "2025-01-12", "05.00.00", Seq((null, "1", "n"), ("   ", "1", "b")))
    spark.sql(s"INSERT INTO $CuratedTable VALUES (NULL,'1','n'), ('  ','1','b2')")
    untouched {
      assert(reconcile(feed(), "hr-6") == 0)
      val c = checks("hr-6")
      assert(c("source_keys_present_in_curated") == ("0", "0", true),
        s"null/blank source keys can never have a curated row: ${c("source_keys_present_in_curated")}")
      assert(c("curated_keys_absent_from_source") == ("1", "1", true),
        s"null/blank curated keys must not pollute the absent count: ${c("curated_keys_absent_from_source")}")
    }
  }

  test("keyless feed: cardinality asserts curated is not short of the source") {
    untouched {
      // under where: 6 source rows (A1,A2,A3,L1,null,blank) vs 7 curated rows
      assert(reconcile(feed(keys = None), "hr-7") == 0)
      val ok = checks("hr-7")
      assert(ok.keySet == Set("source_curated_cardinality"), "no keys — Tier 2 is skipped")
      assert(ok("source_curated_cardinality") == (">=6", "7", true), ok.toString)
      // without where the F snapshot adds 4 more source rows: curated IS short
      assert(reconcile(feed(keys = None, where = None), "hr-8") == 0)
      val short = checks("hr-8")("source_curated_cardinality")
      assert(short == (">=10", "7", false), short.toString)
      assert(ledger("hr-8") == Seq(("reconcile", "FAILED")))
    }
  }

  test("CFG_022: no curated block to compare with — CONFIGURATION, recorded, lock released") {
    untouched {
      val exit = reconcile(feed(curated = false), "hr-9")
      assert(exit == FailureClass.Configuration.exitCode, s"exit=$exit")
      assert(ledger("hr-9").contains(("reconcile", "FAILED")))
      assert(latestLock().contains(("hr-9", "RELEASE")))
    }
  }

  test("CFG_026: an unsupported source type is refused before the lock is taken") {
    untouched {
      Seq("file" -> "hr-10", "kafka" -> "hr-11").foreach { case (t, runId) =>
        val exit = reconcile(feed(sourceType = t), runId)
        assert(exit == FailureClass.Configuration.exitCode, s"$t: exit=$exit")
        assert(lockRows(runId) == 0L, s"$t: no lock row may exist for a refused dispatch")
        assert(ledger(runId).isEmpty, s"$t: no ledger row may exist for a refused dispatch")
      }
    }
  }

  test("contract aliases resolve the source key name; keys come from the contract's business_key") {
    val contract =
      """schema { version = "1", columns = [
        |  { name = "acct_nbr", type = "string", aliases = ["acct_number"], business_key = true },
        |  { name = "sub_seq",  type = "string", business_key = true },
        |  { name = "val",      type = "string", required = false },
        |  { name = "inc_ful_flag", type = "string", required = false },
        |  { name = "file_date",    type = "string", required = false },
        |  { name = "file_time",    type = "string", required = false }
        |] }""".stripMargin
    untouched {
      val exit = reconcile(feed(table = "bstar_alias", keys = None, schema = Some(contract)), "hr-12")
      assert(exit == 0)
      val c = checks("hr-12")
      assert(c("source_keys_present_in_curated") == ("0", "0", true), c.toString)
      assert(c("curated_keys_absent_from_source") == ("0", "0", true), c.toString)
      assert(c("source_curated_cardinality") == ("source=2", "curated=2", true))
    }
  }
}
