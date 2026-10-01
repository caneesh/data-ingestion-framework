package com.hcsc.generic.ingest.app

import com.hcsc.generic.ingest.model.Cli
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Path => JPath}

/**
  * A hive source through the FULL pipeline, driven exactly as Control-M
  * drives it (`IngestMain.execute`, minus the JVM exit), on a Hive
  * metastore fixture.
  *
  * The `COPY` cases are the golden for H11: they were written and green
  * BEFORE `raw.mode = SOURCE` existed and must stay byte-for-byte green
  * after it (invariant R-17). The `SOURCE` cases follow.
  *
  * One source table serves every case; partitions are ADDED by the tests
  * in sequence (never rewritten — the owner's guarantee), and every
  * pipeline run is wrapped in an assertion that it left the source's
  * partition set and row count untouched (scenario K7).
  */
class HivePipelineIntegrationSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private var tempDir: JPath = _

  private val Src = "hp_src"
  private val Raw = "hp_raw"
  private val Cur = "hp_cur"
  private val Ctl = "hp_ctl"
  private val T = s"$Src.bstar"
  private val Initial = "1900-01-01|00.00.00"

  override def beforeAll(): Unit = {
    super.beforeAll()
    tempDir = Files.createTempDirectory("hive-pipeline-spec-")
    System.setProperty("derby.stream.error.file", tempDir.resolve("derby.log").toString)
    spark = SparkSession.builder()
      .master("local[2]").appName("hive-pipeline-spec")
      .config("spark.sql.warehouse.dir", tempDir.resolve("warehouse").toAbsolutePath.toString)
      .config("javax.jdo.option.ConnectionURL",
        s"jdbc:derby:;databaseName=${tempDir.resolve("metastore_db").toAbsolutePath};create=true")
      .config("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver")
      .config("hive.metastore.schema.verification", "false")
      .config("spark.sql.shuffle.partitions", "2").config("spark.ui.enabled", "false")
      .enableHiveSupport().getOrCreate()

    IngestMain.registerConnectors()
    Seq(Src, Raw, Cur, Ctl).foreach(db => spark.sql(s"CREATE DATABASE IF NOT EXISTS $db"))
    spark.sql(s"CREATE TABLE $T (k1 STRING, k2 INT, val STRING, src_ts STRING, actn_cd STRING) USING orc " +
      "PARTITIONED BY (inc_ful_flag STRING, file_date STRING, file_time STRING)")
    insert(T, "I", "2026-01-05", "04.00.00",
      Seq(("A", 1, "a1", "2026-01-04 23:00:00.000001", "PT"), ("B", 1, "b1", "2026-01-04 23:00:00.000002", "PT")))
    insert(T, "I", "2026-01-06", "04.00.00",
      Seq(("A", 1, "a2", "2026-01-05 23:00:00.000001", "UP"), ("C", 1, "c1", "2026-01-05 23:00:00.000003", "PT")))
  }

  override def afterAll(): Unit = {
    try { if (spark != null) spark.stop() } finally {
      import scala.collection.JavaConverters._
      Files.walk(tempDir).iterator().asScala.toSeq.reverse.foreach(Files.deleteIfExists)
    }
    super.afterAll()
  }

  // ---- fixture helpers ------------------------------------------------------

  private def insert(table: String, flag: String, date: String, time: String,
                     rows: Seq[(String, Int, String, String, String)]): Unit =
    spark.sql(s"INSERT INTO $table PARTITION (inc_ful_flag='$flag', file_date='$date', file_time='$time') VALUES " +
      rows.map { case (k1, k2, v, ts, a) =>
        val vv = if (v == null) "NULL" else s"'$v'"
        s"('$k1', $k2, $vv, '$ts', '$a')"
      }.mkString(", "))

  private def sourceSnapshot(table: String): (Seq[String], Long) =
    (spark.sql(s"SHOW PARTITIONS $table").collect().map(_.getString(0)).sorted.toSeq, spark.table(table).count())

  /** The stage exactly as Control-M runs it, minus the JVM exit, wrapped in
    * the K7 invariant: the pipeline never writes, alters or drops the source. */
  private def run(feed: Config, cli: Cli, source: String = T): Int = {
    val before = sourceSnapshot(source)
    val exit = IngestMain.execute(spark, ConfigFactory.empty(), feed, cli)
    assert(sourceSnapshot(source) == before, s"the pipeline changed the source table $source")
    exit
  }

  private def rawTable(entity: String) = s"$Raw.raw_$entity"
  private def curTable(entity: String) = s"$Cur.cur_$entity"

  private def feed(
    entity: String,
    rawMode: String,
    lookback: String = "",
    decoupled: Boolean = false,
    source: String = T,
    where: String = "",
    extra: String = ""
  ): Config = {
    val Array(srcDb, srcTable) = source.split("\\.")
    val rawBlock = rawMode match {
      case "COPY" =>
        s"""raw {
           |  database = $Raw
           |  table = raw_$entity
           |  path = "${tempDir.resolve(s"raw_$entity").toAbsolutePath}"
           |  format = parquet
           |  record_hash = true
           |  lineage_extended = true
           |  partitioning { keys = ["ingest_dt"], derive { ingest_dt = "date_format(current_timestamp(), 'yyyy-MM-dd')" } }
           |}""".stripMargin
      case "SOURCE" =>
        """raw { mode = "SOURCE", record_hash = true, lineage_extended = true }"""
    }
    val execution = if (decoupled)
      """ingestion { execution = "DECOUPLED" }
        |watermark { advance_after = "RAW" }""".stripMargin
    else ""
    ConfigFactory.parseString(
      s"""
         |entity = $entity
         |mode = "INCR"
         |schema { version = "1", columns = [
         |  { name = "k1", type = "string", business_key = true },
         |  { name = "k2", type = "int", business_key = true },
         |  { name = "val", type = "string", required = false },
         |  { name = "src_ts", type = "string", required = false },
         |  { name = "actn_cd", type = "string", required = false },
         |  { name = "inc_ful_flag", type = "string", required = false, category = "audit" },
         |  { name = "file_date", type = "string", required = false, category = "audit" },
         |  { name = "file_time", type = "string", required = false, category = "audit" }
         |] }
         |source {
         |  type = "hive"
         |  system = "bstar"
         |  database = $srcDb
         |  table = $srcTable
         |  ${if (where.nonEmpty) s"""where = "$where"""" else ""}
         |  incremental {
         |    watermark_columns = ["file_date", "file_time"]
         |    initial_value = "$Initial"
         |    watermark_formats = ["yyyy-MM-dd", "HH.mm.ss"]
         |    $lookback
         |    watermark_store { type = "hive", database = $Ctl }
         |  }
         |}
         |$execution
         |audit { enabled = true, database = $Ctl }
         |rejects { enabled = true, database = $Ctl, table = ingest_rejects }
         |concurrency { lock = "REQUIRED", database = $Ctl, table = ingest_run_locks, lease_minutes = 5 }
         |$rawBlock
         |curated {
         |  enabled = true
         |  database = $Cur
         |  table = cur_$entity
         |  path = "${tempDir.resolve(s"cur_$entity").toAbsolutePath}"
         |  format = parquet
         |  merge {
         |    keys = ["k1", "k2"]
         |    freshness { column = "src_ts", compare_as = "timestamp", tie_breakers = ["file_date", "file_time"] }
         |    deletes { mode = "SOFT", indicator_column = "actn_cd", indicator_values = ["dl"] }
         |  }
         |}
         |$extra
       """.stripMargin)
  }

  // ---- assertion helpers ----------------------------------------------------

  private case class Ledger(status: String, windowStart: String, windowEnd: String, rawCount: Long, acceptedCount: Long)

  /** Terminal ledger row for (run, stage): STARTED rows are skipped. */
  private def ledger(runId: String, stage: String): Option[Ledger] =
    spark.table(s"$Ctl.ingest_run_audit")
      .filter(col("run_id") === runId && col("stage") === stage && col("status") =!= "STARTED")
      .orderBy(col("event_ts").desc)
      .select("status", "window_start", "window_end", "raw_count", "accepted_count")
      .collect().headOption
      .map(r => Ledger(r.getString(0), r.getString(1), r.getString(2), r.getLong(3), r.getLong(4)))

  private def checks(runId: String): Map[String, Boolean] =
    if (!spark.catalog.tableExists(Ctl, "ingest_reconciliation")) Map.empty
    else spark.table(s"$Ctl.ingest_reconciliation").filter(col("run_id") === runId)
      .select("check_name", "passed").collect().map(r => r.getString(0) -> r.getBoolean(1)).toMap

  private def watermark(entity: String): Option[(String, Long)] =
    if (!spark.catalog.tableExists(Ctl, "ingest_watermarks")) None
    else spark.table(s"$Ctl.ingest_watermarks").filter(col("entity") === entity)
      .orderBy(col("watermark_version").desc, col("updated_ts").desc)
      .select("watermark_value", "watermark_version").collect().headOption
      .map(r => (r.getString(0), r.getLong(1)))

  private def watermarkRows(entity: String): Long =
    if (!spark.catalog.tableExists(Ctl, "ingest_watermarks")) 0L
    else spark.table(s"$Ctl.ingest_watermarks").filter(col("entity") === entity).count()

  /** (k1,k2) -> (val, last_modified_op). */
  private def curated(entity: String): Map[(String, Int), (String, String)] =
    spark.table(curTable(entity)).select("k1", "k2", "val", "last_modified_op").collect()
      .map(r => (r.getString(0), r.getInt(1)) -> (r.getString(2), r.getString(3))).toMap

  private def curatedCount(entity: String): Long = spark.table(curTable(entity)).count()

  private def rawRows(entity: String, runId: String): Long =
    spark.table(rawTable(entity)).filter(col("run_id") === runId).count()

  private def tablesIn(db: String): Set[String] =
    spark.catalog.listTables(db).collect().map(_.name.toLowerCase).toSet

  private def cli(entity: String, runId: String, stage: String = "all", mode: String = "INCR"): Cli =
    Cli(entity = entity, mode = mode, stage = stage, runId = Some(runId))

  // ==========================================================================
  // COPY — the golden (R-17)
  // ==========================================================================

  private val Strict = "copy_strict"

  test("COPY first light: RAW created and partitioned by ingest_dt, lineage stamped, curated latest-per-key, watermark committed") {
    assert(run(feed(Strict, "COPY"), cli(Strict, "cs-1")) == 0)

    val raw = spark.table(rawTable(Strict))
    assert(raw.count() == 4)
    assert(spark.catalog.listColumns(Raw, s"raw_$Strict").collect().exists(c => c.name == "ingest_dt" && c.isPartition),
      "RAW must be partitioned by ingest_dt")
    val meta = raw.filter(col("run_id") === "cs-1")
      .select("record_hash", "source_table", "source_database", "extract_start_ts", "extract_end_ts").collect()
    assert(meta.length == 4)
    assert(meta.forall(r => r.getString(0) != null && r.getString(0).length == 64), "record_hash is SHA-256 hex")
    assert(meta.forall(r => r.getString(1) == "bstar" && r.getString(2) == Src))
    assert(meta.forall(r => r.getString(3) == Initial && r.getString(4) == "2026-01-06|04.00.00"))

    val rawLedger = ledger("cs-1", "raw").get
    assert(rawLedger.status == "SUCCESS")
    assert(rawLedger.windowStart == Initial && rawLedger.windowEnd == "2026-01-06|04.00.00")
    assert(rawLedger.rawCount == 4 && rawLedger.acceptedCount == 4)
    assert(ledger("cs-1", "curated").exists(_.status == "SUCCESS"))

    assert(curated(Strict) == Map(("A", 1) -> ("a2", "I"), ("B", 1) -> ("b1", "I"), ("C", 1) -> ("c1", "I")))
    assert(watermark(Strict).contains(("2026-01-06|04.00.00", 1L)))
    val c = checks("cs-1")
    assert(c.get("source_equals_accepted_plus_rejected").contains(true))
    assert(c.get("raw_equals_accepted").contains(true))
    assert(c.get("curated_accounts_for_accepted_rows").contains(true))
  }

  test("COPY steady state: only the new partition is read; the merge updates the key; continuity passes") {
    insert(T, "I", "2026-01-07", "04.00.00", Seq(("B", 1, "b2", "2026-01-06 23:00:00.000002", "UP")))
    assert(run(feed(Strict, "COPY"), cli(Strict, "cs-2")) == 0)
    assert(rawRows(Strict, "cs-2") == 1)
    assert(curated(Strict)(("B", 1)) == ("b2", "U"))
    assert(curatedCount(Strict) == 3)
    assert(watermark(Strict).contains(("2026-01-07|04.00.00", 2L)))
    assert(checks("cs-2").get("watermark_continuity").contains(true))
    assert(ledger("cs-2", "raw").exists(l => l.windowStart == "2026-01-06|04.00.00" && l.windowEnd == "2026-01-07|04.00.00"))
  }

  test("COPY empty window: zero rows, no publish, watermark version unchanged") {
    assert(run(feed(Strict, "COPY"), cli(Strict, "cs-3")) == 0)
    assert(rawRows(Strict, "cs-3") == 0)
    assert(ledger("cs-3", "raw").exists(l => l.status == "SUCCESS" && l.rawCount == 0))
    assert(watermark(Strict).contains(("2026-01-07|04.00.00", 2L)))
    assert(watermarkRows(Strict) == 2)
    assert(curatedCount(Strict) == 3)
  }

  test("COPY DL row tombstones the key: is_deleted = true, last_modified_op = 'D'") {
    spark.sql(s"ALTER TABLE ${curTable(Strict)} ADD COLUMNS (is_deleted BOOLEAN)")
    insert(T, "I", "2026-01-08", "04.00.00", Seq(("C", 1, null, "2026-01-07 23:00:00.000003", "DL")))
    assert(run(feed(Strict, "COPY"), cli(Strict, "cs-4")) == 0)
    val row = spark.table(curTable(Strict)).filter(col("k1") === "C")
      .select("last_modified_op", "is_deleted").collect().head
    assert(row.getString(0) == "D" && row.getBoolean(1))
    assert(curatedCount(Strict) == 3)
    assert(watermark(Strict).contains(("2026-01-08|04.00.00", 3L)))
  }

  test("COPY --stage curated --run-id replays the RAW slice and leaves curated unchanged") {
    val before = curated(Strict)
    assert(run(feed(Strict, "COPY"), cli(Strict, "cs-2", stage = "curated")) == 0)
    assert(curated(Strict) == before)
    assert(curatedCount(Strict) == 3)
    assert(watermark(Strict).contains(("2026-01-08|04.00.00", 3L)), "replay never touches the watermark")
  }

  test("COPY lookback: an already-processed partition is re-read; curated has no duplicate keys") {
    val lb = "copy_lb"
    assert(run(feed(lb, "COPY", lookback = "lookback = { days = 2 }"), cli(lb, "cl-1")) == 0)
    assert(rawRows(lb, "cl-1") == 6)
    assert(watermark(lb).map(_._1).contains("2026-01-08|04.00.00"))
    insert(T, "I", "2026-01-09", "04.00.00", Seq(("D", 1, "d1", "2026-01-08 23:00:00.000004", "PT")))
    assert(run(feed(lb, "COPY", lookback = "lookback = { days = 2 }"), cli(lb, "cl-2")) == 0)
    // rewind 2026-01-08 by 2 days -> (2026-01-06, "") -> P2, P3, P4 re-read + P5
    assert(rawRows(lb, "cl-2") == 5)
    assert(curatedCount(lb) == 4)
    assert(spark.table(curTable(lb)).groupBy("k1", "k2").count().filter(col("count") > 1).count() == 0)
    assert(curated(lb)(("D", 1)) == ("d1", "I"))
    assert(checks("cl-2").get("watermark_continuity").contains(true))
    assert(ledger("cl-2", "raw").exists(_.windowStart == "2026-01-08|04.00.00"),
      "the recorded window lower is the stored watermark, never the rewound bound")
  }

  test("COPY DECOUPLED: --stage raw records and advances, --stage curated --pending drains the batch") {
    val dec = "copy_dec"
    val f = feed(dec, "COPY", decoupled = true)
    assert(run(f, cli(dec, "cd-1", stage = "raw")) == 0)
    assert(rawRows(dec, "cd-1") == 7)
    assert(ledger("cd-1", "raw").exists(_.status == "SUCCESS"))
    assert(ledger("cd-1", "curated").exists(_.status == "SKIPPED"))
    assert(watermark(dec).map(_._1).contains("2026-01-09|04.00.00"), "advance_after = RAW")
    assert(!spark.catalog.tableExists(Cur, s"cur_$dec"))

    assert(run(f, Cli(entity = dec, mode = "INCR", stage = "curated", pending = true)) == 0)
    assert(ledger("cd-1", "curated").exists(_.status == "SUCCESS"))
    assert(curatedCount(dec) == 4)
    assert(curated(dec)(("C", 1))._2 == "D")
    // nothing left pending
    assert(run(f, Cli(entity = dec, mode = "INCR", stage = "curated", pending = true)) == 0)
    assert(curatedCount(dec) == 4)
  }

  test("COPY --validate-only and --dry-run write nothing") {
    val v = "copy_val"
    assert(run(feed(v, "COPY"), Cli(entity = v, mode = "INCR", validateOnly = true)) == 0)
    assert(run(feed(v, "COPY"), Cli(entity = v, mode = "INCR", runId = Some("cv-dry"), dryRun = true)) == 0)
    assert(!spark.catalog.tableExists(Raw, s"raw_$v"))
    assert(!spark.catalog.tableExists(Cur, s"cur_$v"))
    assert(watermarkRows(v) == 0)
  }

  // ==========================================================================
  // SOURCE — the source table is the raw layer (H11)
  // ==========================================================================

  private val SStrict = "src_strict"
  private val ExitConfiguration = com.hcsc.generic.ingest.runtime.FailureClass.Configuration.exitCode
  private val ExitDataIntegrity = com.hcsc.generic.ingest.runtime.FailureClass.DataIntegrity.exitCode

  private def duplicateKeys(entity: String): Long =
    spark.table(curTable(entity)).groupBy("k1", "k2").count().filter(col("count") > 1).count()

  test("SOURCE first light: no RAW table; the ledger row carries the window and raw_count = accepted; curated built; watermark committed") {
    val rawTablesBefore = tablesIn(Raw)
    assert(run(feed(SStrict, "SOURCE"), cli(SStrict, "ss-1")) == 0)
    assert(tablesIn(Raw) == rawTablesBefore, "SOURCE mode must create no RAW table")
    assert(!tablesIn(Ctl).exists(_.startsWith("raw_")))

    val l = ledger("ss-1", "raw").get
    assert(l.status == "SUCCESS")
    assert(l.windowStart == Initial && l.windowEnd == "2026-01-09|04.00.00")
    assert(l.rawCount == 7 && l.acceptedCount == 7, "raw_count records the accepted count: nothing was written")
    assert(ledger("ss-1", "curated").exists(_.status == "SUCCESS"))

    val cur = curated(SStrict)
    assert(cur.size == 4)
    assert(cur(("A", 1))._1 == "a2" && cur(("B", 1))._1 == "b2" && cur(("D", 1))._1 == "d1")
    assert(cur(("C", 1))._2 == "D", "the DL row is the latest version of C: tombstoned")
    assert(watermark(SStrict).contains(("2026-01-09|04.00.00", 1L)))

    val c = checks("ss-1")
    assert(!c.contains("raw_equals_accepted"), "there is no write to verify under SOURCE")
    assert(!c.contains("raw_overlap_reread"))
    assert(!c.contains("raw_duplicate_versions"))
    assert(c.get("source_equals_accepted_plus_rejected").contains(true))
    assert(c.get("curated_accounts_for_accepted_rows").contains(true))
  }

  test("SOURCE steady state and empty window") {
    insert(T, "I", "2026-01-10", "04.00.00", Seq(("E", 1, "e1", "2026-01-09 23:00:00.000005", "PT")))
    assert(run(feed(SStrict, "SOURCE"), cli(SStrict, "ss-2")) == 0)
    assert(ledger("ss-2", "raw").exists(l => l.rawCount == 1 && l.acceptedCount == 1 &&
      l.windowStart == "2026-01-09|04.00.00" && l.windowEnd == "2026-01-10|04.00.00"))
    assert(curated(SStrict)(("E", 1)) == ("e1", "I"))
    assert(curatedCount(SStrict) == 5)
    assert(watermark(SStrict).contains(("2026-01-10|04.00.00", 2L)))
    assert(checks("ss-2").get("watermark_continuity").contains(true))

    assert(run(feed(SStrict, "SOURCE"), cli(SStrict, "ss-3")) == 0)
    assert(ledger("ss-3", "raw").exists(l => l.status == "SUCCESS" && l.rawCount == 0))
    assert(watermark(SStrict).contains(("2026-01-10|04.00.00", 2L)))
    assert(watermarkRows(SStrict) == 2)
    assert(curatedCount(SStrict) == 5)
    assert(tablesIn(Raw).forall(!_.contains(SStrict)))
  }

  test("SOURCE DL row tombstones the key: is_deleted = true, last_modified_op = 'D'") {
    spark.sql(s"ALTER TABLE ${curTable(SStrict)} ADD COLUMNS (is_deleted BOOLEAN)")
    insert(T, "I", "2026-01-11", "04.00.00", Seq(("E", 1, null, "2026-01-10 23:00:00.000005", "DL")))
    assert(run(feed(SStrict, "SOURCE"), cli(SStrict, "ss-4")) == 0)
    val row = spark.table(curTable(SStrict)).filter(col("k1") === "E")
      .select("last_modified_op", "is_deleted").collect().head
    assert(row.getString(0) == "D" && row.getBoolean(1))
    assert(curatedCount(SStrict) == 5)
    assert(watermark(SStrict).contains(("2026-01-11|04.00.00", 3L)))
  }

  test("SOURCE --stage curated --run-id re-reads the source for the recorded window; a stale replay changes nothing") {
    val before = curated(SStrict)
    // ss-2 delivered E = e1 (PT); E has since been tombstoned by a NEWER row.
    // Replaying ss-2 must re-read exactly that window from the source and
    // lose the freshness contest, leaving curated unchanged.
    assert(run(feed(SStrict, "SOURCE"), cli(SStrict, "ss-2", stage = "curated")) == 0)
    assert(curated(SStrict) == before)
    assert(curatedCount(SStrict) == 5 && duplicateKeys(SStrict) == 0)
    assert(watermark(SStrict).contains(("2026-01-11|04.00.00", 3L)), "replay never touches the watermark")
    assert(checks("ss-2").get("curated_accounts_for_replayed_rows").contains(true),
      "the replay accounts for the ledger's raw_count (= accepted) of the replayed run")
  }

  test("SOURCE --stage curated without a replayable run fails as PIPE_003; --resume-ingest-dt is CFG_028 (CONFIGURATION)") {
    assert(run(feed(SStrict, "SOURCE"),
      Cli(entity = SStrict, mode = "INCR", stage = "curated", resumeIngestDt = Some("2026-01-01"))) == ExitConfiguration)
    // a run that never recorded a raw SUCCESS cannot be replayed (unclassified PIPE_003 rethrown)
    val e = intercept[IllegalArgumentException](
      run(feed(SStrict, "SOURCE"), cli(SStrict, "never-ran", stage = "curated")))
    assert(e.getMessage.contains("PIPE_003") && e.getMessage.contains("raw SUCCESS row"), e.getMessage)
    assert(curatedCount(SStrict) == 5)
  }

  test("SOURCE lookback: an already-processed partition is re-read; curated has no duplicate keys") {
    val lb = "src_lb"
    assert(run(feed(lb, "SOURCE", lookback = "lookback = { days = 2 }"), cli(lb, "sl-1")) == 0)
    assert(ledger("sl-1", "raw").exists(l => l.acceptedCount == 9 && l.rawCount == 9))
    assert(watermark(lb).map(_._1).contains("2026-01-11|04.00.00"))
    insert(T, "I", "2026-01-12", "04.00.00", Seq(("F", 1, "f1", "2026-01-11 23:00:00.000006", "PT")))
    assert(run(feed(lb, "SOURCE", lookback = "lookback = { days = 2 }"), cli(lb, "sl-2")) == 0)
    // rewind 2026-01-11 by 2 days -> (2026-01-09, "") -> P5, P6, P7 re-read + P8
    assert(ledger("sl-2", "raw").exists(l => l.acceptedCount == 4 && l.windowStart == "2026-01-11|04.00.00"),
      "the recorded window lower is the stored watermark, never the rewound bound")
    assert(curatedCount(lb) == 6 && duplicateKeys(lb) == 0)
    assert(curated(lb)(("F", 1)) == ("f1", "I"))
    assert(checks("sl-2").get("watermark_continuity").contains(true))
    assert(!checks("sl-2").contains("raw_overlap_reread"))
  }

  test("SOURCE DECOUPLED: --stage raw records the window and advances; --stage curated --pending drains it from the source") {
    val dec = "src_dec"
    val f = feed(dec, "SOURCE", decoupled = true)
    val rawTablesBefore = tablesIn(Raw)
    assert(run(f, cli(dec, "sd-1", stage = "raw")) == 0)
    assert(tablesIn(Raw) == rawTablesBefore)
    assert(ledger("sd-1", "raw").exists(l => l.status == "SUCCESS" && l.windowEnd == "2026-01-12|04.00.00" && l.rawCount == 10))
    assert(ledger("sd-1", "curated").exists(_.status == "SKIPPED"))
    assert(watermark(dec).map(_._1).contains("2026-01-12|04.00.00"), "advance_after = RAW")
    assert(!spark.catalog.tableExists(Cur, s"cur_$dec"))

    assert(run(f, Cli(entity = dec, mode = "INCR", stage = "curated", pending = true)) == 0)
    assert(ledger("sd-1", "curated").exists(_.status == "SUCCESS"))
    assert(curatedCount(dec) == 6 && duplicateKeys(dec) == 0)
    assert(curated(dec)(("C", 1))._2 == "D" && curated(dec)(("E", 1))._2 == "D")
    assert(checks("sd-1").get("curated_accounts_for_replayed_rows").contains(true))
    // nothing left pending
    assert(run(f, Cli(entity = dec, mode = "INCR", stage = "curated", pending = true)) == 0)
    assert(curatedCount(dec) == 6)
    // --replay-source-system has no RAW table to consult: it matches the feed's one source.system
    assert(run(f, Cli(entity = dec, mode = "INCR", stage = "curated", replaySourceSystem = Some("other"))) == 0)
    assert(run(f, Cli(entity = dec, mode = "INCR", stage = "curated", replaySourceSystem = Some("bstar"))) == 0)
    assert(curatedCount(dec) == 6)
  }

  test("SOURCE --resume after a raw SUCCESS replays curated from the source window and commits the watermark") {
    val res = "src_res"
    val f = feed(res, "SOURCE")
    // Simulate a run that died after raw SUCCESS: raw-only, no watermark advance.
    assert(run(f, cli(res, "sr-1", stage = "raw")) == 0)
    assert(ledger("sr-1", "raw").exists(_.status == "SUCCESS"))
    assert(watermark(res).isEmpty, "a raw-only run without advance_after = RAW holds the watermark")
    assert(run(f, Cli(entity = res, mode = "INCR", runId = Some("sr-1"), resume = true)) == 0)
    assert(ledger("sr-1", "raw").exists(_.status == "SKIPPED"))
    assert(ledger("sr-1", "curated").exists(_.status == "SUCCESS"))
    assert(curatedCount(res) == 6 && duplicateKeys(res) == 0)
    assert(watermark(res).contains(("2026-01-12|04.00.00", 1L)), "committed for the window the raw run recorded")
  }

  test("SOURCE HIVE_008: a replay whose window was purged from the source fails loudly (DATA_INTEGRITY) in the curated stage") {
    val gone = "src_gone"
    val G = s"$Src.bstar_gone"
    spark.sql(s"CREATE TABLE $G (k1 STRING, k2 INT, val STRING, src_ts STRING, actn_cd STRING) USING orc " +
      "PARTITIONED BY (inc_ful_flag STRING, file_date STRING, file_time STRING)")
    insert(G, "I", "2026-02-01", "04.00.00", Seq(("X", 1, "x1", "2026-01-31 23:00:00.000001", "PT")))
    insert(G, "I", "2026-02-02", "04.00.00", Seq(("Y", 1, "y1", "2026-02-01 23:00:00.000002", "PT")))
    assert(run(feed(gone, "SOURCE", source = G), cli(gone, "sg-1"), source = G) == 0)
    assert(curatedCount(gone) == 2)

    spark.sql(s"ALTER TABLE $G DROP PARTITION (inc_ful_flag='I', file_date='2026-02-01', file_time='04.00.00')")
    spark.sql(s"ALTER TABLE $G DROP PARTITION (inc_ful_flag='I', file_date='2026-02-02', file_time='04.00.00')")
    assert(run(feed(gone, "SOURCE", source = G), cli(gone, "sg-1", stage = "curated"), source = G) == ExitDataIntegrity)
    assert(ledger("sg-1", "curated").exists(_.status == "FAILED"), "the vanished window is a curated-stage failure")
    assert(curatedCount(gone) == 2, "nothing published")
  }

  test("SOURCE --validate-only and --dry-run write nothing") {
    val v = "src_val"
    val rawTablesBefore = tablesIn(Raw)
    assert(run(feed(v, "SOURCE"), Cli(entity = v, mode = "INCR", validateOnly = true)) == 0)
    assert(run(feed(v, "SOURCE"), Cli(entity = v, mode = "INCR", runId = Some("sv-dry"), dryRun = true)) == 0)
    assert(tablesIn(Raw) == rawTablesBefore)
    assert(!spark.catalog.tableExists(Cur, s"cur_$v"))
    assert(watermarkRows(v) == 0)
    assert(ledger("sv-dry", "raw").exists(l => l.status == "SUCCESS" && l.rawCount == 0))
  }

  test("SOURCE misconfiguration fails before any extraction (CFG_028, CONFIGURATION)") {
    val bad = "src_bad"
    val withRawTable = feed(bad, "SOURCE", extra = s"""raw { database = $Raw, table = raw_$bad }""")
    assert(run(withRawTable, cli(bad, "sb-1")) == ExitConfiguration)
    val withRetention = feed(bad, "SOURCE", extra = """retention { raw = "30d" }""")
    assert(run(withRetention, cli(bad, "sb-2")) == ExitConfiguration)
    assert(!spark.catalog.tableExists(Cur, s"cur_$bad"))
    assert(watermarkRows(bad) == 0)
    assert(ledger("sb-1", "raw").isEmpty && ledger("sb-2", "raw").isEmpty, "rejected before the raw stage started")
  }

  test("SOURCE --stage retention: rejects/audit/watermarks are purged, the source is never touched") {
    val f = feed(SStrict, "SOURCE",
      extra = """retention { rejects = "30d", audit = "30d", watermarks_keep_last = 2 }""")
    assert(run(f, cli(SStrict, "ss-ret", stage = "retention")) == 0)
    assert(watermarkRows(SStrict) == 2)
    assert(curatedCount(SStrict) == 5)
    assert(ledger("ss-ret", "retention").exists(_.status == "SUCCESS"))
  }
}
