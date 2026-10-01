package com.hcsc.generic.ingest.app

import com.hcsc.generic.ingest.hive.HiveTestSeams
import com.hcsc.generic.ingest.model.Cli
import com.hcsc.generic.ingest.runtime.FailureClass
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Path => JPath}

/**
  * The scenario-matrix cases for a hive source that `HivePipelineIntegrationSpec`
  * (the COPY/SOURCE goldens) does not cover: the operator-facing failure
  * exits (HIVE_002/003/004/007, a missing required column, the volume floor),
  * late partitions inside and outside the lookback, reject-watermark HOLD vs
  * ADVANCE, cross-JVM `--resume` / `--pending` recovery from the ledger, and
  * retention of the shared watermark table.
  *
  * Driven exactly as Control-M drives it (`IngestMain.execute`, minus the
  * JVM exit). Every run is wrapped in the K7 invariant: the pipeline never
  * writes, alters or drops the source table. Tests that ADD partitions use a
  * source table of their own so declaration order never leaks between cases.
  */
class HivePipelineScenarioSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private var tempDir: JPath = _

  private val Src = "hs_src"
  private val Raw = "hs_raw"
  private val Cur = "hs_cur"
  private val Ctl = "hs_ctl"
  private val T = s"$Src.bstar"
  private val Initial = "1900-01-01|00.00.00"
  private val P2 = "2026-01-06|04.00.00"

  private val ExitTransient = FailureClass.Transient.exitCode
  private val ExitDataIntegrity = FailureClass.DataIntegrity.exitCode
  private val ExitConfiguration = FailureClass.Configuration.exitCode

  override def beforeAll(): Unit = {
    super.beforeAll()
    tempDir = Files.createTempDirectory("hive-scenario-spec-")
    System.setProperty("derby.stream.error.file", tempDir.resolve("derby.log").toString)
    spark = SparkSession.builder()
      .master("local[2]").appName("hive-scenario-spec")
      .config("spark.sql.warehouse.dir", tempDir.resolve("warehouse").toAbsolutePath.toString)
      .config("javax.jdo.option.ConnectionURL",
        s"jdbc:derby:;databaseName=${tempDir.resolve("metastore_db").toAbsolutePath};create=true")
      .config("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver")
      .config("hive.metastore.schema.verification", "false")
      .config("spark.sql.shuffle.partitions", "2").config("spark.ui.enabled", "false")
      .enableHiveSupport().getOrCreate()

    IngestMain.registerConnectors()
    Seq(Src, Raw, Cur, Ctl).foreach(db => spark.sql(s"CREATE DATABASE IF NOT EXISTS $db"))
    mkTable(T)
  }

  override def afterAll(): Unit = {
    try { if (spark != null) spark.stop() } finally {
      import scala.collection.JavaConverters._
      Files.walk(tempDir).iterator().asScala.toSeq.reverse.foreach(Files.deleteIfExists)
    }
    super.afterAll()
  }

  // ---- fixture helpers ------------------------------------------------------

  /** The standard two-partition source: A/B in P1, A updated + C in P2 — 4
    * rows, 3 keys, newest partition 2026-01-06|04.00.00. */
  private def mkTable(table: String, fileDateType: String = "STRING"): String = {
    spark.sql(s"CREATE TABLE $table (k1 STRING, k2 INT, val STRING, src_ts STRING, actn_cd STRING) USING orc " +
      s"PARTITIONED BY (inc_ful_flag STRING, file_date $fileDateType, file_time STRING)")
    insert(table, "I", "2026-01-05", "04.00.00",
      Seq(("A", 1, "a1", "2026-01-04 23:00:00.000001", "PT"), ("B", 1, "b1", "2026-01-04 23:00:00.000002", "PT")))
    insert(table, "I", "2026-01-06", "04.00.00",
      Seq(("A", 1, "a2", "2026-01-05 23:00:00.000001", "UP"), ("C", 1, "c1", "2026-01-05 23:00:00.000003", "PT")))
    table
  }

  private def insert(table: String, flag: String, date: String, time: String,
                     rows: Seq[(String, Int, String, String, String)]): Unit =
    spark.sql(s"INSERT INTO $table PARTITION (inc_ful_flag='$flag', file_date='$date', file_time='$time') VALUES " +
      rows.map { case (k1, k2, v, ts, a) =>
        val vv = if (v == null) "NULL" else s"'$v'"
        s"('$k1', $k2, $vv, '$ts', '$a')"
      }.mkString(", "))

  private def sourceSnapshot(table: String): (Seq[String], Long) =
    (spark.sql(s"SHOW PARTITIONS $table").collect().map(_.getString(0)).sorted.toSeq, spark.table(table).count())

  /** The stage exactly as Control-M runs it, wrapped in the K7 invariant. */
  private def run(feed: Config, cli: Cli, source: String = T): Int = {
    val before = sourceSnapshot(source)
    val exit = IngestMain.execute(spark, ConfigFactory.empty(), feed, cli)
    assert(sourceSnapshot(source) == before, s"the pipeline changed the source table $source")
    exit
  }

  private def curTable(entity: String) = s"$Cur.cur_$entity"

  private def feed(
    entity: String,
    rawMode: String = "SOURCE",
    lookback: String = "",
    decoupled: Boolean = false,
    source: String = T,
    where: String = "",
    extraColumns: String = "",
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
         |  { name = "inc_ful_flag", type = "string", required = true, category = "audit" },
         |  { name = "file_date", type = "string", required = true, category = "audit" },
         |  { name = "file_time", type = "string", required = true, category = "audit" }
         |  $extraColumns
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

  private def ledger(runId: String, stage: String): Option[Ledger] =
    if (!spark.catalog.tableExists(Ctl, "ingest_run_audit")) None
    else spark.table(s"$Ctl.ingest_run_audit")
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

  private def curatedExists(entity: String): Boolean = spark.catalog.tableExists(Cur, s"cur_$entity")
  private def curatedCount(entity: String): Long = spark.table(curTable(entity)).count()
  private def duplicateKeys(entity: String): Long =
    spark.table(curTable(entity)).groupBy("k1", "k2").count().filter(col("count") > 1).count()

  private def rejects(runId: String): Long =
    if (!spark.catalog.tableExists(Ctl, "ingest_rejects")) 0L
    else spark.table(s"$Ctl.ingest_rejects").filter(col("run_id") === runId).count()

  private def cli(entity: String, runId: String, stage: String = "all", mode: String = "INCR"): Cli =
    Cli(entity = entity, mode = mode, stage = stage, runId = Some(runId))

  // ==========================================================================
  // Operator-facing failures: the exit code is the contract with Control-M
  // ==========================================================================

  test("HIVE_007: FULL is refused once a watermark row exists — exit 30, nothing read, nothing published") {
    val e = "full_refused"
    assert(run(feed(e), cli(e, "fr-1")) == 0)
    assert(watermark(e).contains((P2, 1L)) && curatedCount(e) == 3)

    assert(run(feed(e), cli(e, "fr-2", mode = "FULL")) == ExitConfiguration)
    assert(watermark(e).contains((P2, 1L)), "the refusal leaves the watermark exactly as it was")
    assert(watermarkRows(e) == 1)
    assert(curatedCount(e) == 3)
    assert(!ledger("fr-2", "curated").exists(_.status == "SUCCESS"))
  }

  test("a required contract column absent from the source fails before anything is written — exit 20 (DATA_INTEGRITY)") {
    val e = "req_missing"
    val f = feed(e, extraColumns = """, { name = "missing_col", type = "string", required = true }""")
    assert(run(f, cli(e, "rm-1")) == ExitDataIntegrity)
    assert(!curatedExists(e))
    assert(watermarkRows(e) == 0)
    assert(!ledger("rm-1", "raw").exists(_.status == "SUCCESS"))
  }

  test("HIVE_003: a DATE-typed partition column cannot be a watermark component — exit 30 from the run AND from --validate-only") {
    val D = mkTable(s"$Src.bstar_typed", fileDateType = "DATE")
    val e = "typed_part"
    assert(run(feed(e, source = D), Cli(entity = e, mode = "INCR", validateOnly = true), source = D) == ExitConfiguration)
    assert(run(feed(e, source = D), cli(e, "tp-1"), source = D) == ExitConfiguration)
    assert(!curatedExists(e))
    assert(watermarkRows(e) == 0)
  }

  test("HIVE_002: a source table that does not exist is a configuration failure (exit 30), also under --validate-only") {
    val e = "no_table"
    val f = feed(e, source = s"$Src.does_not_exist")
    // No invariant wrapper here: SHOW PARTITIONS on a missing table cannot be snapshotted.
    assert(IngestMain.execute(spark, ConfigFactory.empty(), f, Cli(entity = e, mode = "INCR", validateOnly = true)) == ExitConfiguration)
    assert(IngestMain.execute(spark, ConfigFactory.empty(), f, cli(e, "nt-1")) == ExitConfiguration)
    assert(!curatedExists(e) && watermarkRows(e) == 0)
  }

  test("HIVE_004: source.where on a data column is refused (exit 30); on a partition column it prunes") {
    val bad = "where_data"
    assert(run(feed(bad, where = "val = 'a1'"), cli(bad, "wd-1")) == ExitConfiguration)
    assert(!curatedExists(bad) && watermarkRows(bad) == 0)

    // A partition-column filter is pushed to the metastore and selects normally.
    val ok = "where_part"
    assert(run(feed(ok, where = "inc_ful_flag = 'I'"), cli(ok, "wp-1")) == 0)
    assert(ledger("wp-1", "raw").exists(l => l.status == "SUCCESS" && l.rawCount == 4))
    assert(curatedCount(ok) == 3)
    assert(watermark(ok).contains((P2, 1L)))
  }

  test("an EMPTY first light exits 20 by design (nothing to build under allow_empty = false); a later empty window is a no-op") {
    // Every fixture partition is inc_ful_flag = 'I', so 'F' selects nothing.
    // With no curated table yet there is nothing to build: PublishService
    // refuses the empty staging table (allow_empty = false) and the run is
    // DATA_INTEGRITY — a feed scheduled before its source ever landed
    // deserves a page, not a green no-op. The runbook's first-light variant
    // says so: first light must select at least one partition.
    val e = "empty_first"
    assert(run(feed(e, where = "inc_ful_flag = 'F'"), cli(e, "ef-1")) == ExitDataIntegrity)
    assert(ledger("ef-1", "raw").exists(l => l.status == "SUCCESS" && l.rawCount == 0), "the raw stage itself is fine")
    assert(!curatedExists(e), "no curated table is created for an empty first light")
    assert(watermarkRows(e) == 0, "nothing was committed")

    // After a REAL first light, an empty window is the normal daily no-op.
    val later = "empty_later"
    assert(run(feed(later), cli(later, "el-1")) == 0)
    assert(watermark(later).contains((P2, 1L)) && curatedCount(later) == 3)
    assert(run(feed(later), cli(later, "el-2")) == 0, "no new partition: clean no-op")
    assert(ledger("el-2", "raw").exists(l => l.status == "SUCCESS" && l.rawCount == 0))
    assert(watermark(later).contains((P2, 1L)) && watermarkRows(later) == 1, "watermark unchanged")
    assert(curatedCount(later) == 3)
  }

  test("volume floor: min_accepted_rows = 1 turns an empty window into a reconciliation failure (exit 20) and holds the watermark") {
    val e = "floor"
    val f = feed(e, extra = "audit { reconciliation { min_accepted_rows = 1 } }")
    assert(run(f, cli(e, "fl-1")) == 0)
    assert(checks("fl-1").get("accepted_meets_minimum").contains(true))
    assert(watermark(e).contains((P2, 1L)))

    assert(run(f, cli(e, "fl-2")) == ExitDataIntegrity, "an empty window is below the floor")
    assert(checks("fl-2").get("accepted_meets_minimum").contains(false))
    assert(watermark(e).contains((P2, 1L)) && watermarkRows(e) == 1, "a failed run never advances")
    assert(curatedCount(e) == 3)
  }

  // ==========================================================================
  // Late partitions — inside and outside the lookback
  // ==========================================================================

  test("late partition INSIDE the lookback: re-read, stale version loses on freshness, new key lands, watermark value AND version unchanged") {
    val L = mkTable(s"$Src.bstar_late")
    val e = "late_in"
    val f = feed(e, source = L, lookback = "lookback = { days = 2 }")
    assert(run(f, cli(e, "li-1"), source = L) == 0)
    assert(watermark(e).contains((P2, 1L)) && curatedCount(e) == 3)

    // A partition for an OLDER file_date lands after the watermark passed it:
    // a stale version of A (older capture ts than a2) and a genuinely new key.
    insert(L, "I", "2026-01-05", "10.00.00",
      Seq(("A", 1, "a0-stale", "2026-01-04 22:00:00.000001", "UP"), ("D", 1, "d1", "2026-01-05 09:00:00.000004", "PT")))
    assert(run(f, cli(e, "li-2"), source = L) == 0)
    // rewind 2026-01-06 by 2 days -> (2026-01-04, "") -> P1 (2) + late (2) + P2 (2) all re-read
    val l = ledger("li-2", "raw").get
    assert(l.status == "SUCCESS" && l.acceptedCount == 6 && l.rawCount == 6)
    assert(l.windowStart == P2, "window_start is the STORED watermark, never the rewound bound")
    assert(l.windowEnd == P2, "nothing newer than the watermark was read")
    val cur = curated(e)
    assert(cur(("A", 1))._1 == "a2", "the stale late version must lose the freshness contest")
    assert(cur(("D", 1)) == ("d1", "I"), "the late new key lands")
    assert(curatedCount(e) == 4 && duplicateKeys(e) == 0)
    assert(watermark(e).contains((P2, 1L)), "upper == stored watermark: no advance, no new version")
    assert(watermarkRows(e) == 1)
    assert(checks("li-2").get("watermark_continuity").contains(true))
  }

  test("late partition OUTSIDE the lookback is not read; --stage reconcile REPORTs its keys (exit 0, FAILED check row)") {
    val V = mkTable(s"$Src.bstar_vlate")
    val e = "late_out"
    val f = feed(e, source = V, lookback = "lookback = { days = 2 }",
      extra = """reconcile { enabled = true, on_mismatch = "REPORT" }""")
    assert(run(f, cli(e, "lo-1"), source = V) == 0)
    assert(curatedCount(e) == 3)

    insert(V, "I", "2025-12-01", "04.00.00", Seq(("E", 1, "e1", "2025-11-30 23:00:00.000005", "PT")))
    assert(run(f, cli(e, "lo-2"), source = V) == 0)
    assert(ledger("lo-2", "raw").exists(l => l.status == "SUCCESS"))
    assert(!curated(e).contains(("E", 1)), "below the rewound bound: invisible to the incremental read")
    assert(curatedCount(e) == 3)

    // The detector: the whole source key set against curated.
    assert(run(f, Cli(entity = e, mode = "INCR", stage = "reconcile", runId = Some("lo-rec")), source = V) == 0,
      "REPORT mode exits 0 with findings")
    val c = checks("lo-rec")
    assert(c.get("source_keys_present_in_curated").contains(false), c.toString)
    assert(c.get("curated_keys_absent_from_source").contains(true))
    assert(curatedCount(e) == 3, "reconcile never writes curated")
    assert(watermark(e).contains((P2, 1L)), "reconcile never touches the watermark")
  }

  // ==========================================================================
  // Rejects and the watermark
  // ==========================================================================

  private val Rule = """rules = [ { name = "bad_val", condition = "val = 'BAD'", error_code = "RULE_001", category = "RULE" } ]"""

  test("rejects.on_reject_watermark = HOLD: a rejected row keeps the window open; ADVANCE commits past it") {
    val R = mkTable(s"$Src.bstar_rej")
    insert(R, "I", "2026-01-07", "04.00.00",
      Seq(("F", 1, "BAD", "2026-01-06 23:00:00.000006", "PT"), ("G", 1, "g1", "2026-01-06 23:00:00.000007", "PT")))
    val P3 = "2026-01-07|04.00.00"

    val hold = "rej_hold"
    val fh = feed(hold, source = R, extra = s"""rejects { $Rule, on_reject_watermark = "HOLD" }""")
    assert(run(fh, cli(hold, "rh-1"), source = R) == 0, "a reject below the (unbounded) threshold is not a failure")
    assert(rejects("rh-1") == 1, "the BAD row is in the reject table")
    assert(curatedCount(hold) == 4 && !curated(hold).contains(("F", 1)))
    assert(watermark(hold).isEmpty, "HELD: no watermark was committed")
    assert(ledger("rh-1", "raw").exists(l => l.status == "SUCCESS" && l.acceptedCount == 5))
    // Next run re-extracts the same window; curated stays consistent, still held.
    assert(run(fh, cli(hold, "rh-2"), source = R) == 0)
    assert(rejects("rh-2") == 1)
    assert(curatedCount(hold) == 4 && duplicateKeys(hold) == 0)
    assert(watermark(hold).isEmpty)

    val adv = "rej_adv"
    val fa = feed(adv, source = R, extra = s"""rejects { $Rule, on_reject_watermark = "ADVANCE" }""")
    assert(run(fa, cli(adv, "ra-1"), source = R) == 0)
    assert(rejects("ra-1") == 1)
    assert(curatedCount(adv) == 4)
    assert(watermark(adv).contains((P3, 1L)), "ADVANCE: the window closes over the rejected row")
    assert(run(fa, cli(adv, "ra-2"), source = R) == 0)
    assert(rejects("ra-2") == 0, "the window is closed; the row is not re-read")
    assert(ledger("ra-2", "raw").exists(_.rawCount == 0))
  }

  test("rejects.max_reject_count: the ceiling fails the run (exit 20) and nothing is published or committed") {
    val R2 = mkTable(s"$Src.bstar_rej_cap")
    insert(R2, "I", "2026-01-07", "04.00.00", Seq(("F", 1, "BAD", "2026-01-06 23:00:00.000006", "PT")))
    val e = "rej_cap"
    val f = feed(e, source = R2, extra = s"""rejects { $Rule, max_reject_count = 0 }""")
    assert(run(f, cli(e, "rc-1"), source = R2) == ExitDataIntegrity)
    assert(!curatedExists(e))
    assert(watermarkRows(e) == 0)
  }

  // ==========================================================================
  // Recovery in a NEW driver JVM: the in-memory read window is gone, the
  // ledger must carry it
  // ==========================================================================

  test("--resume --run-id after the driver died (read windows cleared): curated published from the ledger's window, watermark committed") {
    val e = "res_jvm"
    val f = feed(e)
    assert(run(f, cli(e, "rj-1", stage = "raw")) == 0)
    assert(ledger("rj-1", "raw").exists(l => l.status == "SUCCESS" && l.windowEnd == P2))
    assert(watermark(e).isEmpty, "raw-only without advance_after = RAW holds the watermark")

    HiveTestSeams.clearReadWindows()   // a new JVM knows nothing of rj-1's window
    assert(run(f, Cli(entity = e, mode = "INCR", runId = Some("rj-1"), resume = true)) == 0)
    assert(ledger("rj-1", "raw").exists(_.status == "SKIPPED"))
    assert(ledger("rj-1", "curated").exists(_.status == "SUCCESS"))
    assert(curatedCount(e) == 3 && duplicateKeys(e) == 0, "curated is rebuilt from the ledger's window")
    // The raw stage was skipped, so the source has no in-memory window: the
    // commit must recover (window_start, window_end] from the raw SUCCESS
    // ledger row — which needs the INJECTED audit coordinates. Regression
    // guard for the un-injected-config defect found 2026-10-01.
    assert(watermark(e).contains((P2, 1L)),
      "store version advanced to 1 with value = the resumed run's window_end, committed from the ledger")
    assert(watermarkRows(e) == 1)
  }

  test("DECOUPLED --pending after the driver died: the batch is drained from the source by its recorded window") {
    val e = "pend_jvm"
    val f = feed(e, decoupled = true)
    assert(run(f, cli(e, "pj-1", stage = "raw")) == 0)
    assert(watermark(e).contains((P2, 1L)), "advance_after = RAW")
    assert(!curatedExists(e))

    HiveTestSeams.clearReadWindows()
    assert(run(f, Cli(entity = e, mode = "INCR", stage = "curated", pending = true)) == 0)
    assert(ledger("pj-1", "curated").exists(_.status == "SUCCESS"))
    assert(curatedCount(e) == 3 && duplicateKeys(e) == 0)
    assert(checks("pj-1").get("curated_accounts_for_replayed_rows").contains(true))
    // Drained: a second --pending finds nothing and changes nothing.
    assert(run(f, Cli(entity = e, mode = "INCR", stage = "curated", pending = true)) == 0)
    assert(curatedCount(e) == 3)
  }

  // ==========================================================================
  // Retention of the shared watermark table
  // ==========================================================================

  test("--stage retention trims watermark history to keep_last, preserves every entity's current position, never touches the source") {
    val K = mkTable(s"$Src.bstar_keep")
    val a = "keep_a"; val b = "keep_b"
    def both(tag: String): Unit = Seq(a, b).foreach(e => assert(run(feed(e, source = K), cli(e, s"$e-$tag"), source = K) == 0))
    both("1")
    insert(K, "I", "2026-01-07", "04.00.00", Seq(("H", 1, "h1", "2026-01-06 23:00:00.000008", "PT")))
    both("2")
    insert(K, "I", "2026-01-08", "04.00.00", Seq(("J", 1, "j1", "2026-01-07 23:00:00.000009", "PT")))
    both("3")
    val P4 = "2026-01-08|04.00.00"
    assert(watermarkRows(a) == 3 && watermarkRows(b) == 3)
    assert(watermark(a).contains((P4, 3L)) && watermark(b).contains((P4, 3L)))

    val ret = feed(a, source = K, extra = """retention { rejects = "30d", audit = "30d", watermarks_keep_last = 2 }""")
    assert(run(ret, cli(a, "keep-ret", stage = "retention"), source = K) == 0)
    assert(ledger("keep-ret", "retention").exists(_.status == "SUCCESS"))
    assert(watermarkRows(a) == 2, "history beyond the last 2 versions is gone")
    assert(watermark(a).contains((P4, 3L)), "the CURRENT position survives trimming")
    assert(watermark(b).contains((P4, 3L)), "another entity's current position is untouched")
    assert(watermarkRows(b) >= 2 && watermarkRows(b) <= 3)
    assert(curatedCount(a) == 5 && curatedCount(b) == 5, "retention never touches curated")

    // Trimming did not break continuity: the next run still starts where the current position says.
    insert(K, "I", "2026-01-09", "04.00.00", Seq(("L", 1, "l1", "2026-01-08 23:00:00.000010", "PT")))
    assert(run(feed(a, source = K), cli(a, "keep-a-4"), source = K) == 0)
    assert(ledger("keep-a-4", "raw").exists(l => l.windowStart == P4 && l.acceptedCount == 1))
    assert(checks("keep-a-4").get("watermark_continuity").contains(true))
    assert(watermark(a).contains(("2026-01-09|04.00.00", 4L)))
  }
}
