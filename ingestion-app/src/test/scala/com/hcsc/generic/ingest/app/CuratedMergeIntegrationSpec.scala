package com.hcsc.generic.ingest.app

import com.hcsc.generic.ingest.publish.{PublishRequest, PublishService, PublishValidationException}
import com.hcsc.generic.ingest.reject.RejectService
import com.hcsc.generic.ingest.runtime.RunContext
import com.hcsc.generic.ingest.stage.CuratedService
import com.typesafe.config.ConfigFactory
import java.nio.file.{Files, Path}
import java.sql.Timestamp
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
  * Acceptance-criteria harness for the curated merge semantics:
  *  - key hygiene (dedup + null-key quarantine) on EVERY publish path,
  *    including the first run of an incremental feed;
  *  - freshness-compared upsert: the latest source version wins, a
  *    late-arriving older record never overwrites a newer curated row,
  *    exact ties deterministically keep the target;
  *  - business-key uniqueness and volume guardrails at publish time.
  */
class CuratedMergeIntegrationSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var tempDir: Path = _
  private var spark: SparkSession = _
  private val logger = Logger.getLogger(getClass.getName)

  override def beforeAll(): Unit = {
    super.beforeAll()
    tempDir = Files.createTempDirectory("curated-merge-")
    System.setProperty("derby.stream.error.file",
      tempDir.resolve("derby.log").toAbsolutePath.toString)

    spark = SparkSession.builder()
      .master("local[2]")
      .appName("curated-merge-test")
      .config("spark.sql.warehouse.dir", tempDir.resolve("warehouse").toAbsolutePath.toString)
      .config("javax.jdo.option.ConnectionURL",
        s"jdbc:derby:;databaseName=${tempDir.resolve("metastore_db").toAbsolutePath};create=true")
      .config("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver")
      .config("datanucleus.schema.autoCreateTables", "true")
      .config("hive.metastore.schema.verification", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.caseSensitive", "false")
      .config("spark.sql.session.timeZone", "UTC")
      .enableHiveSupport()
      .getOrCreate()

    Seq("m_curated", "m_audit").foreach(db => spark.sql(s"CREATE DATABASE IF NOT EXISTS $db"))
  }

  override def afterAll(): Unit = {
    try { if (spark != null) spark.stop() }
    finally {
      import scala.collection.JavaConverters._
      Files.walk(tempDir).iterator().asScala.toSeq.reverse.foreach(p => Files.deleteIfExists(p))
    }
    super.afterAll()
  }

  // ---------------------------------------------------------------------------
  // Fixtures
  // ---------------------------------------------------------------------------

  private val curatedConf = ConfigFactory.parseString(
    """
      |database = m_curated
      |table = members
      |format = parquet
      |merge {
      |  keys = ["member_id"]
      |  freshness { column = "src_modified_ts", tie_breakers = ["src_seq"] }
      |}
      |""".stripMargin)

  private val rejectsConf = ConfigFactory.parseString(
    """{ enabled = true, database = "m_audit", table = "ingest_rejects" }""")

  private def service = new CuratedService(spark, curatedConf)
  private def rejects = new RejectService(spark, Some(rejectsConf), None, logger)
  private def ctx(runId: String) = RunContext(runId, "members_feed", "INCR", "I")

  /** (member_id, name, src_modified_ts, src_seq) */
  private def batch(rows: Seq[(String, String, String, Int)]): DataFrame = {
    val s = spark
    import s.implicits._
    rows.toDF("member_id", "name", "src_modified_ts", "src_seq")
      .withColumn("src_modified_ts", col("src_modified_ts").cast("timestamp"))
  }

  private def curatedRow(memberId: String): org.apache.spark.sql.Row =
    spark.table("m_curated.members").filter(col("member_id") === memberId).collect().head

  // ---------------------------------------------------------------------------
  // 1: first-run hygiene (C12) — dedup + null-key quarantine before any publish
  // ---------------------------------------------------------------------------

  test("initial publish of an INCR feed deduplicates and quarantines null keys") {
    val df = batch(Seq(
      ("M1", "alice_v1", "2026-01-01 10:00:00", 1),
      ("M1", "alice_v2", "2026-01-01 11:00:00", 2), // in-batch duplicate; newer wins
      ("M2", "bob_v1", "2026-01-01 10:00:00", 1),
      (null.asInstanceOf[String], "orphan", "2026-01-01 10:00:00", 1)
    ))

    val result = service.process(df, "INCR", ctx("mrun-1"), None, Some(rejects)).get

    assert(result.publishedCount == 2)
    assert(result.insertCount == 2)
    assert(result.nullKeyCount == 1)
    assert(result.dedupedCount == 1)

    val table = spark.table("m_curated.members")
    assert(table.count() == 2)
    assert(curatedRow("M1").getAs[String]("name") == "alice_v2")

    // The null-key row is quarantined with lineage, not silently dropped
    val quarantined = spark.table("m_audit.ingest_rejects")
      .filter(col("run_id") === "mrun-1" && col("error_code") === CuratedService.NullKeyErrorCode)
    assert(quarantined.count() == 1)
    assert(quarantined.collect().head.getAs[String]("raw_record").contains("orphan"))
  }

  // ---------------------------------------------------------------------------
  // 2: late-arriving older record never overwrites newer curated (C1/§9)
  // ---------------------------------------------------------------------------

  test("late-arriving older record is ignored and does not restamp the target") {
    val stale = batch(Seq(("M1", "alice_stale", "2026-01-01 09:00:00", 9)))

    val result = service.process(stale, "INCR", ctx("mrun-2"), None, Some(rejects)).get

    assert(result.ignoredCount == 1)
    assert(result.insertCount == 0)
    assert(result.updateCount == 0)

    val m1 = curatedRow("M1")
    assert(m1.getAs[String]("name") == "alice_v2", "stale record must not overwrite newer curated row")
    assert(m1.getAs[String]("last_modified_op") == "I",
      "an ignored stale row must not restamp the target as updated")
  }

  // ---------------------------------------------------------------------------
  // 3: newer backfilled record updates curated and preserves create_timestamp
  // ---------------------------------------------------------------------------

  test("newer record updates curated, stamps 'U' and inherits create_timestamp") {
    val originalCreate = curatedRow("M1").getAs[Timestamp]("create_timestamp")

    val fresh = batch(Seq(("M1", "alice_new", "2026-01-01 12:00:00", 5)))
    val result = service.process(fresh, "INCR", ctx("mrun-3"), None, Some(rejects)).get

    assert(result.updateCount == 1)
    assert(result.ignoredCount == 0)

    val m1 = curatedRow("M1")
    assert(m1.getAs[String]("name") == "alice_new")
    assert(m1.getAs[String]("last_modified_op") == "U")
    assert(m1.getAs[Timestamp]("create_timestamp") == originalCreate,
      "updates must keep the original creation timestamp")
  }

  // ---------------------------------------------------------------------------
  // 4: exact freshness + tie-breaker tie keeps the target deterministically
  // ---------------------------------------------------------------------------

  test("equal freshness and tie-breakers deterministically keep the target row") {
    val challenger = batch(Seq(("M1", "alice_challenger", "2026-01-01 12:00:00", 5)))
    val result = service.process(challenger, "INCR", ctx("mrun-4"), None, Some(rejects)).get

    assert(result.ignoredCount == 1)
    assert(result.updateCount == 0)
    assert(curatedRow("M1").getAs[String]("name") == "alice_new",
      "an exact tie must keep the existing curated row")
  }

  // ---------------------------------------------------------------------------
  // 5: mixed batch — new key inserts while a stale row is ignored
  // ---------------------------------------------------------------------------

  test("mixed batch inserts new keys while ignoring stale ones") {
    val mixed = batch(Seq(
      ("M3", "carol_v1", "2026-01-01 10:00:00", 1), // brand new key
      ("M2", "bob_stale", "2026-01-01 09:00:00", 1) // older than curated bob_v1@10:00
    ))
    val result = service.process(mixed, "INCR", ctx("mrun-5"), None, Some(rejects)).get

    assert(result.insertCount == 1)
    assert(result.ignoredCount == 1)
    assert(result.updateCount == 0)
    assert(spark.table("m_curated.members").count() == 3)
    assert(curatedRow("M2").getAs[String]("name") == "bob_v1")
    assert(curatedRow("M3").getAs[String]("name") == "carol_v1")
  }

  // ---------------------------------------------------------------------------
  // 6: accounting identity across a merge run
  // ---------------------------------------------------------------------------

  test("curated counts account for every incoming row") {
    val df = batch(Seq(
      ("M4", "dave_v1", "2026-01-01 10:00:00", 1),
      ("M4", "dave_v2", "2026-01-01 11:00:00", 2), // dedup loser
      ("M1", "alice_stale2", "2026-01-01 08:00:00", 1), // ignored (stale)
      (null.asInstanceOf[String], "orphan2", "2026-01-01 10:00:00", 1) // quarantined
    ))
    val result = service.process(df, "INCR", ctx("mrun-6"), None, Some(rejects)).get

    val accounted = result.insertCount + result.updateCount + result.ignoredCount +
      result.nullKeyCount + result.dedupedCount
    assert(accounted == 4, s"every incoming row must be accounted for (got $accounted)")
    assert(result.insertCount == 1)   // M4
    assert(result.ignoredCount == 1)  // stale M1
    assert(result.nullKeyCount == 1)
    assert(result.dedupedCount == 1)
  }

  // ---------------------------------------------------------------------------
  // 6b: drop_null_keys=false — distinct keyless records pass through unmerged
  // ---------------------------------------------------------------------------

  test("null-key rows pass through unmerged and never collapse when drop_null_keys=false") {
    val passConf = ConfigFactory.parseString(
      """
        |database = m_curated
        |table = members_pass
        |format = parquet
        |merge {
        |  keys = ["member_id"]
        |  freshness { column = "src_modified_ts", tie_breakers = ["src_seq"] }
        |  null_handling { drop_null_keys = false }
        |}
        |""".stripMargin)
    val svc = new CuratedService(spark, passConf)

    // Two DISTINCT keyless records + one keyed record: PARTITION BY on a
    // null key treats NULL = NULL as a match, so before the fix one of the
    // keyless records was silently destroyed.
    val first = batch(Seq(
      ("P1", "keyed_v1", "2026-01-01 10:00:00", 1),
      (null.asInstanceOf[String], "orphan_a", "2026-01-01 10:00:00", 1),
      (null.asInstanceOf[String], "orphan_b", "2026-01-01 10:00:00", 1)
    ))
    val r1 = svc.process(first, "INCR", ctx("mrun-pass-1"), None, None).get
    assert(r1.passthroughCount == 2)
    assert(r1.nullKeyCount == 0)
    val table1 = spark.table("m_curated.members_pass")
    assert(table1.filter(col("member_id").isNull).count() == 2,
      "both distinct keyless records must survive")

    // Second run: keyless history accumulates append-only; keyed rows merge
    val second = batch(Seq(
      ("P1", "keyed_v2", "2026-01-01 11:00:00", 2),
      (null.asInstanceOf[String], "orphan_c", "2026-01-01 10:00:00", 1)
    ))
    val r2 = svc.process(second, "INCR", ctx("mrun-pass-2"), None, None).get
    assert(r2.passthroughCount == 1)
    assert(r2.updateCount == 1)
    val table2 = spark.table("m_curated.members_pass")
    assert(table2.filter(col("member_id").isNull).count() == 3,
      "keyless history is append-only, never merged or collapsed")
    assert(table2.filter(col("member_id") === "P1").count() == 1)
    assert(curatedRowIn("members_pass", "P1").getAs[String]("name") == "keyed_v2")
  }

  private def curatedRowIn(table: String, memberId: String): org.apache.spark.sql.Row =
    spark.table(s"m_curated.$table").filter(col("member_id") === memberId).collect().head

  // ---------------------------------------------------------------------------
  // 7: publish guardrails — business-key uniqueness and shrink protection
  // ---------------------------------------------------------------------------

  test("publish fails when staged data violates business-key uniqueness") {
    val s = spark
    import s.implicits._
    val dupes = Seq(("K1", "a"), ("K1", "b"), ("K2", "c")).toDF("k", "v")
    val publisher = new PublishService(spark, logger)
    val req = PublishRequest(
      database = "m_curated", table = "uniq_guard", format = "parquet", path = None,
      enforceUniqueKeys = Seq("k"))

    val e = intercept[PublishValidationException] {
      publisher.publish(dupes, req, ctx("mrun-uniq"))
    }
    assert(e.getMessage.contains("uniqueness"))
    assert(!spark.catalog.tableExists("m_curated.uniq_guard"), "target must remain untouched")
  }

  test("publish fails when the staged data would shrink the target beyond the guard") {
    val s = spark
    import s.implicits._
    val publisher = new PublishService(spark, logger)
    val base = PublishRequest(database = "m_curated", table = "shrink_guard", format = "parquet", path = None)

    publisher.publish((1 to 10).map(i => (s"K$i", i)).toDF("k", "v"), base, ctx("mrun-shrink-1"))
    assert(spark.table("m_curated.shrink_guard").count() == 10)

    val guarded = base.copy(maxShrinkPercent = Some(20.0))
    val e = intercept[PublishValidationException] {
      publisher.publish(Seq(("K1", 1)).toDF("k", "v"), guarded, ctx("mrun-shrink-2"))
    }
    assert(e.getMessage.contains("max_shrink_percent"))
    assert(spark.table("m_curated.shrink_guard").count() == 10, "target must remain untouched")
  }

  // ---------------------------------------------------------------------------
  // 6c: record_hash change detection — no-change rows never churn curated
  // ---------------------------------------------------------------------------

  test("identical record_hash skips the rewrite even when freshness advanced") {
    val hashConf = ConfigFactory.parseString(
      """
        |database = m_curated
        |table = members_hash
        |format = parquet
        |merge {
        |  keys = ["member_id"]
        |  freshness { column = "src_modified_ts", tie_breakers = ["src_seq"] }
        |}
        |""".stripMargin)
    val svc = new CuratedService(spark, hashConf)
    def hashed(rows: Seq[(String, String, String, Int)], hash: String) =
      batch(rows).withColumn("record_hash", org.apache.spark.sql.functions.lit(hash))

    // Seed
    svc.process(hashed(Seq(("H1", "original", "2026-01-01 10:00:00", 1)), "hashA"),
      "INCR", ctx("mrun-rh-1"), None, None)

    // Same content (same hash), newer freshness: the source touched the
    // timestamp without changing data — curated must not churn.
    val r2 = svc.process(hashed(Seq(("H1", "original", "2026-01-01 12:00:00", 2)), "hashA"),
      "INCR", ctx("mrun-rh-2"), None, None).get
    assert(r2.ignoredCount == 1)
    assert(r2.updateCount == 0)
    assert(curatedRowIn("members_hash", "H1").getAs[String]("last_modified_op") == "I",
      "a no-change row must not be restamped as updated")

    // Different content (new hash), newer freshness: real update
    val r3 = svc.process(hashed(Seq(("H1", "changed", "2026-01-01 13:00:00", 3)), "hashB"),
      "INCR", ctx("mrun-rh-3"), None, None).get
    assert(r3.updateCount == 1)
    assert(curatedRowIn("members_hash", "H1").getAs[String]("name") == "changed")
  }

  // ---------------------------------------------------------------------------
  // 7b: drift hardening — casts must not silently null values (CUR_002)
  // ---------------------------------------------------------------------------

  test("a cast that would NULL non-null values fails fast (CUR_002)") {
    val badCast = ConfigFactory.parseString(
      """
        |database = m_curated
        |table = cast_guard
        |format = parquet
        |column_types { name = "int" }
        |""".stripMargin)
    val df = batch(Seq(("C1", "not_a_number", "2026-01-01 10:00:00", 1)))
    val e = intercept[IllegalStateException] {
      new CuratedService(spark, badCast).process(df, "FULL", ctx("mrun-cast-1"), None, None)
    }
    assert(e.getMessage.contains("CUR_002"))
    assert(e.getMessage.contains("name"))
  }

  test("on_cast_error=WARN accepts the loss explicitly") {
    val warnCast = ConfigFactory.parseString(
      """
        |database = m_curated
        |table = cast_warn
        |format = parquet
        |column_types { name = "int" }
        |on_cast_error = "WARN"
        |""".stripMargin)
    val df = batch(Seq(("C2", "not_a_number", "2026-01-01 10:00:00", 1)))
    val result = new CuratedService(spark, warnCast).process(df, "FULL", ctx("mrun-cast-2"), None, None)
    assert(result.get.publishedCount == 1)
  }

  test("CUR_002 guard sees contract-transformed values, not raw ones") {
    // Raw value casts cleanly to int, but the contract transform mangles it
    // first — the guard must validate the frame the cast actually runs on.
    val contract = com.hcsc.generic.ingest.schema.SchemaContract.parse(
      ConfigFactory.parseString(
        """schema {
          |  version = "1.0"
          |  columns = [ { name = "name", type = "string", transform = "concat({col}, 'x')" } ]
          |}""".stripMargin))
    val cfg = ConfigFactory.parseString(
      """
        |database = m_curated
        |table = cast_guard_xform
        |format = parquet
        |column_types { name = "int" }
        |""".stripMargin)
    val df = batch(Seq(("C3", "42", "2026-01-01 10:00:00", 1)))
    val e = intercept[IllegalStateException] {
      new CuratedService(spark, cfg).process(df, "FULL", ctx("mrun-cast-3"), contract, None)
    }
    assert(e.getMessage.contains("CUR_002"))
    assert(e.getMessage.contains("name"))
  }

  test("CUR_002 guard does not false-positive when the transform repairs the value") {
    // Raw value would NOT cast ("1,234"), but the contract transform strips
    // the separator before the cast — guarding the raw frame would wrongly
    // block a publish that loses nothing.
    val contract = com.hcsc.generic.ingest.schema.SchemaContract.parse(
      ConfigFactory.parseString(
        """schema {
          |  version = "1.0"
          |  columns = [ { name = "name", type = "string", transform = "regexp_replace({col}, ',', '')" } ]
          |}""".stripMargin))
    val cfg = ConfigFactory.parseString(
      """
        |database = m_curated
        |table = cast_relief
        |format = parquet
        |column_types { name = "int" }
        |""".stripMargin)
    val df = batch(Seq(("C4", "1,234", "2026-01-01 10:00:00", 1)))
    val result = new CuratedService(spark, cfg).process(df, "FULL", ctx("mrun-cast-4"), contract, None)
    assert(result.get.publishedCount == 1)
    assert(spark.table("m_curated.cast_relief").collect().head.getAs[Int]("name") == 1234)
  }

  // ---------------------------------------------------------------------------
  // 7c: reject payload redaction — HASHED stores no plaintext values
  // ---------------------------------------------------------------------------

  test("rejects.payload=HASHED quarantines null-key rows without plaintext") {
    val hashedRejects = new RejectService(spark,
      Some(ConfigFactory.parseString(
        """{ enabled = true, database = "m_audit", table = "ingest_rejects", payload = "HASHED" }""")),
      None, logger)
    val df = batch(Seq(
      ("M8", "visible", "2026-01-01 10:00:00", 1),
      (null.asInstanceOf[String], "secret_name", "2026-01-01 10:00:00", 1)
    ))
    val result = service.process(df, "INCR", ctx("mrun-hash-1"), None, Some(hashedRejects)).get
    assert(result.nullKeyCount == 1)

    val quarantined = spark.table("m_audit.ingest_rejects")
      .filter(col("run_id") === "mrun-hash-1").collect().head
    val payload = quarantined.getAs[String]("raw_record")
    assert(!payload.contains("secret_name"), "HASHED payload must not carry plaintext values")
    assert(payload.contains("name"), "HASHED payload keeps the column structure")
  }

  // ---------------------------------------------------------------------------
  // 8a: composite business keys — every key column participates in the match
  // (kills the keys.take(1) mutation class: single-key fixtures cannot)
  // ---------------------------------------------------------------------------

  test("composite business keys match on ALL columns, not a prefix") {
    val s = spark
    import s.implicits._
    val compConf = ConfigFactory.parseString(
      """
        |database = m_curated
        |table = members_comp
        |format = parquet
        |merge {
        |  keys = ["src_sys_nm", "member_id"]
        |  freshness { column = "src_modified_ts", tie_breakers = ["src_seq"] }
        |}
        |""".stripMargin)
    def comp(rows: Seq[(String, String, String, String, Int)]) =
      rows.toDF("src_sys_nm", "member_id", "name", "src_modified_ts", "src_seq")
        .withColumn("src_modified_ts", col("src_modified_ts").cast("timestamp"))
    val svc = new CuratedService(spark, compConf)

    // Same member_id under two systems: distinct composite keys, must NOT collapse
    svc.process(comp(Seq(
      ("SYS_A", "M1", "a_m1", "2026-01-01 10:00:00", 1),
      ("SYS_B", "M1", "b_m1", "2026-01-01 10:00:00", 1)
    )), "INCR", ctx("mrun-comp-1"), None, None)
    assert(spark.table("m_curated.members_comp").count() == 2,
      "two systems sharing a member_id are DIFFERENT composite keys")

    // An update addressed to (SYS_A, M1) must touch only that tuple — a
    // prefix-only or single-column match would also update SYS_B's row
    val r = svc.process(comp(Seq(("SYS_A", "M1", "a_m1_v2", "2026-01-01 12:00:00", 2))),
      "INCR", ctx("mrun-comp-2"), None, None).get
    assert(r.updateCount == 1 && r.insertCount == 0)
    val rows = spark.table("m_curated.members_comp").collect()
      .map(x => (x.getAs[String]("src_sys_nm"), x.getAs[String]("name"))).toMap
    assert(rows("SYS_A") == "a_m1_v2", "the addressed composite key must update")
    assert(rows("SYS_B") == "b_m1", "the sibling composite key must be untouched")
  }

  // ---------------------------------------------------------------------------
  // 8b: reject replay semantics — a replayed run REPLACES its reject slice
  // ---------------------------------------------------------------------------

  test("re-splitting the same run does not double-append raw-stage rejects") {
    val s = spark
    import s.implicits._
    val guarded = new RejectService(spark,
      Some(ConfigFactory.parseString(
        """{ enabled = true, database = "m_audit", table = "ingest_rejects",
          |  rules = [ { name = "blank_id", condition = "member_id IS NULL",
          |              error_code = "R900", category = "MISSING_KEY" } ] }""".stripMargin)),
      None, logger)
    val df = Seq((null.asInstanceOf[String], "orphan", "f1", "file1.csv", 0L))
      .toDF("member_id", "name", "file_id", "source_file", "row_idx")
    val rctx = RunContext("mrun-resplit-1", "members_feed", "INCR", "I")

    guarded.split(df, rctx)
    val after1 = spark.table("m_audit.ingest_rejects")
      .filter(col("run_id") === "mrun-resplit-1" && col("error_code") === "R900").count()
    assert(after1 == 1)
    // A crashed-and-retried run re-enters split() with the SAME run id: the
    // idempotency guard must skip the append, never duplicate the rows
    guarded.split(df, rctx)
    val after2 = spark.table("m_audit.ingest_rejects")
      .filter(col("run_id") === "mrun-resplit-1" && col("error_code") === "R900").count()
    assert(after2 == 1, s"replaying the same run must not double-append rejects (got $after2)")
  }

  test("a replay with a different reject set replaces the previous attempt's rows") {
    val s = spark
    import s.implicits._
    val rctx = RunContext("mrun-replay-1", "members_feed", "INCR", "I")
    val otherCtx = RunContext("mrun-replay-other", "members_feed", "INCR", "I")

    // Another run's rows must survive the slice rewrite untouched
    rejects.persistRows(Seq(("X1", "keep")).toDF("member_id", "name"),
      otherCtx, "CUR_001", "null key", "NULL_BUSINESS_KEY")

    rejects.persistRows(Seq(("A1", "first"), ("A2", "first")).toDF("member_id", "name"),
      rctx, "CUR_001", "null key", "NULL_BUSINESS_KEY")

    def sliceRecords(runId: String): Set[String] = spark.table("m_audit.ingest_rejects")
      .filter(col("run_id") === runId && col("error_code") === "CUR_001")
      .collect().map(_.getAs[String]("raw_record")).toSet

    // Identical replay: idempotent, no duplicates
    rejects.persistRows(Seq(("A1", "first"), ("A2", "first")).toDF("member_id", "name"),
      rctx, "CUR_001", "null key", "NULL_BUSINESS_KEY")
    assert(sliceRecords("mrun-replay-1").size == 2)

    // Changed replay: the stale rows must be replaced, not skipped
    rejects.persistRows(Seq(("A3", "second")).toDF("member_id", "name"),
      rctx, "CUR_001", "null key", "NULL_BUSINESS_KEY")
    val afterChange = sliceRecords("mrun-replay-1")
    assert(afterChange.size == 1, s"stale replay rows must be replaced, got $afterChange")
    assert(afterChange.head.contains("A3"))
    assert(sliceRecords("mrun-replay-other").size == 1, "other runs' rows must survive the rewrite")

    // Replay that produces ZERO rejects: the stale slice is purged
    rejects.persistRows(Seq.empty[(String, String)].toDF("member_id", "name"),
      rctx, "CUR_001", "null key", "NULL_BUSINESS_KEY")
    assert(sliceRecords("mrun-replay-1").isEmpty, "a clean replay must purge the stale slice")
    assert(sliceRecords("mrun-replay-other").size == 1)
  }

  // ---------------------------------------------------------------------------
  // 9: soft deletes — tombstones compete by freshness (spec §8)
  // ---------------------------------------------------------------------------

  test("SOFT deletes tombstone the winning key and never resurrect by staleness") {
    val delConf = ConfigFactory.parseString(
      """
        |database = m_curated
        |table = members_del
        |format = parquet
        |merge {
        |  keys = ["member_id"]
        |  freshness { column = "src_modified_ts", tie_breakers = ["src_seq"] }
        |  deletes { mode = "SOFT", indicator_column = "status", indicator_values = ["d"] }
        |}
        |""".stripMargin)
    val svc = new CuratedService(spark, delConf)
    def withStatus(rows: Seq[(String, String, String, Int)], status: String) =
      batch(rows).withColumn("status", org.apache.spark.sql.functions.lit(status))

    // Seed an active record
    val r1 = svc.process(withStatus(Seq(("D1", "active", "2026-01-01 10:00:00", 1)), "a"),
      "INCR", ctx("mrun-del-1"), None, None).get
    assert(r1.deleteCount == 0)

    // A NEWER delete record wins and tombstones the key
    val r2 = svc.process(withStatus(Seq(("D1", "gone", "2026-01-01 12:00:00", 2)), "d"),
      "INCR", ctx("mrun-del-2"), None, None).get
    assert(r2.deleteCount == 1)
    val tombstone = spark.table("m_curated.members_del")
      .filter(col("member_id") === "D1").collect().head
    assert(tombstone.getAs[String]("last_modified_op") == "D")

    // A STALE (older) non-delete record must not resurrect the tombstone
    val r3 = svc.process(withStatus(Seq(("D1", "zombie", "2026-01-01 11:00:00", 1)), "a"),
      "INCR", ctx("mrun-del-3"), None, None).get
    assert(r3.ignoredCount == 1)
    assert(spark.table("m_curated.members_del").filter(col("member_id") === "D1")
      .collect().head.getAs[String]("last_modified_op") == "D",
      "a stale record must not undo a newer delete")
  }

  test("SOFT delete tombstones survive the record_hash no-change pre-filter") {
    val delHashConf = ConfigFactory.parseString(
      """
        |database = m_curated
        |table = members_del_hash
        |format = parquet
        |merge {
        |  keys = ["member_id"]
        |  freshness { column = "src_modified_ts", tie_breakers = ["src_seq"] }
        |  deletes { mode = "SOFT", indicator_column = "status", indicator_values = ["d"] }
        |}
        |""".stripMargin)
    val svc = new CuratedService(spark, delHashConf)
    def row(rows: Seq[(String, String, String, Int)], status: String, hash: String) =
      batch(rows)
        .withColumn("status", org.apache.spark.sql.functions.lit(status))
        .withColumn("record_hash", org.apache.spark.sql.functions.lit(hash))

    svc.process(row(Seq(("DH1", "active", "2026-01-01 10:00:00", 1)), "a", "hashA"),
      "INCR", ctx("mrun-delhash-1"), None, None)

    // The source flags the row deleted WITHOUT touching business content:
    // identical record_hash. The tombstone must not be swallowed by the
    // no-change pre-filter — the delete is the change.
    val r2 = svc.process(row(Seq(("DH1", "active", "2026-01-01 12:00:00", 2)), "d", "hashA"),
      "INCR", ctx("mrun-delhash-2"), None, None).get
    assert(r2.deleteCount == 1, "an unchanged-content tombstone must still delete")
    assert(r2.ignoredCount == 0)
    val tombstone = spark.table("m_curated.members_del_hash")
      .filter(col("member_id") === "DH1").collect().head
    assert(tombstone.getAs[String]("last_modified_op") == "D")
  }

  test("delete counts are disjoint from insert and update counts") {
    val delConf = ConfigFactory.parseString(
      """
        |database = m_curated
        |table = members_del_disjoint
        |format = parquet
        |merge {
        |  keys = ["member_id"]
        |  freshness { column = "src_modified_ts", tie_breakers = ["src_seq"] }
        |  deletes { mode = "SOFT", indicator_column = "status", indicator_values = ["d"] }
        |}
        |""".stripMargin)
    val svc = new CuratedService(spark, delConf)
    def withStatus(rows: Seq[(String, String, String, Int)], status: String) =
      batch(rows).withColumn("status", org.apache.spark.sql.functions.lit(status))

    svc.process(withStatus(Seq(("DJ1", "live", "2026-01-01 10:00:00", 1)), "a"),
      "INCR", ctx("mrun-dj-1"), None, None)

    // Contested tombstone: counted as delete, NOT also as update
    val r2 = svc.process(withStatus(Seq(("DJ1", "gone", "2026-01-01 12:00:00", 2)), "d"),
      "INCR", ctx("mrun-dj-2"), None, None).get
    assert(r2.deleteCount == 1 && r2.updateCount == 0 && r2.insertCount == 0)
    assert(r2.insertCount + r2.updateCount + r2.deleteCount + r2.ignoredCount == 1,
      "the disjoint counts must total the accepted row")

    // Brand-new-key tombstone: counted as delete, NOT also as insert
    val r3 = svc.process(withStatus(Seq(("DJ2", "never_lived", "2026-01-01 12:00:00", 1)), "d"),
      "INCR", ctx("mrun-dj-3"), None, None).get
    assert(r3.deleteCount == 1 && r3.insertCount == 0 && r3.updateCount == 0)
  }

  test("INCR passthrough rows get delete marking like the FULL path") {
    val delConf = ConfigFactory.parseString(
      """
        |database = m_curated
        |table = members_del_pass
        |format = parquet
        |merge {
        |  keys = ["member_id"]
        |  freshness { column = "src_modified_ts", tie_breakers = ["src_seq"] }
        |  null_handling { drop_null_keys = false }
        |  deletes { mode = "SOFT", indicator_column = "status", indicator_values = ["d"] }
        |}
        |""".stripMargin)
    val svc = new CuratedService(spark, delConf)
    def withStatus(rows: Seq[(String, String, String, Int)], status: String) =
      batch(rows).withColumn("status", org.apache.spark.sql.functions.lit(status))

    svc.process(withStatus(Seq(("PD1", "live", "2026-01-01 10:00:00", 1)), "a"),
      "INCR", ctx("mrun-pd-1"), None, None)

    // A keyless tombstone passes through unmerged but must still be MARKED
    val r2 = svc.process(
      withStatus(Seq((null.asInstanceOf[String], "keyless_gone", "2026-01-01 12:00:00", 1)), "d"),
      "INCR", ctx("mrun-pd-2"), None, None).get
    assert(r2.passthroughCount == 1)
    assert(r2.deleteCount == 0, "passthrough tombstones stay in passthroughCount")
    val keyless = spark.table("m_curated.members_del_pass")
      .filter(col("member_id").isNull).collect().head
    assert(keyless.getAs[String]("last_modified_op") == "D",
      "an INCR passthrough tombstone must be delete-marked like on the FULL path")
  }

  test("INCR passthrough rows are covered by the CUR_002 alignment cast guard") {
    val s = spark
    import s.implicits._
    val passConf = ConfigFactory.parseString(
      """
        |database = m_curated
        |table = members_pass_guard
        |format = parquet
        |merge {
        |  keys = ["member_id"]
        |  freshness { column = "src_modified_ts" }
        |  null_handling { drop_null_keys = false }
        |}
        |""".stripMargin)
    val svc = new CuratedService(spark, passConf)
    svc.process(batch(Seq(("PG1", "seed", "2026-01-01 10:00:00", 1))),
      "INCR", ctx("mrun-pg-1"), None, None)

    // Keyed row casts cleanly; the KEYLESS passthrough row would silently
    // NULL on the string->int alignment cast — the guard must catch it.
    val mixed = Seq(
      ("PG1", "ok", "2026-01-01 11:00:00", "7"),
      (null.asInstanceOf[String], "bad", "2026-01-01 11:00:00", "not_a_number")
    ).toDF("member_id", "name", "src_modified_ts", "src_seq")
      .withColumn("src_modified_ts", col("src_modified_ts").cast("timestamp"))
    val e = intercept[IllegalStateException] {
      svc.process(mixed, "INCR", ctx("mrun-pg-2"), None, None)
    }
    assert(e.getMessage.contains("CUR_002"))
    assert(e.getMessage.contains("src_seq"))
  }

  // ---------------------------------------------------------------------------
  // 10: contract-driven configuration — key derivation and MASKED payloads
  // ---------------------------------------------------------------------------

  private val attributedContract = com.hcsc.generic.ingest.schema.SchemaContract.parse(
    ConfigFactory.parseString(
      """schema {
        |  version = "1.0"
        |  columns = [
        |    { name = "member_id", type = "string", business_key = true, nullable = false },
        |    { name = "name", type = "string", sensitivity = "PHI", required = false },
        |    { name = "src_modified_ts", type = "timestamp", incremental = true, required = false },
        |    { name = "src_seq", type = "int", required = false }
        |  ]
        |}""".stripMargin))

  test("merge keys derive from contract business_key columns when unconfigured") {
    val keyless = ConfigFactory.parseString(
      """
        |database = m_curated
        |table = members_derived
        |format = parquet
        |merge { freshness { column = "src_modified_ts", tie_breakers = ["src_seq"] } }
        |""".stripMargin)
    val df = batch(Seq(
      ("K1", "v1", "2026-01-01 10:00:00", 1),
      ("K1", "v2", "2026-01-01 11:00:00", 2) // dedup only happens if keys derived
    ))
    val result = new CuratedService(spark, keyless)
      .process(df, "INCR", ctx("mrun-derive-1"), attributedContract, None).get
    assert(result.dedupedCount == 1, "derived keys must drive hygiene dedup")
    assert(spark.table("m_curated.members_derived").count() == 1)
  }

  test("merge.keys = [] explicitly opts out of contract-derived merge semantics") {
    val optOut = ConfigFactory.parseString(
      """
        |database = m_curated
        |table = members_optout
        |format = parquet
        |merge { keys = [] }
        |""".stripMargin)
    val df = batch(Seq(
      ("K1", "v1", "2026-01-01 10:00:00", 1),
      ("K1", "v2", "2026-01-01 11:00:00", 2) // would dedup if keys were derived
    ))
    val result = new CuratedService(spark, optOut)
      .process(df, "FULL", ctx("mrun-optout-1"), attributedContract, None).get
    assert(result.dedupedCount == 0, "explicit empty keys must disable key hygiene entirely")
    assert(result.nullKeyCount == 0)
    assert(spark.table("m_curated.members_optout").count() == 2,
      "keyless publish keeps every row — no silent contract-derived merge")
  }

  test("configured merge keys contradicting the contract's business_key fail (HDR_017)") {
    val contradicting = ConfigFactory.parseString(
      """
        |database = m_curated
        |table = members_conflict
        |format = parquet
        |merge { keys = ["name"] }
        |""".stripMargin)
    val e = intercept[IllegalArgumentException] {
      new CuratedService(spark, contradicting)
        .process(batch(Seq(("K1", "v", "2026-01-01 10:00:00", 1))),
          "INCR", ctx("mrun-derive-2"), attributedContract, None)
    }
    assert(e.getMessage.contains("HDR_017"))
    assert(e.getMessage.contains("business_key"))
  }

  test("rejects.payload=MASKED hashes only sensitivity-tagged columns") {
    val maskedRejects = new RejectService(spark,
      Some(ConfigFactory.parseString(
        """{ enabled = true, database = "m_audit", table = "ingest_rejects", payload = "MASKED" }""")),
      attributedContract, logger)
    val df = batch(Seq(
      ("M9", "ok", "2026-01-01 10:00:00", 1),
      (null.asInstanceOf[String], "secret_phi_name", "2026-01-01 10:00:00", 7)
    ))
    val result = service.process(df, "INCR", ctx("mrun-mask-1"), None, Some(maskedRejects)).get
    assert(result.nullKeyCount == 1)

    val payload = spark.table("m_audit.ingest_rejects")
      .filter(col("run_id") === "mrun-mask-1").collect().head.getAs[String]("raw_record")
    assert(!payload.contains("secret_phi_name"), "PHI column must be hashed")
    assert(payload.contains("\"src_seq\":7"), "non-sensitive columns stay readable for triage")
  }

  test("rejects.payload=KEYS_ONLY hashes sensitivity-tagged key columns") {
    val keysOnlyRejects = new RejectService(spark,
      Some(ConfigFactory.parseString(
        """{ enabled = true, database = "m_audit", table = "ingest_rejects",
          |  payload = "KEYS_ONLY", payload_columns = ["member_id", "name", "src_seq"] }""".stripMargin)),
      attributedContract, logger)
    val df = batch(Seq(
      ("K9", "ok", "2026-01-01 10:00:00", 1),
      (null.asInstanceOf[String], "secret_phi_name", "2026-01-01 10:00:00", 8)
    ))
    val result = service.process(df, "INCR", ctx("mrun-keysonly-1"), None, Some(keysOnlyRejects)).get
    assert(result.nullKeyCount == 1)

    val payload = spark.table("m_audit.ingest_rejects")
      .filter(col("run_id") === "mrun-keysonly-1").collect().head.getAs[String]("raw_record")
    assert(!payload.contains("secret_phi_name"),
      "a sensitivity-tagged column must be hashed even under KEYS_ONLY")
    assert(payload.contains("\"src_seq\":8"), "non-sensitive payload columns stay readable")
  }

  test("curated.partitioning creates a partitioned target on the FULL path") {
    val partitioned = ConfigFactory.parseString(
      """
        |database = m_curated
        |table = members_part
        |format = parquet
        |partitioning { keys = ["name"] }
        |""".stripMargin)
    val r = new CuratedService(spark, partitioned)
      .process(batch(Seq(("P1", "east", "2026-01-01 10:00:00", 1),
        ("P2", "west", "2026-01-01 10:00:00", 1))), "FULL", ctx("mrun-part-1"), None, None).get
    assert(r.publishedCount == 2)
    val parts = spark.sql("SHOW PARTITIONS m_curated.members_part").collect().map(_.getString(0))
    assert(parts.toSet == Set("name=east", "name=west"),
      s"the curated target must carry the configured partition layout, got ${parts.mkString(",")}")
  }

  // ---------------------------------------------------------------------------
  // 8: freshness column colliding with framework audit columns is rejected
  // ---------------------------------------------------------------------------

  test("freshness column colliding with a framework audit column fails fast") {
    val bad = ConfigFactory.parseString(
      """
        |database = m_curated
        |table = members
        |merge {
        |  keys = ["member_id"]
        |  freshness { column = "last_modified_ts" }
        |}
        |""".stripMargin)
    val df = batch(Seq(("M9", "x", "2026-01-01 10:00:00", 1)))
    val e = intercept[IllegalArgumentException] {
      new CuratedService(spark, bad).process(df, "INCR", ctx("mrun-bad"), None, None)
    }
    assert(e.getMessage.contains("collides with a framework audit column"))
  }
}
