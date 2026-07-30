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
