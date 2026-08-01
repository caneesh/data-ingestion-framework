package com.hcsc.generic.ingest.app

import com.hcsc.generic.ingest.runtime.RunContext
import com.hcsc.generic.ingest.stage.CuratedService
import com.typesafe.config.ConfigFactory
import java.nio.file.{Files, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/** Curated partitioning end to end on non-transactional datasource tables:
  * single- and multi-key layouts, PARTITION_OVERWRITE scoping (only affected
  * partitions rewritten), moved business keys, null-partition policies,
  * idempotent reprocessing, the semi-join fallback and explicit
  * FULL_OVERWRITE. The unpartitioned path is covered exhaustively by
  * CuratedMergeIntegrationSpec — same writer, same semantics. */
class CuratedPartitioningSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var tempDir: Path = _
  private var spark: SparkSession = _
  private val logger = Logger.getLogger(getClass.getName)

  override def beforeAll(): Unit = {
    super.beforeAll()
    tempDir = Files.createTempDirectory("curated-part-")
    System.setProperty("derby.stream.error.file",
      tempDir.resolve("derby.log").toAbsolutePath.toString)
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("curated-partitioning-test")
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
    spark.sql("CREATE DATABASE IF NOT EXISTS cp")
  }

  override def afterAll(): Unit = {
    try { if (spark != null) spark.stop() }
    finally {
      import scala.collection.JavaConverters._
      Files.walk(tempDir).iterator().asScala.toSeq.reverse.foreach(p => Files.deleteIfExists(p))
    }
    super.afterAll()
  }

  private def ctx(runId: String) = RunContext(runId, "cp_feed", "INCR", "I")

  /** (member_id, name, src_modified_ts, src_seq, src_sys_nm, corp_ent_cd) */
  private def batch(rows: Seq[(String, String, String, Int, String, String)]): DataFrame = {
    val s = spark
    import s.implicits._
    rows.toDF("member_id", "name", "src_modified_ts", "src_seq", "src_sys_nm", "corp_ent_cd")
      .withColumn("src_modified_ts", col("src_modified_ts").cast("timestamp"))
  }

  private def conf(table: String, extra: String = ""): com.typesafe.config.Config =
    ConfigFactory.parseString(
      s"""
         |database = cp
         |table = $table
         |format = parquet
         |merge {
         |  keys = ["member_id"]
         |  freshness { column = "src_modified_ts", tie_breakers = ["src_seq"] }
         |}
         |$extra
         |""".stripMargin)

  private def rows(table: String): Map[String, (String, String)] =
    spark.table(s"cp.$table").collect()
      .map(r => r.getAs[String]("member_id") ->
        ((r.getAs[String]("name"), r.getAs[String]("src_sys_nm")))).toMap

  private def partitions(table: String): Set[String] =
    spark.sql(s"SHOW PARTITIONS cp.$table").collect().map(_.getString(0)).toSet

  // ---------------------------------------------------------------------------
  // Single partition column: only affected partitions are rewritten
  // ---------------------------------------------------------------------------

  test("PARTITION_OVERWRITE rewrites only the affected partitions (single key)") {
    val c = conf("m_single",
      """partitioning { keys = ["src_sys_nm"] }
        |publish { mode = "PARTITION_OVERWRITE" }""".stripMargin)
    val svc = new CuratedService(spark, c)

    // Initial build (table absent -> full publish creating the layout)
    svc.process(batch(Seq(
      ("M1", "a_v1", "2026-01-01 10:00:00", 1, "SYS_A", "C1"),
      ("M2", "b_v1", "2026-01-01 10:00:00", 1, "SYS_B", "C1")
    )), "FULL", ctx("cp-1"), None, None)
    assert(partitions("m_single") == Set("src_sys_nm=SYS_A", "src_sys_nm=SYS_B"))

    // Freeze SYS_B's physical files: an untouched partition must not be rewritten
    val sysBFilesBefore = spark.sql(
      "SELECT input_file_name() AS f FROM cp.m_single WHERE src_sys_nm='SYS_B'")
      .collect().map(_.getString(0)).toSet

    // INCR batch touches only SYS_A: update M1, insert M3, plus a stale row
    val r = svc.process(batch(Seq(
      ("M1", "a_v2", "2026-01-01 12:00:00", 2, "SYS_A", "C1"),
      ("M1", "a_stale", "2026-01-01 09:00:00", 1, "SYS_A", "C1"), // in-batch loser
      ("M3", "c_v1", "2026-01-01 12:00:00", 1, "SYS_A", "C1")
    )), "INCR", ctx("cp-2"), None, None).get

    assert(r.updateCount == 1 && r.insertCount == 1 && r.dedupedCount == 1)
    val all = rows("m_single")
    assert(all("M1")._1 == "a_v2", "newer record must win")
    assert(all("M2")._1 == "b_v1", "unaffected partition rows must be unchanged")
    assert(all("M3")._1 == "c_v1")

    val sysBFilesAfter = spark.sql(
      "SELECT input_file_name() AS f FROM cp.m_single WHERE src_sys_nm='SYS_B'")
      .collect().map(_.getString(0)).toSet
    assert(sysBFilesAfter == sysBFilesBefore,
      "the unaffected partition's physical files must not be rewritten")
  }

  test("stale incoming record does not overwrite a newer target row (partitioned)") {
    val svc = new CuratedService(spark, conf("m_single",
      """partitioning { keys = ["src_sys_nm"] }
        |publish { mode = "PARTITION_OVERWRITE" }""".stripMargin))
    val r = svc.process(batch(Seq(
      ("M1", "a_older", "2026-01-01 11:00:00", 1, "SYS_A", "C1")
    )), "INCR", ctx("cp-3"), None, None).get
    assert(r.ignoredCount == 1 && r.updateCount == 0)
    assert(rows("m_single")("M1")._1 == "a_v2", "stale update must be ignored")
  }

  // ---------------------------------------------------------------------------
  // Multiple partition columns
  // ---------------------------------------------------------------------------

  test("PARTITION_OVERWRITE with two partition keys scopes by the tuple") {
    val c = conf("m_multi",
      """partitioning { keys = ["src_sys_nm", "corp_ent_cd"] }
        |publish { mode = "PARTITION_OVERWRITE" }""".stripMargin)
    val svc = new CuratedService(spark, c)
    svc.process(batch(Seq(
      ("M1", "v1", "2026-01-01 10:00:00", 1, "SYS_A", "C1"),
      ("M2", "v1", "2026-01-01 10:00:00", 1, "SYS_A", "C2"),
      ("M3", "v1", "2026-01-01 10:00:00", 1, "SYS_B", "C1")
    )), "FULL", ctx("cp-m1"), None, None)
    assert(partitions("m_multi").size == 3)

    // Touch only (SYS_A, C2)
    val r = svc.process(batch(Seq(
      ("M2", "v2", "2026-01-01 12:00:00", 2, "SYS_A", "C2")
    )), "INCR", ctx("cp-m2"), None, None).get
    assert(r.updateCount == 1)
    val all = rows("m_multi")
    assert(all("M1")._1 == "v1" && all("M3")._1 == "v1", "sibling tuples untouched")
    assert(all("M2")._1 == "v2")
  }

  // ---------------------------------------------------------------------------
  // Moved business key: partition columns are NOT part of the business key
  // ---------------------------------------------------------------------------

  test("a business key moving between partitions leaves no obsolete record") {
    val c = conf("m_moved",
      """partitioning { keys = ["src_sys_nm"] }
        |publish { mode = "PARTITION_OVERWRITE" }""".stripMargin)
    val svc = new CuratedService(spark, c)
    svc.process(batch(Seq(
      ("M1", "in_A", "2026-01-01 10:00:00", 1, "SYS_A", "C1"), // alone in SYS_A
      ("M2", "in_B", "2026-01-01 10:00:00", 1, "SYS_B", "C1")
    )), "FULL", ctx("cp-mv1"), None, None)

    // M1 moves SYS_A -> SYS_B; SYS_A becomes empty and must not keep a stale row
    val r = svc.process(batch(Seq(
      ("M1", "moved_to_B", "2026-01-01 12:00:00", 2, "SYS_B", "C1")
    )), "INCR", ctx("cp-mv2"), None, None).get
    assert(r.updateCount == 1)

    val all = spark.table("cp.m_moved").collect()
      .map(r0 => (r0.getAs[String]("member_id"), r0.getAs[String]("src_sys_nm")))
    assert(all.count(_._1 == "M1") == 1, s"exactly one M1 must survive, got ${all.toSeq}")
    assert(all.contains(("M1", "SYS_B")))
    assert(!partitions("m_moved").contains("src_sys_nm=SYS_A"),
      "the emptied old partition must be dropped, not left holding a stale row")
  }

  // ---------------------------------------------------------------------------
  // Null partition values
  // ---------------------------------------------------------------------------

  test("null partition values are rejected by default and defaulted when configured") {
    val reject = new CuratedService(spark, conf("m_null_r",
      """partitioning { keys = ["src_sys_nm"] }"""))
    val e = intercept[IllegalStateException] {
      reject.process(batch(Seq(
        ("M1", "v", "2026-01-01 10:00:00", 1, null.asInstanceOf[String], "C1")
      )), "FULL", ctx("cp-n1"), None, None)
    }
    assert(e.getMessage.contains("CUR_006"))
    assert(!spark.catalog.tableExists("cp.m_null_r"), "REJECT must fail before any write")

    val defaulted = new CuratedService(spark, conf("m_null_d",
      """partitioning { keys = ["src_sys_nm"], null_values = "DEFAULT", default_value = "UNKNOWN" }"""))
    defaulted.process(batch(Seq(
      ("M1", "v", "2026-01-01 10:00:00", 1, null.asInstanceOf[String], "C1")
    )), "FULL", ctx("cp-n2"), None, None)
    assert(partitions("m_null_d") == Set("src_sys_nm=UNKNOWN"))
  }

  // ---------------------------------------------------------------------------
  // Validation + empty batch + idempotency
  // ---------------------------------------------------------------------------

  test("a configured partition column missing from the data fails validation") {
    val svc = new CuratedService(spark, conf("m_missing",
      """partitioning { keys = ["not_a_column"] }"""))
    val e = intercept[IllegalArgumentException] {
      svc.process(batch(Seq(("M1", "v", "2026-01-01 10:00:00", 1, "SYS_A", "C1"))),
        "FULL", ctx("cp-x1"), None, None)
    }
    assert(e.getMessage.contains("CUR_006"))
    assert(e.getMessage.contains("not_a_column"))
  }

  test("an empty incoming batch touches nothing under PARTITION_OVERWRITE") {
    val svc = new CuratedService(spark, conf("m_single",
      """partitioning { keys = ["src_sys_nm"] }
        |publish { mode = "PARTITION_OVERWRITE" }""".stripMargin))
    val before = spark.table("cp.m_single").count()
    val r = svc.process(batch(Seq.empty), "INCR", ctx("cp-e1"), None, None).get
    assert(r.publishedCount == 0)
    assert(spark.table("cp.m_single").count() == before, "no partition may be touched")
  }

  test("reprocessing the same batch is idempotent") {
    val svc = new CuratedService(spark, conf("m_idem",
      """partitioning { keys = ["src_sys_nm"] }
        |publish { mode = "PARTITION_OVERWRITE" }""".stripMargin))
    val seed = batch(Seq(
      ("M1", "v1", "2026-01-01 10:00:00", 1, "SYS_A", "C1"),
      ("M2", "v1", "2026-01-01 10:00:00", 1, "SYS_B", "C1")))
    svc.process(seed, "FULL", ctx("cp-i1"), None, None)
    val change = batch(Seq(("M1", "v2", "2026-01-01 12:00:00", 2, "SYS_A", "C1")))

    svc.process(change, "INCR", ctx("cp-i2"), None, None)
    val afterFirst = spark.table("cp.m_idem").collect()
      .map(_.getValuesMap[Any](Seq("member_id", "name", "src_sys_nm"))).toSet
    val r2 = svc.process(change, "INCR", ctx("cp-i3"), None, None).get
    val afterSecond = spark.table("cp.m_idem").collect()
      .map(_.getValuesMap[Any](Seq("member_id", "name", "src_sys_nm"))).toSet
    assert(afterFirst == afterSecond, "same batch twice must produce the same snapshot")
    assert(r2.ignoredCount == 1, "the replayed row is a no-op ignore, not a churn rewrite")
  }

  // ---------------------------------------------------------------------------
  // Scalable pruning path + explicit FULL_OVERWRITE
  // ---------------------------------------------------------------------------

  test("affected partitions beyond the cap use the distributed semi-join path") {
    val svc = new CuratedService(spark, conf("m_single",
      """partitioning { keys = ["src_sys_nm"], max_affected_partitions = 1 }
        |publish { mode = "PARTITION_OVERWRITE" }""".stripMargin))
    // Two affected partitions with a cap of 1 forces the semi-join branch
    val r = svc.process(batch(Seq(
      ("M1", "a_v3", "2026-01-02 10:00:00", 3, "SYS_A", "C1"),
      ("M2", "b_v2", "2026-01-02 10:00:00", 2, "SYS_B", "C1")
    )), "INCR", ctx("cp-big1"), None, None).get
    assert(r.updateCount == 2)
    val all = rows("m_single")
    assert(all("M1")._1 == "a_v3" && all("M2")._1 == "b_v2")
  }

  test("FULL_OVERWRITE rebuilds the whole table only when explicitly configured") {
    val po = new CuratedService(spark, conf("m_full",
      """partitioning { keys = ["src_sys_nm"] }
        |publish { mode = "PARTITION_OVERWRITE" }""".stripMargin))
    po.process(batch(Seq(
      ("M1", "keep", "2026-01-01 10:00:00", 1, "SYS_A", "C1"),
      ("M2", "keep", "2026-01-01 10:00:00", 1, "SYS_B", "C1"))),
      "FULL", ctx("cp-f1"), None, None)

    // PARTITION_OVERWRITE never wipes unrelated partitions...
    po.process(batch(Seq(("M3", "new", "2026-01-01 11:00:00", 1, "SYS_C", "C1"))),
      "INCR", ctx("cp-f2"), None, None)
    assert(spark.table("cp.m_full").count() == 3)

    // ...FULL_OVERWRITE, explicitly configured, rebuilds the snapshot
    val full = new CuratedService(spark, conf("m_full",
      """partitioning { keys = ["src_sys_nm"] }
        |publish { mode = "FULL_OVERWRITE", allow_empty = false, max_shrink_percent = 100.0 }""".stripMargin))
    full.process(batch(Seq(("M9", "only", "2026-01-02 10:00:00", 1, "SYS_Z", "C1"))),
      "FULL", ctx("cp-f3"), None, None)
    assert(spark.table("cp.m_full").collect().map(_.getAs[String]("member_id")).toSeq == Seq("M9"),
      "explicit FULL_OVERWRITE replaces the complete snapshot")
    assert(partitions("m_full") == Set("src_sys_nm=SYS_Z"),
      "no stale partitions may survive an explicit full rebuild")
  }
}
