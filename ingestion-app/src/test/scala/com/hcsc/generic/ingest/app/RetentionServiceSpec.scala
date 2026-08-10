package com.hcsc.generic.ingest.app

import com.hcsc.generic.ingest.retention.RetentionService
import com.typesafe.config.ConfigFactory
import java.nio.file.{Files, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/** Config-driven retention (spec §17): raw partition drops, reject/audit
  * row purges via staged rewrite, watermark history trimming — with a
  * dry-run mode that touches nothing. */
class RetentionServiceSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var tempDir: Path = _
  private var spark: SparkSession = _
  private val logger = Logger.getLogger(getClass.getName)

  override def beforeAll(): Unit = {
    super.beforeAll()
    tempDir = Files.createTempDirectory("retention-")
    System.setProperty("derby.stream.error.file",
      tempDir.resolve("derby.log").toAbsolutePath.toString)

    spark = SparkSession.builder()
      .master("local[2]")
      .appName("retention-test")
      .config("spark.sql.warehouse.dir", tempDir.resolve("warehouse").toAbsolutePath.toString)
      .config("javax.jdo.option.ConnectionURL",
        s"jdbc:derby:;databaseName=${tempDir.resolve("metastore_db").toAbsolutePath};create=true")
      .config("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver")
      .config("datanucleus.schema.autoCreateTables", "true")
      .config("hive.metastore.schema.verification", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("CREATE DATABASE IF NOT EXISTS r_raw")
    spark.sql("CREATE DATABASE IF NOT EXISTS r_audit")
    spark.sql("SET hive.exec.dynamic.partition=true")
    spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")

    // Partitioned RAW table: one ancient partition, one recent
    spark.sql(
      """CREATE TABLE r_raw.member (id STRING) PARTITIONED BY (ingest_dt STRING) STORED AS ORC""")
    spark.sql("INSERT INTO r_raw.member PARTITION (ingest_dt='2020-01-01') VALUES ('old')")
    val recentDt = java.time.LocalDate.now.minusDays(5).toString
    spark.sql(s"INSERT INTO r_raw.member PARTITION (ingest_dt='$recentDt') VALUES ('new')")

    // Reject table with old + recent rows
    spark.sql(
      """CREATE TABLE r_audit.ingest_rejects (run_id STRING, reject_ts TIMESTAMP) USING ORC""")
    spark.sql("INSERT INTO r_audit.ingest_rejects VALUES ('old-run', timestamp'2020-01-01 00:00:00')")
    spark.sql("INSERT INTO r_audit.ingest_rejects VALUES ('new-run', current_timestamp())")

    // Watermark history: 5 versions for one entity
    spark.sql(
      """CREATE TABLE r_audit.ingest_watermarks (
        |  entity STRING, watermark_value STRING, run_id STRING,
        |  watermark_version BIGINT, lower_value STRING, query_hash STRING, updated_ts TIMESTAMP
        |) USING ORC""".stripMargin)
    (1 to 5).foreach { v =>
      spark.sql(s"INSERT INTO r_audit.ingest_watermarks VALUES " +
        s"('e1', 'wm$v', 'r$v', $v, 'l$v', null, current_timestamp())")
    }
  }

  override def afterAll(): Unit = {
    try { if (spark != null) spark.stop() }
    finally {
      import scala.collection.JavaConverters._
      Files.walk(tempDir).iterator().asScala.toSeq.reverse.foreach(p => Files.deleteIfExists(p))
    }
    super.afterAll()
  }

  private val feedConf = ConfigFactory.parseString(
    """
      |raw { database = r_raw, table = member }
      |rejects { database = r_audit, table = ingest_rejects }
      |source { incremental { watermark_store { type = hive, database = r_audit } } }
      |retention {
      |  raw = "365d"
      |  rejects = "90d"
      |  watermarks_keep_last = 2
      |}
      |""".stripMargin)

  test("dry-run reports what would be purged and touches nothing") {
    val results = new RetentionService(spark, feedConf, logger).run(dryRun = true)
    assert(results.exists { case (t, _, n) => t == "r_raw.member" && n == 1 })
    assert(results.exists { case (t, _, n) => t == "r_audit.ingest_rejects" && n == 1 })
    assert(results.exists { case (t, _, n) => t == "r_audit.ingest_watermarks" && n == 3 })

    assert(spark.sql("SHOW PARTITIONS r_raw.member").count() == 2)
    assert(spark.table("r_audit.ingest_rejects").count() == 2)
    assert(spark.table("r_audit.ingest_watermarks").count() == 5)
  }

  test("retention purges expired partitions, rows and watermark history") {
    new RetentionService(spark, feedConf, logger).run(dryRun = false)

    val partitions = spark.sql("SHOW PARTITIONS r_raw.member").collect().map(_.getString(0))
    assert(partitions.length == 1 && !partitions.head.contains("2020-01-01"),
      "the ancient partition must be dropped, the recent one kept")

    val rejects = spark.table("r_audit.ingest_rejects").collect().map(_.getString(0))
    assert(rejects.toSeq == Seq("new-run"), "only recent reject rows survive")

    import org.apache.spark.sql.functions.col
    val versions = spark.table("r_audit.ingest_watermarks")
      .select("watermark_version").collect().map(_.getLong(0)).sorted
    assert(versions.toSeq == Seq(4L, 5L), "only the newest 2 versions survive")
  }

  test("multi-key partitioned raw tables are skipped loudly, never silently") {
    spark.sql(
      """CREATE TABLE r_raw.member_mk (id STRING)
        |PARTITIONED BY (ingest_dt STRING, region STRING) STORED AS ORC""".stripMargin)
    spark.sql(
      "INSERT INTO r_raw.member_mk PARTITION (ingest_dt='2020-01-01', region='east') VALUES ('old')")

    val conf = ConfigFactory.parseString(
      """
        |raw { database = r_raw, table = member_mk }
        |retention { raw = "365d" }
        |""".stripMargin)
    val results = new RetentionService(spark, conf, logger).run(dryRun = false)
    assert(results.exists { case (t, action, n) =>
      t == "r_raw.member_mk" && action.contains("multi-key") && n == 0L },
      s"multi-key layout must be reported as skipped, got $results")
    assert(spark.sql("SHOW PARTITIONS r_raw.member_mk").count() == 1,
      "no partition may be dropped from an unsupported layout")
  }

  test("a raw table partitioned under another name is skipped loudly, never silently") {
    // The quietest failure of the three: SHOW PARTITIONS succeeds because the
    // table IS partitioned, and there is no '/' to trip the multi-key guard,
    // so every partition simply fails the ingest_dt name match. Reported as
    // "0 dropped", that is indistinguishable from a table with nothing yet
    // expired — and RAW would never be purged, run after run, silently.
    spark.sql(
      """CREATE TABLE r_raw.member_ld (id STRING)
        |PARTITIONED BY (load_dt STRING) STORED AS ORC""".stripMargin)
    spark.sql("INSERT INTO r_raw.member_ld PARTITION (load_dt='2020-01-01') VALUES ('old')")

    val conf = ConfigFactory.parseString(
      """
        |raw { database = r_raw, table = member_ld }
        |retention { raw = "365d" }
        |""".stripMargin)
    val results = new RetentionService(spark, conf, logger).run(dryRun = false)

    assert(results.exists { case (t, action, n) =>
      t == "r_raw.member_ld" && action.contains("not ingest_dt") && n == 0L },
      s"a differently-named partition column must be reported as skipped, got $results")
    assert(spark.sql("SHOW PARTITIONS r_raw.member_ld").count() == 1,
      "nothing may be dropped from a layout retention cannot interpret")
  }

  test("a retention run without a retention block fails with guidance") {
    val e = intercept[IllegalArgumentException] {
      new RetentionService(spark, ConfigFactory.parseString("{}"), logger).run(dryRun = false)
    }
    assert(e.getMessage.contains("PIPE_004"))
  }
}
