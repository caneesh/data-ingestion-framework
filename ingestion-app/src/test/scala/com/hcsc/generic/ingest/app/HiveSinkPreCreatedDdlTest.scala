package com.hcsc.generic.ingest.app

import com.hcsc.generic.ingest.sink.HiveSink
import com.hcsc.generic.ingest.transform.{RawLineage, RawMetadata}
import com.typesafe.config.ConfigFactory
import java.nio.file.{Files, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
  * The pre-created-DDL path (production deployment) previously had zero
  * coverage: a raw table created without the framework metadata columns
  * silently dropped run_id, disabling replay guards, reconciliation and
  * --resume. The sink now refuses by default (RAW_001), can ALTER the
  * columns in, or degrade explicitly with WARN.
  */
class HiveSinkPreCreatedDdlTest extends AnyFunSuite with BeforeAndAfterAll {

  private var tempDir: Path = _
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    tempDir = Files.createTempDirectory("hive-sink-ddl-")
    System.setProperty("derby.stream.error.file",
      tempDir.resolve("derby.log").toAbsolutePath.toString)

    spark = SparkSession.builder()
      .master("local[2]")
      .appName("hive-sink-ddl-test")
      .config("spark.sql.warehouse.dir", tempDir.resolve("warehouse").toAbsolutePath.toString)
      .config("javax.jdo.option.ConnectionURL",
        s"jdbc:derby:;databaseName=${tempDir.resolve("metastore_db").toAbsolutePath};create=true")
      .config("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver")
      .config("datanucleus.schema.autoCreateTables", "true")
      .config("hive.metastore.schema.verification", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.caseSensitive", "false")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("CREATE DATABASE IF NOT EXISTS d_raw")
  }

  override def afterAll(): Unit = {
    try { if (spark != null) spark.stop() }
    finally {
      import scala.collection.JavaConverters._
      Files.walk(tempDir).iterator().asScala.toSeq.reverse.foreach(p => Files.deleteIfExists(p))
    }
    super.afterAll()
  }

  /** Business columns + full framework metadata, as the pipeline stamps it. */
  private def stampedBatch(runId: String): DataFrame = {
    val s = spark
    import s.implicits._
    val df = Seq(("S001", "H1"), ("S002", "H2")).toDF("subscriber_id", "hios_id")
    RawMetadata.add(df, "F", RawLineage(
      runId = runId,
      sourceSystem = Some("membership"),
      sourceTable = Some("members"),
      extractStart = Some("1900-01-01 00:00:00"),
      extractEnd = Some("2026-01-01 00:00:00")))
  }

  private def legacyDdl(table: String): Unit =
    spark.sql(
      s"""CREATE TABLE d_raw.$table (
         |  subscriber_id STRING, hios_id STRING,
         |  source_file STRING, row_idx BIGINT, load_timestamp TIMESTAMP,
         |  file_type STRING, file_id STRING
         |) USING ORC""".stripMargin)

  private def sinkConf(table: String, extra: String = "") = ConfigFactory.parseString(
    s"""{ database = d_raw, table = $table, format = orc $extra }""")

  test("pre-created table missing metadata columns fails loudly by default (RAW_001)") {
    legacyDdl("member_fail")
    val e = intercept[IllegalStateException] {
      HiveSink.write(spark, stampedBatch("run-ddl-1"), sinkConf("member_fail"))
    }
    assert(e.getMessage.contains("RAW_001"))
    assert(e.getMessage.contains("run_id"))
    assert(spark.table("d_raw.member_fail").count() == 0, "no partial write on failure")
  }

  test("metadata_columns=ALTER adds the missing columns and preserves lineage") {
    legacyDdl("member_alter")
    HiveSink.write(spark, stampedBatch("run-ddl-2"),
      sinkConf("member_alter", """, metadata_columns = ALTER"""))

    val table = spark.table("d_raw.member_alter")
    assert(table.columns.map(_.toLowerCase).contains("run_id"))
    assert(table.columns.map(_.toLowerCase).contains("extract_end_ts"))
    val rows = table.filter(col("run_id") === "run-ddl-2")
    assert(rows.count() == 2)
    assert(rows.select("source_system").distinct().collect().head.getString(0) == "membership")
    assert(rows.select("extract_end_ts").distinct().collect().head.getString(0) == "2026-01-01 00:00:00")
  }

  test("metadata_columns=WARN keeps the degraded legacy behavior explicitly") {
    legacyDdl("member_warn")
    HiveSink.write(spark, stampedBatch("run-ddl-3"),
      sinkConf("member_warn", """, metadata_columns = WARN"""))
    val table = spark.table("d_raw.member_warn")
    assert(table.count() == 2)
    assert(!table.columns.map(_.toLowerCase).contains("run_id"))
  }

  test("a partition-column mismatch names both sides, not just the missing column") {
    // The real-world shape: a table recreated with PARTITIONED BY ingest_dt
    // while the feed config still derives the old partition name. The bare
    // "DataFrame is missing target columns: ingest_dt" says nothing about
    // WHERE ingest_dt was supposed to come from, so the operator has no path
    // from the error to the one-line config fix.
    spark.sql(
      """CREATE TABLE d_raw.part_skew (
        |  subscriber_id STRING, hios_id STRING,
        |  source_file STRING, row_idx BIGINT, load_timestamp TIMESTAMP,
        |  file_type STRING, file_id STRING
        |) USING ORC PARTITIONED BY (ingest_dt STRING)""".stripMargin)

    val conf = sinkConf("part_skew",
      """, partitioning { keys = ["load_dt"]
        |    derive { load_dt = "date_format(load_timestamp, 'yyyy-MM-dd')" } }""".stripMargin)

    val e = intercept[IllegalArgumentException] {
      HiveSink.write(spark, stampedBatch("skew-1"), conf)
    }
    val m = String.valueOf(e.getMessage)
    assert(m.contains("ingest_dt"), s"must name the missing column: $m")
    assert(m.toUpperCase.contains("PARTITION"),
      s"must say the missing column is a partition column: $m")
    assert(m.contains("load_dt"),
      s"must name what the config DOES produce, so the disagreement is visible: $m")
    assert(m.contains("raw.partitioning"),
      s"must name the config key to change: $m")
  }

  test("business columns absent from the target still warn-and-drop (unchanged)") {
    legacyDdl("member_biz")
    val withExtra = stampedBatch("run-ddl-4").withColumn("brand_new_col",
      org.apache.spark.sql.functions.lit("x"))
    // Business extra 'brand_new_col' drops with a warning; metadata extras
    // still fail — proving the two classes are enforced independently.
    val e = intercept[IllegalStateException] {
      HiveSink.write(spark, withExtra, sinkConf("member_biz"))
    }
    assert(e.getMessage.contains("RAW_001"))
    assert(!e.getMessage.contains("brand_new_col"))
  }
}
