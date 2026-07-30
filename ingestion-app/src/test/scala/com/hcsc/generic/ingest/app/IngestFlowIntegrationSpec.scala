package com.hcsc.generic.ingest.app

import com.hcsc.generic.ingest.file.FileSource
import com.hcsc.generic.ingest.model.Cli
import com.hcsc.generic.ingest.sink.{HiveSink, SinkRegistry}
import com.hcsc.generic.ingest.source.SourceRegistry
import com.hcsc.generic.ingest.stage.{CuratedStageRunner, RawStageRunner}
import com.typesafe.config.ConfigFactory
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
  * End-to-end integration test that exercises the full ingestion pipeline:
  *   FileSource.read -> RawMetadata.add -> HiveSink.write  (RAW stage)
  *   then CuratedTransform -> align -> HiveSink             (Curated stage)
  *
  * Uses a Hive-enabled local SparkSession backed by a temporary Derby metastore
  * so no external services are needed.  This test lives in ingestion-app so it
  * can import all connectors while running in its own JVM fork — avoiding a
  * getOrCreate() clash with the non-Hive session in the unit-test modules.
  */
class IngestFlowIntegrationSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var tempDir: Path = _
  private var spark: SparkSession = _
  private val logger = Logger.getLogger(getClass.getName)

  override def beforeAll(): Unit = {
    super.beforeAll()

    tempDir = Files.createTempDirectory("ingest-integration-")

    // Direct Derby log away from the repo root.
    System.setProperty("derby.stream.error.file",
      tempDir.resolve("derby.log").toAbsolutePath.toString)
    System.setProperty("derby.system.home",
      tempDir.toAbsolutePath.toString)

    val warehouseDir = tempDir.resolve("warehouse").toAbsolutePath.toString
    val metastoreUrl =
      s"jdbc:derby:;databaseName=${tempDir.resolve("metastore_db").toAbsolutePath};create=true"

    spark = SparkSession.builder()
      .master("local[2]")
      .appName("ingest-integration-test")
      .config("spark.sql.warehouse.dir", warehouseDir)
      .config("javax.jdo.option.ConnectionURL", metastoreUrl)
      .config("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver")
      .config("javax.jdo.option.ConnectionUserName", "APP")
      .config("javax.jdo.option.ConnectionPassword", "")
      .config("datanucleus.schema.autoCreateTables", "true")
      .config("hive.metastore.schema.verification", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.caseSensitive", "false")
      .enableHiveSupport()
      .getOrCreate()

    // Register connectors (mirrors IngestMain.registerConnectors)
    FileSource.register()
    HiveSink.register()

    // Create databases
    spark.sql("CREATE DATABASE IF NOT EXISTS test_raw")
    spark.sql("CREATE DATABASE IF NOT EXISTS test_curated")
  }

  override def afterAll(): Unit = {
    try {
      if (spark != null) spark.stop()
    } finally {
      if (tempDir != null) {
        import scala.collection.JavaConverters._
        Files.walk(tempDir).iterator().asScala.toSeq.reverse
          .foreach(p => Files.deleteIfExists(p))
      }
    }
    super.afterAll()
  }

  // ---------------------------------------------------------------------------
  // Helper: write a CSV file to tempDir
  // ---------------------------------------------------------------------------

  private def writeCsv(name: String, content: String): Path = {
    val f = tempDir.resolve(name)
    Files.write(f, content.getBytes(StandardCharsets.UTF_8))
    f
  }

  // ---------------------------------------------------------------------------
  // Test 1: Full pipeline — RAW then Curated (FULL mode)
  // ---------------------------------------------------------------------------

  test("full pipeline: file -> raw hive table -> curated hive table") {
    val csvFile = writeCsv("members.csv",
      "subscriber id,plan_hios_id\n" +
        "S001,H123\n" +
        "S002,H456\n" +
        "S002,H456\n"  // duplicate row to confirm it lands in curated
    )

    val rawPath     = tempDir.resolve("raw_member").toAbsolutePath.toString
    val curatedPath = tempDir.resolve("curated_member").toAbsolutePath.toString

    val feedConfStr =
      s"""
         |source {
         |  type = file
         |  path = "${csvFile.toUri.toString}"
         |  header = true
         |  header_aliases {
         |    "subscriber id" = subscriber_id
         |    plan_hios_id    = hios_id
         |  }
         |}
         |raw {
         |  database = test_raw
         |  table    = member
         |  path     = "$rawPath"
         |  format   = parquet
         |}
         |""".stripMargin

    val curatedConfStr =
      s"""
         |enabled  = true
         |database = test_curated
         |table    = member
         |path     = "$curatedPath"
         |format   = parquet
         |""".stripMargin

    val feedConf    = ConfigFactory.parseString(feedConfStr)
    val curatedConf = ConfigFactory.parseString(curatedConfStr)
    val cli         = Cli(entity = "member", mode = "FULL")

    // ---- RAW stage ----
    val rawDf = new RawStageRunner(
      spark    = spark,
      feedConf = feedConf,
      cli      = cli,
      rawFlag  = "F",
      logger   = logger
    ).run()

    // ---- Curated stage ----
    new CuratedStageRunner(
      spark       = spark,
      curatedConf = Some(curatedConf),
      logger      = logger
    ).run(rawDf, "FULL",
      com.hcsc.generic.ingest.runtime.RunContext(
        java.util.UUID.randomUUID().toString, "member", "FULL", "F"),
      None)

    // ---- RAW assertions ----
    val rawTable = spark.table("test_raw.member")
    assert(rawTable.count() == 3, "RAW table should have all 3 source rows")

    val rawCols = rawTable.columns.toSet
    assert(rawCols.contains("subscriber_id"), "RAW must have subscriber_id")
    assert(rawCols.contains("hios_id"),       "RAW must have hios_id")
    assert(rawCols.contains("source_file"),   "RAW must have source_file")
    assert(rawCols.contains("row_idx"),       "RAW must have row_idx")
    assert(rawCols.contains("load_timestamp"),"RAW must have load_timestamp")
    assert(rawCols.contains("file_type"),     "RAW must have file_type")
    assert(rawCols.contains("file_id"),       "RAW must have file_id")

    val fileTypes = rawTable.select("file_type").as[String](
      org.apache.spark.sql.Encoders.STRING).collect().toSet
    assert(fileTypes == Set("F"), "file_type must equal the rawFlag 'F'")

    val subscriberIds = rawTable.select("subscriber_id").as[String](
      org.apache.spark.sql.Encoders.STRING).collect().toSet
    assert(subscriberIds == Set("S001", "S002"),
      "RAW subscriber_id values should be S001 and S002")

    // ---- Curated assertions ----
    val curatedTable = spark.table("test_curated.member")
    // FULL mode, no merge keys => all 3 rows land (no dedup)
    assert(curatedTable.count() == 3, "Curated table should have 3 rows in FULL mode")

    val curatedCols = curatedTable.columns.toSet
    assert(curatedCols.contains("create_timestamp"),  "Curated must have create_timestamp")
    assert(curatedCols.contains("last_modified_ts"),  "Curated must have last_modified_ts")
    assert(curatedCols.contains("last_modified_op"),  "Curated must have last_modified_op")

    val ops = curatedTable.select("last_modified_op").as[String](
      org.apache.spark.sql.Encoders.STRING).collect().toSet
    assert(ops == Set("I"), "last_modified_op should be 'I' for a FULL load")

    val nullTsCount = curatedTable
      .filter(curatedTable("create_timestamp").isNull)
      .count()
    assert(nullTsCount == 0, "create_timestamp should be non-null for all rows")

    val curatedIds = curatedTable.select("subscriber_id").as[String](
      org.apache.spark.sql.Encoders.STRING).collect().toSet
    assert(curatedIds == Set("S001", "S002"),
      "Curated subscriber_id values should round-trip correctly")
  }

  // ---------------------------------------------------------------------------
  // Test 2: RAW-only re-run appends rows (SaveMode.Append)
  // ---------------------------------------------------------------------------

  test("re-running RAW stage appends to existing raw table") {
    val csvFile = writeCsv("members2.csv",
      "subscriber id,plan_hios_id\nS003,H789\n")

    val feedConfStr =
      s"""
         |source {
         |  type = file
         |  path = "${csvFile.toUri.toString}"
         |  header = true
         |  header_aliases {
         |    "subscriber id" = subscriber_id
         |    plan_hios_id    = hios_id
         |  }
         |}
         |raw {
         |  database = test_raw
         |  table    = member
         |  path     = "${tempDir.resolve("raw_member").toAbsolutePath}"
         |  format   = parquet
         |}
         |""".stripMargin

    val feedConf = ConfigFactory.parseString(feedConfStr)
    val cli      = Cli(entity = "member", mode = "FULL")

    // Count before
    val before = spark.table("test_raw.member").count()

    new RawStageRunner(
      spark    = spark,
      feedConf = feedConf,
      cli      = cli,
      rawFlag  = "F",
      logger   = logger
    ).run()

    val after = spark.table("test_raw.member").count()
    assert(after == before + 1,
      s"Re-running RAW should append 1 new row; was $before, now $after")
  }
}
