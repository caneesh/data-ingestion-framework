package com.hcsc.generic.ingest.app

import com.hcsc.generic.ingest.jdbc.JdbcSource
import com.hcsc.generic.ingest.jdbc.watermark.InMemoryWatermarkStore
import com.hcsc.generic.ingest.model.Cli
import com.hcsc.generic.ingest.pipeline.IngestPipeline
import com.hcsc.generic.ingest.publish.PublishValidationException
import com.hcsc.generic.ingest.sink.HiveSink
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Path => JPath}
import java.sql.DriverManager

/**
  * End-to-end JDBC ingestion through the full pipeline: H2 source ->
  * incremental watermark read -> RAW Hive table -> transactional CURATED
  * publish -> watermark advance ONLY after success.
  */
class JdbcPipelineIntegrationSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var tempDir: JPath = _
  private var spark: SparkSession = _
  private val logger = Logger.getLogger(getClass.getName)

  private val h2Url = "jdbc:h2:mem:jdbc_pipeline;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false"

  private def h2(statements: String*): Unit = {
    Class.forName("org.h2.Driver")
    val conn = DriverManager.getConnection(h2Url, "sa", "")
    try {
      val stmt = conn.createStatement()
      try statements.foreach(stmt.execute)
      finally stmt.close()
    } finally conn.close()
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    tempDir = Files.createTempDirectory("jdbc-pipeline-")
    System.setProperty("derby.stream.error.file",
      tempDir.resolve("derby.log").toAbsolutePath.toString)

    spark = SparkSession.builder()
      .master("local[2]")
      .appName("jdbc-pipeline-test")
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

    JdbcSource.register()
    HiveSink.register()
    Seq("j_raw", "j_curated").foreach(db => spark.sql(s"CREATE DATABASE IF NOT EXISTS $db"))

    h2(
      "DROP TABLE IF EXISTS claims",
      """CREATE TABLE claims (
        |  claim_id VARCHAR(20),
        |  amount INT,
        |  modified_ts TIMESTAMP
        |)""".stripMargin,
      "INSERT INTO claims VALUES ('C001', 100, '2026-01-01 09:00:00')",
      "INSERT INTO claims VALUES ('C002', 200, '2026-01-02 09:00:00')"
    )
    InMemoryWatermarkStore.clear()
  }

  override def afterAll(): Unit = {
    try { if (spark != null) spark.stop() }
    finally {
      import scala.collection.JavaConverters._
      Files.walk(tempDir).iterator().asScala.toSeq.reverse.foreach(p => Files.deleteIfExists(p))
    }
    super.afterAll()
  }

  private def feedConf(extraCurated: String = ""): Config = ConfigFactory.parseString(
    s"""
       |source {
       |  type = "jdbc"
       |  url = "$h2Url"
       |  dialect = "generic"
       |  driver = "org.h2.Driver"
       |  auth { user = "sa", password = { provider = "inline", value = "" } }
       |  mode = "INCREMENTAL"
       |  table = "claims"
       |  health_check { enabled = false }
       |  incremental {
       |    watermark_type = "TIMESTAMP"
       |    watermark_columns = ["modified_ts"]
       |    initial_value = "1900-01-01 00:00:00"
       |    watermark_store { type = "memory" }
       |  }
       |}
       |raw {
       |  database = j_raw
       |  table = claims
       |  path = "${tempDir.resolve("raw_claims").toAbsolutePath}"
       |  format = parquet
       |}
       |curated {
       |  enabled = true
       |  database = j_curated
       |  table = claims
       |  path = "${tempDir.resolve("curated_claims").toAbsolutePath}"
       |  format = parquet
       |  merge { keys = [] }
       |  $extraCurated
       |}
    """.stripMargin)

  private val entity = "claims_feed"

  test("incremental JDBC feed loads, publishes, and advances the watermark") {
    new IngestPipeline(spark, feedConf(), Cli(entity = entity, mode = "FULL", runId = Some("jrun-1")), logger).run()

    assert(spark.table("j_raw.claims").count() == 2)
    assert(spark.table("j_curated.claims").count() == 2)
    assert(InMemoryWatermarkStore.latest(entity).get.values.head.startsWith("2026-01-02 09:00:00"))

    // New source row -> second run ingests only the delta
    h2("INSERT INTO claims VALUES ('C003', 300, '2026-01-03 09:00:00')")
    new IngestPipeline(spark, feedConf(), Cli(entity = entity, mode = "FULL", runId = Some("jrun-2")), logger).run()

    import org.apache.spark.sql.functions.col
    assert(spark.table("j_raw.claims").count() == 3)
    assert(spark.table("j_raw.claims").filter(col("run_id") === "jrun-2").count() == 1)
    assert(InMemoryWatermarkStore.latest(entity).get.values.head.startsWith("2026-01-03 09:00:00"))
  }

  test("failed publish does not advance the watermark; replay picks the rows up again") {
    h2("INSERT INTO claims VALUES ('C004', 400, '2026-01-04 09:00:00')")
    val before = InMemoryWatermarkStore.latest(entity).get

    // Publish validation rejects everything -> curated fails after RAW
    val failing = feedConf(
      """publish { validation_query = "SELECT * FROM {table} WHERE claim_id = 'C004'" }""")
    intercept[PublishValidationException] {
      new IngestPipeline(spark, failing, Cli(entity = entity, mode = "FULL", runId = Some("jrun-3")), logger).run()
    }

    // Watermark untouched by the failed run
    assert(InMemoryWatermarkStore.latest(entity).get == before)

    // Replay with fixed config: same row extracted again (watermark never
    // moved), published, and only now does the watermark advance
    new IngestPipeline(spark, feedConf(), Cli(entity = entity, mode = "FULL", runId = Some("jrun-4")), logger).run()
    assert(InMemoryWatermarkStore.latest(entity).get.values.head.startsWith("2026-01-04 09:00:00"))
    assert(spark.table("j_curated.claims").count() == 1)
  }
}
