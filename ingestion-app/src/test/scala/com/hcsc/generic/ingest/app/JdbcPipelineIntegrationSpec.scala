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
       |audit { enabled = false } # explicit opt-out (the ledger is mandatory otherwise)
       |raw {
       |  database = j_raw
       |  table = claims
       |  path = "${tempDir.resolve("raw_claims").toAbsolutePath}"
       |  format = parquet
       |  record_hash = true
       |}
       |curated {
       |  enabled = true
       |  database = j_curated
       |  table = claims
       |  path = "${tempDir.resolve("curated_claims").toAbsolutePath}"
       |  format = parquet
       |  merge { keys = ["claim_id"] }
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

    // RAW rows carry the extraction window and source identity (lineage)
    val meta = spark.table("j_raw.claims").filter(col("run_id") === "jrun-2")
      .select("extract_start_ts", "extract_end_ts", "source_table").collect().head
    assert(meta.getString(0).startsWith("2026-01-02 09:00:00"), "extract_start_ts = previous watermark")
    assert(meta.getString(1).startsWith("2026-01-03 09:00:00"), "extract_end_ts = captured upper")
    assert(meta.getString(2) == "claims")

    // raw.record_hash = true: every RAW row carries the business fingerprint
    val hash = spark.table("j_raw.claims").select("record_hash").collect().map(_.getString(0))
    assert(hash.forall(h => h != null && h.length == 64), "record_hash must be a SHA-256 hex string")
  }

  test("configuration compatibility (CFG) failures stop the run before extraction") {
    // Incremental JDBC into a keyless state-deriving curated layer would
    // keep only the latest window (CFG_009).
    val keyless = feedConf().withValue("curated.merge.keys",
      com.typesafe.config.ConfigValueFactory.fromIterable(java.util.Collections.emptyList[String]()))
    val e = intercept[IllegalStateException] {
      new IngestPipeline(spark, keyless,
        Cli(entity = entity, mode = "FULL", runId = Some("jrun-cfg")), logger).run()
    }
    assert(e.getMessage.contains("CFG_009"))
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

    // Window idempotency: jrun-3 already appended C004 to RAW before its
    // publish failed; jrun-4 (a NEW run id, no --resume) re-extracts the
    // identical bounded window and must NOT append the same rows twice.
    import org.apache.spark.sql.functions.col
    assert(spark.table("j_raw.claims").filter(col("claim_id") === "C004").count() == 1,
      "rerunning a failed window under a new run id must not duplicate RAW")
  }

  test("the run ledger is mandatory unless a feed opts out explicitly") {
    val noAudit = feedConf().withoutPath("audit")
    val e = intercept[IllegalStateException] {
      new IngestPipeline(spark, noAudit,
        Cli(entity = entity, mode = "FULL", runId = Some("jrun-ledger")), logger).run()
    }
    assert(e.getMessage.contains("PIPE_002"))
  }

  test("--stage raw does not advance the watermark; advance_after=RAW opts in") {
    h2("INSERT INTO claims VALUES ('C005', 500, '2026-01-05 09:00:00')")
    val before = InMemoryWatermarkStore.latest(entity).get

    // RAW-only run: the source window must NOT be burned, because curated
    // never processed it.
    new IngestPipeline(spark, feedConf(),
      Cli(entity = entity, mode = "FULL", runId = Some("jrun-5"), stage = "raw"), logger).run()

    import org.apache.spark.sql.functions.col
    assert(spark.table("j_raw.claims").filter(col("run_id") === "jrun-5").count() == 1,
      "raw must still be loaded by a --stage raw run")
    assert(InMemoryWatermarkStore.latest(entity).get == before,
      "--stage raw must not advance the watermark: curated never saw the window")

    // Declared raw-only topology: the same raw-only run advances once the
    // feed opts in explicitly.
    val rawOnly = ConfigFactory.parseString("""watermark { advance_after = "RAW" }""")
      .withFallback(feedConf())
    new IngestPipeline(spark, rawOnly,
      Cli(entity = entity, mode = "FULL", runId = Some("jrun-6"), stage = "raw"), logger).run()
    assert(InMemoryWatermarkStore.latest(entity).get.values.head.startsWith("2026-01-05 09:00:00"))
  }
}
