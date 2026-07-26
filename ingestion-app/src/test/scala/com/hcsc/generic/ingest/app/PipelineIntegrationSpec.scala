package com.hcsc.generic.ingest.app

import com.hcsc.generic.ingest.file.FileSource
import com.hcsc.generic.ingest.model.Cli
import com.hcsc.generic.ingest.pipeline.IngestPipeline
import com.hcsc.generic.ingest.publish.PublishValidationException
import com.hcsc.generic.ingest.sink.HiveSink
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path => JPath}

/**
  * End-to-end pipeline test exercising the managed file lifecycle
  * (validate/quarantine), content idempotency, record rejects, audit and
  * reconciliation, transactional publish, dry-run and resume — all against a
  * Hive-enabled local session with a temp Derby metastore.
  */
class PipelineIntegrationSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var tempDir: JPath = _
  private var spark: SparkSession = _
  private val logger = Logger.getLogger(getClass.getName)

  override def beforeAll(): Unit = {
    super.beforeAll()
    tempDir = Files.createTempDirectory("pipeline-integration-")

    System.setProperty("derby.stream.error.file",
      tempDir.resolve("derby.log").toAbsolutePath.toString)

    val warehouseDir = tempDir.resolve("warehouse").toAbsolutePath.toString
    val metastoreUrl =
      s"jdbc:derby:;databaseName=${tempDir.resolve("metastore_db").toAbsolutePath};create=true"

    spark = SparkSession.builder()
      .master("local[2]")
      .appName("pipeline-integration-test")
      .config("spark.sql.warehouse.dir", warehouseDir)
      .config("javax.jdo.option.ConnectionURL", metastoreUrl)
      .config("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver")
      .config("datanucleus.schema.autoCreateTables", "true")
      .config("hive.metastore.schema.verification", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.caseSensitive", "false")
      .enableHiveSupport()
      .getOrCreate()

    FileSource.register()
    HiveSink.register()

    Seq("p_raw", "p_curated", "p_audit").foreach(db =>
      spark.sql(s"CREATE DATABASE IF NOT EXISTS $db"))

    Seq("landing", "inprogress", "processed", "archive", "quarantine")
      .foreach(d => Files.createDirectories(tempDir.resolve(d)))
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
  // Helpers
  // ---------------------------------------------------------------------------

  private def writeLanding(name: String, content: String): JPath = {
    val f = tempDir.resolve("landing").resolve(name)
    Files.write(f, content.getBytes(StandardCharsets.UTF_8))
    f
  }

  private def listNames(dir: String): Set[String] = {
    import scala.collection.JavaConverters._
    Files.list(tempDir.resolve(dir)).iterator().asScala
      .map(_.getFileName.toString)
      .filterNot(n => n.startsWith(".") || n.startsWith("_"))
      .toSet
  }

  private def feedConf(extra: String = ""): Config = ConfigFactory.parseString(
    s"""
       |schema {
       |  version = "1.0"
       |  columns = [
       |    { name = "subscriber_id", type = "string", nullable = false,
       |      aliases = ["subscriber id"], position = 0 },
       |    { name = "hios_id", type = "string", aliases = ["plan_hios_id"], position = 1 }
       |  ]
       |}
       |source {
       |  type = file
       |  header = true
       |  folders {
       |    landing = "${tempDir.resolve("landing")}"
       |    inprogress = "${tempDir.resolve("inprogress")}"
       |    processed = "${tempDir.resolve("processed")}"
       |    archive = "${tempDir.resolve("archive")}"
       |    quarantine = "${tempDir.resolve("quarantine")}"
       |  }
       |  validation {
       |    filename_pattern = "member_.*[.]csv"
       |    allowed_extensions = ["csv"]
       |    encoding = "UTF-8"
       |    min_size_bytes = 1
       |    header { expected_columns = 2 }
       |  }
       |}
       |idempotency {
       |  database = p_audit
       |  duplicate_policy = "SKIP"
       |}
       |rejects {
       |  database = p_audit
       |  use_contract_nullability = true
       |  max_reject_percent = 60.0
       |}
       |audit {
       |  database = p_audit
       |  control_total_expr = "count(*)"
       |}
       |raw {
       |  database = p_raw
       |  table = member
       |  path = "${tempDir.resolve("raw_tbl").toAbsolutePath}"
       |  format = parquet
       |}
       |curated {
       |  enabled = true
       |  database = p_curated
       |  table = member
       |  path = "${tempDir.resolve("curated_tbl").toAbsolutePath}"
       |  format = parquet
       |  merge { keys = [] }
       |  $extra
       |}
    """.stripMargin)

  private def run(conf: Config, cli: Cli): Unit =
    new IngestPipeline(spark, conf, cli, logger).run()

  private val baseCli = Cli(entity = "member", mode = "FULL")

  // ---------------------------------------------------------------------------
  // 02 + 04 + 05 + 06: full managed run
  // ---------------------------------------------------------------------------

  test("managed run: quarantine invalid files, reject bad records, audit everything") {
    writeLanding("member_ok.csv",
      "subscriber id,plan_hios_id\nS001,H1\nS002,H2\n,H3\n") // 3rd row: blank key -> reject
    writeLanding("badname.dat", "junk")
    writeLanding("member_short.csv", "only_one_column\nx\n")

    run(feedConf(), baseCli.copy(runId = Some("run-full-1")))

    // File lifecycle (02)
    assert(listNames("quarantine") == Set("badname.dat", "member_short.csv"))
    assert(listNames("processed") == Set("member_ok.csv"))
    assert(listNames("archive") == Set("member_ok.csv"))
    assert(listNames("landing").isEmpty)
    assert(listNames("inprogress").isEmpty)

    // RAW holds accepted rows only, with run lineage (04, 07)
    val raw = spark.table("p_raw.member")
    assert(raw.count() == 2)
    assert(raw.columns.contains("run_id"))

    // Curated published transactionally (06)
    assert(spark.table("p_curated.member").count() == 2)
    assert(spark.catalog.listTables("p_curated").collect()
      .forall(t => !t.name.startsWith("__stg_")), "no staging tables left behind")

    // Rejected record persisted with lineage (04)
    val rejects = spark.table("p_audit.ingest_rejects")
    assert(rejects.count() == 1)
    val reject = rejects.collect().head
    assert(reject.getAs[String]("run_id") == "run-full-1")
    assert(reject.getAs[String]("error_code") == "NULLABILITY")
    assert(reject.getAs[String]("raw_record").contains("H3"))

    // Audit trail (05)
    import org.apache.spark.sql.functions.col
    val fileAudit = spark.table("p_audit.ingest_file_audit")
      .filter(col("run_id") === "run-full-1")
    val statuses = fileAudit.select("file_name", "status").collect()
      .map(r => (r.getString(0), r.getString(1))).toSet
    assert(statuses.contains(("badname.dat", "QUARANTINED")))
    assert(statuses.contains(("member_short.csv", "QUARANTINED")))
    assert(statuses.contains(("member_ok.csv", "VALIDATED")))
    assert(statuses.contains(("member_ok.csv", "PROCESSED")))

    val runAudit = spark.table("p_audit.ingest_run_audit")
      .filter(col("run_id") === "run-full-1")
    val stageStatuses = runAudit.select("stage", "status").collect()
      .map(r => (r.getString(0), r.getString(1))).toSet
    assert(stageStatuses.contains(("validate", "SUCCESS")))
    assert(stageStatuses.contains(("raw", "SUCCESS")))
    assert(stageStatuses.contains(("curated", "SUCCESS")))

    val rawSuccess = runAudit
      .filter(col("stage") === "raw" && col("status") === "SUCCESS").collect().head
    assert(rawSuccess.getAs[Long]("source_count") == 3L)
    assert(rawSuccess.getAs[Long]("accepted_count") == 2L)
    assert(rawSuccess.getAs[Long]("rejected_count") == 1L)
    assert(rawSuccess.getAs[String]("control_total") == "2")

    // Reconciliation recorded and passing (05)
    val recon = spark.table("p_audit.ingest_reconciliation")
      .filter(col("run_id") === "run-full-1")
    assert(recon.count() >= 2)
    assert(recon.filter(col("passed") === false).count() == 0)

    // File registry recorded (03)
    val registry = spark.table("p_audit.ingest_file_registry")
    assert(registry.filter(col("run_id") === "run-full-1").count() == 1)
  }

  // ---------------------------------------------------------------------------
  // 03: content idempotency
  // ---------------------------------------------------------------------------

  test("duplicate content is skipped by checksum even under a new file name") {
    writeLanding("member_copy.csv",
      "subscriber id,plan_hios_id\nS001,H1\nS002,H2\n,H3\n") // same bytes as member_ok.csv

    val rawBefore = spark.table("p_raw.member").count()
    run(feedConf(), baseCli.copy(runId = Some("run-dup-1")))

    assert(spark.table("p_raw.member").count() == rawBefore, "duplicate must not load")
    assert(listNames("processed").contains("member_copy.csv"), "SKIP policy moves duplicate to processed")

    import org.apache.spark.sql.functions.col
    val skipped = spark.table("p_audit.ingest_file_audit")
      .filter(col("run_id") === "run-dup-1" && col("status") === "SKIPPED_DUPLICATE")
    assert(skipped.count() == 1)
  }

  test("duplicate content is quarantined under REJECT policy") {
    writeLanding("member_copy2.csv",
      "subscriber id,plan_hios_id\nS001,H1\nS002,H2\n,H3\n")

    val conf = ConfigFactory.parseString("""idempotency.duplicate_policy = "REJECT" """)
      .withFallback(feedConf())
    run(conf, baseCli.copy(runId = Some("run-dup-2")))

    assert(listNames("quarantine").contains("member_copy2.csv"))
  }

  // ---------------------------------------------------------------------------
  // 07: dry-run
  // ---------------------------------------------------------------------------

  test("dry-run validates and audits but writes and moves nothing") {
    writeLanding("member_dry.csv", "subscriber id,plan_hios_id\nS900,H9\n")

    val rawBefore = spark.table("p_raw.member").count()
    run(feedConf(), baseCli.copy(runId = Some("run-dry-1"), dryRun = true))

    assert(spark.table("p_raw.member").count() == rawBefore)
    assert(listNames("landing") == Set("member_dry.csv"), "dry-run leaves files in landing")

    import org.apache.spark.sql.functions.col
    val stages = spark.table("p_audit.ingest_run_audit")
      .filter(col("run_id") === "run-dry-1" && col("status") === "SUCCESS")
    assert(stages.count() >= 2)

    Files.delete(tempDir.resolve("landing").resolve("member_dry.csv"))
  }

  // ---------------------------------------------------------------------------
  // Header contract: unknown required header -> audit + quarantine, no RAW write
  // ---------------------------------------------------------------------------

  test("unknown renamed required header fails before RAW, audits and quarantines the file") {
    writeLanding("member_hdr.csv", "totally_unknown,plan_hios_id\nS777,H7\n")

    val rawBefore = spark.table("p_raw.member").count()
    val conf = ConfigFactory.parseString(
      """schema.header_validation.quarantine_on_failure = true""")
      .withFallback(feedConf())

    intercept[com.hcsc.generic.ingest.schema.SchemaContractViolationException] {
      run(conf, baseCli.copy(runId = Some("run-hdr-1")))
    }

    // No RAW write happened and the file is quarantined, not left inprogress
    assert(spark.table("p_raw.member").count() == rawBefore)
    assert(listNames("quarantine").contains("member_hdr.csv"))
    assert(!listNames("inprogress").contains("member_hdr.csv"))

    // Header validation outcome persisted with a stable error code
    import org.apache.spark.sql.functions.col
    val headerAudit = spark.table("p_audit.ingest_header_audit")
      .filter(col("run_id") === "run-hdr-1")
    assert(headerAudit.count() == 1)
    val record = headerAudit.collect().head
    assert(record.getAs[String]("validation_status") == "FAILED")
    assert(record.getAs[String]("error_code") == "HDR_001")
    assert(record.getAs[String]("missing_required_columns").contains("subscriber_id"))
    assert(record.getAs[String]("actual_source_headers").contains("totally_unknown"))
  }

  // ---------------------------------------------------------------------------
  // 06 + 07: failed publish -> rollback -> resume replays curated from RAW
  // ---------------------------------------------------------------------------

  test("failed publish rolls back, then --resume replays curated from the RAW slice") {
    writeLanding("member_resume.csv", "subscriber id,plan_hios_id\nS500,H5\n")

    val curatedBefore = spark.table("p_curated.member").count()

    // Publish validation rejects any row with subscriber_id S500 -> curated fails
    val failingConf = feedConf(
      """publish { validation_query = "SELECT * FROM {table} WHERE subscriber_id = 'S500'" }""")

    val runId = "run-resume-1"
    intercept[PublishValidationException] {
      run(failingConf, baseCli.copy(runId = Some(runId)))
    }

    // Rollback: target untouched, file still inprogress, raw stage committed
    assert(spark.table("p_curated.member").count() == curatedBefore)
    assert(listNames("inprogress") == Set("member_resume.csv"))
    import org.apache.spark.sql.functions.col
    val rawSlice = spark.table("p_raw.member").filter(col("run_id") === runId)
    assert(rawSlice.count() == 1)

    // Resume with fixed config: raw is skipped (no duplicate append), curated
    // replays from the RAW slice, file completes to processed.
    run(feedConf(), baseCli.copy(runId = Some(runId), resume = true))

    assert(spark.table("p_raw.member").filter(col("run_id") === runId).count() == 1,
      "resume must not re-append to RAW")
    assert(spark.table("p_curated.member").count() == 1, "curated FULL replay from slice")
    assert(listNames("processed").contains("member_resume.csv"))

    val stageStatuses = spark.table("p_audit.ingest_run_audit")
      .filter(col("run_id") === runId)
      .select("stage", "status").collect()
      .map(r => (r.getString(0), r.getString(1))).toSet
    assert(stageStatuses.contains(("curated", "FAILED")))
    assert(stageStatuses.contains(("raw", "SKIPPED")))
    assert(stageStatuses.contains(("curated", "SUCCESS")))
  }
}
