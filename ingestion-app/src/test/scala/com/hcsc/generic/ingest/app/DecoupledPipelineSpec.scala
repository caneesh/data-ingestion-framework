package com.hcsc.generic.ingest.app

import com.hcsc.generic.ingest.audit.AuditService
import com.hcsc.generic.ingest.model.CliParser
import com.hcsc.generic.ingest.pipeline.CuratedBatchDriver
import com.hcsc.generic.ingest.runtime.{RunContext, StageStatus, Stages}
import com.typesafe.config.ConfigFactory
import java.nio.file.{Files, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/** Decoupled operation end to end: the pending driver sequences governed
  * curatedReplay per RAW batch, checkpointing via the batch's own curated
  * SUCCESS ledger row. RAW batches and ledger rows are arranged directly
  * (the raw job's own behavior — including advance_after=RAW — is covered
  * by JdbcPipelineIntegrationSpec); curatedReplay's no-watermark guarantee
  * is pinned there too. */
class DecoupledPipelineSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var tempDir: Path = _
  private var spark: SparkSession = _
  private val logger = Logger.getLogger(getClass.getName)

  override def beforeAll(): Unit = {
    super.beforeAll()
    tempDir = Files.createTempDirectory("decoupled-")
    System.setProperty("derby.stream.error.file",
      tempDir.resolve("derby.log").toAbsolutePath.toString)
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("decoupled-test")
      .config("spark.sql.warehouse.dir", tempDir.resolve("warehouse").toAbsolutePath.toString)
      .config("javax.jdo.option.ConnectionURL",
        s"jdbc:derby:;databaseName=${tempDir.resolve("metastore_db").toAbsolutePath};create=true")
      .config("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver")
      .config("datanucleus.schema.autoCreateTables", "true")
      .config("hive.metastore.schema.verification", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.session.timeZone", "UTC")
      .enableHiveSupport()
      .getOrCreate()
    Seq("d_raw", "d_curated", "d_audit").foreach(db =>
      spark.sql(s"CREATE DATABASE IF NOT EXISTS $db"))
  }

  override def afterAll(): Unit = {
    try { if (spark != null) spark.stop() }
    finally {
      import scala.collection.JavaConverters._
      Files.walk(tempDir).iterator().asScala.toSeq.reverse.foreach(p => Files.deleteIfExists(p))
    }
    super.afterAll()
  }

  private val entity = "d_feed"
  private val feedConf = ConfigFactory.parseString(
    s"""
       |source { type = "file", path = "unused" }
       |audit { database = d_audit }
       |raw { database = d_raw, table = members }
       |curated {
       |  database = d_curated
       |  table = members
       |  format = parquet
       |  merge { keys = ["member_id"], freshness { column = "src_modified_ts" } }
       |  # poison pill for failure-path tests: publishing any 'poison' row fails
       |  publish { validation_query = "SELECT * FROM {table} WHERE name = 'poison'" }
       |}
       |""".stripMargin)
  private lazy val audit = AuditService(spark, feedConf)

  /** Lands a RAW batch: rows in d_raw.members under runId + a raw SUCCESS
    * ledger row (the state a decoupled raw-only job leaves behind). */
  private def landRawBatch(runId: String, rows: Seq[(String, String, String)]): Unit = {
    val s = spark
    import s.implicits._
    val df = rows.toDF("member_id", "name", "src_modified_ts")
      .withColumn("src_modified_ts", col("src_modified_ts").cast("timestamp"))
      .withColumn("run_id", lit(runId))
      .withColumn("file_type", lit("I"))
      .withColumn("source_system", lit(if (runId.endsWith("sap")) "SAP" else "CORE"))
    if (!spark.catalog.tableExists("d_raw.members"))
      df.write.format("parquet").saveAsTable("d_raw.members")
    else df.write.format("parquet").mode("append").insertInto("d_raw.members")
    val ctx = RunContext(runId, entity, "INCR", "I")
    audit.recordStage(ctx, Stages.Raw, StageStatus.Started)
    audit.recordStage(ctx, Stages.Raw, StageStatus.Success)
  }

  private def driver(args: String*): CuratedBatchDriver = {
    val cli = CliParser.parse(
      (Seq("--entity", entity, "--stage", "curated") ++ args).toArray)
    new CuratedBatchDriver(spark, feedConf, cli, logger)
  }

  test("--pending publishes every raw-SUCCESS batch and checkpoints under its run id") {
    landRawBatch("run-a", Seq(("M1", "a_v1", "2026-01-01 10:00:00")))
    landRawBatch("run-b", Seq(("M1", "a_v2", "2026-01-01 12:00:00"),
      ("M2", "b_v1", "2026-01-01 12:00:00")))

    driver("--pending").run()

    val curated = spark.table("d_curated.members").collect()
      .map(r => r.getAs[String]("member_id") -> r.getAs[String]("name")).toMap
    assert(curated == Map("M1" -> "a_v2", "M2" -> "b_v1"),
      "both batches merged; freshness picked the newer M1")
    assert(audit.hasStageSuccess("run-a", entity, Stages.Curated) &&
      audit.hasStageSuccess("run-b", entity, Stages.Curated),
      "curated SUCCESS must checkpoint under the ORIGINAL raw run ids")
    assert(audit.pendingBatches(entity).isEmpty)
  }

  test("a second --pending pass is a clean no-op") {
    val before = spark.table("d_curated.members").count()
    driver("--pending").run() // empty pending set: no error, nothing written
    assert(spark.table("d_curated.members").count() == before)
  }

  test("resumed raw runs (SKIPPED after SUCCESS) are still selectable and replayable") {
    landRawBatch("run-resumed", Seq(("M3", "c_v1", "2026-01-02 10:00:00")))
    audit.recordStage(RunContext("run-resumed", entity, "INCR", "I"),
      Stages.Raw, StageStatus.Skipped, message = "resume: already completed")

    driver("--pending").run()
    assert(audit.hasStageSuccess("run-resumed", entity, Stages.Curated),
      "guard alignment: the later SKIPPED row must not block the replay")
  }

  test("STOP halts at the failing batch; it stays pending; CONTINUE works around it") {
    // run-bad carries a poison row the publish validation query rejects.
    // (An EMPTY raw slice, by contrast, checkpoints as a graceful zero-row
    // success via the zero-incoming early return — correct and covered by
    // the merge suite's empty-batch tests.)
    landRawBatch("run-bad", Seq(("MX", "poison", "2026-01-03 09:00:00")))
    landRawBatch("run-good", Seq(("M4", "d_v1", "2026-01-03 10:00:00")))

    val e = intercept[IllegalStateException] { driver("--pending").run() }
    assert(e.getMessage.contains("PIPE_005"))
    assert(!audit.hasStageSuccess("run-good", entity, Stages.Curated),
      "STOP: batches after the failure must not be attempted")
    assert(audit.pendingBatches(entity).map(_.runId).toSet == Set("run-bad", "run-good"))

    val continueConf = ConfigFactory.parseString(
      """curated.pending.on_failure = "CONTINUE"""").withFallback(feedConf)
    val cli = CliParser.parse(Array("--entity", entity, "--stage", "curated", "--pending"))
    val e2 = intercept[IllegalStateException] {
      new CuratedBatchDriver(spark, continueConf, cli, logger).run()
    }
    assert(e2.getMessage.contains("PIPE_005") && e2.getMessage.contains("run-bad"))
    assert(audit.hasStageSuccess("run-good", entity, Stages.Curated),
      "CONTINUE: the good batch behind the failure must be processed and checkpointed")
    assert(audit.failedBatches(entity).map(_.runId) == Seq("run-bad"))
  }

  test("--replay-failed selects only failed-and-pending batches") {
    // The operator 'fixes the feed' by lifting the poison validation query;
    // --replay-failed retries exactly the failed batch and checkpoints it.
    val fixedConf = feedConf.withoutPath("curated.publish.validation_query")
    val cli = CliParser.parse(Array("--entity", entity, "--stage", "curated", "--replay-failed"))
    new CuratedBatchDriver(spark, fixedConf, cli, logger).run()
    assert(audit.hasStageSuccess("run-bad", entity, Stages.Curated))
    assert(audit.pendingBatches(entity).isEmpty)
  }

  test("--replay-source-system filters batches by RAW lineage; --replay-last forces rebuilds") {
    landRawBatch("run-sap", Seq(("S1", "sap_v1", "2026-01-05 10:00:00")))
    landRawBatch("run-core2", Seq(("M6", "f_v1", "2026-01-05 11:00:00")))

    // The poison row from the failure test now lives in curated, so these
    // publishes use the lifted-validation config (staging re-stages the
    // whole merged table, poison included).
    val cleanConf = feedConf.withoutPath("curated.publish.validation_query")
    def cleanDriver(args: String*) = new CuratedBatchDriver(spark, cleanConf,
      CliParser.parse((Seq("--entity", entity, "--stage", "curated") ++ args).toArray), logger)

    cleanDriver("--pending", "--replay-source-system", "SAP").run()
    assert(audit.hasStageSuccess("run-sap", entity, Stages.Curated))
    assert(!audit.hasStageSuccess("run-core2", entity, Stages.Curated),
      "the filter must exclude other source systems")

    cleanDriver("--replay-last", "2").run() // forced rebuild ignores curated state
    assert(audit.pendingBatches(entity).isEmpty)
  }

  test("a batch with no recorded run_mode fails closed (PIPE_008), never defaults to FULL") {
    // Pre-migration ledger row: raw SUCCESS with empty run_mode. Defaulting
    // to the CLI mode (FULL) would FULL_OVERWRITE the table with one slice.
    audit.recordStage(RunContext("run-nomode", entity, "", "I"), Stages.Raw, StageStatus.Success)
    val e = intercept[IllegalStateException] { driver("--pending").run() }
    assert(e.getMessage.contains("PIPE_008"))
    assert(e.getMessage.contains("run-nomode"))
    // clean up so later selectors aren't poisoned
    audit.recordStage(RunContext("run-nomode", entity, "INCR", "I"),
      Stages.Curated, StageStatus.Success)
  }

  test("preconditions: driver refuses to run without a ledger or with curated disabled") {
    val noLedger = ConfigFactory.parseString("""audit { enabled = false }""").withFallback(feedConf)
    val cli = CliParser.parse(Array("--entity", entity, "--stage", "curated", "--pending"))
    val e1 = intercept[IllegalArgumentException] {
      new CuratedBatchDriver(spark, noLedger, cli, logger).run()
    }
    assert(e1.getMessage.contains("PIPE_006"))

    val curatedOff = ConfigFactory.parseString("""curated.enabled = false""").withFallback(feedConf)
    val e2 = intercept[IllegalArgumentException] {
      new CuratedBatchDriver(spark, curatedOff, cli, logger).run()
    }
    assert(e2.getMessage.contains("PIPE_006"))
  }
}
