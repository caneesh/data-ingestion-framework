package com.hcsc.generic.ingest.app

import com.hcsc.generic.ingest.model.Cli
import com.hcsc.generic.ingest.pipeline.IngestPipeline
import com.hcsc.generic.ingest.sink.HiveSink
import com.hcsc.generic.ingest.source.{Source, SourceRegistry}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Path => JPath}

/** Records the configuration the pipeline hands a source. */
object CapturingSource extends Source {
  @volatile var captured: Option[Config] = None
  override def sourceType: String = "capture"
  override def read(spark: SparkSession, sourceConf: Config): DataFrame = {
    captured = Some(sourceConf)
    import spark.implicits._
    Seq(("a", 1), ("b", 2)).toDF("id", "n")
  }
}

/**
  * R-10: the execution context a source receives from the pipeline is a
  * contract. HiveSource reads run_mode for the HIVE_007 gate and the
  * run-ledger coordinates to recover a read window in a fresh JVM; and the
  * key is run_mode — never `mode`, which is JDBC's extraction mode.
  */
class SourceContextInjectionSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var tempDir: JPath = _
  private var spark: SparkSession = _
  private val logger = Logger.getLogger(getClass.getName)

  override def beforeAll(): Unit = {
    super.beforeAll()
    tempDir = Files.createTempDirectory("source-context-")
    System.setProperty("derby.stream.error.file",
      tempDir.resolve("derby.log").toAbsolutePath.toString)
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("source-context-injection")
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
    SourceRegistry.register(CapturingSource)
    HiveSink.register()
    Seq("c_raw", "c_audit").foreach(db => spark.sql(s"CREATE DATABASE IF NOT EXISTS $db"))
  }

  override def afterAll(): Unit = {
    try {
      if (spark != null) spark.stop()
    } finally {
      if (tempDir != null) {
        import scala.collection.JavaConverters._
        Files.walk(tempDir).iterator().asScala.toSeq.reverse.foreach(p => Files.deleteIfExists(p))
      }
    }
    super.afterAll()
  }

  private def feed(audit: String): Config = ConfigFactory.parseString(
    s"""
       |source { type = capture }
       |$audit
       |concurrency { settle_ms = 0 }
       |raw {
       |  database = c_raw
       |  table = captured
       |  path = "${tempDir.resolve("raw_tbl").toAbsolutePath}"
       |  format = parquet
       |}
    """.stripMargin)

  test("the pipeline injects entity, run_id, run_mode and the run-ledger coordinates") {
    CapturingSource.captured = None
    new IngestPipeline(spark, feed("""audit { database = c_audit, run_table = run_ledger }"""),
      Cli(entity = "cap_entity", mode = "INCR", runId = Some("ctx-run-1")), logger).run()
    val c = CapturingSource.captured.getOrElse(fail("the source was never read"))
    assert(c.getString("entity") == "cap_entity")
    assert(c.getString("run_id") == "ctx-run-1")
    assert(c.getString("run_mode") == "INCR")
    assert(c.getString("audit_database") == "c_audit")
    assert(c.getString("audit_run_table") == "run_ledger")
    assert(!c.hasPath("mode"), "the run mode must never land on `mode`: that key is JDBC's extraction mode")
  }

  test("audit_run_table defaults to ingest_run_audit and run_mode follows the CLI") {
    CapturingSource.captured = None
    new IngestPipeline(spark, feed("""audit { database = c_audit }"""),
      Cli(entity = "cap_entity", mode = "FULL", runId = Some("ctx-run-2")), logger).run()
    val c = CapturingSource.captured.getOrElse(fail("the source was never read"))
    assert(c.getString("audit_run_table") == "ingest_run_audit")
    assert(c.getString("run_mode") == "FULL")
  }
}
