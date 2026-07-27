package com.hcsc.generic.ingest.app

import com.hcsc.generic.ingest.jdbc.watermark.{HiveWatermarkStore, WatermarkValue}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Path => JPath}

/**
  * Hive's watermark version check is best-effort (non-transactional): two
  * racing commits can land at the same version. latestVersioned must detect
  * the duplicate and deterministically pick the most recent commit
  * (updated_ts) instead of returning whichever row the scan surfaced first.
  */
class HiveWatermarkDuplicateVersionSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var tempDir: JPath = _
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    tempDir = Files.createTempDirectory("hive-wm-dup-")
    System.setProperty("derby.stream.error.file",
      tempDir.resolve("derby.log").toAbsolutePath.toString)

    spark = SparkSession.builder()
      .master("local[2]")
      .appName("hive-watermark-duplicate-test")
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
  }

  override def afterAll(): Unit = {
    try { if (spark != null) spark.stop() }
    finally super.afterAll()
  }

  test("duplicate versions resolve to the most recent commit deterministically") {
    val store = new HiveWatermarkStore(spark, "wm_dup_db", "ingest_watermarks")

    // Normal commit lands version 1.
    store.recordIfVersion("dup_feed", WatermarkValue(Seq("100")), "run-a", expectedVersion = 0L)

    // Simulate the racing commit the non-transactional check cannot stop:
    // a second row at the SAME version, written later with a higher value.
    spark.sql(
      """INSERT INTO wm_dup_db.ingest_watermarks
        |VALUES ('dup_feed', '250', 'run-b', 1, '100', NULL,
        |        timestamp '2100-01-01 00:00:00')""".stripMargin)

    val resolved = store.latestVersioned("dup_feed").get
    assert(resolved.version == 1L)
    assert(resolved.value.serialized == "250",
      "the later updated_ts wins the tie, deterministically")
  }
}
