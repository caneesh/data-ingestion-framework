package com.hcsc.generic.ingest.app

import com.hcsc.generic.ingest.jdbc.watermark.{HiveWatermarkStore, VersionedWatermark, WatermarkConflictException, WatermarkValue}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Path => JPath}

/** Hive-backed watermark store: versioned commits, conflict detection, and
  * append-only history — against a real (temp Derby) Hive metastore. */
class HiveWatermarkStoreSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var tempDir: JPath = _
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    tempDir = Files.createTempDirectory("hive-watermark-")
    System.setProperty("derby.stream.error.file",
      tempDir.resolve("derby.log").toAbsolutePath.toString)

    spark = SparkSession.builder()
      .master("local[2]")
      .appName("hive-watermark-test")
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
    finally {
      import scala.collection.JavaConverters._
      Files.walk(tempDir).iterator().asScala.toSeq.reverse.foreach(p => Files.deleteIfExists(p))
    }
    super.afterAll()
  }

  private def store = new HiveWatermarkStore(spark, "wm_db", "watermarks")

  test("empty store returns no watermark") {
    assert(store.latestVersioned("nothing").isEmpty)
    assert(store.latest("nothing").isEmpty)
  }

  test("versioned commits advance and persist full history") {
    store.recordIfVersion("claims", WatermarkValue(Seq("2026-01-01 00:00:00")), "run-1", 0L)
    assert(store.latestVersioned("claims").contains(
      VersionedWatermark(WatermarkValue(Seq("2026-01-01 00:00:00")), 1L)))

    store.recordIfVersion("claims", WatermarkValue(Seq("2026-02-01 00:00:00")), "run-2", 1L)
    assert(store.latestVersioned("claims").get.version == 2L)
    assert(store.latest("claims").get.values.head == "2026-02-01 00:00:00")

    // Append-only history: both commits remain queryable
    import org.apache.spark.sql.functions.col
    val history = spark.table("wm_db.watermarks").filter(col("entity") === "claims")
    assert(history.count() == 2)
    assert(history.select("run_id").collect().map(_.getString(0)).toSet == Set("run-1", "run-2"))
  }

  test("stale version commit conflicts with JDBC_005") {
    val ex = intercept[WatermarkConflictException] {
      store.recordIfVersion("claims", WatermarkValue(Seq("2026-03-01 00:00:00")), "run-stale", 1L)
    }
    assert(ex.getMessage.contains("JDBC_005"))
    // The conflicting commit did not append
    assert(store.latestVersioned("claims").get.version == 2L)
  }

  test("entities are isolated; composite values round-trip through Hive") {
    store.recordIfVersion("members", WatermarkValue(Seq("2026-01-15 10:30:00", "42")), "run-m1", 0L)
    val vw = store.latestVersioned("members").get
    assert(vw.value.values == Seq("2026-01-15 10:30:00", "42"))
    assert(store.latestVersioned("claims").get.version == 2L, "other entities untouched")
  }

  test("unsafe store identifiers are rejected at construction") {
    intercept[IllegalArgumentException] {
      new HiveWatermarkStore(spark, "wm;drop", "watermarks")
    }
    intercept[IllegalArgumentException] {
      new HiveWatermarkStore(spark, "wm_db", "water marks")
    }
  }
}
