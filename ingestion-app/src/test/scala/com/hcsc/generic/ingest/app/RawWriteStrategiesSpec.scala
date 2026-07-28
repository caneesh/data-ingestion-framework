package com.hcsc.generic.ingest.app

import com.hcsc.generic.ingest.raw._
import com.hcsc.generic.ingest.transform.PartitionSpec
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Path => JPath}
import java.sql.Timestamp

/** Raw write strategies against a real Hive metastore with ORC storage:
  * append accumulation, dynamic partitions, batch idempotency, snapshot
  * partition replacement, and the CDC column contract. */
class RawWriteStrategiesSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var tempDir: JPath = _
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    tempDir = Files.createTempDirectory("raw-strategies-")
    System.setProperty("derby.stream.error.file",
      tempDir.resolve("derby.log").toAbsolutePath.toString)
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("raw-strategies-test")
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
    spark.sql("CREATE DATABASE IF NOT EXISTS r_raw")
  }

  override def afterAll(): Unit = {
    try { if (spark != null) spark.stop() }
    finally {
      import scala.collection.JavaConverters._
      Files.walk(tempDir).iterator().asScala.toSeq.reverse.foreach(p => Files.deleteIfExists(p))
    }
    super.afterAll()
  }

  private def context(batchId: String, day: String = "2026-07-28", dryRun: Boolean = false) =
    BatchContext("member", "health_sherpa", s"run-$batchId", batchId,
      loadTimestamp = Timestamp.valueOf(s"$day 12:00:00"), dryRun = dryRun)

  private def target(table: String, partitioned: Boolean = true) = RawTarget(
    database = "r_raw",
    table = table,
    partitions =
      if (partitioned) PartitionSpec(Seq("ingest_dt"), Map("ingest_dt" -> "date_format(load_timestamp, 'yyyy-MM-dd')"))
      else PartitionSpec(Seq.empty, Map.empty))

  private def writer(kind: String) =
    new RawWriter(new RawMetadataStamper, RawWriteStrategies.forKind(kind))

  private def df(rows: (String, String)*) = {
    val stable = spark; import stable.implicits._
    rows.toDF("subscriber_id", "hios_id")
  }

  test("APPEND_BATCH creates the ORC table, stamps metadata, and accumulates dynamic partitions") {
    val w = writer(RawWriteStrategies.AppendBatch)
    val r1 = w.write(spark, df("S1" -> "H1", "S2" -> "H2"), context("b1"), target("member_append"))
    val r2 = w.write(spark, df("S3" -> "H3"), context("b2", day = "2026-07-29"), target("member_append"))

    assert(r1.rowsWritten == 2 && r2.rowsWritten == 1)
    val table = spark.table("r_raw.member_append")
    assert(table.count() == 3)
    assert(RawMetadataStamper.StampedColumns.forall(table.columns.contains), "all metadata columns persisted")
    assert(spark.sql("SHOW PARTITIONS r_raw.member_append").count() == 2, "one dynamic partition per day")
    assert(spark.sql("DESCRIBE FORMATTED r_raw.member_append").collect()
      .exists(r => String.valueOf(r.get(1)).toLowerCase.contains("orc")), "stored as ORC")
  }

  test("a replayed batch id is skipped, not double-loaded") {
    val w = writer(RawWriteStrategies.AppendBatch)
    val replay = w.write(spark, df("S1" -> "H1", "S2" -> "H2"), context("b1"), target("member_append"))
    assert(replay.skipped && replay.detail.contains("idempotent"))
    assert(spark.table("r_raw.member_append").count() == 3, "count unchanged after replay")
  }

  test("dry-run counts but writes nothing") {
    val w = writer(RawWriteStrategies.AppendBatch)
    val r = w.write(spark, df("S9" -> "H9"), context("b-dry", dryRun = true), target("member_dry"))
    assert(r.skipped && r.rowsWritten == 1)
    assert(!spark.catalog.tableExists("r_raw.member_dry"))
  }

  test("SNAPSHOT replaces only its own snapshot_dt partition") {
    val w = writer(RawWriteStrategies.Snapshot)
    val t = target("member_snapshot", partitioned = false)

    w.write(spark, df("S1" -> "H1", "S2" -> "H2", "S3" -> "H3"), context("s1", day = "2026-07-27"), t)
    w.write(spark, df("S1" -> "H1", "S4" -> "H4"), context("s2", day = "2026-07-28"), t)
    // Corrected re-run of the 07-28 snapshot: same date, different batch id
    w.write(spark, df("S1" -> "H1"), context("s3", day = "2026-07-28"), t)

    import org.apache.spark.sql.functions.col
    val table = spark.table("r_raw.member_snapshot")
    assert(table.filter(col("snapshot_dt") === "2026-07-27").count() == 3, "earlier snapshot untouched")
    assert(table.filter(col("snapshot_dt") === "2026-07-28").count() == 1, "same-day snapshot replaced")
    assert(table.count() == 4)
  }

  test("CDC_EVENTS refuses batches without operation and sequence columns") {
    val w = writer(RawWriteStrategies.CdcEvents)
    val ex = intercept[IllegalArgumentException] {
      w.write(spark, df("S1" -> "H1"), context("c1"), target("member_cdc"))
    }
    assert(ex.getMessage.contains("RAW_002"))
    assert(ex.getMessage.contains("cdc_operation"))
  }

  test("CDC_EVENTS appends events verbatim — no collapsing") {
    val stable = spark; import stable.implicits._
    val events = Seq(
      ("K1", "I", 1L, "v1"),
      ("K1", "U", 2L, "v2"),
      ("K1", "D", 3L, null.asInstanceOf[String])
    ).toDF("business_key", "cdc_operation", "cdc_sequence", "payload")

    val w = writer(RawWriteStrategies.CdcEvents)
    val r = w.write(spark, events, context("c2"), target("member_cdc"))
    assert(r.rowsWritten == 3)
    val stored = spark.table("r_raw.member_cdc")
    assert(stored.count() == 3, "the full change history is preserved")
    import org.apache.spark.sql.functions.col
    assert(stored.filter(col("cdc_operation") === "D").count() == 1)
  }
}
