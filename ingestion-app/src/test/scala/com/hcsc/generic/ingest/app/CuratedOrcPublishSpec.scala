package com.hcsc.generic.ingest.app

import com.hcsc.generic.ingest.runtime.RunContext
import com.hcsc.generic.ingest.stage.CuratedService
import com.typesafe.config.ConfigFactory
import java.nio.file.{Files, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
  * ORC curated publishes — the production storage format for the SmartIQ
  * feeds. Every other curated spec pins Parquet, so without this the ORC
  * path (staged datasource-ORC write -> INSERT OVERWRITE into a
  * Hive-format `STORED AS ORC` target) was never exercised.
  *
  * The pre-created EXTERNAL `STORED AS ORC` case is the shape the real
  * DDLs produce, and it is the one most likely to behave differently from
  * a framework-created table.
  */
class CuratedOrcPublishSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var tempDir: Path = _
  private var spark: SparkSession = _
  private val logger = Logger.getLogger(getClass.getName)

  override def beforeAll(): Unit = {
    super.beforeAll()
    tempDir = Files.createTempDirectory("curated-orc-")
    System.setProperty("derby.stream.error.file",
      tempDir.resolve("derby.log").toAbsolutePath.toString)
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("curated-orc-test")
      .config("spark.sql.warehouse.dir", tempDir.resolve("warehouse").toAbsolutePath.toString)
      .config("javax.jdo.option.ConnectionURL",
        s"jdbc:derby:;databaseName=${tempDir.resolve("metastore_db").toAbsolutePath};create=true")
      .config("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver")
      .config("datanucleus.schema.autoCreateTables", "true")
      .config("hive.metastore.schema.verification", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.caseSensitive", "false")
      .config("spark.sql.session.timeZone", "UTC")
      .enableHiveSupport()
      .getOrCreate()
    spark.sql("CREATE DATABASE IF NOT EXISTS orc_cur")
  }

  override def afterAll(): Unit = {
    try { if (spark != null) spark.stop() }
    finally {
      import scala.collection.JavaConverters._
      Files.walk(tempDir).iterator().asScala.toSeq.reverse.foreach(p => Files.deleteIfExists(p))
    }
    super.afterAll()
  }

  private def ctx(runId: String) = RunContext(runId, "orc_feed", "INCR", "I")

  /** (member_id, name, src_modified_ts as string, src_seq) */
  private def batch(rows: Seq[(String, String, String, Int)]): DataFrame = {
    val s = spark
    import s.implicits._
    rows.toDF("member_id", "name", "src_modified_ts", "src_seq")
      .withColumn("src_modified_ts", col("src_modified_ts").cast("timestamp"))
  }

  private def conf(table: String) = ConfigFactory.parseString(
    s"""
       |database = orc_cur
       |table = $table
       |format = orc
       |merge {
       |  keys = ["member_id"]
       |  freshness { column = "src_modified_ts", tie_breakers = ["src_seq"] }
       |}
       |""".stripMargin)

  private def row(table: String, id: String) =
    spark.table(s"orc_cur.$table").filter(col("member_id") === id).collect().head

  /** Timestamp rendered in the SESSION zone (UTC). java.sql.Timestamp
    * comparisons would convert through the JVM default zone instead and
    * report a spurious offset — verified: ORC and Parquet behave
    * identically here, the instant is preserved by both. */
  private def tsOf(table: String, id: String, column: String): String =
    spark.table(s"orc_cur.$table").filter(col("member_id") === id)
      .selectExpr(s"date_format($column, 'yyyy-MM-dd HH:mm:ss')").collect().head.getString(0)

  test("ORC curated: create, merge, and preserve timestamp values across the swap") {
    val svc = new CuratedService(spark, conf("members_orc"))
    svc.process(batch(Seq(
      ("M1", "alice_v1", "2026-01-01 10:00:00", 1),
      ("M2", "bob_v1", "2026-01-01 10:00:00", 1))), "INCR", ctx("orc-1"), None, None)

    assert(spark.table("orc_cur.members_orc").count() == 2)
    // the framework-created table really is ORC
    val provider = spark.sql("DESCRIBE FORMATTED orc_cur.members_orc").collect()
      .find(r => Option(r.getString(0)).exists(_.trim.equalsIgnoreCase("Provider")))
      .map(_.getString(1).trim.toLowerCase)
    assert(provider.contains("orc"), s"expected an ORC table, got $provider")

    // Incremental merge: staged ORC write + INSERT OVERWRITE swap
    val r = svc.process(batch(Seq(("M1", "alice_v2", "2026-01-01 12:00:00", 2))),
      "INCR", ctx("orc-2"), None, None).get
    assert(r.updateCount == 1)
    val m1 = row("members_orc", "M1")
    assert(m1.getAs[String]("name") == "alice_v2")
    // TIMESTAMP round-trip through ORC must be exact in SESSION terms.
    assert(tsOf("members_orc", "M1", "src_modified_ts") == "2026-01-01 12:00:00",
      "the instant must survive the ORC write/read round-trip unchanged")
    assert(row("members_orc", "M2").getAs[String]("name") == "bob_v1",
      "the untouched key must survive the swap intact")

    // Stale record still loses under ORC
    val stale = svc.process(batch(Seq(("M1", "alice_stale", "2026-01-01 09:00:00", 9))),
      "INCR", ctx("orc-3"), None, None).get
    assert(stale.ignoredCount == 1 && stale.updateCount == 0)
    assert(row("members_orc", "M1").getAs[String]("name") == "alice_v2")
  }

  test("ORC curated into a PRE-CREATED EXTERNAL 'STORED AS ORC' target (the real DDL shape)") {
    val loc = tempDir.resolve("ext_orc").toAbsolutePath.toString
    // Exactly what raw_ddl/curated_ddl produce: Hive-format, EXTERNAL, ORC.
    spark.sql(
      s"""CREATE EXTERNAL TABLE orc_cur.members_ext (
         |  member_id STRING, name STRING, src_modified_ts TIMESTAMP, src_seq INT,
         |  create_timestamp TIMESTAMP, last_modified_ts TIMESTAMP, last_modified_op STRING
         |) STORED AS ORC LOCATION '$loc'""".stripMargin)

    val svc = new CuratedService(spark, conf("members_ext"))
    svc.process(batch(Seq(("E1", "v1", "2026-02-01 10:00:00", 1))),
      "INCR", ctx("orc-ext-1"), None, None)
    assert(spark.table("orc_cur.members_ext").count() == 1)

    // The merge path against a Hive-format ORC target: this is where a
    // format mismatch between staging and target would surface.
    val r = svc.process(batch(Seq(
      ("E1", "v2", "2026-02-01 12:00:00", 2),
      ("E2", "new", "2026-02-01 12:00:00", 1))), "INCR", ctx("orc-ext-2"), None, None).get
    assert(r.updateCount == 1 && r.insertCount == 1)
    assert(row("members_ext", "E1").getAs[String]("name") == "v2")
    assert(tsOf("members_ext", "E1", "src_modified_ts") == "2026-02-01 12:00:00")

    // Still EXTERNAL and still ORC after the publish swap — the framework
    // must not silently replace the operator's table definition.
    val desc = spark.sql("DESCRIBE FORMATTED orc_cur.members_ext").collect()
      .map(r0 => (Option(r0.getString(0)).map(_.trim.toLowerCase).getOrElse(""),
                  Option(r0.getString(1)).map(_.trim).getOrElse("")))
    assert(desc.exists { case (k, v) => k == "type" && v.equalsIgnoreCase("EXTERNAL") },
      s"table must still be EXTERNAL after publish: ${desc.filter(_._1 == "type").toSeq}")
    assert(spark.table("orc_cur.members_ext").schema("src_modified_ts").dataType ==
      org.apache.spark.sql.types.TimestampType)
  }
}
