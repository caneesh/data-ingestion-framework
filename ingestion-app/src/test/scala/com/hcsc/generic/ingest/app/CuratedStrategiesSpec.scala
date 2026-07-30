package com.hcsc.generic.ingest.app

import com.hcsc.generic.ingest.curated._
import com.hcsc.generic.ingest.runtime.RunContext
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Path => JPath}

/** Curated strategies against a real Hive metastore: append accumulation,
  * Type 1 upsert with dedup/metrics, snapshot replacement with delete
  * counting, and the transactional publish (no staging leftovers). */
class CuratedStrategiesSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var tempDir: JPath = _
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    tempDir = Files.createTempDirectory("curated-strategies-")
    System.setProperty("derby.stream.error.file",
      tempDir.resolve("derby.log").toAbsolutePath.toString)
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("curated-strategies-test")
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
    spark.sql("CREATE DATABASE IF NOT EXISTS c_cur")
  }

  override def afterAll(): Unit = {
    try { if (spark != null) spark.stop() }
    finally {
      import scala.collection.JavaConverters._
      Files.walk(tempDir).iterator().asScala.toSeq.reverse.foreach(p => Files.deleteIfExists(p))
    }
    super.afterAll()
  }

  private def ctx(runId: String) = RunContext(runId, "claims", "INCR", "I")

  private def df(rows: (String, Int, String)*) = {
    val stable = spark; import stable.implicits._
    rows.toDF("claim_id", "seq", "payload")
  }

  test("APPEND accumulates and counts everything as inserts") {
    val strategy = CuratedStrategies.forKind(spark, CuratedStrategies.Append)
    val target = CuratedTarget("c_cur", "claims_append", format = "parquet")

    val m1 = strategy.process(df(("K1", 1, "a")), target, ctx("r1"))
    val m2 = strategy.process(df(("K2", 1, "b")), target, ctx("r2"))
    assert(m1.insertRows == 1 && m2.insertRows == 1)
    assert(spark.table("c_cur.claims_append").count() == 2)
  }

  test("TYPE1_MERGE deduplicates, upserts by business key, and reports full metrics") {
    val strategy = CuratedStrategies.forKind(spark, CuratedStrategies.Type1Merge)
    val target = CuratedTarget("c_cur", "claims_merge", format = "parquet",
      businessKeys = Seq("claim_id"), orderBy = Seq("seq"))

    // First run: duplicate K1 rows collapse to the latest by seq
    val first = strategy.process(
      df(("K1", 1, "old"), ("K1", 2, "new"), ("K2", 1, "b")), target, ctx("r1"))
    assert(first.inputRows == 3 && first.duplicateRows == 1)
    assert(first.insertRows == 2 && first.updateRows == 0)

    // Second run: K2 updates, K3 inserts; unmatched K1 survives
    val second = strategy.process(df(("K2", 2, "b2"), ("K3", 1, "c")), target, ctx("r2"))
    assert(second.insertRows == 1 && second.updateRows == 1)

    import org.apache.spark.sql.functions.col
    val rows = spark.table("c_cur.claims_merge")
      .select("claim_id", "payload").collect().map(r => r.getString(0) -> r.getString(1)).toMap
    assert(rows == Map("K1" -> "new", "K2" -> "b2", "K3" -> "c"))
    assert(spark.catalog.listTables("c_cur").collect()
      .forall(t => !t.name.startsWith("__stg_")), "transactional publish leaves no staging behind")
  }

  test("TYPE1_MERGE requires business keys") {
    val strategy = CuratedStrategies.forKind(spark, CuratedStrategies.Type1Merge)
    val ex = intercept[IllegalArgumentException] {
      strategy.process(df(("K1", 1, "a")), CuratedTarget("c_cur", "claims_nokeys", format = "parquet"), ctx("r"))
    }
    assert(ex.getMessage.contains("CUR_001"))
  }

  test("SNAPSHOT_REPLACE replaces the image and derives insert/update/delete from key overlap") {
    val strategy = CuratedStrategies.forKind(spark, CuratedStrategies.SnapshotReplace)
    val target = CuratedTarget("c_cur", "claims_snapshot", format = "parquet",
      businessKeys = Seq("claim_id"), orderBy = Seq("seq"))

    strategy.process(df(("K1", 1, "a"), ("K2", 1, "b"), ("K3", 1, "c")), target, ctx("r1"))
    val second = strategy.process(df(("K2", 2, "b2"), ("K4", 1, "d")), target, ctx("r2"))

    assert(second.publishedRows == 2)
    assert(second.updateRows == 1, "K2 carried over")
    assert(second.insertRows == 1, "K4 is new")
    assert(second.deleteRows == 2, "K1 and K3 vanished")
    assert(spark.table("c_cur.claims_snapshot").count() == 2, "table IS the new image")
  }
}
