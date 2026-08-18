package com.hcsc.generic.ingest.app

import com.hcsc.generic.ingest.retention.RetentionService
import com.typesafe.config.ConfigFactory
import java.nio.file.{Files, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
  * Retention must not overwrite a table it no longer owns.
  *
  * Every purge here is read-then-overwrite: survivors are staged, then
  * `INSERT OVERWRITE` replaces the target. If the entity lease lapses in
  * between — which it could, because retention was the one lock holder that
  * never heartbeated — another run legitimately claims the entity and
  * appends ledger, reject or watermark rows, and the overwrite discards
  * them. Nothing fails, and the rows that vanish are the audit trail that
  * would have recorded them.
  *
  * The heartbeat closes the ordinary case. These tests pin the case it
  * cannot: once a renewal has FAILED, ownership may already belong to
  * someone else, and the purge has to abort with the target untouched
  * rather than finish and be wrong.
  */
class RetentionOwnershipSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var tempDir: Path = _
  private var spark: SparkSession = _
  private val logger = Logger.getLogger(getClass.getName)

  override def beforeAll(): Unit = {
    super.beforeAll()
    tempDir = Files.createTempDirectory("retention-own-")
    System.setProperty("derby.stream.error.file",
      tempDir.resolve("derby.log").toAbsolutePath.toString)

    spark = SparkSession.builder()
      .master("local[2]")
      .appName("retention-ownership-test")
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

    spark.sql("CREATE DATABASE IF NOT EXISTS o_raw")
    spark.sql("CREATE DATABASE IF NOT EXISTS o_audit")
    spark.sql("SET hive.exec.dynamic.partition=true")
    spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")

    spark.sql("CREATE TABLE o_raw.member (id STRING) PARTITIONED BY (ingest_dt STRING) STORED AS ORC")
    spark.sql("INSERT INTO o_raw.member PARTITION (ingest_dt='2020-01-01') VALUES ('old')")

    spark.sql("CREATE TABLE o_audit.ingest_rejects (run_id STRING, reject_ts TIMESTAMP) USING ORC")
    spark.sql("INSERT INTO o_audit.ingest_rejects VALUES ('old-run', timestamp'2020-01-01 00:00:00')")
    spark.sql("INSERT INTO o_audit.ingest_rejects VALUES ('new-run', current_timestamp())")
  }

  override def afterAll(): Unit = {
    try { if (spark != null) spark.stop() }
    finally {
      import scala.collection.JavaConverters._
      Files.walk(tempDir).iterator().asScala.toSeq.reverse.foreach(p => Files.deleteIfExists(p))
    }
    super.afterAll()
  }

  private val feedConf = ConfigFactory.parseString(
    """
      |raw { database = o_raw, table = member }
      |rejects { database = o_audit, table = ingest_rejects }
      |retention { raw = "365d", rejects = "90d" }
      |""".stripMargin)

  /** Stands in for a heartbeat that has already lost the lease. */
  private def lostOwnership: () => Unit = () =>
    throw new com.hcsc.generic.ingest.lock.PipelineLockException(
      "PIPE_001 lease ownership lost mid-retention; aborting BEFORE the purge overwrite")

  private def rejectRunIds: Set[String] =
    spark.table("o_audit.ingest_rejects").collect().map(_.getString(0)).toSet

  private def partitions: Seq[String] =
    spark.sql("SHOW PARTITIONS o_raw.member").collect().map(_.getString(0)).toSeq

  test("a lost lease aborts the purge and leaves every table byte-for-byte untouched") {
    val rejectsBefore = rejectRunIds
    val partitionsBefore = partitions

    val thrown = intercept[com.hcsc.generic.ingest.lock.PipelineLockException](
      new RetentionService(spark, feedConf, logger, lostOwnership).run(dryRun = false))
    assert(thrown.getMessage.contains("PIPE_001"))

    // The whole point: aborting is only worth anything if it aborts BEFORE
    // the destructive step, not after.
    assert(rejectRunIds == rejectsBefore,
      "the expired reject row must still be there — an aborted purge purges nothing")
    assert(partitions == partitionsBefore,
      "the expired partition must still be there")
  }

  test("a dry-run is unaffected by ownership: it has nothing to overwrite") {
    // Reporting what WOULD be purged writes nothing, so it must not be
    // blocked by a lease that lapsed — operators inspect during incidents,
    // which is exactly when the lease is contended.
    val results = new RetentionService(spark, feedConf, logger, lostOwnership).run(dryRun = true)
    assert(results.exists { case (t, _, n) => t == "o_audit.ingest_rejects" && n == 1 })
    assert(results.exists { case (t, _, n) => t == "o_raw.member" && n == 1 })
  }

  test("with ownership intact the purge proceeds exactly as before") {
    // The guard must be invisible on the happy path — this is the
    // regression check for every existing retention behaviour.
    var guardCalls = 0
    val results = new RetentionService(spark, feedConf, logger, () => guardCalls += 1)
      .run(dryRun = false)

    assert(rejectRunIds == Set("new-run"), "only the recent reject row survives")
    assert(partitions.isEmpty, "the expired partition is dropped")
    assert(results.nonEmpty)
    assert(guardCalls >= 2,
      "ownership must be re-checked at each destructive step, not once at the start — " +
        s"a long purge can lose its lease midway; got $guardCalls")
  }

  test("the default guard keeps embedded callers working unchanged") {
    // Three-argument construction is still valid and never blocks: only the
    // CLI, which owns the lock, supplies a real guard.
    val svc = new RetentionService(spark, ConfigFactory.parseString(
      """rejects { database = o_audit, table = ingest_rejects }
        |retention { rejects = "90d" }""".stripMargin), logger)
    assert(svc.run(dryRun = true).nonEmpty)
  }
}
