package com.hcsc.generic.ingest.app

import com.hcsc.generic.ingest.lock.{LockService, PipelineLockException}
import com.typesafe.config.ConfigFactory
import java.nio.file.{Files, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/** Entity-level run lease: contention, release/reacquire, lease expiry,
  * force-release and config resolution. */
class LockServiceSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var tempDir: Path = _
  private var spark: SparkSession = _
  private val logger = Logger.getLogger(getClass.getName)

  override def beforeAll(): Unit = {
    super.beforeAll()
    tempDir = Files.createTempDirectory("lock-service-")
    System.setProperty("derby.stream.error.file",
      tempDir.resolve("derby.log").toAbsolutePath.toString)

    spark = SparkSession.builder()
      .master("local[2]")
      .appName("lock-service-test")
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

  private def service(leaseMillis: Long = 60000L) =
    new LockService(spark, "l_audit", "ingest_run_locks", leaseMillis, settleMillis = 0L, logger)

  test("second run of the same entity is rejected while the lease is held") {
    val lock = service()
    lock.acquire("orders", "run-1")

    val e = intercept[PipelineLockException] {
      lock.acquire("orders", "run-2")
    }
    assert(e.getMessage.contains("PIPE_001"))
    assert(e.getMessage.contains("run-1"))

    // A different entity is unaffected
    lock.acquire("claims", "run-3")
    lock.release("claims", "run-3")

    // Release lets the next run through
    lock.release("orders", "run-1")
    lock.acquire("orders", "run-2")
    lock.release("orders", "run-2")
  }

  test("lease is honored under a session timezone different from the JVM default") {
    // Regression: a JVM-formatted timestamp literal parsed in a different
    // session zone skews lease_until by the offset — on a non-UTC cluster
    // that skew exceeded the lease and produced locks born expired.
    // UTC+14: guaranteed to differ from any plausible JVM default zone.
    val prev = spark.conf.get("spark.sql.session.timeZone")
    spark.conf.set("spark.sql.session.timeZone", "Pacific/Kiritimati")
    try {
      val lock = service()
      lock.acquire("tz_feed", "run-tz-1")
      val e = intercept[PipelineLockException] {
        lock.acquire("tz_feed", "run-tz-2")
      }
      assert(e.getMessage.contains("run-tz-1"))
      lock.release("tz_feed", "run-tz-1")
    } finally spark.conf.set("spark.sql.session.timeZone", prev)
  }

  test("an expired lease can be taken over (crashed holder does not wedge the entity)") {
    // Lease granularity is one second (timestampadd SECOND in SQL).
    val shortLease = service(leaseMillis = 1000L)
    shortLease.acquire("expiry_feed", "crashed-run")
    Thread.sleep(1600)
    shortLease.acquire("expiry_feed", "takeover-run")
    shortLease.release("expiry_feed", "takeover-run")
  }

  test("forceRelease evicts the active holder") {
    val lock = service()
    lock.acquire("stuck_feed", "wedged-run")
    lock.forceRelease("stuck_feed")
    lock.acquire("stuck_feed", "next-run")
    lock.release("stuck_feed", "next-run")
  }

  test("fromConfig: OFF disables, audit database is reused, explicit block without database fails") {
    val off = LockService.fromConfig(spark,
      ConfigFactory.parseString("""concurrency { lock = OFF }"""), logger)
    assert(off.isEmpty)

    val viaAudit = LockService.fromConfig(spark,
      ConfigFactory.parseString("""audit { database = l_audit }"""), logger)
    assert(viaAudit.isDefined)

    val unlocked = LockService.fromConfig(spark, ConfigFactory.parseString("{}"), logger)
    assert(unlocked.isEmpty)

    val e = intercept[IllegalArgumentException] {
      LockService.fromConfig(spark,
        ConfigFactory.parseString("""concurrency { settle_ms = 0 }"""), logger)
    }
    assert(e.getMessage.contains("PIPE_001"))
  }
}
