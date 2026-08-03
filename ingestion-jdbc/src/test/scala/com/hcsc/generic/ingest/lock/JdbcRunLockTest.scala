package com.hcsc.generic.ingest.lock

import com.typesafe.config.ConfigFactory
import org.apache.log4j.Logger
import org.scalatest.funsuite.AnyFunSuite

import java.util.concurrent.{CountDownLatch, CyclicBarrier}
import java.util.concurrent.atomic.AtomicInteger

/** JDBC run-lock provider: atomic compare-and-set semantics on H2 — the
  * guarantees the best-effort Hive provider cannot make. */
class JdbcRunLockTest extends AnyFunSuite {

  private val logger = Logger.getLogger(getClass.getName)

  /** In-memory H2 shared across connections in this JVM for the test's
    * lifetime. A unique database per test isolates control tables. */
  private def lock(db: String, leaseMillis: Long = 60000L): JdbcRunLock =
    new JdbcRunLock(
      url = s"jdbc:h2:mem:$db;DB_CLOSE_DELAY=-1",
      user = None, password = None, driver = None,
      table = "ingestion_lock",
      leaseMillis = leaseMillis,
      logger = logger)

  test("two simultaneous acquirers: exactly one wins, every iteration") {
    val l = lock("race_db")
    (1 to 20).foreach { i =>
      val entity = s"race_entity_$i"
      val wins = new AtomicInteger(0)
      val losses = new AtomicInteger(0)
      val barrier = new CyclicBarrier(2)
      val done = new CountDownLatch(2)
      val winner = new java.util.concurrent.atomic.AtomicReference[String]()
      def contender(runId: String): Thread = new Thread(new Runnable {
        override def run(): Unit = {
          barrier.await()
          try { l.acquire(entity, runId); wins.incrementAndGet(); winner.set(runId) }
          catch { case _: PipelineLockException => losses.incrementAndGet() }
          finally done.countDown()
        }
      })
      contender("run-a").start()
      contender("run-b").start()
      done.await()
      assert(wins.get() == 1, s"iteration $i: expected exactly one winner, got ${wins.get()}")
      assert(losses.get() == 1, s"iteration $i: expected exactly one loser, got ${losses.get()}")
      l.release(entity, winner.get())
    }
  }

  test("acquire while held is rejected with the holder in the message") {
    val l = lock("held_db")
    l.acquire("orders", "run-1")
    val e = intercept[PipelineLockException] { l.acquire("orders", "run-2") }
    assert(e.getMessage.contains("PIPE_001"))
    assert(e.getMessage.contains("run-1"))
    // Re-acquiring one's own lease is idempotent; other entities unaffected.
    l.acquire("orders", "run-1")
    l.acquire("claims", "run-3")
    l.release("claims", "run-3")
    l.release("orders", "run-1")
  }

  test("an expired lease can be taken over (crashed holder does not wedge the entity)") {
    val short = lock("expiry_db", leaseMillis = 400L)
    short.acquire("expiry_feed", "crashed-run")
    Thread.sleep(900)
    short.acquire("expiry_feed", "takeover-run")
    short.release("expiry_feed", "takeover-run")
  }

  test("self-renew extends the lease past the original expiry") {
    val short = lock("renew_db", leaseMillis = 2000L)
    short.acquire("renew_feed", "long-run")
    Thread.sleep(1400)
    short.renew("renew_feed", "long-run") // extends to ~t+3400ms
    Thread.sleep(1200)
    // ~2.6s after acquire: the ORIGINAL lease is gone; the renewal keeps the
    // entity locked against a competitor.
    val e = intercept[PipelineLockException] { short.acquire("renew_feed", "intruder-run") }
    assert(e.getMessage.contains("long-run"))
    short.release("renew_feed", "long-run")
  }

  test("renew by a non-holder fails with PIPE_001 (ownership lost)") {
    val l = lock("foreign_db")
    l.acquire("stolen_feed", "owner-run")
    val e = intercept[PipelineLockException] { l.renew("stolen_feed", "foreign-run") }
    assert(e.getMessage.contains("PIPE_001"))
    assert(e.getMessage.contains("owner-run"))
    l.release("stolen_feed", "owner-run")
    // After release there is no holder either: renew still fails.
    val gone = intercept[PipelineLockException] { l.renew("stolen_feed", "owner-run") }
    assert(gone.getMessage.contains("PIPE_001"))
  }

  test("release then reacquire by another run") {
    val l = lock("release_db")
    l.acquire("orders", "run-1")
    l.release("orders", "run-1")
    l.acquire("orders", "run-2")
    l.release("orders", "run-2")
  }

  test("release by a non-holder warns and does not throw") {
    val l = lock("nonholder_db")
    l.acquire("orders", "run-1")
    l.release("orders", "someone-else") // no-op, warn only
    // run-1 still holds the lease
    val e = intercept[PipelineLockException] { l.acquire("orders", "run-2") }
    assert(e.getMessage.contains("run-1"))
    l.release("orders", "run-1")
    l.release("orders", "run-1") // second release of a released lease: warn only
  }

  test("RunLock.fromConfig: JDBC provider selection and config validation (PIPE_001)") {
    // Provider selection needs no SparkSession for the JDBC/off/error paths.
    val jdbc = RunLock.fromConfig(null, ConfigFactory.parseString(
      """concurrency { provider = JDBC, jdbc { url = "jdbc:h2:mem:cfg_db", lease_minutes = 10 } }"""),
      logger)
    assert(jdbc.exists(_.isInstanceOf[JdbcRunLock]))
    assert(jdbc.get.leaseMillis == 10L * 60000L)

    val off = RunLock.fromConfig(null, ConfigFactory.parseString(
      """concurrency { lock = OFF, provider = JDBC }"""), logger)
    assert(off.isEmpty)

    val badProvider = intercept[IllegalArgumentException] {
      RunLock.fromConfig(null, ConfigFactory.parseString(
        """concurrency { provider = ZOOKEEPER }"""), logger)
    }
    assert(badProvider.getMessage.contains("PIPE_001"))

    val missingJdbc = intercept[IllegalArgumentException] {
      RunLock.fromConfig(null, ConfigFactory.parseString(
        """concurrency { provider = JDBC }"""), logger)
    }
    assert(missingJdbc.getMessage.contains("PIPE_001"))

    val missingUrl = intercept[IllegalArgumentException] {
      RunLock.fromConfig(null, ConfigFactory.parseString(
        """concurrency { provider = JDBC, jdbc { table = ingestion_lock } }"""), logger)
    }
    assert(missingUrl.getMessage.contains("PIPE_001"))

    val badMode = intercept[IllegalArgumentException] {
      RunLock.fromConfig(null, ConfigFactory.parseString(
        """concurrency { lock = MAYBE }"""), logger)
    }
    assert(badMode.getMessage.contains("PIPE_001"))
  }
}
