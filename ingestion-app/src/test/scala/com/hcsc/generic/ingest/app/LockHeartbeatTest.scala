package com.hcsc.generic.ingest.app

import com.hcsc.generic.ingest.lock.{LockHeartbeat, PipelineLockException, RunLock}
import org.apache.log4j.Logger
import org.scalatest.funsuite.AnyFunSuite

import java.util.concurrent.atomic.AtomicInteger

/** Active lease heartbeat: recurring renewal on a daemon thread, loud
  * ownership loss on any renewal failure, clean idempotent shutdown. */
class LockHeartbeatTest extends AnyFunSuite {

  private val logger = Logger.getLogger(getClass.getName)

  /** Stub provider with controllable renewal failure — the heartbeat's
    * contract is against RunLock, not a specific provider. */
  private final class StubRunLock extends RunLock {
    val renewals = new AtomicInteger(0)
    @volatile var failing = false
    override def acquire(entity: String, runId: String): Unit = ()
    override def renew(entity: String, runId: String): Unit = {
      if (failing) throw new PipelineLockException("PIPE_001 stub renewal failure")
      renewals.incrementAndGet()
      ()
    }
    override def release(entity: String, runId: String): Unit = ()
    override val leaseMillis: Long = 300L
  }

  private def eventually(timeoutMs: Long)(cond: => Boolean): Boolean = {
    val deadline = System.currentTimeMillis() + timeoutMs
    while (System.currentTimeMillis() < deadline) {
      if (cond) return true
      Thread.sleep(20)
    }
    cond
  }

  private def heartbeat(stub: StubRunLock, intervalMillis: Long = 100L) =
    new LockHeartbeat(stub, "hb_entity", "hb-run", intervalMillis, logger)

  test("renewal recurs on the interval while the run is healthy") {
    val stub = new StubRunLock
    val hb = heartbeat(stub)
    try {
      hb.start()
      // >= 2 renewals well within 3 intervals' worth of waiting budget.
      assert(eventually(3000)(stub.renewals.get() >= 2),
        s"expected >= 2 renewals, got ${stub.renewals.get()}")
      assert(!hb.ownershipLost)
    } finally hb.stop()
  }

  test("a renewal failure sets ownershipLost, logs and stops the heartbeat") {
    val stub = new StubRunLock
    stub.failing = true
    val hb = heartbeat(stub)
    try {
      hb.start()
      assert(eventually(3000)(hb.ownershipLost), "ownershipLost never set")
      // The heartbeat stopped itself: even after the failure is cleared, no
      // further renewals happen.
      stub.failing = false
      val frozen = stub.renewals.get()
      Thread.sleep(400)
      assert(stub.renewals.get() == frozen, "heartbeat kept renewing after a failure")
      assert(eventually(3000)(hb.terminated), "executor not terminated after failure")
    } finally hb.stop()
  }

  test("stop() is idempotent, terminates the thread, and no renewal runs after it") {
    val stub = new StubRunLock
    val hb = heartbeat(stub)
    hb.start()
    eventually(3000)(stub.renewals.get() >= 1)
    hb.stop()
    hb.stop() // idempotent
    assert(hb.terminated, "executor not terminated after stop()")
    val frozen = stub.renewals.get()
    Thread.sleep(350)
    assert(stub.renewals.get() == frozen, "renewal happened after stop()")
    assert(!hb.ownershipLost)
  }

  test("stop() before start() and double start() are safe") {
    val stub = new StubRunLock
    val hb = heartbeat(stub)
    hb.stop() // never started: still terminates cleanly
    assert(hb.terminated)

    val stub2 = new StubRunLock
    val hb2 = heartbeat(stub2)
    try {
      hb2.start()
      hb2.start() // second start is a no-op, not a second schedule
      assert(eventually(3000)(stub2.renewals.get() >= 2))
    } finally hb2.stop()
  }
}
