package com.hcsc.generic.ingest.lock

import org.apache.log4j.Logger

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{Executors, ScheduledExecutorService, ThreadFactory, TimeUnit}

/**
  * Active lease heartbeat: renews the entity lease on a single daemon
  * scheduler thread every `intervalMillis` (the pipeline uses lease/3, so a
  * healthy run can miss two beats before the lease lapses).
  *
  * On ANY renewal failure the heartbeat sets `ownershipLost`, logs the
  * error and stops beating — it never retries, because a failed renewal
  * means the lease may already belong to another run. The pipeline checks
  * `ownershipLost` at the danger points (before the curated publish and
  * before the watermark advance) and aborts with PIPE_001 instead of
  * writing concurrently.
  *
  * `stop()` is idempotent, awaits scheduler termination briefly, and must
  * be called in the owning run's `finally` — the thread is daemon so a
  * leak cannot pin the JVM, but tests assert clean termination anyway.
  */
final class LockHeartbeat(
  lock: RunLock,
  entity: String,
  runId: String,
  intervalMillis: Long,
  logger: Logger
) {
  private val lost = new AtomicBoolean(false)
  private val started = new AtomicBoolean(false)

  private val executor: ScheduledExecutorService =
    Executors.newSingleThreadScheduledExecutor(new ThreadFactory {
      override def newThread(r: Runnable): Thread = {
        val t = new Thread(r, s"ingest-lock-heartbeat-$entity")
        t.setDaemon(true)
        t
      }
    })

  def start(): Unit =
    if (started.compareAndSet(false, true)) {
      val interval = math.max(intervalMillis, 1L)
      executor.scheduleAtFixedRate(new Runnable {
        override def run(): Unit =
          try lock.renew(entity, runId)
          catch {
            case e: Throwable =>
              lost.set(true)
              logger.error(s"[Lock] Heartbeat renewal FAILED for entity=$entity run=$runId: " +
                s"${e.getMessage} — lease ownership is lost; the run will abort before " +
                "publish/watermark. Heartbeat stopped.")
              executor.shutdown()
          }
      }, interval, interval, TimeUnit.MILLISECONDS)
      logger.info(s"[Lock] Heartbeat started for entity=$entity run=$runId " +
        s"(renewing every ${interval}ms)")
    }

  /** True once any renewal has failed: the lease may belong to another run
    * and this run must not publish or advance the watermark. */
  def ownershipLost: Boolean = lost.get()

  /** Idempotent shutdown; no renewal runs after it returns (periodic tasks
    * do not survive shutdown; a beat in flight is awaited briefly). */
  def stop(): Unit = {
    executor.shutdown()
    try {
      if (!executor.awaitTermination(5, TimeUnit.SECONDS)) executor.shutdownNow()
    } catch {
      case _: InterruptedException =>
        executor.shutdownNow()
        Thread.currentThread().interrupt()
    }
  }

  /** Test visibility: the scheduler thread is fully terminated. */
  private[ingest] def terminated: Boolean = executor.isTerminated
}
