package com.hcsc.generic.ingest.lock

import com.hcsc.generic.ingest.config.ConfigUtils
import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

/**
  * Entity-level run lease: prevents two pipeline runs of the same entity
  * from extracting overlapping windows and interleaving curated writes.
  *
  * Two providers implement this contract with very different guarantees:
  *
  *  - HIVE ([[LockService]], the default): best-effort append-only claim
  *    protocol on a Hive table. NOT atomic — see its scaladoc. Suitable for
  *    dev/test and single-scheduler deployments.
  *  - JDBC ([[JdbcRunLock]]): true atomic compare-and-set via single UPDATE
  *    statements on a relational control table. Use this in production.
  *
  * All operations throw [[PipelineLockException]] (PIPE_001) on contention
  * or lost ownership; `release` is best-effort and never throws for a
  * non-holder.
  */
trait RunLock {

  /** Acquires the entity lease for `runId`, or throws PIPE_001 when another
    * run holds an unexpired lease. Re-acquiring one's own lease succeeds. */
  def acquire(entity: String, runId: String): Unit

  /** Extends this holder's lease. Throws PIPE_001 when ownership was lost
    * (lease expired and/or another run claimed the entity). */
  def renew(entity: String, runId: String): Unit

  /** Releases this holder's lease. Best-effort: releasing a lease no longer
    * held only warns. */
  def release(entity: String, runId: String): Unit

  /** Configured lease duration in milliseconds. The pipeline's
    * [[LockHeartbeat]] renews every `leaseMillis / 3` so a healthy run can
    * miss two renewals before the lease lapses. */
  def leaseMillis: Long
}

object RunLock {

  /**
    * Provider-selecting factory — the single entry point for pipeline code.
    *
    * concurrency {
    *   lock     = REQUIRED | OFF     # default REQUIRED
    *   provider = HIVE | JDBC        # default HIVE
    *   # HIVE provider settings: database, table, lease_minutes, settle_ms,
    *   #   wait_ms (see LockService)
    *   jdbc {                        # JDBC provider settings
    *     url           = "jdbc:..."  # required
    *     user          = ...         # optional
    *     password      = ...         # optional (inline value; never logged)
    *     driver        = ...         # optional JDBC driver class
    *     table         = "ingestion_lock"
    *     lease_minutes = 240
    *   }
    * }
    *
    * Returns None when locking is OFF or (HIVE provider) no lock database is
    * resolvable — the existing LockService semantics are preserved verbatim.
    */
  def fromConfig(spark: SparkSession, feedConf: Config, logger: Logger): Option[RunLock] = {
    val conc = ConfigUtils.optConfig(feedConf, "concurrency")
    val mode = conc.flatMap(c => ConfigUtils.optString(c, "lock")).getOrElse("REQUIRED").toUpperCase

    mode match {
      case "OFF" =>
        logger.warn("[Lock] concurrency.lock=OFF: concurrent runs of this entity are NOT prevented")
        None
      case "REQUIRED" =>
        conc.flatMap(c => ConfigUtils.optString(c, "provider")).getOrElse("HIVE").toUpperCase match {
          case "HIVE" =>
            LockService.fromConfig(spark, feedConf, logger)
          case "JDBC" =>
            val jdbcConf = conc.flatMap(c => ConfigUtils.optConfig(c, "jdbc")).getOrElse(
              throw new IllegalArgumentException(
                "PIPE_001 concurrency.provider=JDBC requires a concurrency.jdbc { url = ... } block"))
            Some(JdbcRunLock.fromConfig(jdbcConf, logger))
          case other =>
            throw new IllegalArgumentException(
              s"PIPE_001 concurrency.provider '$other' must be HIVE or JDBC")
        }
      case other =>
        throw new IllegalArgumentException(s"PIPE_001 concurrency.lock '$other' must be REQUIRED or OFF")
    }
  }
}
