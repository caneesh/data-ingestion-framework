package com.hcsc.generic.ingest.lock

import com.hcsc.generic.ingest.config.ConfigUtils
import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import java.sql.Timestamp

final class PipelineLockException(message: String) extends RuntimeException(message)

/**
  * Best-effort entity-level run lease preventing two pipeline runs of the
  * same entity from extracting overlapping windows and interleaving curated
  * INSERT OVERWRITEs (lost updates).
  *
  * Same non-transactional caveat as the watermark store: Hive appends are
  * not atomic, so the claim protocol is claim -> settle -> re-read. Two
  * claims landing in the same settle window are resolved deterministically
  * (earliest claim wins; ties by run id), shrinking the unprotected window
  * from an entire pipeline run to the settle interval.
  *
  * A crashed run's lease expires after lease_minutes (default 4h); an
  * operator can release earlier via forceRelease (or by appending a RELEASE
  * row manually).
  *
  * concurrency {
  *   lock = REQUIRED | OFF        # default REQUIRED
  *   database = "ingest_audit"    # defaults to audit.database
  *   table = "ingest_run_locks"
  *   lease_minutes = 240
  *   settle_ms = 2000
  * }
  */
final class LockService(
  spark: SparkSession,
  database: String,
  table: String,
  leaseMillis: Long,
  settleMillis: Long,
  logger: Logger,
  /** concurrency.wait_ms: total budget to WAIT for a held entity instead of
    * failing fast (the default, 0). The claim is retried until the budget
    * runs out; the last contention error propagates unchanged. */
  waitMillis: Long = 0L
) {
  require(database.matches("[a-zA-Z_][a-zA-Z0-9_]*"), s"lock database '$database' is not a safe SQL identifier")
  require(table.matches("[a-zA-Z_][a-zA-Z0-9_]*"), s"lock table '$table' is not a safe SQL identifier")

  private val fullTable = s"$database.$table"

  private final case class Claim(holder: String, action: String, leaseUntil: Option[Timestamp], eventTs: Timestamp)

  def acquire(entity: String, runId: String): Unit = {
    val deadline = System.currentTimeMillis() + waitMillis
    var attempt = 0
    while (true) {
      attempt += 1
      try { acquireOnce(entity, runId); return }
      catch {
        case e: PipelineLockException =>
          val remaining = deadline - System.currentTimeMillis()
          if (waitMillis <= 0 || remaining <= 0) throw e
          val pause = math.min(math.max(settleMillis, 1000L), remaining)
          logger.info(s"[Lock] entity=$entity held by another run; retrying in ${pause}ms " +
            s"(attempt $attempt, ${remaining}ms left of concurrency.wait_ms)")
          Thread.sleep(pause)
      }
    }
  }

  private def acquireOnce(entity: String, runId: String): Unit = {
    validateKey("entity", entity)
    validateKey("run id", runId)
    ensureTable()

    activeClaims(entity).filterNot(_.holder == runId).headOption.foreach { held =>
      throw new PipelineLockException(contention(entity, held))
    }

    appendRow(entity, runId, "CLAIM", Some(leaseMillis))
    if (settleMillis > 0) Thread.sleep(settleMillis)

    // Settle re-read: if a competing claim landed before (or tied with)
    // ours, back off deterministically and let it win.
    val mine = latestOf(entity, runId).getOrElse(
      throw new IllegalStateException(s"PIPE_001 lock claim for $entity/$runId not visible after append"))
    val winners = activeClaims(entity).filterNot(_.holder == runId).filter { c =>
      c.eventTs.before(mine.eventTs) || (c.eventTs.equals(mine.eventTs) && c.holder < runId)
    }
    if (winners.nonEmpty) {
      release(entity, runId)
      throw new PipelineLockException(contention(entity, winners.head))
    }
    logger.info(s"[Lock] entity=$entity lease acquired by run $runId " +
      s"(expires ${new Timestamp(System.currentTimeMillis() + leaseMillis)})")
  }

  /** Extends this holder's lease mid-run. A run outliving lease_minutes
    * silently loses its concurrency protection otherwise — the pipeline
    * renews at stage boundaries so the lease only needs to outlast a single
    * stage, not the whole run. Fails loudly when the lease has expired AND
    * another run has claimed the entity in the meantime: continuing would
    * mean two writers. */
  def renew(entity: String, runId: String): Unit = {
    validateKey("entity", entity)
    validateKey("run id", runId)
    ensureTable()
    activeClaims(entity).filterNot(_.holder == runId).headOption.foreach { thief =>
      throw new PipelineLockException(
        s"PIPE_001 lease renewal failed for run '$runId': ${contention(entity, thief)} " +
          "This run's lease expired mid-flight and the entity was claimed by another run; " +
          "aborting rather than writing concurrently.")
    }
    appendRow(entity, runId, "CLAIM", Some(leaseMillis))
    logger.info(s"[Lock] entity=$entity lease renewed by run $runId " +
      s"(expires ${new Timestamp(System.currentTimeMillis() + leaseMillis)})")
  }

  def release(entity: String, runId: String): Unit = {
    validateKey("entity", entity)
    validateKey("run id", runId)
    ensureTable()
    appendRow(entity, runId, "RELEASE", None)
    logger.info(s"[Lock] entity=$entity lease released by run $runId")
  }

  /** Operator escape hatch: releases every active holder for the entity. */
  def forceRelease(entity: String): Unit =
    activeClaims(entity).foreach { c =>
      logger.warn(s"[Lock] FORCE releasing entity=$entity holder=${c.holder}")
      release(entity, c.holder)
    }

  private def contention(entity: String, held: Claim): String =
    s"PIPE_001 entity '$entity' is locked by run '${held.holder}' " +
      s"(lease until ${held.leaseUntil.map(_.toString).getOrElse("?")}). Two concurrent runs of one " +
      "entity would extract overlapping windows and overwrite each other's curated writes. Wait for " +
      "the holder to finish, let the lease expire, or force-release the lock if the holder crashed."

  /** Latest row per holder decides its state; a holder is active when that
    * row is a CLAIM whose lease has not expired. */
  private def activeClaims(entity: String): Seq[Claim] = {
    val now = System.currentTimeMillis()
    claimsFor(entity)
      .groupBy(_.holder)
      .flatMap { case (_, rows) => rows.sortBy(r => (r.eventTs.getTime, r.action)).lastOption }
      .filter(c => c.action == "CLAIM" && c.leaseUntil.exists(_.getTime > now))
      .toSeq
  }

  private def latestOf(entity: String, runId: String): Option[Claim] =
    claimsFor(entity).filter(_.holder == runId).sortBy(_.eventTs.getTime).lastOption

  private def claimsFor(entity: String): Seq[Claim] =
    spark.table(fullTable)
      .filter(col("entity") === entity)
      .collect()
      .map(r => Claim(
        holder = r.getAs[String]("holder_run_id"),
        action = r.getAs[String]("action"),
        leaseUntil = Option(r.getAs[Timestamp]("lease_until")),
        eventTs = r.getAs[Timestamp]("event_ts")))
      .toSeq

  private def appendRow(entity: String, runId: String, action: String, leaseForMillis: Option[Long]): Unit = {
    // lease_until is computed IN SQL from the same clock/session semantics
    // as event_ts. A JVM-formatted timestamp literal would be rendered in
    // the JVM default zone but parsed in the Spark session zone (UTC by
    // default) — on a non-UTC cluster that skew can exceed the lease and
    // create locks that are born expired.
    val lease = leaseForMillis
      .map(ms => s"timestampadd(SECOND, ${math.max(ms / 1000L, 1L)}, current_timestamp())")
      .getOrElse("CAST(NULL AS TIMESTAMP)")
    spark.sql(
      s"INSERT INTO $fullTable VALUES ('$entity', '$runId', '$action', $lease, current_timestamp())")
  }

  private def ensureTable(): Unit = {
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $database")
    spark.sql(
      s"""CREATE TABLE IF NOT EXISTS $fullTable (
         |  entity STRING, holder_run_id STRING, action STRING,
         |  lease_until TIMESTAMP, event_ts TIMESTAMP
         |) USING ORC""".stripMargin)
  }

  /** Values are interpolated into INSERT statements — restrict to the same
    * safe alphabet the CLI enforces for run ids. */
  private def validateKey(what: String, value: String): Unit =
    require(value.matches("[A-Za-z0-9._-]{1,200}"),
      s"PIPE_001 $what '$value' contains characters outside [A-Za-z0-9._-]")
}

object LockService {

  /**
    * Lock resolution: OFF disables loudly; otherwise the lock table lives in
    * concurrency.database, falling back to audit.database (so audited feeds
    * are protected without extra config). A feed with neither database is
    * left unlocked with a warning — an explicit concurrency block without a
    * resolvable database is a configuration error.
    */
  def fromConfig(spark: SparkSession, feedConf: Config, logger: Logger): Option[LockService] = {
    val conc = ConfigUtils.optConfig(feedConf, "concurrency")
    val mode = conc.flatMap(c => ConfigUtils.optString(c, "lock")).getOrElse("REQUIRED").toUpperCase

    mode match {
      case "OFF" =>
        logger.warn("[Lock] concurrency.lock=OFF: concurrent runs of this entity are NOT prevented")
        None
      case "REQUIRED" =>
        val database = conc.flatMap(c => ConfigUtils.optString(c, "database"))
          .orElse(ConfigUtils.optConfig(feedConf, "audit").flatMap(a => ConfigUtils.optString(a, "database")))
        database match {
          case Some(db) =>
            Some(new LockService(
              spark = spark,
              database = db,
              table = conc.flatMap(c => ConfigUtils.optString(c, "table")).getOrElse("ingest_run_locks"),
              leaseMillis = conc.flatMap(c => ConfigUtils.optLong(c, "lease_minutes")).getOrElse(240L) * 60000L,
              settleMillis = conc.flatMap(c => ConfigUtils.optLong(c, "settle_ms")).getOrElse(2000L),
              logger = logger,
              waitMillis = conc.flatMap(c => ConfigUtils.optLong(c, "wait_ms")).getOrElse(0L)))
          case None if conc.isDefined =>
            throw new IllegalArgumentException(
              "PIPE_001 concurrency lock requires concurrency.database (or an audit.database to reuse)")
          case None =>
            logger.warn("[Lock] No concurrency or audit database configured: entity-level run " +
              "locking is DISABLED — concurrent runs of this entity can corrupt curated data")
            None
        }
      case other =>
        throw new IllegalArgumentException(s"PIPE_001 concurrency.lock '$other' must be REQUIRED or OFF")
    }
  }
}
