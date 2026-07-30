package com.hcsc.generic.ingest.jdbc.extraction.store

import com.hcsc.generic.ingest.jdbc.extraction._
import org.apache.log4j.Logger

import java.sql.{Connection, DriverManager}
import java.util.Properties
import scala.util.control.NonFatal

/**
  * Relational checkpoint persistence with TRUE optimistic locking: one row
  * per entity, advanced by `UPDATE ... WHERE version = expected` — the
  * database enforces the compare-and-set, unlike the best-effort Hive store.
  * The table is created on first use:
  *
  *   CREATE TABLE <table> (
  *     entity VARCHAR(256) PRIMARY KEY, strategy_kind VARCHAR(64),
  *     checkpoint_value VARCHAR(1024), version BIGINT,
  *     run_id VARCHAR(128), committed_ts TIMESTAMP)
  */
final class JdbcCheckpointStore(
  url: String,
  connectionProperties: Map[String, String],
  table: String = "ingest_checkpoints",
  driver: Option[String] = None
) extends CheckpointStore {

  require(table.matches("[A-Za-z_][A-Za-z0-9_]*"),
    s"EXT_001 checkpoint table '$table' is not a safe identifier")

  // See JdbcBoundaryProbe: --jars-delivered drivers need explicit loading.
  driver.foreach(Class.forName)

  @volatile private var tableEnsured = false

  override def latest(entity: String): Option[Checkpoint] = withConnection { connection =>
    ensureTable(connection)
    val statement = connection.prepareStatement(
      s"SELECT entity, strategy_kind, checkpoint_value, version, run_id, committed_ts FROM $table WHERE entity = ?")
    try {
      statement.setString(1, entity)
      val rs = statement.executeQuery()
      try {
        if (!rs.next()) None
        else Some(Checkpoint(
          entity = rs.getString(1), strategyKind = rs.getString(2),
          value = BoundaryValue.deserialize(rs.getString(3)),
          version = rs.getLong(4), runId = rs.getString(5), committedAt = rs.getTimestamp(6)))
      } finally rs.close()
    } finally statement.close()
  }

  override def commit(checkpoint: Checkpoint, expectedVersion: Long): Unit = withConnection { connection =>
    ensureTable(connection)
    if (expectedVersion == 0L) insertFirst(connection, checkpoint)
    else casUpdate(connection, checkpoint, expectedVersion)
  }

  private def insertFirst(connection: Connection, checkpoint: Checkpoint): Unit = {
    val statement = connection.prepareStatement(
      s"INSERT INTO $table (entity, strategy_kind, checkpoint_value, version, run_id, committed_ts) VALUES (?,?,?,?,?,?)")
    try {
      statement.setString(1, checkpoint.entity)
      statement.setString(2, checkpoint.strategyKind)
      statement.setString(3, checkpoint.value.serialized)
      statement.setLong(4, checkpoint.version)
      statement.setString(5, checkpoint.runId)
      statement.setTimestamp(6, checkpoint.committedAt)
      statement.executeUpdate()
    } catch {
      // Duplicate primary key = someone inserted concurrently: a CAS loss.
      case e: java.sql.SQLIntegrityConstraintViolationException =>
        throw conflict(checkpoint.entity, 0L, e)
      case e: java.sql.SQLException if isDuplicateKey(e) =>
        throw conflict(checkpoint.entity, 0L, e)
    } finally statement.close()
  }

  private def casUpdate(connection: Connection, checkpoint: Checkpoint, expectedVersion: Long): Unit = {
    val statement = connection.prepareStatement(
      s"UPDATE $table SET strategy_kind=?, checkpoint_value=?, version=?, run_id=?, committed_ts=? " +
        "WHERE entity=? AND version=?")
    try {
      statement.setString(1, checkpoint.strategyKind)
      statement.setString(2, checkpoint.value.serialized)
      statement.setLong(3, checkpoint.version)
      statement.setString(4, checkpoint.runId)
      statement.setTimestamp(5, checkpoint.committedAt)
      statement.setString(6, checkpoint.entity)
      statement.setLong(7, expectedVersion)
      if (statement.executeUpdate() == 0)
        throw conflict(checkpoint.entity, expectedVersion, cause = null)
    } finally statement.close()
  }

  private def conflict(entity: String, expected: Long, cause: Throwable): CheckpointConflictException = {
    val e = new CheckpointConflictException(entity, expected, actualVersionBestEffort(entity))
    if (cause != null) e.initCause(cause)
    e
  }

  private def actualVersionBestEffort(entity: String): Long =
    try latest(entity).map(_.version).getOrElse(0L) catch { case NonFatal(_) => -1L }

  private def isDuplicateKey(e: java.sql.SQLException): Boolean =
    Option(e.getSQLState).exists(_.startsWith("23")) // integrity constraint violation class

  /** Portable ensure: `CREATE TABLE IF NOT EXISTS` is a syntax error on SQL
    * Server, Oracle (pre-23c) and DB2, so existence is checked through
    * DatabaseMetaData (case-folded per engine) and the plain CREATE's lost
    * race is resolved by re-checking existence. */
  private def ensureTable(connection: Connection): Unit = {
    if (tableEnsured) return
    if (!tableExists(connection)) {
      val statement = connection.createStatement()
      try statement.execute(
        s"CREATE TABLE $table (" +
          "entity VARCHAR(256) PRIMARY KEY, strategy_kind VARCHAR(64), " +
          "checkpoint_value VARCHAR(1024), version BIGINT, " +
          "run_id VARCHAR(128), committed_ts TIMESTAMP)")
      catch {
        case e: java.sql.SQLException =>
          if (!tableExists(connection)) throw e // real failure, not a lost create race
      } finally statement.close()
    }
    tableEnsured = true
  }

  private def tableExists(connection: Connection): Boolean = {
    val metadata = connection.getMetaData
    Seq(table, table.toUpperCase, table.toLowerCase).distinct.exists { candidate =>
      val rs = metadata.getTables(null, null, candidate, Array("TABLE"))
      try rs.next() finally rs.close()
    }
  }

  private def withConnection[A](body: Connection => A): A = {
    val props = new Properties()
    connectionProperties.foreach { case (k, v) => props.setProperty(k, v) }
    val connection = DriverManager.getConnection(url, props)
    try body(connection) finally connection.close()
  }
}

/**
  * Retry decorator (composition): transient store failures — network blips,
  * failovers — retry with linear backoff; a CheckpointConflictException is
  * TERMINAL and rethrown immediately, because retrying a lost compare-and-set
  * would overwrite a concurrent run's advancement.
  */
final class RetryingCheckpointStore(
  delegate: CheckpointStore,
  maxAttempts: Int = 3,
  backoffMs: Long = 1000L
) extends CheckpointStore {
  private val logger = Logger.getLogger(getClass.getName)

  override def latest(entity: String): Option[Checkpoint] =
    withRetries(s"checkpoint read '$entity'")(delegate.latest(entity))

  /** Commit is NOT idempotent, so an ambiguous failure (the statement may
    * have applied before the connection died) is resolved by reading back:
    * if the store already holds exactly our checkpoint (entity, version,
    * run id), the earlier attempt succeeded — return instead of retrying
    * into a false EXT_005 conflict against ourselves. */
  override def commit(checkpoint: Checkpoint, expectedVersion: Long): Unit = {
    def committedByThisRun: Boolean =
      try delegate.latest(checkpoint.entity)
        .exists(c => c.version == checkpoint.version && c.runId == checkpoint.runId)
      catch { case NonFatal(_) => false }

    var attempt = 1
    while (true) {
      try { delegate.commit(checkpoint, expectedVersion); return }
      catch {
        case e: CheckpointConflictException =>
          if (committedByThisRun) return else throw e
        case NonFatal(e) if attempt < maxAttempts =>
          if (committedByThisRun) return
          sleepBetween(s"checkpoint commit '${checkpoint.entity}'", attempt, e)
          attempt += 1
      }
    }
  }

  private def withRetries[A](operation: String)(body: => A): A = {
    var attempt = 1
    while (true) {
      try return body
      catch {
        case e: CheckpointConflictException => throw e // terminal: never retry a lost CAS
        case NonFatal(e) if attempt < maxAttempts =>
          sleepBetween(operation, attempt, e)
          attempt += 1
      }
    }
    throw new IllegalStateException("unreachable")
  }

  private def sleepBetween(operation: String, attempt: Int, cause: Throwable): Unit = {
    val delay = backoffMs * attempt
    logger.warn(s"[CheckpointStore] $operation attempt $attempt/$maxAttempts failed " +
      s"(${cause.getClass.getSimpleName}: ${cause.getMessage}); retrying in ${delay}ms")
    try Thread.sleep(delay)
    catch {
      case ie: InterruptedException =>
        Thread.currentThread().interrupt() // restore the flag, then abort the retry loop
        throw ie
    }
  }
}
