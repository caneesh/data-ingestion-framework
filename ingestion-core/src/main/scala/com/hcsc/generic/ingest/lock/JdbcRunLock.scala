package com.hcsc.generic.ingest.lock

import com.hcsc.generic.ingest.config.ConfigUtils
import com.typesafe.config.Config
import org.apache.log4j.Logger

import java.sql.{Connection, DriverManager, PreparedStatement, SQLException, Timestamp}
import java.util.Properties

/**
  * Production run-lock provider: true atomic mutual exclusion via
  * single-statement compare-and-set UPDATEs on a relational control table
  * (one row per entity). Unlike the best-effort Hive [[LockService]], a
  * relational UPDATE is atomic — of N concurrent acquirers exactly one
  * observes updateCount == 1 and wins; there is no settle window and no
  * race to shrink.
  *
  * Control table (auto-created when missing):
  * {{{
  *   ingestion_lock(
  *     entity_name       VARCHAR(200) PRIMARY KEY,
  *     holder_run_id     VARCHAR(200) NULL,
  *     lease_acquired_ts TIMESTAMP,
  *     lease_expires_ts  TIMESTAMP,
  *     lease_version     BIGINT
  *   )
  * }}}
  * The auto-create DDL is ANSI and works on H2. On SQL Server, pre-create
  * the table with DATETIME2 columns instead — SQL Server's TIMESTAMP type
  * is a rowversion, not a datetime. The DML (parameterized UPDATEs and
  * CURRENT_TIMESTAMP) is portable across both.
  *
  * Clock model: lease expiry COMPARISONS happen on the database's single
  * CURRENT_TIMESTAMP clock, so all competing runs are judged by one clock.
  * The stored `lease_expires_ts` is, however, computed from THIS JVM's
  * clock (System.currentTimeMillis() + lease): JVM-vs-database clock skew
  * lengthens or shortens the effective lease by the skew. Keep JVM and
  * database clocks NTP-synced; size lease_minutes with margin (default 4h,
  * heartbeat renews every lease/3).
  *
  * Failure semantics: acquire/renew throw [[PipelineLockException]]
  * (PIPE_001) on contention or lost ownership; release by a non-holder only
  * warns. Connection/SQL failures also surface as PIPE_001 with the JDBC
  * URL and credentials scrubbed from the message — the URL may embed
  * userinfo and must never reach the logs.
  */
final class JdbcRunLock(
  url: String,
  user: Option[String],
  password: Option[String],
  driver: Option[String],
  table: String,
  val leaseMillis: Long,
  logger: Logger
) extends RunLock {
  require(table.matches("[a-zA-Z_][a-zA-Z0-9_]*"), s"lock table '$table' is not a safe SQL identifier")
  require(leaseMillis > 0, s"PIPE_001 lock lease must be positive (got ${leaseMillis}ms)")

  @volatile private var tableEnsured = false

  override def acquire(entity: String, runId: String): Unit = {
    validateKey("entity", entity)
    validateKey("run id", runId)
    withConnection { conn =>
      ensureTable(conn)
      ensureRow(conn, entity)
      val expires = new Timestamp(System.currentTimeMillis() + leaseMillis)
      val updated = withStatement(conn,
        s"""UPDATE $table
           |SET holder_run_id = ?, lease_acquired_ts = CURRENT_TIMESTAMP,
           |    lease_expires_ts = ?, lease_version = lease_version + 1
           |WHERE entity_name = ?
           |  AND (holder_run_id IS NULL
           |       OR lease_expires_ts < CURRENT_TIMESTAMP
           |       OR holder_run_id = ?)""".stripMargin) { ps =>
        ps.setString(1, runId)
        ps.setTimestamp(2, expires)
        ps.setString(3, entity)
        ps.setString(4, runId)
        ps.executeUpdate()
      }
      if (updated == 1)
        logger.info(s"[Lock] entity=$entity lease acquired by run $runId " +
          s"(provider=JDBC, expires $expires)")
      else {
        val held = holderOf(conn, entity)
        throw new PipelineLockException(
          s"PIPE_001 entity '$entity' is locked by run '${held.map(_._1).getOrElse("?")}' " +
            s"(lease until ${held.flatMap(_._2).map(_.toString).getOrElse("?")}). Two concurrent " +
            "runs of one entity would extract overlapping windows and overwrite each other's " +
            "curated writes. Wait for the holder to finish, let the lease expire, or clear " +
            s"holder_run_id in $table if the holder crashed.")
      }
    }
  }

  override def renew(entity: String, runId: String): Unit = {
    validateKey("entity", entity)
    validateKey("run id", runId)
    withConnection { conn =>
      ensureTable(conn)
      val expires = new Timestamp(System.currentTimeMillis() + leaseMillis)
      val updated = withStatement(conn,
        s"""UPDATE $table
           |SET lease_expires_ts = ?, lease_version = lease_version + 1
           |WHERE entity_name = ? AND holder_run_id = ?""".stripMargin) { ps =>
        ps.setTimestamp(1, expires)
        ps.setString(2, entity)
        ps.setString(3, runId)
        ps.executeUpdate()
      }
      if (updated == 1)
        logger.info(s"[Lock] entity=$entity lease renewed by run $runId (expires $expires)")
      else {
        val held = holderOf(conn, entity)
        throw new PipelineLockException(
          s"PIPE_001 lease renewal failed for run '$runId': entity '$entity' is no longer held " +
            s"by this run (current holder: ${held.map(_._1).getOrElse("none")}). The lease " +
            "expired and/or was claimed by another run; aborting rather than writing concurrently.")
      }
    }
  }

  override def release(entity: String, runId: String): Unit = {
    validateKey("entity", entity)
    validateKey("run id", runId)
    withConnection { conn =>
      ensureTable(conn)
      val updated = withStatement(conn,
        s"UPDATE $table SET holder_run_id = NULL WHERE entity_name = ? AND holder_run_id = ?") { ps =>
        ps.setString(1, entity)
        ps.setString(2, runId)
        ps.executeUpdate()
      }
      if (updated == 1)
        logger.info(s"[Lock] entity=$entity lease released by run $runId")
      else
        logger.warn(s"[Lock] release for entity=$entity run=$runId matched no held lease " +
          "(already released, expired-and-taken, or never acquired) — nothing to do")
    }
  }

  /** Row-per-entity model: the entity row is created once, unheld, then only
    * ever UPDATEd. The portable "insert if missing" is try-INSERT and treat a
    * primary-key violation as somebody else creating it first. */
  private def ensureRow(conn: Connection, entity: String): Unit = {
    if (rowExists(conn, entity)) return
    try {
      withStatement(conn,
        s"""INSERT INTO $table (entity_name, holder_run_id, lease_acquired_ts, lease_expires_ts, lease_version)
           |VALUES (?, NULL, NULL, NULL, 0)""".stripMargin) { ps =>
        ps.setString(1, entity)
        ps.executeUpdate()
      }
      ()
    } catch {
      case e: SQLException =>
        // A concurrent acquirer won the INSERT race — the row exists, which
        // is all this method promises. Anything else is a real failure.
        if (!rowExists(conn, entity)) throw e
    }
  }

  private def rowExists(conn: Connection, entity: String): Boolean =
    withStatement(conn, s"SELECT 1 FROM $table WHERE entity_name = ?") { ps =>
      ps.setString(1, entity)
      val rs = ps.executeQuery()
      try rs.next() finally rs.close()
    }

  private def holderOf(conn: Connection, entity: String): Option[(String, Option[Timestamp])] =
    withStatement(conn,
      s"SELECT holder_run_id, lease_expires_ts FROM $table WHERE entity_name = ?") { ps =>
      ps.setString(1, entity)
      val rs = ps.executeQuery()
      try {
        if (rs.next() && rs.getString(1) != null)
          Some((rs.getString(1), Option(rs.getTimestamp(2))))
        else None
      } finally rs.close()
    }

  private def ensureTable(conn: Connection): Unit = {
    if (tableEnsured) return
    if (!tableExists(conn)) {
      try {
        val st = conn.createStatement()
        try st.executeUpdate(
          s"""CREATE TABLE $table (
             |  entity_name VARCHAR(200) NOT NULL PRIMARY KEY,
             |  holder_run_id VARCHAR(200),
             |  lease_acquired_ts TIMESTAMP,
             |  lease_expires_ts TIMESTAMP,
             |  lease_version BIGINT NOT NULL
             |)""".stripMargin)
        finally st.close()
      } catch {
        case e: SQLException =>
          // Lost a concurrent CREATE race: fine as long as the table exists.
          if (!tableExists(conn)) throw e
      }
    }
    tableEnsured = true
  }

  private def tableExists(conn: Connection): Boolean = {
    val md = conn.getMetaData
    Seq(table, table.toUpperCase, table.toLowerCase).distinct.exists { name =>
      val rs = md.getTables(null, null, name, null)
      try rs.next() finally rs.close()
    }
  }

  private def withStatement[T](conn: Connection, sql: String)(body: PreparedStatement => T): T = {
    val ps = conn.prepareStatement(sql)
    try body(ps) finally ps.close()
  }

  private def withConnection[T](body: Connection => T): T =
    try {
      driver.foreach(Class.forName)
      val props = new Properties()
      user.foreach(props.setProperty("user", _))
      password.foreach(props.setProperty("password", _))
      val conn = DriverManager.getConnection(url, props)
      try body(conn) finally conn.close()
    } catch {
      case e: PipelineLockException => throw e
      case e: SQLException =>
        // Driver/connection messages can echo the JDBC URL (which may embed
        // userinfo). Scrub before the message can reach any log.
        throw new PipelineLockException(
          s"PIPE_001 JDBC lock provider failure (SQLState=${e.getSQLState}): ${sanitize(e.getMessage)}")
    }

  /** Never let the configured URL or password appear in diagnostics. */
  private def sanitize(message: String): String = {
    val base = Option(message).getOrElse("")
    val noSecret = password.filter(_.nonEmpty).fold(base)(p => base.replace(p, "***"))
    noSecret.replace(url, "<jdbc-url>")
  }

  /** Same safe alphabet the Hive provider (and the CLI) enforces — defense
    * in depth even though every value here is bound as a parameter. */
  private def validateKey(what: String, value: String): Unit =
    require(value.matches("[A-Za-z0-9._-]{1,200}"),
      s"PIPE_001 $what '$value' contains characters outside [A-Za-z0-9._-]")
}

object JdbcRunLock {

  /** Builds the provider from the `concurrency.jdbc` block. No connection is
    * opened here — the control table is created lazily on first use. */
  def fromConfig(jdbcConf: Config, logger: Logger): JdbcRunLock = {
    val url = ConfigUtils.optString(jdbcConf, "url").getOrElse(
      throw new IllegalArgumentException(
        "PIPE_001 concurrency.jdbc.url is required for the JDBC lock provider"))
    new JdbcRunLock(
      url = url,
      user = ConfigUtils.optString(jdbcConf, "user"),
      password = ConfigUtils.optString(jdbcConf, "password"),
      driver = ConfigUtils.optString(jdbcConf, "driver"),
      table = ConfigUtils.optString(jdbcConf, "table").getOrElse("ingestion_lock"),
      leaseMillis = ConfigUtils.optLong(jdbcConf, "lease_minutes").getOrElse(240L) * 60000L,
      logger = logger)
  }
}
