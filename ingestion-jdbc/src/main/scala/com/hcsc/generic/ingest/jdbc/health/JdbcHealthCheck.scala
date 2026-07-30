package com.hcsc.generic.ingest.jdbc.health

import com.hcsc.generic.ingest.jdbc.JdbcSourceConfig

import java.sql.DriverManager
import java.util.Properties

/** Lightweight connectivity probe run before the Spark read: fails fast with
  * JDBC_001/JDBC_002 instead of a mid-job executor failure. Also used by
  * --validate-only for jdbc feeds. */
object JdbcHealthCheck {

  def check(cfg: JdbcSourceConfig): Either[String, String] = {
    com.hcsc.generic.ingest.jdbc.JdbcMetrics.increment("jdbc_connection_attempt_total")
    try {
      Class.forName(cfg.driver)
      val props = new Properties()
      cfg.user.foreach(props.setProperty("user", _))
      cfg.password.foreach(props.setProperty("password", _))
      cfg.connectionProperties.foreach { case (k, v) => props.setProperty(k, v) }

      val connection = DriverManager.getConnection(cfg.url, props)
      try {
        val statement = connection.createStatement()
        try {
          statement.setQueryTimeout(30)
          statement.execute(cfg.dialect.validationQuery)
          Right(s"Connection healthy (${cfg.dialect.name}, ${sanitized(cfg.url)})")
        } finally statement.close()
      } finally connection.close()
    } catch {
      // Exception text is sanitized too: driver messages can echo the FULL
      // connection URL (e.g. the JDK's "No suitable driver found for <url>"),
      // credentials included.
      case e: ClassNotFoundException =>
        com.hcsc.generic.ingest.jdbc.JdbcMetrics.increment("jdbc_connection_failure_total")
        Left(s"JDBC_001 Driver class '${cfg.driver}' not found on classpath: ${sanitizedMessage(e)}")
      case e: java.sql.SQLInvalidAuthorizationSpecException =>
        com.hcsc.generic.ingest.jdbc.JdbcMetrics.increment("jdbc_connection_failure_total")
        Left(s"JDBC_002 Authentication failed for ${sanitized(cfg.url)}: ${sanitizedMessage(e)}")
      case e: Exception =>
        com.hcsc.generic.ingest.jdbc.JdbcMetrics.increment("jdbc_connection_failure_total")
        Left(s"JDBC_001 Connection to ${sanitized(cfg.url)} failed: ${sanitizedMessage(e)}")
    }
  }

  /**
    * Optional distributed connectivity probe: the driver health check only
    * proves driver -> database; executors open their own connections and may
    * sit behind different firewall rules. Runs one lightweight validation
    * per probe partition (rate-limited by design; keep probe_partitions
    * small).
    */
  def executorProbe(spark: org.apache.spark.sql.SparkSession, cfg: JdbcSourceConfig, partitions: Int): Unit = {
    val url = cfg.url
    val driver = cfg.driver
    val user = cfg.user
    val password = cfg.password
    val props = cfg.connectionProperties
    val validation = cfg.dialect.validationQuery

    spark.sparkContext.parallelize(1 to partitions, partitions).foreachPartition { _ =>
      Class.forName(driver)
      val p = new java.util.Properties()
      user.foreach(p.setProperty("user", _))
      password.foreach(p.setProperty("password", _))
      props.foreach { case (k, v) => p.setProperty(k, v) }
      val conn = java.sql.DriverManager.getConnection(url, p)
      try {
        val st = conn.createStatement()
        try { st.setQueryTimeout(30); st.execute(validation) }
        finally st.close()
      } finally conn.close()
    }
  }

  /** URL (or any driver-produced text) with embedded credentials masked for
    * logs. Handles the SQL Server brace-escaping convention
    * (password={value;with;specials}) — a naive [^;&]* stops at the first
    * embedded ';' and leaks the credential's tail. */
  def sanitized(url: String): String =
    url.replaceAll("(?i)(password|pwd)=(\\{[^}]*\\}?|[^;&]*)", "$1=***")

  /** Exception message with the same masking; null-safe. */
  def sanitizedMessage(e: Throwable): String =
    sanitized(Option(e.getMessage).getOrElse(e.getClass.getSimpleName))
}
