package com.hcsc.generic.ingest.jdbc.health

import com.hcsc.generic.ingest.jdbc.JdbcSourceConfig

import java.sql.DriverManager
import java.util.Properties

/** Lightweight connectivity probe run before the Spark read: fails fast with
  * JDBC_001/JDBC_002 instead of a mid-job executor failure. Also used by
  * --validate-only for jdbc feeds. */
object JdbcHealthCheck {

  def check(cfg: JdbcSourceConfig): Either[String, String] = {
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
      case e: ClassNotFoundException =>
        Left(s"JDBC_001 Driver class '${cfg.driver}' not found on classpath: ${e.getMessage}")
      case e: java.sql.SQLInvalidAuthorizationSpecException =>
        Left(s"JDBC_002 Authentication failed for ${sanitized(cfg.url)}: ${e.getMessage}")
      case e: Exception =>
        Left(s"JDBC_001 Connection to ${sanitized(cfg.url)} failed: ${e.getMessage}")
    }
  }

  /** URL with any embedded credentials masked for logs. */
  def sanitized(url: String): String =
    url.replaceAll("(?i)(password|pwd)=[^;&]*", "$1=***")
}
