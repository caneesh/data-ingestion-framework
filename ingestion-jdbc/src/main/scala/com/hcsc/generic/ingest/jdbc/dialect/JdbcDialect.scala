package com.hcsc.generic.ingest.jdbc.dialect

/**
  * Database-specific behavior for the JDBC subsystem. One implementation per
  * engine; everything else in the read path is dialect-agnostic. New engines
  * plug in via DialectRegistry.register without touching existing code.
  */
trait JdbcDialect {
  /** Registry key, matched against source.dialect (case-insensitive). */
  def name: String

  /** Default driver class when source.driver is not configured. */
  def defaultDriver: String

  /** Expected JDBC URL prefix, validated at config parse (JDBC_003). */
  def urlPrefix: String

  /** Connection properties merged into every connection (encryption etc.).
    * Explicit source.options override these. */
  def defaultConnectionProperties: Map[String, String] = Map.empty

  /** Identifier quoting for predicates built by the framework. */
  def quoteIdentifier(identifier: String): String =
    "\"" + identifier.replace("\"", "\"\"") + "\""

  /** Health-check probe statement. */
  def validationQuery: String = "SELECT 1"

  /** SELECT of one row ordered as given (SQL-standard FETCH FIRST default;
    * engine-specific overrides where needed). Used for composite upper
    * watermark capture. */
  def selectTopOne(projection: String, from: String, orderBy: String): String =
    s"SELECT $projection FROM $from ORDER BY $orderBy FETCH FIRST 1 ROWS ONLY"
}

object SqlServerDialect extends JdbcDialect {
  val name = "sqlserver"
  val defaultDriver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
  val urlPrefix = "jdbc:sqlserver://"
  // Azure SQL requires encrypted connections; never trust arbitrary certs.
  override val defaultConnectionProperties = Map(
    "encrypt" -> "true",
    "trustServerCertificate" -> "false",
    "loginTimeout" -> "30"
  )
  override def quoteIdentifier(identifier: String): String =
    "[" + identifier.replace("]", "]]") + "]"
  override def selectTopOne(projection: String, from: String, orderBy: String): String =
    s"SELECT TOP 1 $projection FROM $from ORDER BY $orderBy"
}

object PostgresDialect extends JdbcDialect {
  val name = "postgresql"
  val defaultDriver = "org.postgresql.Driver"
  val urlPrefix = "jdbc:postgresql://"
}

object OracleDialect extends JdbcDialect {
  val name = "oracle"
  val defaultDriver = "oracle.jdbc.OracleDriver"
  val urlPrefix = "jdbc:oracle:"
  override def validationQuery: String = "SELECT 1 FROM DUAL"
}

object Db2Dialect extends JdbcDialect {
  val name = "db2"
  val defaultDriver = "com.ibm.db2.jcc.DB2Driver"
  val urlPrefix = "jdbc:db2://"
  override def validationQuery: String = "SELECT 1 FROM SYSIBM.SYSDUMMY1"
}

object MySqlDialect extends JdbcDialect {
  val name = "mysql"
  val defaultDriver = "com.mysql.cj.jdbc.Driver"
  val urlPrefix = "jdbc:mysql://"
  override def quoteIdentifier(identifier: String): String =
    "`" + identifier.replace("`", "``") + "`"
  override def selectTopOne(projection: String, from: String, orderBy: String): String =
    s"SELECT $projection FROM $from ORDER BY $orderBy LIMIT 1"
}

/** Fallback for engines without a dedicated dialect (H2 in tests, etc.).
  * Requires an explicit source.driver. */
object GenericDialect extends JdbcDialect {
  val name = "generic"
  val defaultDriver = ""
  val urlPrefix = "jdbc:"
}

object DialectRegistry {
  private val dialects = new java.util.concurrent.ConcurrentHashMap[String, JdbcDialect]()

  def register(dialect: JdbcDialect): Unit = dialects.put(dialect.name.toLowerCase, dialect)

  Seq(SqlServerDialect, PostgresDialect, OracleDialect, Db2Dialect, MySqlDialect, GenericDialect)
    .foreach(register)

  def resolve(name: String): JdbcDialect =
    Option(dialects.get(name.toLowerCase)).getOrElse(
      throw new IllegalArgumentException(
        s"JDBC_003 Unknown JDBC dialect '$name'. Available: ${availableNames.mkString(", ")}"))

  def availableNames: Seq[String] = {
    import scala.collection.JavaConverters._
    dialects.keySet.asScala.toSeq.sorted
  }
}
