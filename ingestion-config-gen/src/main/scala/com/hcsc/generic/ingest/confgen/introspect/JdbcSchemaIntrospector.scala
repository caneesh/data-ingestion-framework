package com.hcsc.generic.ingest.confgen.introspect

import com.hcsc.generic.ingest.confgen.io.ConsoleIO
import com.hcsc.generic.ingest.jdbc.JdbcSourceConfig
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}

import java.sql.{DriverManager, Types}
import java.util.Properties
import scala.collection.JavaConverters._

/**
  * Generates schema-contract columns by introspecting the source table via
  * JDBC `DatabaseMetaData.getColumns` (the driver's portable view of
  * INFORMATION_SCHEMA.COLUMNS). One contract column is emitted per source
  * column with its Spark type, nullability and 0-based position — the
  * starting point for a wide (e.g. 350-column) mapping document, written to
  * `<entity>-schema.conf` for review and refinement.
  *
  * Uses the feed's own connection settings (URL, driver, auth, TLS
  * properties), so what the generator can see is exactly what the pipeline
  * will see.
  */
object JdbcSchemaIntrospector {

  final case class IntrospectedColumn(
    name: String,
    sparkType: String,
    nullable: Boolean,
    position: Int
  )

  /** Introspects the feed's source.table and returns contract column
    * blocks, or a human-readable problem. */
  def introspect(feed: Config, console: ConsoleIO): Either[String, Seq[Config]] = {
    val cfg =
      try JdbcSourceConfig.parse(feed.getConfig("source"))
      catch { case e: Exception => return Left(s"source config invalid: ${e.getMessage}") }

    val table = cfg.table.getOrElse(
      return Left("schema introspection requires source.table (SQL modes: supply columns explicitly)"))

    val (schemaName, tableName) = table.split("\\.") match {
      case Array(t) => (None, t)
      case Array(s, t) => (Some(s), t)
      case Array(_, s, t) => (Some(s), t) // database.schema.table
      case _ => return Left(s"cannot parse qualified table name '$table'")
    }

    val columns =
      try connectAndRead(cfg, schemaName, tableName)
      catch { case e: Exception => return Left(s"introspection failed for $table: ${e.getMessage}") }

    if (columns.isEmpty)
      return Left(s"no columns found for table '$table' — check the name, schema qualifier and grants")

    console.success(s"Introspected ${columns.size} column(s) from $table: " +
      columns.take(5).map(c => s"${c.name} (${c.sparkType}${if (c.nullable) "" else ", not null"})")
        .mkString(", ") + (if (columns.size > 5) ", ..." else ""))

    Right(columns.map(toContractColumn))
  }

  private def connectAndRead(
    cfg: JdbcSourceConfig,
    schemaName: Option[String],
    tableName: String
  ): Seq[IntrospectedColumn] = {
    Class.forName(cfg.driver)
    val props = new Properties()
    cfg.user.foreach(props.setProperty("user", _))
    cfg.password.foreach(props.setProperty("password", _))
    cfg.connectionProperties.foreach { case (k, v) => props.setProperty(k, v) }

    val connection = DriverManager.getConnection(cfg.url, props)
    try {
      // Identifier case is driver-defined; try as-given, then upper, then lower.
      val candidates = Seq(
        (schemaName, tableName),
        (schemaName.map(_.toUpperCase), tableName.toUpperCase),
        (schemaName.map(_.toLowerCase), tableName.toLowerCase)
      ).distinct
      candidates.iterator.map { case (s, t) => read(connection, s, t) }
        .find(_.nonEmpty).getOrElse(Seq.empty)
    } finally connection.close()
  }

  private def read(
    connection: java.sql.Connection,
    schemaName: Option[String],
    tableName: String
  ): Seq[IntrospectedColumn] = {
    val metaData = connection.getMetaData
    // getColumns takes LIKE patterns, not literal names: an unescaped '_'
    // (ubiquitous in table names) is a single-character wildcard and would
    // silently merge columns from every similarly-named table.
    val esc = Option(metaData.getSearchStringEscape).filter(_.nonEmpty)
    def literal(s: String): String = esc.fold(s) { e =>
      s.replace(e, e + e).replace("_", e + "_").replace("%", e + "%")
    }
    val rs = metaData.getColumns(null, schemaName.map(literal).orNull, literal(tableName), null)
    val out = scala.collection.mutable.ArrayBuffer.empty[IntrospectedColumn]
    try {
      while (rs.next()) {
        // Belt and braces: even with escaping, only accept rows from the
        // exact table requested.
        if (Option(rs.getString("TABLE_NAME")).exists(_.equalsIgnoreCase(tableName)))
          out += IntrospectedColumn(
            name = rs.getString("COLUMN_NAME"),
            sparkType = sparkType(
              rs.getInt("DATA_TYPE"),
              rs.getString("TYPE_NAME"),
              rs.getInt("COLUMN_SIZE"),
              rs.getInt("DECIMAL_DIGITS")),
            nullable = rs.getInt("NULLABLE") != java.sql.DatabaseMetaData.columnNoNulls,
            position = rs.getInt("ORDINAL_POSITION") - 1) // contract positions are 0-based
      }
    } finally rs.close()
    out.sortBy(_.position).toSeq
  }

  /** java.sql.Types -> Spark SQL DDL type, matching what the Spark JDBC
    * reader will produce so on_type_change validation stays green. */
  private[introspect] def sparkType(sqlType: Int, typeName: String, size: Int, scale: Int): String =
    sqlType match {
      case Types.CHAR | Types.VARCHAR | Types.LONGVARCHAR |
           Types.NCHAR | Types.NVARCHAR | Types.LONGNVARCHAR | Types.CLOB | Types.NCLOB => "string"
      case Types.TINYINT => "tinyint"
      case Types.SMALLINT => "smallint"
      case Types.INTEGER => "int"
      case Types.BIGINT => "bigint"
      case Types.NUMERIC | Types.DECIMAL =>
        if (size > 0) s"decimal($size,${math.max(scale, 0)})" else "decimal(38,18)"
      case Types.REAL => "float"
      case Types.FLOAT | Types.DOUBLE => "double"
      case Types.BIT | Types.BOOLEAN => "boolean"
      case Types.DATE => "date"
      case Types.TIMESTAMP => "timestamp"
      case Types.TIMESTAMP_WITH_TIMEZONE => "timestamp"
      case Types.TIME | Types.TIME_WITH_TIMEZONE => "string"
      case Types.BINARY | Types.VARBINARY | Types.LONGVARBINARY | Types.BLOB => "binary"
      case -155 => "string" // SQL Server datetimeoffset: Spark's dialect reads it as string
      case _ =>
        // Unknown engine-specific type: string is always readable; the
        // operator refines it in <entity>-schema.conf.
        "string"
    }

  private def toContractColumn(c: IntrospectedColumn): Config =
    ConfigFactory.empty()
      .withValue("name", ConfigValueFactory.fromAnyRef(c.name))
      .withValue("type", ConfigValueFactory.fromAnyRef(c.sparkType))
      .withValue("nullable", ConfigValueFactory.fromAnyRef(c.nullable))
      .withValue("required", ConfigValueFactory.fromAnyRef(true))
      .withValue("position", ConfigValueFactory.fromAnyRef(c.position))
}
