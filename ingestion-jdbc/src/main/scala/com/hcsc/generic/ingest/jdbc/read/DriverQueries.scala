package com.hcsc.generic.ingest.jdbc.read

import com.hcsc.generic.ingest.jdbc.JdbcSourceConfig
import org.apache.log4j.Logger

import java.sql.DriverManager
import java.util.Properties

/** Small driver-side JDBC queries (upper watermark capture, bound
  * discovery). These run on the driver under the classified retry policy —
  * the layer of the retry model that actually protects them. */
object DriverQueries {

  /** Executes sql and returns the first row's values as strings (None when
    * no row / all-null). java.sql temporal and numeric types are converted
    * through their canonical toString so watermark parsing round-trips. */
  def firstRow(cfg: JdbcSourceConfig, sql: String, logger: Logger): Option[Seq[Option[String]]] =
    RetryPolicy.withRetries("driver query", cfg.retry, logger) {
      Class.forName(cfg.driver)
      val props = new Properties()
      cfg.user.foreach(props.setProperty("user", _))
      cfg.password.foreach(props.setProperty("password", _))
      cfg.connectionProperties.foreach { case (k, v) => props.setProperty(k, v) }

      val connection = DriverManager.getConnection(cfg.url, props)
      try {
        val statement = connection.createStatement()
        try {
          statement.setQueryTimeout(300)
          val rs = statement.executeQuery(sql)
          try {
            if (!rs.next()) None
            else {
              val columns = rs.getMetaData.getColumnCount
              val values = (1 to columns).map(i => Option(rs.getObject(i)).map(stringify))
              if (values.forall(_.isEmpty)) None else Some(values)
            }
          } finally rs.close()
        } finally statement.close()
      } finally connection.close()
    }

  private def stringify(value: Object): String = value match {
    case t: java.sql.Timestamp => t.toString
    case d: java.sql.Date => d.toString
    case odt: java.time.OffsetDateTime =>
      // Full DATETIMEOFFSET(7) precision — millisecond truncation here
      // excluded the captured-max row from its own bounded window.
      com.hcsc.generic.ingest.jdbc.watermark.Watermarks.formatOffset(odt)
    case bytes: Array[Byte] => bytes.map("%02X".format(_)).mkString
    case other => String.valueOf(other)
  }
}
