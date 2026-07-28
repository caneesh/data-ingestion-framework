package com.hcsc.generic.ingest.config

import com.typesafe.config.Config
import scala.collection.JavaConverters._

object ConfigUtils {
  def optString(c: Config, path: String): Option[String] =
    if (c.hasPath(path)) Some(c.getString(path)) else None

  def optBoolean(c: Config, path: String): Option[Boolean] =
    if (c.hasPath(path)) Some(c.getBoolean(path)) else None

  def optInt(c: Config, path: String): Option[Int] =
    if (c.hasPath(path)) Some(c.getInt(path)) else None

  def optLong(c: Config, path: String): Option[Long] =
    if (c.hasPath(path)) Some(c.getLong(path)) else None

  def optConfig(c: Config, path: String): Option[Config] =
    if (c.hasPath(path)) Some(c.getConfig(path)) else None

  def stringList(c: Config, path: String): Seq[String] =
    if (c.hasPath(path)) c.getStringList(path).asScala.toSeq else Seq.empty

  def stringMap(c: Config, path: String): Map[String, String] = {
    if (!c.hasPath(path)) Map.empty
    else {
      val nested = c.getConfig(path)
      nested.entrySet().asScala.map { e =>
        try e.getKey -> nested.getString(e.getKey)
        catch {
          case ex: com.typesafe.config.ConfigException.WrongType =>
            throw new IllegalArgumentException(
              s"Config block '$path' must contain only string values; key '${e.getKey}' is not a string", ex
            )
        }
      }.toMap
    }
  }

  def configList(c: Config, path: String): Seq[Config] =
    if (c.hasPath(path)) c.getConfigList(path).asScala.toSeq else Seq.empty

  /** The one definition of what may be interpolated into Spark SQL as a
    * database/table identifier. */
  def isSqlIdentifier(value: String): Boolean = value.matches("[a-zA-Z_][a-zA-Z0-9_]*")

  /** Fails with the standard message when `value` is not a safe SQL
    * identifier; `label` names the offending config surface. */
  def requireSqlIdentifier(value: String, label: String): String = {
    require(isSqlIdentifier(value), s"$label '$value' is not a safe SQL identifier")
    value
  }

  /** Validates a database/table name is a safe SQL identifier before it is
    * interpolated into Spark SQL statements. */
  def sqlIdentifier(c: Config, path: String): String = {
    val value = c.getString(path)
    require(
      isSqlIdentifier(value),
      s"Config '$path' value '$value' is not a safe SQL identifier (letters, digits, underscore only)"
    )
    value
  }

  /** All entries of a config block as a string map. Keys are HOCON path
    * expressions: plain keys come back verbatim; keys that needed quoting in
    * the config (e.g. `"a.b"`) keep their quoted path-escaped form — the
    * long-standing behavior of every call site this helper replaced. */
  def flatStringMap(c: Config): Map[String, String] = stringMap(c.atKey("p"), "p")
}
