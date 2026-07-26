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
      nested.entrySet().asScala.map(e => e.getKey -> nested.getString(e.getKey)).toMap
    }
  }

  def configList(c: Config, path: String): Seq[Config] =
    if (c.hasPath(path)) c.getConfigList(path).asScala.toSeq else Seq.empty
}
