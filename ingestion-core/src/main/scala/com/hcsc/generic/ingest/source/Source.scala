package com.hcsc.generic.ingest.source

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Base trait for all data sources.
  * Implementations provide source-specific logic for reading data.
  */
trait Source {
  def sourceType: String
  def read(spark: SparkSession, sourceConf: Config): DataFrame
}

/**
  * Registry for discovering and instantiating sources by type.
  */
object SourceRegistry {
  private val sources = new java.util.concurrent.ConcurrentHashMap[String, Source]()

  def register(source: Source): Unit =
    sources.put(source.sourceType.toLowerCase, source)

  def get(sourceType: String): Option[Source] =
    Option(sources.get(sourceType.toLowerCase))

  def resolve(sourceType: String): Source = {
    get(sourceType).getOrElse(
      throw new IllegalArgumentException(
        s"Unknown source type: $sourceType. Available types: ${availableTypes.mkString(", ")}"
      )
    )
  }

  def availableTypes: Set[String] = {
    import scala.collection.JavaConverters._
    sources.keySet.asScala.toSet
  }
}
