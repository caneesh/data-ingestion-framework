package com.hcsc.generic.ingest.sink

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Base trait for all data sinks.
  * Implementations provide sink-specific logic for writing data.
  */
trait Sink {
  def sinkType: String
  def write(spark: SparkSession, df: DataFrame, sinkConf: Config): Unit
}

/**
  * Registry for discovering and instantiating sinks by type.
  */
object SinkRegistry {
  private val sinks = new java.util.concurrent.ConcurrentHashMap[String, Sink]()

  def register(sink: Sink): Unit =
    sinks.put(sink.sinkType.toLowerCase, sink)

  def get(sinkType: String): Option[Sink] =
    Option(sinks.get(sinkType.toLowerCase))

  def resolve(sinkType: String): Sink = {
    get(sinkType).getOrElse(
      throw new IllegalArgumentException(
        s"Unknown sink type: $sinkType. Available types: ${availableTypes.mkString(", ")}"
      )
    )
  }

  def availableTypes: Set[String] = {
    import scala.collection.JavaConverters._
    sinks.keySet.asScala.toSet
  }
}
