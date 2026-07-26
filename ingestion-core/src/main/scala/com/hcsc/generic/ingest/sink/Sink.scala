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
  private var sinks: Map[String, Sink] = Map.empty

  def register(sink: Sink): Unit = {
    sinks = sinks + (sink.sinkType.toLowerCase -> sink)
  }

  def get(sinkType: String): Option[Sink] = {
    sinks.get(sinkType.toLowerCase)
  }

  def resolve(sinkType: String): Sink = {
    get(sinkType).getOrElse(
      throw new IllegalArgumentException(
        s"Unknown sink type: $sinkType. Available types: ${sinks.keys.mkString(", ")}"
      )
    )
  }

  def availableTypes: Set[String] = sinks.keySet
}
