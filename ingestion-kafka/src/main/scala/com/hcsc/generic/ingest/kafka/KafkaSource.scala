package com.hcsc.generic.ingest.kafka

import com.hcsc.generic.ingest.config.ConfigUtils
import com.hcsc.generic.ingest.source.{Source, SourceRegistry}
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

object KafkaSource extends Source {

  override def sourceType: String = "kafka"

  override def read(spark: SparkSession, sourceConf: Config): DataFrame = {
    val bootstrapServers = sourceConf.getString("bootstrap.servers")
    val topic = sourceConf.getString("topic")

    var reader = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topic)

    ConfigUtils.optString(sourceConf, "startingOffsets").foreach(v =>
      reader = reader.option("startingOffsets", v)
    )
    ConfigUtils.optString(sourceConf, "endingOffsets").foreach(v =>
      reader = reader.option("endingOffsets", v)
    )

    ConfigUtils.optString(sourceConf, "kafka.security.protocol").foreach(v =>
      reader = reader.option("kafka.security.protocol", v)
    )
    ConfigUtils.optString(sourceConf, "kafka.sasl.mechanism").foreach(v =>
      reader = reader.option("kafka.sasl.mechanism", v)
    )
    ConfigUtils.optString(sourceConf, "kafka.sasl.jaas.config").foreach(v =>
      reader = reader.option("kafka.sasl.jaas.config", v)
    )

    val rawDf = reader.load()

    val valueFormat = ConfigUtils.optString(sourceConf, "value.format").getOrElse("string")
    valueFormat.toLowerCase match {
      case "json" =>
        import org.apache.spark.sql.functions._
        import org.apache.spark.sql.types.{DataType, StructType}
        val schemaString = ConfigUtils.optString(sourceConf, "value.schema")
          .getOrElse(throw new IllegalArgumentException("value.schema required for JSON format"))
        val schema = DataType.fromJson(schemaString).asInstanceOf[StructType]
        rawDf.select(
          col("key").cast("string"),
          from_json(col("value").cast("string"), schema).as("value"),
          col("topic"),
          col("partition"),
          col("offset"),
          col("timestamp")
        ).select(col("key"), col("value.*"), col("topic"), col("partition"), col("offset"), col("timestamp"))

      case "avro" =>
        throw new UnsupportedOperationException("Avro format requires spark-avro dependency - not yet implemented")

      case _ =>
        import org.apache.spark.sql.functions._
        rawDf.select(
          col("key").cast("string").as("key"),
          col("value").cast("string").as("value"),
          col("topic"),
          col("partition"),
          col("offset"),
          col("timestamp")
        )
    }
  }

  def register(): Unit = SourceRegistry.register(this)
}
