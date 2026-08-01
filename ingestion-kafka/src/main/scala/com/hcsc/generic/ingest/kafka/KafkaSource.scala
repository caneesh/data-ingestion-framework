package com.hcsc.generic.ingest.kafka

import com.hcsc.generic.ingest.config.ConfigUtils
import com.hcsc.generic.ingest.source.{Source, SourceRegistry, WatermarkAdvancing}
import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
  * Kafka BATCH source. Produces a standardized DataFrame like every other
  * source — the curated layer never sees topics or offsets.
  *
  * Metadata columns (excluded from curated by default via SourceMetadata):
  * kafka_topic, kafka_partition, kafka_offset, kafka_timestamp,
  * kafka_tombstone (true when the record VALUE is null — captured BEFORE
  * payload parsing so tombstones survive JSON decoding and can drive
  * SOFT deletes via deletes.indicator_column = kafka_tombstone, which
  * requires curated.retain_source_metadata = true).
  *
  * Offset tracking (source.offsets.track = true): starting offsets come
  * from the committed ledger (MAX until-offset per partition), end offsets
  * are PINNED via the consumer API before the read — a deterministic
  * (start, end] window, the same pattern as the JDBC captured upper. The
  * pipeline commits the window through advanceWatermark ONLY after raw +
  * curated + audit succeed; a failed run never moves offsets and the
  * replay re-reads the same window idempotently.
  *
  * source {
  *   type = kafka
  *   bootstrap.servers = "..."
  *   topic = "..."
  *   value.format = string | json          # value.schema required for json
  *   retain_kafka_metadata = true          # false drops kafka_* after read
  *   offsets {
  *     track = true
  *     database = "ingest_audit"
  *     table = "ingest_kafka_offsets"
  *     default_start = earliest | latest   # partitions never consumed
  *   }
  * }
  */
object KafkaSource extends Source with WatermarkAdvancing {
  private val logger = Logger.getLogger(getClass.getName)

  override def sourceType: String = "kafka"

  private final case class PendingWindow(
    topic: String, database: String, table: String,
    start: Map[Int, Long], end: Map[Int, Long])

  private val readWindows = new java.util.concurrent.ConcurrentHashMap[String, PendingWindow]()
  private def windowKey(entity: String, runId: String) = s"$entity|$runId"

  private[kafka] val SecurityKeys = Seq(
    "kafka.security.protocol", "kafka.sasl.mechanism", "kafka.sasl.jaas.config")

  override def read(spark: SparkSession, sourceConf: Config): DataFrame = {
    val bootstrapServers = sourceConf.getString("bootstrap.servers")
    val topic = sourceConf.getString("topic")

    var reader = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topic)
    SecurityKeys.foreach(k =>
      ConfigUtils.optString(sourceConf, k).foreach(v => reader = reader.option(k, v)))

    val tracking = ConfigUtils.optConfig(sourceConf, "offsets")
      .filter(o => ConfigUtils.optBoolean(o, "track").getOrElse(false))
    tracking match {
      case Some(o) =>
        val entity = ConfigUtils.optString(sourceConf, "entity").getOrElse(
          throw new IllegalArgumentException(
            "KAF_001 offsets.track = true requires the pipeline-injected entity " +
              "(run via IngestPipeline, or disable tracking for ad-hoc reads)"))
        val runId = ConfigUtils.optString(sourceConf, "run_id").getOrElse("adhoc")
        val database = ConfigUtils.optString(o, "database").getOrElse("ingest_audit")
        val table = ConfigUtils.optString(o, "table").getOrElse("ingest_kafka_offsets")
        val defaultStart =
          if (ConfigUtils.optString(o, "default_start").getOrElse("earliest")
            .equalsIgnoreCase("latest")) -1L else -2L

        val (partitions, endOffsets) = fetchEndOffsets(bootstrapServers, topic, sourceConf)
        val committed = new KafkaOffsetLedger(spark, database, table).committedUntil(entity, topic)
        val start = partitions.map(p => p -> committed.getOrElse(p, defaultStart)).toMap
        val startJson = KafkaOffsets.toJson(topic, start)
        val endJson = KafkaOffsets.toJson(topic, endOffsets)
        logger.info(s"[KafkaSource] entity=$entity window start=$startJson end=$endJson " +
          "(end pinned; committed only after curated success)")
        readWindows.put(windowKey(entity, runId), PendingWindow(topic, database, table, start, endOffsets))
        reader = reader.option("startingOffsets", startJson).option("endingOffsets", endJson)

      case None =>
        // Explicit offsets (manual range replay) or Spark defaults —
        // replaying an offset range is idempotent at the curated layer.
        ConfigUtils.optString(sourceConf, "startingOffsets")
          .foreach(v => reader = reader.option("startingOffsets", v))
        ConfigUtils.optString(sourceConf, "endingOffsets")
          .foreach(v => reader = reader.option("endingOffsets", v))
    }

    decode(reader.load(), sourceConf)
  }

  /** Shared batch/streaming decode: tombstone flag FIRST (before payload
    * parsing), then spec-named metadata columns; retention configurable.
    * Public: it is a pure transform over the kafka reader's frame shape —
    * the streaming runner and the (broker-less) tests both use it. */
  def decode(rawDf: DataFrame, sourceConf: Config): DataFrame = {
    val withMeta = rawDf
      .withColumn("kafka_tombstone", col("value").isNull)
      .withColumn("kafka_topic", col("topic"))
      .withColumn("kafka_partition", col("partition"))
      .withColumn("kafka_offset", col("offset"))
      .withColumn("kafka_timestamp", col("timestamp"))
    val metaCols = Seq("kafka_topic", "kafka_partition", "kafka_offset",
      "kafka_timestamp", "kafka_tombstone").map(col)

    val decoded = ConfigUtils.optString(sourceConf, "value.format").getOrElse("string").toLowerCase match {
      case "json" =>
        import org.apache.spark.sql.types.{DataType, StructType}
        val schemaString = ConfigUtils.optString(sourceConf, "value.schema")
          .getOrElse(throw new IllegalArgumentException("value.schema required for JSON format"))
        val schema = DataType.fromJson(schemaString).asInstanceOf[StructType]
        withMeta.select(
          (Seq(col("key").cast("string").as("key"),
            from_json(col("value").cast("string"), schema).as("value")) ++ metaCols): _*)
          .select((Seq(col("key"), col("value.*")) ++ metaCols): _*)
      case "avro" =>
        throw new UnsupportedOperationException(
          "Avro format requires spark-avro dependency - not yet implemented")
      case _ =>
        withMeta.select(
          (Seq(col("key").cast("string").as("key"),
            col("value").cast("string").as("value")) ++ metaCols): _*)
    }

    if (ConfigUtils.optBoolean(sourceConf, "retain_kafka_metadata").getOrElse(true)) decoded
    else decoded.drop("kafka_topic", "kafka_partition", "kafka_offset",
      "kafka_timestamp", "kafka_tombstone")
  }

  override def advanceWatermark(
    spark: SparkSession,
    sourceConf: Config,
    entity: String,
    runId: String,
    accepted: DataFrame
  ): Unit =
    Option(readWindows.remove(windowKey(entity, runId))).foreach { w =>
      val ranges = KafkaOffsets.rangesRead(w.start, w.end)
      if (ranges.isEmpty)
        logger.info(s"[KafkaSource] entity=$entity: window held no new offsets; nothing committed")
      else {
        new KafkaOffsetLedger(spark, w.database, w.table).commit(entity, w.topic, runId, ranges)
        logger.info(s"[KafkaSource] entity=$entity committed ${ranges.size} offset range(s) " +
          s"for topic ${w.topic} (post-curated success)")
      }
    }

  override def lastWindow(entity: String, runId: Option[String]): Option[(String, Option[String])] =
    Option(readWindows.get(windowKey(entity, runId.getOrElse("adhoc")))).map(w =>
      (KafkaOffsets.toJson(w.topic, w.start), Some(KafkaOffsets.toJson(w.topic, w.end))))

  override def discardWindow(entity: String, runId: Option[String]): Unit =
    runId.foreach(r => readWindows.remove(windowKey(entity, r)))

  /** Pins the read window's upper edge via the consumer API (partition list
    * + end offsets) so the batch read is deterministic and committable. */
  private def fetchEndOffsets(
    bootstrapServers: String,
    topic: String,
    sourceConf: Config
  ): (Seq[Int], Map[Int, Long]) = {
    import org.apache.kafka.clients.consumer.KafkaConsumer
    import org.apache.kafka.common.TopicPartition
    import scala.collection.JavaConverters._
    val props = new java.util.Properties()
    props.put("bootstrap.servers", bootstrapServers)
    props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    SecurityKeys.foreach(k =>
      ConfigUtils.optString(sourceConf, k).foreach(v => props.put(k.stripPrefix("kafka."), v)))
    val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](props)
    try {
      val partitions = consumer.partitionsFor(topic).asScala.map(_.partition()).toSeq.sorted
      val tps = partitions.map(p => new TopicPartition(topic, p))
      val ends = consumer.endOffsets(tps.asJava).asScala
        .map { case (tp, o) => tp.partition() -> o.longValue() }.toMap
      (partitions, ends)
    } finally consumer.close()
  }

  def register(): Unit = SourceRegistry.register(this)
}
