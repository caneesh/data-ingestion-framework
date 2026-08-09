package com.hcsc.generic.ingest.kafka

import com.hcsc.generic.ingest.config.ConfigUtils
import com.hcsc.generic.ingest.reject.RejectService
import com.hcsc.generic.ingest.schema.SchemaContract
import com.hcsc.generic.ingest.stage.CuratedMicroBatch
import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQuery

/**
  * Structured Streaming driver: the micro-batch path into the SAME curated
  * writer every batch source uses — no separate streaming merge algorithm.
  *
  * Semantics on non-transactional Hive targets:
  * - offsets/checkpoint live in `checkpoint_location` (Spark-managed);
  *   Spark advances the checkpoint only after foreachBatch returns, so a
  *   crash replays the micro-batch with the SAME batchId;
  * - the curated merge is content-idempotent, so replays converge
  *   (at-least-once delivery, convergent target; exactly-once is NOT
  *   claimed on this format — see docs/architecture/INPUT_MODES.md);
  * - empty micro-batches are successful no-ops;
  * - FULL_OVERWRITE is refused by CuratedMicroBatch for micro-batches;
  * - deterministic ordering comes from the configured freshness column
  *   (typically kafka_timestamp or a payload sequence) with kafka_offset
  *   among the tie-breakers, which requires
  *   curated.retain_source_metadata = true.
  *
  * feed {
  *   source  { type = kafka, bootstrap.servers = ..., topic = ...,
  *             value.format = json, value.schema = ...,
  *             streaming { checkpoint_location = "...", trigger_seconds = 30 } }
  *   curated { ... merge { keys = [...], freshness { column = ... } } }
  * }
  */
object KafkaStreamingRunner {

  def start(spark: SparkSession, feedConf: Config, entity: String, logger: Logger): StreamingQuery = {
    val sourceConf = feedConf.getConfig("source")
    val streamingConf = ConfigUtils.optConfig(sourceConf, "streaming").getOrElse(
      throw new IllegalArgumentException(
        "KAF_002 streaming requires source.streaming { checkpoint_location = ... }"))
    val checkpoint = ConfigUtils.optString(streamingConf, "checkpoint_location").getOrElse(
      throw new IllegalArgumentException(
        "KAF_002 source.streaming.checkpoint_location is required — it is the offset ledger " +
          "for streaming; without it every restart re-reads from the configured start"))

    val curatedConf = feedConf.getConfig("curated")
    val contract = SchemaContract.parse(feedConf)
      .orElse(SchemaContract.parse(sourceConf))
    val rejects = new RejectService(
      spark, ConfigUtils.optConfig(feedConf, "rejects"), contract, logger)

    var stream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", sourceConf.getString("bootstrap.servers"))
      .option("subscribe", sourceConf.getString("topic"))
    KafkaSource.SecurityKeys.foreach(k =>
      ConfigUtils.optString(sourceConf, k).foreach(v => stream = stream.option(k, v)))
    ConfigUtils.optString(sourceConf, "startingOffsets")
      .foreach(v => stream = stream.option("startingOffsets", v))

    val decoded = KafkaSource.decode(stream.load(), sourceConf)

    var writer = decoded.writeStream
      .option("checkpointLocation", checkpoint)
      .foreachBatch { (microBatch: org.apache.spark.sql.DataFrame, batchId: Long) =>
        CuratedMicroBatch.write(spark, entity, curatedConf, contract, Some(rejects),
          microBatch, batchId.toString, logger)
        () // foreachBatch requires Unit; the result is logged by the writer
      }
    ConfigUtils.optInt(streamingConf, "trigger_seconds").foreach(s =>
      writer = writer.trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime(s * 1000L)))
    writer.start()
  }
}
