package com.hcsc.generic.ingest.kafka

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, max}

/** One consumed offset range of one topic-partition: (from, until] in
  * Kafka's native convention — `from` inclusive as a STARTING offset,
  * `until` exclusive (the next offset to start from). */
final case class OffsetRange(partition: Int, fromOffset: Long, untilOffset: Long)

/**
  * Hive-backed ledger of COMMITTED Kafka offset ranges, append-only like
  * the audit tables (HiveTables discipline). A range is written ONLY by
  * the pipeline's post-success watermark hook, so "committed" always means
  * "raw + curated + audit completed for these offsets". The next run's
  * starting offsets are MAX(until_offset) per partition; unseen partitions
  * start from the configured default. Re-committing a replayed range
  * appends a duplicate row — harmless, MAX() absorbs it, and the replay
  * itself is idempotent at the curated layer.
  */
final class KafkaOffsetLedger(spark: SparkSession, database: String, table: String) {
  com.hcsc.generic.ingest.config.ConfigUtils.requireSqlIdentifier(database, "offsets.database")
  com.hcsc.generic.ingest.config.ConfigUtils.requireSqlIdentifier(table, "offsets.table")
  private val fullTable = s"$database.$table"

  private val ddl =
    "entity STRING, topic STRING, partition INT, from_offset BIGINT, " +
      "until_offset BIGINT, run_id STRING, event_ts TIMESTAMP"

  /** Highest committed until-offset per partition for entity+topic. */
  def committedUntil(entity: String, topic: String): Map[Int, Long] = {
    if (!spark.catalog.tableExists(fullTable)) return Map.empty
    spark.table(fullTable)
      .filter(col("entity") === entity && col("topic") === topic)
      .groupBy(col("partition")).agg(max(col("until_offset")).as("until"))
      .collect()
      .map(r => r.getInt(0) -> r.getLong(1))
      .toMap
  }

  def commit(entity: String, topic: String, runId: String, ranges: Seq[OffsetRange]): Unit = {
    if (ranges.isEmpty) return
    com.hcsc.generic.ingest.hive.HiveTables.ensure(spark, database, fullTable, ddl)
    val s = spark
    import s.implicits._
    val now = new java.sql.Timestamp(System.currentTimeMillis())
    val rows = ranges.map(r => (entity, topic, r.partition, r.fromOffset, r.untilOffset, runId, now))
      .toDF("entity", "topic", "partition", "from_offset", "until_offset", "run_id", "event_ts")
    rows.write.mode(SaveMode.Append).insertInto(fullTable)
  }
}

/** Pure offset-JSON codec for the Spark Kafka source's
  * startingOffsets/endingOffsets format: {"topic":{"0":123,"1":-2}}. */
object KafkaOffsets {

  def toJson(topic: String, offsets: Map[Int, Long]): String =
    s"""{"${escape(topic)}":{${offsets.toSeq.sortBy(_._1)
      .map { case (p, o) => s""""$p":$o""" }.mkString(",")}}}"""

  /** Starting offsets for a run: committed positions where known, the
    * default (-2 = earliest, -1 = latest) for partitions never consumed. */
  def startingOffsets(topic: String, committed: Map[Int, Long], allPartitions: Seq[Int], default: Long): String =
    toJson(topic, allPartitions.map(p => p -> committed.getOrElse(p, default)).toMap)

  /** The ranges actually read given pinned start/end positions; empty and
    * regressed partitions produce no range. */
  def rangesRead(start: Map[Int, Long], end: Map[Int, Long]): Seq[OffsetRange] =
    end.toSeq.sortBy(_._1).flatMap { case (p, until) =>
      val from = start.getOrElse(p, 0L)
      if (until > from && from >= 0) Some(OffsetRange(p, from, until)) else None
    }

  private def escape(s: String): String = s.replace("\\", "\\\\").replace("\"", "\\\"")
}
