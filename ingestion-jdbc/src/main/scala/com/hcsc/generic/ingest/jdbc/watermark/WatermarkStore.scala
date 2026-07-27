package com.hcsc.generic.ingest.jdbc.watermark

import com.hcsc.generic.ingest.jdbc.WatermarkConfig
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.sql.Timestamp

/** Two overlapping runs raced on the same entity's watermark (JDBC_005). */
final class WatermarkConflictException(entity: String, expected: Long, actual: Long)
  extends RuntimeException(
    s"JDBC_005 Watermark version conflict for entity '$entity': expected version $expected " +
      s"but store holds $actual — a concurrent run advanced the watermark. This run must not commit.")

/** A committed watermark with its optimistic-concurrency version. */
final case class VersionedWatermark(value: WatermarkValue, version: Long)

/** Extraction-window metadata persisted with each commit so every run can be
  * reconstructed: the lower bound the window started from and the hash of
  * the executed query. */
final case class WatermarkCommitDetail(lower: Option[String], queryHash: Option[String])
object WatermarkCommitDetail { val empty = WatermarkCommitDetail(None, None) }

/**
  * Persistence for watermark history. Append-only (full advancement history
  * stays auditable) with an optimistic version: commits verify the version
  * observed at read time and fail with JDBC_005 when a concurrent run got
  * there first.
  */
trait WatermarkStore {
  def latest(entity: String): Option[WatermarkValue] = latestVersioned(entity).map(_.value)
  def latestVersioned(entity: String): Option[VersionedWatermark]

  /** Compare-and-set append: expectedVersion must equal the current stored
    * version (0 = nothing stored yet) or WatermarkConflictException is
    * thrown. */
  def recordIfVersion(
    entity: String,
    value: WatermarkValue,
    runId: String,
    expectedVersion: Long,
    detail: WatermarkCommitDetail = WatermarkCommitDetail.empty
  ): Unit

  /** Unconditional append (tests / manual repair). */
  def record(entity: String, value: WatermarkValue, runId: String): Unit =
    recordIfVersion(entity, value, runId, latestVersioned(entity).map(_.version).getOrElse(0L))
}

/**
  * Hive-backed store (production). Append-only history table:
  * entity, watermark_value, run_id, watermark_version, updated_ts.
  *
  * NOTE: Hive is not transactional, so the version check here is
  * best-effort compare-and-set — it closes the common overlap window
  * (both runs read, one commits, the other detects the version change)
  * but two commits in the same instant can still race. For strict
  * serialization use a transactional control table (Delta / RDBMS) via a
  * custom WatermarkStore implementation.
  */
final class HiveWatermarkStore(spark: SparkSession, database: String, table: String) extends WatermarkStore {
  require(database.matches("[a-zA-Z_][a-zA-Z0-9_]*"), s"JDBC_003 watermark_store.database '$database' is not a safe identifier")
  require(table.matches("[a-zA-Z_][a-zA-Z0-9_]*"), s"JDBC_003 watermark_store.table '$table' is not a safe identifier")

  private val fullTable = s"$database.$table"

  override def latestVersioned(entity: String): Option[VersionedWatermark] = {
    if (!spark.catalog.tableExists(fullTable)) return None
    import org.apache.spark.sql.functions._
    spark.table(fullTable)
      .filter(col("entity") === entity)
      .orderBy(col("watermark_version").desc)
      .select("watermark_value", "watermark_version")
      .collect()
      .headOption
      .map(r => VersionedWatermark(WatermarkValue.deserialize(r.getString(0)), r.getLong(1)))
  }

  override def recordIfVersion(
    entity: String,
    value: WatermarkValue,
    runId: String,
    expectedVersion: Long,
    detail: WatermarkCommitDetail = WatermarkCommitDetail.empty
  ): Unit = {
    import spark.implicits._
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $database")
    spark.sql(
      s"""CREATE TABLE IF NOT EXISTS $fullTable (
         |  entity STRING, watermark_value STRING, run_id STRING,
         |  watermark_version BIGINT, lower_value STRING, query_hash STRING,
         |  updated_ts TIMESTAMP
         |) USING ORC""".stripMargin)

    val current = latestVersioned(entity).map(_.version).getOrElse(0L)
    if (current != expectedVersion)
      throw new WatermarkConflictException(entity, expectedVersion, current)

    Seq((entity, value.serialized, runId, expectedVersion + 1,
        detail.lower.orNull, detail.queryHash.orNull,
        new Timestamp(System.currentTimeMillis())))
      .toDF("entity", "watermark_value", "run_id", "watermark_version",
        "lower_value", "query_hash", "updated_ts")
      .write.mode(SaveMode.Append).insertInto(fullTable)
  }
}

/** In-memory store for tests and local runs; genuinely atomic CAS. */
object InMemoryWatermarkStore extends WatermarkStore {
  private val history =
    new java.util.concurrent.ConcurrentHashMap[String, List[(VersionedWatermark, String, WatermarkCommitDetail)]]()

  override def latestVersioned(entity: String): Option[VersionedWatermark] =
    Option(history.get(entity)).flatMap(_.headOption).map(_._1)

  override def recordIfVersion(
    entity: String,
    value: WatermarkValue,
    runId: String,
    expectedVersion: Long,
    detail: WatermarkCommitDetail = WatermarkCommitDetail.empty
  ): Unit = {
    var applied = false
    var observed = 0L
    history.compute(entity, (_, existing) => {
      val entries = Option(existing).getOrElse(Nil)
      observed = entries.headOption.map(_._1.version).getOrElse(0L)
      if (observed != expectedVersion) entries
      else { applied = true; (VersionedWatermark(value, expectedVersion + 1), runId, detail) :: entries }
    })
    if (!applied) throw new WatermarkConflictException(entity, expectedVersion, observed)
  }

  def entries(entity: String): List[(WatermarkValue, String)] =
    Option(history.get(entity)).getOrElse(Nil).map { case (vw, run, _) => (vw.value, run) }

  def latestDetail(entity: String): Option[WatermarkCommitDetail] =
    Option(history.get(entity)).flatMap(_.headOption).map(_._3)

  def clear(): Unit = history.clear()
}

object WatermarkStores {
  def from(cfg: WatermarkConfig, spark: SparkSession): WatermarkStore = cfg.storeType match {
    case "memory" => InMemoryWatermarkStore
    case "hive" =>
      val database = cfg.storeDatabase.getOrElse(
        throw new IllegalArgumentException("JDBC_003 incremental.watermark_store.database is required for the hive store"))
      new HiveWatermarkStore(spark, database, cfg.storeTable)
    case other =>
      throw new IllegalArgumentException(s"JDBC_003 Unknown watermark_store.type '$other'; expected hive or memory")
  }
}
