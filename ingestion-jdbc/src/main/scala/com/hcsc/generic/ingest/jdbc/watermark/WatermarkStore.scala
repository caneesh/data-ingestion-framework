package com.hcsc.generic.ingest.jdbc.watermark

import com.hcsc.generic.ingest.jdbc.WatermarkConfig
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.sql.Timestamp

/** Persistence for watermark history. `record` is append-only so the full
  * advancement history stays auditable; `latest` returns the newest value. */
trait WatermarkStore {
  def latest(entity: String): Option[WatermarkValue]
  def record(entity: String, value: WatermarkValue, runId: String): Unit
}

/** Hive-backed store (production). Append-only history table:
  * entity, watermark_value, run_id, updated_ts. */
final class HiveWatermarkStore(spark: SparkSession, database: String, table: String) extends WatermarkStore {
  require(database.matches("[a-zA-Z_][a-zA-Z0-9_]*"), s"JDBC_003 watermark_store.database '$database' is not a safe identifier")
  require(table.matches("[a-zA-Z_][a-zA-Z0-9_]*"), s"JDBC_003 watermark_store.table '$table' is not a safe identifier")

  private val fullTable = s"$database.$table"

  override def latest(entity: String): Option[WatermarkValue] = {
    if (!spark.catalog.tableExists(fullTable)) return None
    import org.apache.spark.sql.functions._
    spark.table(fullTable)
      .filter(col("entity") === entity)
      .orderBy(col("updated_ts").desc)
      .select("watermark_value")
      .collect()
      .headOption
      .map(r => WatermarkValue.deserialize(r.getString(0)))
  }

  override def record(entity: String, value: WatermarkValue, runId: String): Unit = {
    import spark.implicits._
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $database")
    spark.sql(
      s"""CREATE TABLE IF NOT EXISTS $fullTable (
         |  entity STRING, watermark_value STRING, run_id STRING, updated_ts TIMESTAMP
         |) USING ORC""".stripMargin)
    Seq((entity, value.serialized, runId, new Timestamp(System.currentTimeMillis())))
      .toDF("entity", "watermark_value", "run_id", "updated_ts")
      .write.mode(SaveMode.Append).insertInto(fullTable)
  }
}

/** In-memory store for tests and local runs; per-JVM, append-only history. */
object InMemoryWatermarkStore extends WatermarkStore {
  private val history = new java.util.concurrent.ConcurrentHashMap[String, List[(WatermarkValue, String)]]()

  override def latest(entity: String): Option[WatermarkValue] =
    Option(history.get(entity)).flatMap(_.headOption).map(_._1)

  override def record(entity: String, value: WatermarkValue, runId: String): Unit =
    history.compute(entity, (_, existing) =>
      (value, runId) :: Option(existing).getOrElse(Nil))

  def entries(entity: String): List[(WatermarkValue, String)] =
    Option(history.get(entity)).getOrElse(Nil)

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
