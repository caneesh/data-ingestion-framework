package com.hcsc.generic.ingest.jdbc.extraction.store

import com.hcsc.generic.ingest.hive.HiveTables
import com.hcsc.generic.ingest.jdbc.extraction._
import org.apache.spark.sql.{SaveMode, SparkSession}

/** Instance-scoped in-memory store for tests and local runs — injected where
  * needed, never a global singleton. Fully CAS-checked like production. */
final class InMemoryCheckpointStore extends CheckpointStore {
  private val history = new java.util.concurrent.ConcurrentHashMap[String, List[Checkpoint]]()

  override def latest(entity: String): Option[Checkpoint] =
    Option(history.get(entity)).flatMap(_.headOption)

  override def commit(checkpoint: Checkpoint, expectedVersion: Long): Unit =
    history.compute(checkpoint.entity, (_, existing) => {
      val entries = Option(existing).getOrElse(Nil)
      val current = entries.headOption.map(_.version).getOrElse(0L)
      if (current != expectedVersion)
        throw new CheckpointConflictException(checkpoint.entity, expectedVersion, current)
      checkpoint :: entries
    })

  def entries(entity: String): List[Checkpoint] = Option(history.get(entity)).getOrElse(Nil)
}

/** Hive-backed append-only store (auditable history; latest = highest
  * version). CAS is best-effort, matching the existing watermark store's
  * documented semantics: the version observed at read is re-verified
  * immediately before the append. */
final class HiveCheckpointStore(spark: SparkSession, database: String, table: String)
  extends CheckpointStore {

  com.hcsc.generic.ingest.config.ConfigUtils.requireSqlIdentifier(database, "checkpoint_store.database")
  com.hcsc.generic.ingest.config.ConfigUtils.requireSqlIdentifier(table, "checkpoint_store.table")
  private val fullTable = s"$database.$table"

  private val columnsDdl =
    "entity STRING, strategy_kind STRING, checkpoint_value STRING, " +
      "version BIGINT, run_id STRING, committed_ts TIMESTAMP"

  override def latest(entity: String): Option[Checkpoint] = {
    if (!spark.catalog.tableExists(fullTable)) return None
    import org.apache.spark.sql.functions.col
    spark.table(fullTable)
      .filter(col("entity") === entity)
      .orderBy(col("version").desc)
      .limit(1)
      .collect()
      .headOption
      .map(r => Checkpoint(
        entity = r.getAs[String]("entity"),
        strategyKind = r.getAs[String]("strategy_kind"),
        value = BoundaryValue.deserialize(r.getAs[String]("checkpoint_value")),
        version = r.getAs[Long]("version"),
        runId = r.getAs[String]("run_id"),
        committedAt = r.getAs[java.sql.Timestamp]("committed_ts")))
  }

  override def commit(checkpoint: Checkpoint, expectedVersion: Long): Unit = {
    HiveTables.ensure(spark, database, fullTable, columnsDdl)
    val current = latest(checkpoint.entity).map(_.version).getOrElse(0L)
    if (current != expectedVersion)
      throw new CheckpointConflictException(checkpoint.entity, expectedVersion, current)
    import spark.implicits._
    Seq((checkpoint.entity, checkpoint.strategyKind, checkpoint.value.serialized,
      checkpoint.version, checkpoint.runId, checkpoint.committedAt))
      .toDF("entity", "strategy_kind", "checkpoint_value", "version", "run_id", "committed_ts")
      .write.mode(SaveMode.Append).insertInto(fullTable)
  }
}
