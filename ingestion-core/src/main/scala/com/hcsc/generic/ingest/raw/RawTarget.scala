package com.hcsc.generic.ingest.raw

import com.hcsc.generic.ingest.config.ConfigUtils
import com.hcsc.generic.ingest.transform.PartitionSpec

/**
  * Where a Raw write lands: Hive database/table (ORC), optional explicit
  * location, and the feed's partitioning (keys plus derive expressions,
  * evaluated by the existing Partitioning transform).
  */
final case class RawTarget(
  database: String,
  table: String,
  path: Option[String] = None,
  partitions: PartitionSpec = PartitionSpec(Seq.empty, Map.empty)
) {
  ConfigUtils.requireSqlIdentifier(database, "raw target database")
  ConfigUtils.requireSqlIdentifier(table, "raw target table")

  def fullTable: String = s"$database.$table"
}

/** Outcome of one Raw write, reported for audit and reconciliation. */
final case class RawWriteResult(
  table: String,
  batchId: String,
  rowsWritten: Long,
  skipped: Boolean = false,
  detail: String = ""
)
