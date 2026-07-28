package com.hcsc.generic.ingest.curated

import com.hcsc.generic.ingest.config.ConfigUtils

/**
  * Where and how curated data publishes. Business keys and ordering drive
  * deduplication and merge matching; validation settings feed the
  * transactional publish gate.
  *
  * Note on "Hive transactional tables": Spark cannot write Hive ACID tables
  * (true through Spark 3.5). Transactionality here is achieved the way the
  * framework already guarantees it — materialize to a staging table,
  * validate, then swap atomically (PublishService). That is the supported
  * transactional mechanism, stated plainly rather than pretended otherwise.
  */
final case class CuratedTarget(
  database: String,
  table: String,
  path: Option[String] = None,
  format: String = "orc",
  businessKeys: Seq[String] = Seq.empty,
  orderBy: Seq[String] = Seq.empty,
  allowEmpty: Boolean = false,
  validationQuery: Option[String] = None
) {
  ConfigUtils.requireSqlIdentifier(database, "curated target database")
  ConfigUtils.requireSqlIdentifier(table, "curated target table")

  def fullTable: String = s"$database.$table"
}

/** Metrics for one curated processing run — the audit/observability payload.
  * -1 marks "not measured" (same convention as StageCounts). */
final case class CuratedMetrics(
  inputRows: Long,
  duplicateRows: Long,
  publishedRows: Long,
  insertRows: Long,
  updateRows: Long,
  deleteRows: Long,
  durationMillis: Long
)
