package com.hcsc.generic.ingest.raw

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit}

/**
  * How stamped rows land in the Raw table. Laws every implementation honors:
  *   - never mutates data beyond what the stamper already did
  *   - append-only history (Snapshot replaces whole partitions, never rows)
  *   - reports exactly what it wrote for reconciliation
  * Idempotency lives in RawWriter (batch_id attribution), not here.
  */
trait RawWriteStrategy {
  def kind: String
  def write(spark: SparkSession, stamped: DataFrame, target: RawTarget, context: BatchContext): RawWriteResult
}

/** A batch already present in the target must not be appended twice. */
object BatchIdempotencyGuard {
  def alreadyWritten(spark: SparkSession, fullTable: String, batchId: String): Boolean =
    spark.catalog.tableExists(fullTable) &&
      spark.table(fullTable).columns.contains("batch_id") &&
      spark.table(fullTable).filter(col("batch_id") === lit(batchId)).limit(1).count() > 0
}
