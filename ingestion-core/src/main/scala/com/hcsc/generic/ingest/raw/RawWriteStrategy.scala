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

/** A batch already present in the target must not be appended twice. The
  * check-then-write pair is best-effort under concurrency: the framework's
  * operating assumption (as with the pipeline's run_id guard) is one writer
  * per entity at a time; concurrent same-batch writers are detected by the
  * next run, not prevented here. */
object BatchIdempotencyGuard {
  def alreadyWritten(spark: SparkSession, fullTable: String, batchId: String): Boolean =
    spark.catalog.tableExists(fullTable) &&
      hasBatchIdColumn(spark, fullTable) &&
      spark.table(fullTable).filter(col("batch_id") === lit(batchId)).limit(1).count() > 0

  def hasBatchIdColumn(spark: SparkSession, fullTable: String): Boolean =
    spark.table(fullTable).columns.exists(_.equalsIgnoreCase("batch_id"))
}
