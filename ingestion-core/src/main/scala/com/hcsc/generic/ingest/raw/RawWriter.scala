package com.hcsc.generic.ingest.raw

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Orchestrates one Raw write: stamp -> dry-run gate -> batch idempotency
  * guard -> strategy. Contains no SQL and no strategy logic; both
  * collaborators are constructor-injected, so the writer unit-tests with
  * stubs. Rejects and quarantine are upstream concerns — the writer's world
  * starts with a validated, accepted DataFrame.
  */
final class RawWriter(stamper: RawMetadataStamper, strategy: RawWriteStrategy) {
  private val logger = Logger.getLogger(getClass.getName)

  def write(spark: SparkSession, df: DataFrame, context: BatchContext, target: RawTarget): RawWriteResult = {
    val stamped = stamper.stamp(df, context)

    if (context.dryRun) {
      val rows = stamped.count()
      logger.info(s"[RawWriter] DRY-RUN: would write $rows row(s) to ${target.fullTable} " +
        s"(strategy=${strategy.kind}, batch=${context.batchId})")
      return RawWriteResult(target.fullTable, context.batchId, rows, skipped = true, detail = "dry-run")
    }

    if (BatchIdempotencyGuard.alreadyWritten(spark, target.fullTable, context.batchId)) {
      logger.warn(s"[RawWriter] ${target.fullTable} already holds batch ${context.batchId}; " +
        "skipping write (idempotent replay)")
      return RawWriteResult(target.fullTable, context.batchId, 0L, skipped = true,
        detail = "idempotent replay: batch already written")
    }

    val result = strategy.write(spark, stamped, target, context)
    logger.info(s"[RawWriter] strategy=${strategy.kind} table=${result.table} " +
      s"batch=${result.batchId} rows=${result.rowsWritten}")
    result
  }
}
