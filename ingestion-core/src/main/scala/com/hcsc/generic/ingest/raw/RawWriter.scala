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
    // Persist the stamped frame so the counted rows and the written rows are
    // the SAME evaluation — without this, a source that changes between the
    // count action and the write action makes rowsWritten a lie.
    val stamped = stamper.stamp(df, context)
      .persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
    try {
      if (context.dryRun) {
        val rows = stamped.count()
        logger.info(s"[RawWriter] DRY-RUN: would write $rows row(s) to ${target.fullTable} " +
          s"(strategy=${strategy.kind}, batch=${context.batchId})")
        return RawWriteResult(target.fullTable, context.batchId, rows, skipped = true, detail = "dry-run")
      }

      // A target that predates the framework and lacks batch_id would make
      // the idempotency guard permanently blind while alignToTarget silently
      // dropped the stamped column — every replay would double-load. Refuse.
      if (spark.catalog.tableExists(target.fullTable) &&
          !BatchIdempotencyGuard.hasBatchIdColumn(spark, target.fullTable))
        throw new IllegalArgumentException(
          s"RAW_002 target ${target.fullTable} has no batch_id column; batch idempotency is impossible. " +
            "Add the Raw metadata columns to the table (or load legacy tables through the legacy pipeline)")

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
    } finally stamped.unpersist(false)
  }
}
