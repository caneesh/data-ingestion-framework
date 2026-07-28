package com.hcsc.generic.ingest.raw

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * CDC event loading: a strictly append-only change log. The batch must
  * carry the CDC source-metadata columns — operation (I/U/D) and a
  * monotonic sequence (LSN/SCN/offset) — because without them the events
  * cannot be ordered or collapsed downstream. Events are NEVER collapsed
  * here; current-state derivation (merge by key ordered by cdc_sequence)
  * belongs to Curated. Composition: the physical write is the append
  * strategy, unchanged.
  */
final class CdcEventsStrategy(append: AppendBatchStrategy) extends RawWriteStrategy {

  val kind: String = RawWriteStrategies.CdcEvents

  def write(spark: SparkSession, stamped: DataFrame, target: RawTarget, context: BatchContext): RawWriteResult = {
    val missing = CdcEventsStrategy.RequiredColumns
      .filterNot(required => stamped.columns.exists(_.equalsIgnoreCase(required)))
    if (missing.nonEmpty)
      throw new IllegalArgumentException(
        s"RAW_002 CDC_EVENTS requires source-metadata columns ${missing.mkString(", ")}; " +
          "the CDC connector must supply operation and sequence for every event")
    append.write(spark, stamped, target, context)
  }
}

object CdcEventsStrategy {
  /** cdc_operation: I/U/D; cdc_sequence: monotonic within key (LSN/SCN/offset). */
  val RequiredColumns: Seq[String] = Seq("cdc_operation", "cdc_sequence")
}
