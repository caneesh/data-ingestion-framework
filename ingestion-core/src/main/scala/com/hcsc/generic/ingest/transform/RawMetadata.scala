package com.hcsc.generic.ingest.transform

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object RawMetadata {

  /** Adds RAW metadata plus run lineage for replay and audit. */
  def add(df: DataFrame, rawFlag: String, runId: String): DataFrame =
    add(df, rawFlag).withColumn("run_id", lit(runId))

  def add(df: DataFrame, rawFlag: String): DataFrame = {
    val withSource =
      if (df.columns.exists(_.equalsIgnoreCase("source_file"))) df
      else df.withColumn("source_file", input_file_name())

    withSource
      // row_idx is unique within a run but is NOT a stable ordering — values
      // depend on partition layout and change across re-runs.
      .withColumn("row_idx", monotonically_increasing_id())
      .withColumn("load_timestamp", current_timestamp())
      .withColumn("file_type", lit(rawFlag))
      .withColumn("file_id", sha2(coalesce(col("source_file"), lit("")), 256))
  }
}
