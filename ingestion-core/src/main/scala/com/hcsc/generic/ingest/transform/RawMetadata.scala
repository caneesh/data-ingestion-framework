package com.hcsc.generic.ingest.transform

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object RawMetadata {
  def add(df: DataFrame, rawFlag: String): DataFrame = {
    val withSource =
      if (df.columns.exists(_.equalsIgnoreCase("source_file"))) df
      else df.withColumn("source_file", input_file_name())

    withSource
      .withColumn("row_idx", monotonically_increasing_id())
      .withColumn("load_timestamp", current_timestamp())
      .withColumn("file_type", lit(rawFlag))
      .withColumn("file_id", sha2(coalesce(col("source_file"), lit("")), 256))
  }
}
