package com.hcsc.generic.ingest.transform

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/** Source identity and extraction-window lineage stamped onto every RAW
  * record. For JDBC feeds the extract window is the serialized watermark
  * window (lower, captured upper) observed by this run's read; kept as
  * strings so numeric/rowversion/composite watermarks round-trip. */
final case class RawLineage(
  runId: String,
  sourceSystem: Option[String] = None,
  sourceDatabase: Option[String] = None,
  sourceSchema: Option[String] = None,
  sourceTable: Option[String] = None,
  extractStart: Option[String] = None,
  extractEnd: Option[String] = None
)

object RawMetadata {

  /** Framework metadata columns and their SQL types, in stamping order —
    * the RAW sink uses this to enforce (or ALTER in) missing columns on
    * pre-created targets instead of silently dropping them. */
  val ColumnTypes: Seq[(String, String)] = Seq(
    "source_file" -> "string",
    "row_idx" -> "bigint",
    "load_timestamp" -> "timestamp",
    "file_type" -> "string",
    "file_id" -> "string",
    "run_id" -> "string",
    "source_system" -> "string",
    "source_database" -> "string",
    "source_schema" -> "string",
    "source_table" -> "string",
    "extract_start_ts" -> "string",
    "extract_end_ts" -> "string"
  )

  val ColumnNames: Set[String] = ColumnTypes.map(_._1).toSet

  def sqlType(column: String): String =
    ColumnTypes.collectFirst { case (n, t) if n.equalsIgnoreCase(column) => t }.getOrElse("string")

  /** Full lineage stamping: run id, source identity and extract window. */
  def add(df: DataFrame, rawFlag: String, lineage: RawLineage): DataFrame = {
    def str(v: Option[String]) = lit(v.orNull).cast("string")
    add(df, rawFlag)
      .withColumn("run_id", lit(lineage.runId))
      .withColumn("source_system", str(lineage.sourceSystem))
      .withColumn("source_database", str(lineage.sourceDatabase))
      .withColumn("source_schema", str(lineage.sourceSchema))
      .withColumn("source_table", str(lineage.sourceTable))
      .withColumn("extract_start_ts", str(lineage.extractStart))
      .withColumn("extract_end_ts", str(lineage.extractEnd))
  }

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
