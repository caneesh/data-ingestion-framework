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
    "extract_end_ts" -> "string",
    "record_hash" -> "string" // stamped only when raw.record_hash = true
  )

  val ColumnNames: Set[String] = ColumnTypes.map(_._1).toSet

  def sqlType(column: String): String =
    ColumnTypes.collectFirst { case (n, t) if n.equalsIgnoreCase(column) => t }.getOrElse("string")

  private val LineageColumns = Seq("run_id", "source_system", "source_database",
    "source_schema", "source_table", "extract_start_ts", "extract_end_ts")

  /** withColumn silently REPLACES an existing column: a source that happens
    * to carry a column named like a framework metadata column would lose its
    * data with no error. source_file is the deliberate exception — a preset
    * value (file intake) is kept, never overwritten. */
  private[transform] def requireNoCollisions(df: DataFrame, stamped: Seq[String], where: String): Unit = {
    val collisions = stamped.filter(s => df.columns.exists(_.equalsIgnoreCase(s)))
    if (collisions.nonEmpty)
      throw new IllegalStateException(
        s"RAW_003 Source column(s) [${collisions.mkString(", ")}] collide with framework " +
          s"metadata column(s) stamped by $where and would be silently overwritten. " +
          "Rename or drop them in the source query/mapping before RAW ingestion.")
  }

  /** Full lineage stamping: run id, source identity and extract window. */
  def add(df: DataFrame, rawFlag: String, lineage: RawLineage): DataFrame = {
    def str(v: Option[String]) = lit(v.orNull).cast("string")
    requireNoCollisions(df, LineageColumns, "raw lineage stamping")
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
  def add(df: DataFrame, rawFlag: String, runId: String): DataFrame = {
    requireNoCollisions(df, Seq("run_id"), "raw lineage stamping")
    add(df, rawFlag).withColumn("run_id", lit(runId))
  }

  def add(df: DataFrame, rawFlag: String): DataFrame = {
    requireNoCollisions(df, Seq("row_idx", "load_timestamp", "file_type", "file_id"),
      "raw metadata stamping")
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
