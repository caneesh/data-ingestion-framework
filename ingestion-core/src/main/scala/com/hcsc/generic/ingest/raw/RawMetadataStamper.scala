package com.hcsc.generic.ingest.raw

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Adds the Raw layer's metadata columns — the ONLY mutation Raw permits.
  * Business data passes through untouched (no casts, renames or drops).
  *
  * Stamped groups:
  *   batch    run_id, batch_id, load_timestamp, file_type
  *   source   source_system, feed_name
  *   lineage  source_file (kept when the connector already added it),
  *            row_idx, record_hash
  *
  * record_hash is a SHA-256 over the business columns in case-insensitively
  * SORTED column-name order with explicit null/empty markers: stable across
  * column reordering and header case changes, and it distinguishes null from
  * empty string. Honest limits: adding or removing ANY business column
  * changes every record's hash (cross-schema-version comparison is not
  * supported), and values containing the field separator (U+0001) or a literal
  * null marker (U+0000) can collide with adjacent-field or null encodings. It fingerprints
  * record content within one schema version; it is not a key.
  */
final class RawMetadataStamper {

  def stamp(df: DataFrame, context: BatchContext): DataFrame = {
    val collisions = RawMetadataStamper.Reserved
      .filterNot(RawMetadataStamper.AllowedPreexisting.contains)
      .filter(reserved => df.columns.exists(_.equalsIgnoreCase(reserved)))
    if (collisions.nonEmpty)
      throw new IllegalArgumentException(
        s"RAW_001 source columns collide with reserved Raw metadata columns: ${collisions.mkString(", ")}; " +
          "rename them via the schema contract before ingestion")

    val businessColumns = df.columns
      .filterNot(c => RawMetadataStamper.Reserved.exists(_.equalsIgnoreCase(c)))
      .sortBy(_.toLowerCase) // case-insensitive: a header case change must not shift the hash
    val hashInput = businessColumns.map(c =>
      coalesce(col(c).cast("string"), lit(RawMetadataStamper.NullMarker)))
    val recordHash =
      if (hashInput.isEmpty) lit("")
      else sha2(concat_ws(RawMetadataStamper.FieldSeparator, hashInput: _*), 256)

    val withSource =
      if (df.columns.exists(_.equalsIgnoreCase("source_file"))) df
      else df.withColumn("source_file", input_file_name())

    withSource
      .withColumn("record_hash", recordHash)
      // row_idx is unique within a batch but NOT a stable ordering — values
      // depend on partition layout (same caveat as the legacy stamp).
      .withColumn("row_idx", monotonically_increasing_id())
      .withColumn("run_id", lit(context.runId))
      .withColumn("batch_id", lit(context.batchId))
      .withColumn("load_timestamp", lit(context.loadTimestamp))
      .withColumn("source_system", lit(context.sourceSystem))
      .withColumn("feed_name", lit(context.feedName))
      .withColumn("file_type", lit(context.rawFlag))
  }
}

object RawMetadataStamper {
  /** Names owned by the Raw layer; a source column matching one (other than
    * connector-provided source_file) fails fast rather than being renamed. */
  /** Columns the stamper itself adds to every batch. */
  val StampedColumns: Seq[String] = Seq(
    "run_id", "batch_id", "load_timestamp", "source_system", "feed_name",
    "file_type", "source_file", "row_idx", "record_hash")

  /** Names the Raw layer owns: everything stamped, plus columns claimed by
    * strategies (snapshot_dt) that would otherwise silently overwrite a
    * same-named source column. */
  val Reserved: Seq[String] = StampedColumns :+ "snapshot_dt"

  private val AllowedPreexisting: Set[String] = Set("source_file")

  private val FieldSeparator = "\u0001"
  private val NullMarker = "\u0000"
}
