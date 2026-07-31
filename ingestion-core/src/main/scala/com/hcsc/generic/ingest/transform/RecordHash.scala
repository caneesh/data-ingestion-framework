package com.hcsc.generic.ingest.transform

import com.hcsc.generic.ingest.schema.SchemaContract
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Business-content fingerprint for change detection (§7): a SHA-256 over
  * the business columns in case-insensitively SORTED column-name order with
  * explicit null markers — stable across column reordering and header case
  * changes, and it distinguishes null from empty string. The recipe matches
  * raw.RawMetadataStamper exactly so the two stamping paths produce
  * comparable hashes.
  *
  * With a schema contract, only contract columns of category `business`
  * participate (technical/audit/generated columns never shift the hash);
  * without one, every non-framework column does. Honest limits: adding or
  * removing a business column changes every hash (no cross-schema-version
  * comparison), and values containing U+0001/U+0000 can collide with the
  * separator/null encodings. It fingerprints content; it is not a key.
  */
object RecordHash {
  val Column = "record_hash"
  private val FieldSeparator = "\u0001"
  private val NullMarker = "\u0000"

  def stamp(df: DataFrame, contract: Option[SchemaContract]): DataFrame = {
    RawMetadata.requireNoCollisions(df, Seq(Column), "record_hash stamping")
    val candidates = contract match {
      case Some(c) =>
        c.columns.filter(_.category.equalsIgnoreCase("business"))
          .flatMap(cc => df.columns.find(_.equalsIgnoreCase(cc.name)))
      case None =>
        df.columns.toSeq.filterNot(c => RawMetadata.ColumnNames.contains(c.toLowerCase))
    }
    val cols = candidates.distinct.sortBy(_.toLowerCase)
    val input = cols.map(c => coalesce(col(c).cast("string"), lit(NullMarker)))
    val hash = if (input.isEmpty) lit("") else sha2(concat_ws(FieldSeparator, input: _*), 256)
    df.withColumn(Column, hash)
  }
}
