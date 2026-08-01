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
/** Opt-in value canonicalization (raw.record_hash_options): applied to every
  * participating value BEFORE hashing. Enabling either flag changes every
  * hash, which is why the recipe version embeds the active options — a
  * stored-vs-current recipe mismatch is detectable, never silent. */
final case class RecordHashOptions(trimValues: Boolean = false, uppercaseValues: Boolean = false)

object RecordHash {
  val Column = "record_hash"
  /** Bump on any change to the base recipe (separator, null marker, column
    * selection or ordering rules). */
  val BaseRecipeVersion = "1"
  private val FieldSeparator = "\u0001"
  private val NullMarker = "\u0000"

  /** The full recipe identity recorded on the RAW table
    * (ingest.record_hash.version): base version plus active canonicalization
    * options, so hashes from different recipes are never compared silently. */
  def recipeVersion(options: RecordHashOptions): String = {
    val mods = Seq(
      if (options.trimValues) Some("trim") else None,
      if (options.uppercaseValues) Some("upper") else None).flatten
    if (mods.isEmpty) BaseRecipeVersion else s"$BaseRecipeVersion+${mods.mkString("+")}"
  }

  def stamp(df: DataFrame, contract: Option[SchemaContract]): DataFrame =
    stamp(df, contract, RecordHashOptions())

  def stamp(df: DataFrame, contract: Option[SchemaContract], options: RecordHashOptions): DataFrame = {
    RawMetadata.requireNoCollisions(df, Seq(Column), "record_hash stamping")
    val candidates = contract match {
      case Some(c) =>
        c.columns.filter(_.category.equalsIgnoreCase("business"))
          .flatMap(cc => df.columns.find(_.equalsIgnoreCase(cc.name)))
      case None =>
        df.columns.toSeq.filterNot(c => RawMetadata.ColumnNames.contains(c.toLowerCase))
    }
    val cols = candidates.distinct.sortBy(_.toLowerCase)
    def canonical(name: String): org.apache.spark.sql.Column = {
      val base = col(name).cast("string")
      val trimmed = if (options.trimValues) trim(base) else base
      if (options.uppercaseValues) upper(trimmed) else trimmed
    }
    val input = cols.map(c => coalesce(canonical(c), lit(NullMarker)))
    val hash = if (input.isEmpty) lit("") else sha2(concat_ws(FieldSeparator, input: _*), 256)
    df.withColumn(Column, hash)
  }
}
