package com.hcsc.generic.ingest.schema

import org.apache.spark.sql.{Column, DataFrame}

/**
  * Common utilities for case-insensitive column mapping used across the
  * schema validation, file source, and curated transform layers.
  */
object ColumnMapping {

  /**
    * Column reference for a name that may contain characters Spark's parser
    * treats as syntax.
    *
    * `col("a.b")` does NOT mean the column literally named "a.b" — the dot
    * is parsed as nested-field access, so it resolves to field `b` of column
    * `a` and fails with UNRESOLVED_COLUMN when no such struct exists. Source
    * systems produce such names freely ("AIAccInfo.IsGroupMunicipality/
    * County/School District/StateofTexas"), and they survive into the frame
    * whenever a column is not renamed to its canonical form.
    *
    * Backtick-quoting makes the whole string one identifier. Embedded
    * backticks are doubled, the standard escape.
    */
  def quotedCol(name: String): Column =
    org.apache.spark.sql.functions.col(s"`${name.replace("`", "``")}`")

  /** Finds a column name in the DataFrame case-insensitively. */
  def findColumn(df: DataFrame, name: String): Option[String] =
    df.columns.find(_.equalsIgnoreCase(name))

  /** Checks if a column exists in the DataFrame case-insensitively. */
  def hasColumn(df: DataFrame, name: String): Boolean =
    df.columns.exists(_.equalsIgnoreCase(name))

  /** Finds a column name in a sequence case-insensitively. */
  def findInSeq(columns: Seq[String], name: String): Option[String] =
    columns.find(_.equalsIgnoreCase(name))

  /** Checks if a column exists in a sequence case-insensitively. */
  def existsInSeq(columns: Seq[String], name: String): Boolean =
    columns.exists(_.equalsIgnoreCase(name))

  /**
    * Builds a case-insensitive lookup from actual column names.
    * Keys are lowercased; values are the actual column names.
    */
  def buildLookup(columns: Seq[String]): Map[String, String] =
    columns.map(c => c.toLowerCase -> c).toMap

  /**
    * Applies renames from a HeaderResolution to a DataFrame.
    * Renames columns from their source names to canonical names.
    */
  def applyRenames(df: DataFrame, renames: Seq[(String, String)]): DataFrame =
    renames.foldLeft(df) { case (acc, (from, to)) =>
      acc.withColumnRenamed(from, to)
    }

  /**
    * Resolves source headers against a contract, returning:
    * - the canonical name for each matched header
    * - the renames needed (from source to canonical)
    * - unmatched headers
    */
  def resolveHeaders(
    headers: Seq[String],
    contract: SchemaContract
  ): (Seq[(String, Option[String])], Seq[(String, String)], Seq[String]) = {
    val resolved = headers.map(h => h -> contract.resolve(h))
    val renames = resolved.collect {
      case (h, Some(target)) if h != target => h -> target
    }
    val unmatched = resolved.collect { case (h, None) => h }
    (resolved, renames, unmatched)
  }
}
