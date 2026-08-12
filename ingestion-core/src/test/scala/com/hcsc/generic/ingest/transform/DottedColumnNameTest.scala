package com.hcsc.generic.ingest.transform

import com.hcsc.generic.ingest.schema.ColumnMapping
import org.apache.spark.sql.functions.col
import org.scalatest.funsuite.AnyFunSuite

/**
  * Source column names containing characters Spark's parser treats as
  * syntax — above all the DOT.
  *
  * `col("a.b")` does not mean the column named "a.b": the dot is parsed as
  * nested-field access, so it resolves to field `b` of column `a` and fails
  * with UNRESOLVED_COLUMN. Real feeds carry such names
  * ("AIAccInfo.IsGroupMunicipality/County/School District/StateofTexas"),
  * and they reach these code paths whenever a column is not renamed to its
  * canonical form — an extra column, or one whose alias did not match.
  *
  * Every framework site that references a column BY NAME therefore has to
  * quote it. These tests pin the helper and the RAW-path stampers that
  * broke without it.
  */
class DottedColumnNameTest extends AnyFunSuite with SharedSparkSession {

  import spark.implicits._

  /** The shape that actually failed in production. */
  private val Dotted = "AIAccInfo.IsGroupMunicipality/County/School District/StateofTexas"

  private def frame = Seq(("F1.pdf", "yes"), ("F2.pdf", "no"))
    .toDF("file_name", "v").withColumnRenamed("v", Dotted)

  test("bare col() cannot reference a dotted name — the defect being guarded") {
    val e = intercept[Exception](frame.select(col(Dotted)).collect())
    assert(String.valueOf(e.getMessage).toUpperCase.contains("UNRESOLVED"),
      s"expected an unresolved-column failure, got: ${e.getMessage}")
  }

  test("quotedCol references it as a single identifier") {
    val values = frame.select(ColumnMapping.quotedCol(Dotted)).collect().map(_.getString(0))
    assert(values.toSeq == Seq("yes", "no"))
  }

  test("quotedCol escapes an embedded backtick rather than breaking out of the quoting") {
    val weird = "od`d.name"
    val df = Seq(("x", "1")).toDF("a", "b").withColumnRenamed("b", weird)
    assert(ColumnMapping.quotedCol(weird) != null)
    assert(df.select(ColumnMapping.quotedCol(weird)).collect().head.getString(0) == "1")
  }

  test("record_hash stamps a frame whose columns carry dots") {
    // Without quoting this threw UNRESOLVED_COLUMN before any row was read.
    val hashed = RecordHash.stamp(frame, None)
    val hashes = hashed.select(RecordHash.Column).collect().map(_.getString(0))
    assert(hashes.length == 2)
    assert(hashes.forall(h => h != null && h.nonEmpty))
    assert(hashes.distinct.length == 2, "different content must hash differently")
  }

  test("extended lineage stamps a dotted source-modified column and key") {
    val stamped = RawMetadata.addExtended(frame,
      ExtendedLineage(
        sourceModifiedColumn = Some(Dotted),
        softDeleteIndicator = None,
        primaryKeyColumns = Seq("file_name", Dotted),
        nullFileId = false))
    val rows = stamped.select("source_modified_ts", "source_primary_key").collect()
    assert(rows.length == 2)
    assert(rows.forall(r => Option(r.getString(1)).exists(_.nonEmpty)),
      "the source key must be built from the dotted column, not fail on it")
  }

  test("null-key split references a dotted key without parsing the dot") {
    // CuratedTransform.splitNullKeys builds a predicate per business key.
    val (valid, dropped) = new CuratedTransform(spark).splitNullKeys(
      frame, Seq(Dotted), drop = true, blanks = true)
    assert(valid.count() == 2, "no row has a null value in that column")
    assert(dropped.count() == 0)
  }

  // NOTE: dedup/freshness ORDERING is deliberately not covered here. Those
  // strings come from operator config ("col desc nulls_last"), which uses
  // canonical snake_case names, and OrderSpec splits them on whitespace to
  // read the direction tokens. A raw source name never reaches that parser,
  // so a test forcing one there would assert a scenario the system cannot
  // produce.
}
