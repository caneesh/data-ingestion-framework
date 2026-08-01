package com.hcsc.generic.ingest.transform

import org.scalatest.funsuite.AnyFunSuite

class RawMetadataExtendedTest extends AnyFunSuite with SharedSparkSession {

  import spark.implicits._

  private val base = Seq(
    ("K1", "2026-01-01 09:00:00", "n"),
    ("K2", "2026-01-02 09:00:00", "Y")
  ).toDF("claim_id", "modified_ts", "deleted_flag")

  private def stamped(ext: ExtendedLineage) =
    RawMetadata.addExtended(RawMetadata.add(base, "I", "run-x"), ext)

  test("stamps source_modified_ts, source_operation and source_primary_key") {
    val rows = stamped(ExtendedLineage(
      sourceModifiedColumn = Some("modified_ts"),
      softDeleteIndicator = Some(("deleted_flag", Seq("y", "1", "true"))),
      primaryKeyColumns = Seq("claim_id"),
      nullFileId = true))
      .select("claim_id", "source_modified_ts", "source_operation", "source_primary_key", "file_id")
      .collect().map(r => (r.getString(0), r.getString(1), r.getString(2), r.getString(3), r.getString(4)))
      .sortBy(_._1)

    assert(rows(0) == (("K1", "2026-01-01 09:00:00", "I", "K1", null)))
    assert(rows(1) == (("K2", "2026-01-02 09:00:00", "D", "K2", null)),
      "an indicator match (case-insensitive) stamps operation D and file_id is nulled")
  }

  test("absent designations degrade to null / 'I' and file feeds keep file_id") {
    val rows = stamped(ExtendedLineage(None, None, Seq.empty, nullFileId = false))
      .select("source_modified_ts", "source_operation", "source_primary_key", "file_id")
      .collect()
    assert(rows.forall(_.getString(0) == null))
    assert(rows.forall(_.getString(1) == "I"))
    assert(rows.forall(_.getString(2) == null))
    assert(rows.forall(_.getString(3) != null), "file feeds keep their real file_id")
  }

  test("composite business keys concatenate with the pipe separator") {
    val row = stamped(ExtendedLineage(None, None, Seq("claim_id", "deleted_flag"), nullFileId = false))
      .filter($"claim_id" === "K1").select("source_primary_key").collect().head
    assert(row.getString(0) == "K1|n")
  }

  test("a source column colliding with an extended name fails fast (RAW_003)") {
    val colliding = Seq(("K1", "x")).toDF("claim_id", "source_operation")
    val ex = intercept[IllegalStateException] {
      RawMetadata.addExtended(RawMetadata.add(colliding, "I", "r"),
        ExtendedLineage(None, None, Seq.empty, nullFileId = false))
    }
    assert(ex.getMessage.contains("RAW_003"))
  }
}
