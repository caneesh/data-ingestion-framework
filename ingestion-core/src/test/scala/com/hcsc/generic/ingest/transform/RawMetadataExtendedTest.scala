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

    // Non-delete rows are UPSERT by default: timestamp polling cannot
    // distinguish insert from update, so 'I' would be a lie.
    assert((rows(0)._1, rows(0)._2, rows(0)._3, rows(0)._5) ==
      (("K1", "2026-01-01 09:00:00", "UPSERT", null)))
    assert(rows(0)._4.startsWith("v1:") && rows(0)._4.length == 67,
      "source key is the versioned SHA-256 hash")
    assert((rows(1)._1, rows(1)._3) == (("K2", "D")),
      "an indicator match (case-insensitive) stamps operation D and file_id is nulled")
  }

  test("absent designations degrade to null / UPSERT and file feeds keep file_id") {
    val rows = stamped(ExtendedLineage(None, None, Seq.empty, nullFileId = false))
      .select("source_modified_ts", "source_operation", "source_primary_key", "file_id")
      .collect()
    assert(rows.forall(_.getString(0) == null))
    assert(rows.forall(_.getString(1) == "UPSERT"))
    assert(rows.forall(_.getString(2) == null))
    assert(rows.forall(_.getString(3) != null), "file feeds keep their real file_id")
  }

  test("composite source keys are collision-safe and versioned") {
    // The legacy '|' concat collapsed ["A|B","C"] and ["A","B|C"] into one
    // key and null into "". The v1 hash must keep them all distinct.
    val tricky = Seq(
      ("A|B", "C"), ("A", "B|C"), ("A", null.asInstanceOf[String]), ("A", "")
    ).toDF("k1", "k2")
    val keys = RawMetadata.addExtended(RawMetadata.add(tricky, "I", "r2"),
      ExtendedLineage(None, None, Seq("k1", "k2"), nullFileId = false))
      .select("source_primary_key").collect().map(_.getString(0))
    assert(keys.forall(k => k != null && k.startsWith("v1:")))
    assert(keys.distinct.length == 4,
      s"delimiter-in-value and null-vs-empty must all hash distinctly, got ${keys.toSeq}")

    // Stability: same rows, same hashes on re-execution
    val again = RawMetadata.addExtended(RawMetadata.add(tricky, "I", "r3"),
      ExtendedLineage(None, None, Seq("k1", "k2"), nullFileId = false))
      .select("source_primary_key").collect().map(_.getString(0))
    assert(keys.toSeq == again.toSeq, "the key hash must be deterministic across runs")

    // Column ORDER is part of the identity
    val reordered = RawMetadata.addExtended(RawMetadata.add(tricky, "I", "r4"),
      ExtendedLineage(None, None, Seq("k2", "k1"), nullFileId = false))
      .select("source_primary_key").collect().map(_.getString(0))
    assert(keys.toSeq != reordered.toSeq, "different column order is a different key")
  }

  test("legacy 'I' operation and readable key JSON are explicit opt-ins") {
    val legacy = stamped(ExtendedLineage(None, None, Seq("claim_id"), nullFileId = false,
      nonDeleteOperation = "I", keyJson = true))
    val row = legacy.filter($"claim_id" === "K1")
      .select("source_operation", "source_primary_key_json").collect().head
    assert(row.getString(0) == "I")
    assert(row.getString(1).contains("\"claim_id\":\"K1\""),
      "keyJson=true stores the readable canonical form alongside the hash")
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
