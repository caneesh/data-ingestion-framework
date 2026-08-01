package com.hcsc.generic.ingest.config

import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite

class IngestionPatternTest extends AnyFunSuite {

  private def derive(hocon: String) = IngestionPattern.derive(ConfigFactory.parseString(hocon))

  private def jdbc(mode: String, incremental: String = ""): String =
    s"""source { type = "jdbc", mode = "$mode", table = "t" $incremental }"""

  private val tsBlock =
    """, incremental { watermark_type = "TIMESTAMP", watermark_columns = ["ts"] }"""

  test("derivation matrix covers every implemented shape") {
    assert(derive("""source { type = "file" }""")._1.pattern == IngestionPattern.FullSnapshot)
    assert(derive(jdbc("FULL_TABLE"))._1.pattern == IngestionPattern.FullSnapshot)
    assert(derive(jdbc("FULL_TABLE", tsBlock))._1.pattern == IngestionPattern.FullThenIncremental)
    assert(derive(jdbc("INCREMENTAL", tsBlock))._1.pattern == IngestionPattern.TimestampIncremental)
    assert(derive(jdbc("INCREMENTAL",
      """, incremental { watermark_type = "TIMESTAMP", watermark_columns = ["ts"], overlap = "300" }"""))
      ._1.pattern == IngestionPattern.TimestampOverlap)
    assert(derive(jdbc("INCREMENTAL",
      """, incremental { watermark_type = "COMPOSITE", watermark_columns = ["ts","id"],
        |  column_types = ["TIMESTAMP","NUMERIC"] }""".stripMargin))
      ._1.pattern == IngestionPattern.CompositeWatermark)
    assert(derive(jdbc("INCREMENTAL",
      """, incremental { watermark_type = "ROWVERSION", watermark_columns = ["rv"] }"""))
      ._1.pattern == IngestionPattern.RowversionIncremental)
  }

  test("the decomposed axes are modeled separately") {
    val (spec, errors) = derive(
      jdbc("INCREMENTAL",
        """, incremental { watermark_type = "TIMESTAMP", watermark_columns = ["ts"],
          |  upper_bound = "SOURCE_CLOCK", clock_zone = "UTC" }""".stripMargin) +
        """
          |curated { merge { keys = ["id"], deletes { mode = "SOFT", indicator_column = "d" } } }
        """.stripMargin)
    assert(errors.isEmpty)
    assert(spec.extractionMode == "INCREMENTAL")
    assert(spec.watermarkStrategy == "TIMESTAMP")
    assert(spec.upperBoundStrategy == "SOURCE_CLOCK")
    assert(spec.rawPolicy == "APPEND")
    assert(spec.curatedStrategy == "KEYED_MERGE")
    assert(spec.deleteStrategy == "SOFT")
    assert(spec.watermarkCommitAllowed)
  }

  test("CHANGE_TRACKING and CDC_BATCH are explicit capability errors (CFG_010)") {
    Seq("CHANGE_TRACKING", "CDC_BATCH").foreach { p =>
      val errors = derive(jdbc("INCREMENTAL", tsBlock) + s"""
        ingestion { pattern = "$p" }""")._2
      assert(errors.exists(e => e.startsWith("CFG_010") && e.contains(p)), errors.mkString("; "))
    }
  }

  test("an explicit pattern contradicting the configured source fails (CFG_011)") {
    val errors = derive(jdbc("INCREMENTAL", tsBlock) + """
      ingestion { pattern = "FULL_SNAPSHOT" }""")._2
    assert(errors.exists(_.startsWith("CFG_011")))
    assert(derive(jdbc("INCREMENTAL", tsBlock) + """
      ingestion { pattern = "TIMESTAMP_INCREMENTAL" }""")._2.isEmpty,
      "a matching explicit pattern is legal")
    assert(derive(
      """source { type = "jdbc" }
        |ingestion { pattern = "NOT_A_PATTERN" }""".stripMargin)._2
      .exists(_.startsWith("CFG_011")))
  }

  test("BACKFILL and RAW_REPLAY default to watermark_commit=false") {
    val (backfill, e1) = derive(jdbc("INCREMENTAL", tsBlock) + """
      ingestion { pattern = "BACKFILL" }""")
    assert(e1.isEmpty && backfill.pattern == "BACKFILL" && !backfill.watermarkCommitAllowed)

    val (replay, _) = derive(jdbc("INCREMENTAL", tsBlock) + """
      ingestion { pattern = "RAW_REPLAY" }""")
    assert(!replay.watermarkCommitAllowed)

    // Explicit override makes a deliberate committing backfill possible
    val (committing, e2) = derive(jdbc("INCREMENTAL", tsBlock) + """
      ingestion { pattern = "BACKFILL", watermark_commit = true }""")
    assert(e2.isEmpty && committing.watermarkCommitAllowed)
  }

  test("BACKFILL clashing with watermark.advance_after fails (CFG_012)") {
    val errors = derive(jdbc("INCREMENTAL", tsBlock) + """
      ingestion { pattern = "BACKFILL" }
      watermark { advance_after = "RAW" }""")._2
    assert(errors.exists(_.startsWith("CFG_012")))
  }

  test("legacy feeds without an ingestion block derive cleanly with commit allowed") {
    val (spec, errors) = derive(jdbc("INCREMENTAL", tsBlock))
    assert(errors.isEmpty && spec.watermarkCommitAllowed)
    // and the validator raises no pattern errors for them
    assert(FeedCompatibilityValidator.validate(ConfigFactory.parseString(
      jdbc("INCREMENTAL", tsBlock) + """
        curated { merge { keys = ["id"] } }""")).isEmpty)
  }
}
