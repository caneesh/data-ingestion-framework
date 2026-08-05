package com.hcsc.generic.ingest.schema

import com.hcsc.generic.ingest.config.FeedCompatibilityValidator
import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite

/** Guard for the generated SmartIQ_PDP example (docs/examples/smartiq_pdp):
  * the 364-column contract must parse, carry the business decisions
  * (file_name key, last_modified_datetime freshness/watermark), and the
  * feed must pass compatibility validation — so the example cannot rot
  * against the framework. Skipped when the docs tree is absent. */
class SmartIqMappingConfigTest extends AnyFunSuite {

  private val feedFile = new java.io.File("../docs/examples/smartiq_pdp/feed-smartiq-pdp.conf")

  test("the generated SmartIQ_PDP feed parses and passes compatibility validation") {
    assume(feedFile.exists(), s"docs tree not present at ${feedFile.getAbsolutePath}")
    val feed = ConfigFactory.parseFile(feedFile).resolve().getConfig("feeds.smartiq_pdp")

    val contract = SchemaContract.parse(feed).getOrElse(fail("contract must parse"))
    assert(contract.columns.size == 364, s"expected 364 columns, got ${contract.columns.size}")
    assert(contract.businessKeyColumns == Seq("file_name"),
      "business decision: file_name is the unique key")
    assert(contract.incrementalColumns == Seq("last_modified_datetime"))
    assert(contract.sensitiveColumns.nonEmpty, "PII contact columns must be tagged")
    assert(contract.columns.count(_.transform.isDefined) >= 350,
      "the trim/empty->NULL cast rule must cover the varchar curated columns")
    // Typed exceptions to the all-string contract
    assert(contract.column("last_modified_datetime").exists(_.dataType == "timestamp"))
    assert(contract.column("effective_date").exists(_.dataType == "date"))

    // Aliases preserve source fidelity, including the collision pair the
    // workbook renames (*_incl_esn)
    val aliased = contract.columns.flatMap(_.aliases)
    assert(aliased.contains("FileName") && aliased.contains("LastModifiedDatetime"))

    val problems = FeedCompatibilityValidator.validate(feed)
    assert(problems.isEmpty, s"feed must pass CFG validation, got: ${problems.mkString("; ")}")
  }

  test("consumer request: ALL source columns reach raw AND curated (no drops)") {
    assume(feedFile.exists(), s"docs tree not present at ${feedFile.getAbsolutePath}")
    val feed = ConfigFactory.parseFile(feedFile).resolve().getConfig("feeds.smartiq_pdp")
    val contract = SchemaContract.parse(feed).getOrElse(fail("contract must parse"))

    // Every column is curated-bound: the varchar ones carry the
    // trim/empty->NULL transform, the 3 typed ones are the only exceptions.
    val typed = Set("last_modified_datetime", "current_date_time", "effective_date")
    val untransformed = contract.columns.filterNot(_.transform.isDefined).map(_.name).toSet
    assert(untransformed == typed,
      s"only the typed source columns may lack the cast rule, got: $untransformed")

    // The 7 columns the original business tab dropped are now carried, and
    // the submitter-identity ones are PII-tagged.
    val restored = Seq("file_name", "form_guid", "form", "user_email_id",
      "first_name", "last_name", "matrix")
    restored.foreach(c => assert(contract.column(c).isDefined, s"$c must be in the contract"))
    val sensitive = contract.sensitiveColumns.map(_.toLowerCase).toSet
    Seq("user_email_id", "first_name", "last_name").foreach(c =>
      assert(sensitive.contains(c), s"$c persists in curated and must be tagged PII"))

    // DDL parity — the real "all columns" guarantee: every contract column
    // must exist in BOTH physical tables.
    def ddlColumns(name: String): Set[String] = {
      val f = new java.io.File(s"../docs/examples/smartiq_pdp/$name")
      assume(f.exists(), s"missing $name")
      val src = scala.io.Source.fromFile(f)
      try src.getLines().flatMap(l => "^\\s*`([^`]+)`".r.findFirstMatchIn(l).map(_.group(1)))
        .toSet finally src.close()
    }
    val contractCols = contract.columns.map(_.name).toSet
    val rawDdl = ddlColumns("raw_ddl.sql")
    val curatedDdl = ddlColumns("curated_ddl.sql")
    assert((contractCols -- rawDdl).isEmpty,
      s"columns missing from raw DDL: ${(contractCols -- rawDdl).toSeq.sorted.take(10)}")
    assert((contractCols -- curatedDdl).isEmpty,
      s"columns missing from curated DDL: ${(contractCols -- curatedDdl).toSeq.sorted.take(10)}")

    // Merge keys and the freshness column must exist in the curated target,
    // else the merge fails at run time.
    assert(curatedDdl.contains("file_name") && curatedDdl.contains("last_modified_datetime"))
    // record_hash in the TARGET: without it the no-change skip and the
    // same-hash version advance silently never operate.
    assert(curatedDdl.contains("record_hash"))
    // Latest-per-key curated must not be partitioned (CUR_006 + churn).
    val curatedSrc = scala.io.Source.fromFile(
      new java.io.File("../docs/examples/smartiq_pdp/curated_ddl.sql"))
    val curatedStmt = try curatedSrc.getLines()
      .filterNot(_.trim.startsWith("--")) // comments explain the removal
      .mkString("\n") finally curatedSrc.close()
    assert(!curatedStmt.contains("PARTITIONED BY"),
      "the latest-per-key curated table must be unpartitioned")
  }
}
