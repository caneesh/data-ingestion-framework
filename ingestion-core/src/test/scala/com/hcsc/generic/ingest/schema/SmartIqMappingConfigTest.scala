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

  private val feedFile = new java.io.File("../docs/examples/smartiq_pdp/params/feed-smartiq-pdp.conf")

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
      val f = new java.io.File(s"../docs/examples/smartiq_pdp/ddl/$name")
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

    // The DDLs must create the EXACT tables the feed reads and writes —
    // a prefixed DDL name (raw_smartiq_pdp) against a feed pointing at
    // smartiq_pdp leaves the pre-created table unused and the framework
    // silently creating its own.
    def ddlTable(name: String): String = {
      val f = new java.io.File(s"../docs/examples/smartiq_pdp/ddl/$name")
      val src = scala.io.Source.fromFile(f)
      try src.getLines()
        .flatMap("(?i)CREATE\\s+EXTERNAL\\s+TABLE\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?(\\S+)\\s*\\(".r
          .findFirstMatchIn(_).map(_.group(1)))
        .next() finally src.close()
    }
    assert(ddlTable("raw_ddl.sql") ==
      s"${feed.getString("raw.database")}.${feed.getString("raw.table")}",
      "raw DDL must create exactly the table the feed writes")
    assert(ddlTable("curated_ddl.sql") ==
      s"${feed.getString("curated.database")}.${feed.getString("curated.table")}",
      "curated DDL must create exactly the table the feed publishes")

    // Merge keys and the freshness column must exist in the curated target,
    // else the merge fails at run time.
    assert(curatedDdl.contains("file_name") && curatedDdl.contains("last_modified_datetime"))
    // record_hash in the TARGET: without it the no-change skip and the
    // same-hash version advance silently never operate.
    assert(curatedDdl.contains("record_hash"))
    // Latest-per-key curated must not be partitioned (CUR_006 + churn).
    val curatedSrc = scala.io.Source.fromFile(
      new java.io.File("../docs/examples/smartiq_pdp/ddl/curated_ddl.sql"))
    val curatedStmt = try curatedSrc.getLines()
      .filterNot(_.trim.startsWith("--")) // comments explain the removal
      .mkString("\n") finally curatedSrc.close()
    assert(!curatedStmt.contains("PARTITIONED BY"),
      "the latest-per-key curated table must be unpartitioned")
  }

  test("lower-env E2E feed is a faithful subset of the production contract") {
    val e2eFile = new java.io.File(
      "../docs/examples/smartiq_pdp/lower-env/params/feed-smartiq-pdp-e2e.conf")
    assume(e2eFile.exists() && feedFile.exists(), "docs tree not present")
    val e2e = ConfigFactory.parseFile(e2eFile).resolve().getConfig("feeds.smartiq_pdp_e2e")
    val prod = ConfigFactory.parseFile(feedFile).resolve().getConfig("feeds.smartiq_pdp")

    val sub = SchemaContract.parse(e2e).getOrElse(fail("e2e contract must parse"))
    val full = SchemaContract.parse(prod).getOrElse(fail("prod contract must parse"))
    assert(sub.columns.size == 11, s"expected the 11-column subset, got ${sub.columns.size}")

    // SUBSET, not variant: every e2e column must match the production
    // contract's definition exactly (name, type, aliases, transform, PII).
    sub.columns.foreach { c =>
      val p = full.column(c.name).getOrElse(fail(s"${c.name} is not in the production contract"))
      assert(c.dataType == p.dataType, s"${c.name}: type drifted from production")
      assert(c.aliases == p.aliases, s"${c.name}: aliases drifted from production")
      assert(c.transform == p.transform, s"${c.name}: transform drifted from production")
      assert(c.sensitivity == p.sensitivity, s"${c.name}: sensitivity drifted from production")
    }

    // The mechanisms the subset exists to exercise
    assert(sub.businessKeyColumns == Seq("file_name"))
    assert(sub.incrementalColumns == Seq("last_modified_datetime"))
    assert(sub.sensitiveColumns.map(_.toLowerCase).contains("user_email_id"),
      "the PII column must stay tagged or the masking scenario proves nothing")
    Seq("group_number", "ai_groupand_ba_numbers_section_number").foreach(c =>
      assert(sub.column(c).isDefined, s"$c is needed for the composite-key scenario"))

    // Isolation from production, and DDL/table agreement
    assert(e2e.getString("entity") == "smartiq_pdp_e2e")
    assert(e2e.getString("raw.table") != prod.getString("raw.table"),
      "the test feed must not write the production raw table")
    assert(e2e.getString("curated.table") != prod.getString("curated.table"),
      "the test feed must not publish over the production curated table")

    def ddlCols(name: String): Set[String] = {
      val src = scala.io.Source.fromFile(
        new java.io.File(s"../docs/examples/smartiq_pdp/lower-env/ddl/$name"))
      try src.getLines().flatMap("^\\s*`([^`]+)`".r.findFirstMatchIn(_).map(_.group(1))).toSet
      finally src.close()
    }
    val names = sub.columns.map(_.name).toSet
    assert((names -- ddlCols("raw_ddl_e2e.sql")).isEmpty)
    assert((names -- ddlCols("curated_ddl_e2e.sql")).isEmpty)
    assert(ddlCols("curated_ddl_e2e.sql").contains("record_hash"))

    assert(FeedCompatibilityValidator.validate(e2e).isEmpty,
      s"e2e feed must pass CFG validation: ${FeedCompatibilityValidator.validate(e2e).mkString("; ")}")
  }
}
