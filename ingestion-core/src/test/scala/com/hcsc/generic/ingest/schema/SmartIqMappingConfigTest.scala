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
}
