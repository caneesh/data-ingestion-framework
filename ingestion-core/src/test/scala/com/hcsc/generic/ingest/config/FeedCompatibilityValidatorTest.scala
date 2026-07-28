package com.hcsc.generic.ingest.config

import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite

class FeedCompatibilityValidatorTest extends AnyFunSuite {

  private def errorsOf(hocon: String): Seq[String] =
    FeedCompatibilityValidator.validate(ConfigFactory.parseString(hocon))

  test("a coherent jdbc incremental feed validates cleanly") {
    assert(errorsOf(
      """source { type = "jdbc", extraction { strategy = "TIMESTAMP",
        |  boundary { columns = [ { name = "ts" } ], initial = "1900-01-01 00:00:00" } } }
        |curated { strategy = "TYPE1_MERGE", merge { keys = ["id"] } }
        |audit { enabled = true, reconciliation { on_mismatch = "FAIL" } }
      """.stripMargin).isEmpty)
  }

  test("extraction on a non-jdbc source is rejected (CFG_001)") {
    val errors = errorsOf("""source { type = "file", extraction { strategy = "TIMESTAMP" } }""")
    assert(errors.exists(_.contains("CFG_001")))
  }

  test("incremental extraction without a boundary is rejected (CFG_002)") {
    assert(errorsOf("""source { type = "jdbc", extraction { strategy = "INCREASING_KEY" } }""")
      .exists(_.contains("CFG_002")))
  }

  test("overlap on FULL_SNAPSHOT is contradictory (CFG_003)") {
    assert(errorsOf(
      """source { type = "jdbc", extraction { strategy = "FULL_SNAPSHOT",
        |  boundary { overlap = 300 } } }""".stripMargin)
      .exists(_.contains("CFG_003")))
  }

  test("CDC raw events with an append-only curated layer is rejected (CFG_004)") {
    assert(errorsOf(
      """source { type = "jdbc" }
        |raw { strategy = "CDC_EVENTS" }
        |curated { strategy = "APPEND" }""".stripMargin)
      .exists(_.contains("CFG_004")))
    // A keyed merge makes CDC coherent
    assert(errorsOf(
      """source { type = "jdbc" }
        |raw { strategy = "CDC_EVENTS" }
        |curated { strategy = "TYPE1_MERGE", merge { keys = ["id"] } }""".stripMargin)
      .isEmpty)
  }

  test("keyed merge without keys is rejected at startup (CFG_005)") {
    assert(errorsOf("""curated { strategy = "TYPE1_MERGE", merge { keys = [] } }""")
      .exists(_.contains("CFG_005")))
  }

  test("contract-derived rejects without a contract are rejected (CFG_006)") {
    assert(errorsOf("""rejects { use_contract_nullability = true }""")
      .exists(_.contains("CFG_006")))
  }

  test("failing reconciliation with audit disabled is rejected (CFG_007)") {
    assert(errorsOf(
      """audit { enabled = false, reconciliation { on_mismatch = "FAIL" } }""")
      .exists(_.contains("CFG_007")))
  }

  test("JDBC watermark blocks on file sources are rejected (CFG_008)") {
    assert(errorsOf(
      """source { type = "file", incremental { watermark_type = "TIMESTAMP" } }""")
      .exists(_.contains("CFG_008")))
  }

  test("multiple incompatibilities are all reported") {
    val errors = errorsOf(
      """source { type = "file", extraction { strategy = "TIMESTAMP" } }
        |curated { strategy = "TYPE1_MERGE", merge { keys = [] } }""".stripMargin)
    assert(errors.size >= 2)
  }
}
