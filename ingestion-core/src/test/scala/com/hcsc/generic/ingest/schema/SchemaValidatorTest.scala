package com.hcsc.generic.ingest.schema

import com.typesafe.config.ConfigFactory
import org.apache.log4j.Logger
import org.scalatest.funsuite.AnyFunSuite

class SchemaValidatorTest extends AnyFunSuite {

  private val logger = Logger.getLogger(getClass.getName)

  private def contract(hocon: String): SchemaContract =
    SchemaContract.parse(ConfigFactory.parseString(hocon)).get

  private val base = contract(
    """schema {
      |  version = "1.0"
      |  columns = [
      |    { name = "subscriber_id", nullable = false, aliases = ["subscriber id"], position = 0 },
      |    { name = "hios_id", nullable = false, aliases = ["plan_hios_id"], position = 1 }
      |  ]
      |}""".stripMargin
  )

  private def kinds(violations: Seq[SchemaViolation]): Set[ViolationKind] =
    violations.map(_.kind).toSet

  // ---------------------------------------------------------------------------
  // validateHeaders
  // ---------------------------------------------------------------------------

  test("exact headers produce no violations and no renames") {
    val r = SchemaValidator.validateHeaders(Seq("subscriber_id", "hios_id"), base, logger)
    assert(r.violations.isEmpty)
    assert(r.renames.isEmpty)
  }

  test("aliased header is detected as a rename") {
    val r = SchemaValidator.validateHeaders(Seq("subscriber id", "plan_hios_id"), base, logger)
    assert(r.violations.isEmpty)
    assert(r.renames.toSet == Set("subscriber id" -> "subscriber_id", "plan_hios_id" -> "hios_id"))
  }

  test("missing required column is detected") {
    val r = SchemaValidator.validateHeaders(Seq("subscriber_id"), base, logger)
    assert(kinds(r.violations) == Set(ViolationKind.MissingColumn))
    assert(r.violations.head.message.contains("hios_id"))
  }

  test("unexpected column is detected") {
    val r = SchemaValidator.validateHeaders(Seq("subscriber_id", "hios_id", "surprise"), base, logger)
    assert(kinds(r.violations) == Set(ViolationKind.ExtraColumn))
    assert(r.violations.head.message.contains("surprise"))
  }

  test("column order change is detected") {
    val r = SchemaValidator.validateHeaders(Seq("hios_id", "subscriber_id"), base, logger)
    assert(kinds(r.violations) == Set(ViolationKind.OrderChange))
  }

  test("duplicate headers mapping to the same column are detected") {
    val r = SchemaValidator.validateHeaders(Seq("subscriber_id", "subscriber id", "hios_id"), base, logger)
    assert(kinds(r.violations).contains(ViolationKind.DuplicateHeader))
  }

  // ---------------------------------------------------------------------------
  // versionMismatch
  // ---------------------------------------------------------------------------

  test("versionMismatch detects differing stored version") {
    val v = SchemaValidator.versionMismatch(Some("0.9"), base)
    assert(kinds(v) == Set(ViolationKind.VersionMismatch))
  }

  test("versionMismatch passes for equal or absent stored version") {
    assert(SchemaValidator.versionMismatch(Some("1.0"), base).isEmpty)
    assert(SchemaValidator.versionMismatch(None, base).isEmpty)
  }

  // ---------------------------------------------------------------------------
  // enforce
  // ---------------------------------------------------------------------------

  test("enforce throws SchemaContractViolationException for FAIL violations") {
    val violations = Seq(SchemaViolation(ViolationKind.MissingColumn, "col 'x' missing"))
    val ex = intercept[SchemaContractViolationException] {
      SchemaValidator.enforce(violations, base.policies, logger)
    }
    assert(ex.violations.size == 1)
    assert(ex.getMessage.contains("missing_column"))
  }

  test("enforce does not throw for WARN violations") {
    val violations = Seq(SchemaViolation(ViolationKind.ExtraColumn, "col 'x' unexpected"))
    SchemaValidator.enforce(violations, base.policies, logger)
  }

  test("enforce respects IGNORE policy") {
    val ignoreAll = contract(
      """schema {
        |  version = "1.0"
        |  on_missing_column = "IGNORE"
        |  columns = [{ name = "a" }]
        |}""".stripMargin
    )
    val violations = Seq(SchemaViolation(ViolationKind.MissingColumn, "col 'x' missing"))
    SchemaValidator.enforce(violations, ignoreAll.policies, logger)
  }

  test("enforce aggregates multiple FAIL violations into one exception") {
    val violations = Seq(
      SchemaViolation(ViolationKind.MissingColumn, "col 'x' missing"),
      SchemaViolation(ViolationKind.DuplicateHeader, "dup header"),
      SchemaViolation(ViolationKind.ExtraColumn, "extra col")
    )
    val ex = intercept[SchemaContractViolationException] {
      SchemaValidator.enforce(violations, base.policies, logger)
    }
    // extra_column is WARN by default, so only the two FAIL kinds are carried
    assert(ex.violations.size == 2)
  }
}
