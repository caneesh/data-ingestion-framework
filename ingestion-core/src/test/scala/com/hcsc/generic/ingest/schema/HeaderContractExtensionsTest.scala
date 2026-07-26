package com.hcsc.generic.ingest.schema

import com.typesafe.config.ConfigFactory
import org.apache.log4j.Logger
import org.scalatest.funsuite.AnyFunSuite

class HeaderContractExtensionsTest extends AnyFunSuite {

  private val logger = Logger.getLogger(getClass.getName)

  private def parse(hocon: String): SchemaContract =
    SchemaContract.parse(ConfigFactory.parseString(hocon)).get

  test("columns default to required=true, business category, no default") {
    val c = parse("""schema { version = "1.0", columns = [{ name = "a" }] }""").columns.head
    assert(c.required)
    assert(c.category == "business")
    assert(c.default.isEmpty)
    assert(c.validation.isEmpty)
  }

  test("optional column with null and non-null defaults parse") {
    val contract = parse(
      """schema { version = "1.0", columns = [
        |  { name = "a" },
        |  { name = "middle_name", required = false, default = null },
        |  { name = "status", required = false, default = "UNKNOWN" }
        |]}""".stripMargin)
    assert(contract.requiredColumns.map(_.name) == Seq("a"))
    assert(contract.optionalColumns.map(_.name) == Seq("middle_name", "status"))
    assert(contract.column("middle_name").get.default.isEmpty)
    assert(contract.column("status").get.default.contains("UNKNOWN"))
  }

  test("content validation rules parse per column") {
    val c = parse(
      """schema { version = "1.0", columns = [
        |  { name = "hios_id", regex = "^[0-9]{5}[A-Z][0-9]{4}$", min_length = 10,
        |    max_length = 10, allowed_values = [], nonblank = true }
        |]}""".stripMargin).columns.head
    val v = c.validation.get
    assert(v.regex.contains("^[0-9]{5}[A-Z][0-9]{4}$"))
    assert(v.minLength.contains(10))
    assert(v.maxLength.contains(10))
    assert(v.nonblank)
  }

  test("strategies parse and unknown strategy is rejected") {
    def strategyOf(s: String) = parse(
      s"""schema { version = "1.0", columns = [{ name = "a" }]
         |  header_validation { strategy = "$s" }
         |}""".stripMargin).strategy

    assert(strategyOf("STRICT_NAME") == HeaderStrategy.StrictName)
    assert(strategyOf("NAME_WITH_ALIASES") == HeaderStrategy.NameWithAliases)
    assert(strategyOf("NAME_ALIAS_POSITION_FALLBACK") == HeaderStrategy.NameAliasPositionFallback)
    intercept[IllegalArgumentException] { strategyOf("GUESS") }
  }

  test("header_validation keys map onto policies and quarantine flag") {
    val contract = parse(
      """schema { version = "1.0", columns = [{ name = "a" }]
        |  header_validation {
        |    on_missing_required = "FAIL"
        |    on_extra_columns = "IGNORE"
        |    on_duplicate_columns = "WARN"
        |    on_missing_optional = "FAIL"
        |    quarantine_on_failure = true
        |  }
        |}""".stripMargin)
    assert(contract.policies.onMissingColumn == PolicyAction.Fail)
    assert(contract.policies.onExtraColumn == PolicyAction.Ignore)
    assert(contract.policies.onDuplicateHeader == PolicyAction.Warn)
    assert(contract.policies.failOnMissingOptional)
    assert(contract.quarantineOnFailure)
  }

  test("positional_fallback and content_validation blocks parse") {
    val contract = parse(
      """schema { version = "1.0", columns = [{ name = "a", position = 0 }]
        |  header_validation {
        |    strategy = "NAME_ALIAS_POSITION_FALLBACK"
        |    positional_fallback {
        |      enabled = true
        |      require_exact_column_count = true
        |      require_content_validation = true
        |    }
        |  }
        |  content_validation {
        |    enabled = true
        |    mode = "SAMPLE"
        |    sample_rows = 500
        |    maximum_failure_percentage = 1.5
        |  }
        |}""".stripMargin)
    assert(contract.positionalFallback.enabled)
    assert(contract.positionalFallback.requireContentValidation)
    assert(contract.contentValidation.enabled)
    assert(contract.contentValidation.sampleRows == 500)
    assert(contract.contentValidation.maxFailurePercentage == 1.5)
  }

  test("STRICT_NAME resolves canonical names but not aliases") {
    val contract = parse(
      """schema { version = "1.0", columns = [
        |  { name = "hios_id", aliases = ["plan hios id"] }
        |]
        |  header_validation { strategy = "STRICT_NAME" }
        |}""".stripMargin)
    assert(contract.resolve("HIOS-ID").contains("hios_id"))
    assert(contract.resolve("Plan HIOS ID").isEmpty)
  }

  test("alias matching uses full normalization (Plan HIOS-ID -> hios_id)") {
    val contract = parse(
      """schema { version = "1.0", columns = [
        |  { name = "hios_id", aliases = ["plan hios id"] }
        |]}""".stripMargin)
    assert(contract.resolve("Plan HIOS-ID").contains("hios_id"))
    assert(contract.resolve(" PLAN.HIOS.ID ").contains("hios_id"))
  }

  test("missing optional columns are not violations; missing required are HDR_001") {
    val contract = parse(
      """schema { version = "1.0", columns = [
        |  { name = "subscriber_id" },
        |  { name = "middle_name", required = false }
        |]}""".stripMargin)

    val ok = SchemaValidator.validateHeaders(Seq("subscriber_id"), contract, logger)
    assert(ok.valid)
    assert(ok.missingOptional.map(_.name) == Seq("middle_name"))

    val bad = SchemaValidator.validateHeaders(Seq("middle_name"), contract, logger)
    assert(bad.missingRequired == Seq("subscriber_id"))
    assert(bad.violations.exists(v => v.kind == ViolationKind.MissingColumn && v.toString.contains("HDR_001")))
  }

  test("violation kinds carry stable HDR error codes") {
    assert(ViolationKind.MissingColumn.code == "HDR_001")
    assert(ViolationKind.DuplicatePhysicalHeader.code == "HDR_002")
    assert(ViolationKind.DuplicateHeader.code == "HDR_003")
    assert(ViolationKind.AmbiguousMapping.code == "HDR_004")
    assert(ViolationKind.MultipleSourcesOneCanonical.code == "HDR_005")
    assert(ViolationKind.ExtraColumn.code == "HDR_006")
    assert(ViolationKind.CountMismatch.code == "HDR_007")
    assert(ViolationKind.InvalidPositional.code == "HDR_008")
    assert(ViolationKind.DelimiterMismatch.code == "HDR_009")
    assert(ViolationKind.BlankHeader.code == "HDR_010")
    assert(ViolationKind.MalformedQuote.code == "HDR_011")
    assert(ViolationKind.UnsupportedEncoding.code == "HDR_012")
    assert(ViolationKind.HeaderOnlyFile.code == "HDR_013")
    assert(ViolationKind.VersionMismatch.code == "HDR_014")
    assert(ViolationKind.ContentValidation.code == "HDR_015")
    assert(ViolationKind.RepeatedHeader.code == "HDR_016")
    assert(ViolationKind.ContractCollision.code == "HDR_017")
    assert(ViolationKind.CuratedContract.code == "HDR_018")
  }

  test("distinct headers resolving to one canonical column fail with HDR_005") {
    val contract = parse(
      """schema { version = "1.0", columns = [
        |  { name = "hios_id", aliases = ["plan_hios_id"] }
        |]}""".stripMargin)
    val r = SchemaValidator.validateHeaders(Seq("hios_id", "plan_hios_id"), contract, logger)
    assert(r.violations.exists(_.kind == ViolationKind.MultipleSourcesOneCanonical))
    assert(r.violations.exists(_.toString.contains("HDR_005")))
  }

  test("object-form aliases parse and expired aliases are excluded") {
    val contract = parse(
      """schema { version = "1.0", columns = [
        |  { name = "hios_id", aliases = [
        |    { value = "plan_hios_id", effective_from = "2020-01-01", approval_reference = "CHG1" },
        |    { value = "old_hios", valid_until = "2020-12-31" },
        |    "hios id"
        |  ]}
        |]}""".stripMargin)
    assert(contract.resolve("plan_hios_id").contains("hios_id"))
    assert(contract.resolve("hios id").contains("hios_id"))
    assert(contract.resolve("old_hios").isEmpty, "expired alias must not resolve")
  }

  test("config collisions fail at startup with HDR_017") {
    // canonical name of one column colliding with another column's alias
    val ex = intercept[IllegalArgumentException] {
      parse(
        """schema { version = "1.0", columns = [
          |  { name = "member_id" },
          |  { name = "member_number", aliases = ["member id"] }
          |]}""".stripMargin)
    }
    assert(ex.getMessage.contains("HDR_017"))

    val negative = intercept[IllegalArgumentException] {
      parse("""schema { version = "1.0", columns = [{ name = "a", position = -1 }] }""")
    }
    assert(negative.getMessage.contains("HDR_017"))

    val badType = intercept[IllegalArgumentException] {
      parse("""schema { version = "1.0", columns = [{ name = "a", type = "nope" }] }""")
    }
    assert(badType.getMessage.contains("HDR_017"))
  }
}
