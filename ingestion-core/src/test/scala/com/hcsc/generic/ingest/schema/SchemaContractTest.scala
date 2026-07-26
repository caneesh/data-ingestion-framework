package com.hcsc.generic.ingest.schema

import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite

class SchemaContractTest extends AnyFunSuite {

  private val fullContract =
    """
      |schema {
      |  version = "2.1"
      |  compatibility = "BACKWARD"
      |  on_missing_column = "FAIL"
      |  on_extra_column = "WARN"
      |  on_type_change = "FAIL"
      |  on_order_change = "WARN"
      |  columns = [
      |    { name = "subscriber_id", type = "string", nullable = false,
      |      aliases = ["subscriber id", "subscriberid"], position = 0 },
      |    { name = "hios_id", type = "string", nullable = false,
      |      aliases = ["plan_hios_id", "hios id"], position = 1 }
      |  ]
      |}
    """.stripMargin

  test("parse returns None when no schema block is present") {
    val c = ConfigFactory.parseString("{}")
    assert(SchemaContract.parse(c).isEmpty)
  }

  test("parse reads version, compatibility, columns and policies") {
    val contract = SchemaContract.parse(ConfigFactory.parseString(fullContract)).get
    assert(contract.version == "2.1")
    assert(contract.compatibility == "BACKWARD")
    assert(contract.columnNames == Seq("subscriber_id", "hios_id"))
    assert(contract.policies.onMissingColumn == PolicyAction.Fail)
    assert(contract.policies.onExtraColumn == PolicyAction.Warn)
    assert(contract.policies.onTypeChange == PolicyAction.Fail)
    assert(contract.policies.onOrderChange == PolicyAction.Warn)
  }

  test("policies default to sensible values when not configured") {
    val c = ConfigFactory.parseString(
      """schema { version = "1.0", columns = [{ name = "a" }] }"""
    )
    val contract = SchemaContract.parse(c).get
    assert(contract.policies.onMissingColumn == PolicyAction.Fail)
    assert(contract.policies.onExtraColumn == PolicyAction.Warn)
    assert(contract.policies.onDuplicateHeader == PolicyAction.Fail)
    assert(contract.policies.onNullabilityViolation == PolicyAction.Fail)
    assert(contract.policies.onVersionMismatch == PolicyAction.Warn)
  }

  test("column defaults: type string, nullable true, no aliases, no position") {
    val c = ConfigFactory.parseString(
      """schema { version = "1.0", columns = [{ name = "a" }] }"""
    )
    val col = SchemaContract.parse(c).get.columns.head
    assert(col.dataType == "string")
    assert(col.nullable)
    assert(col.aliases.isEmpty)
    assert(col.position.isEmpty)
  }

  test("resolve matches canonical name, alias, and is case/whitespace insensitive") {
    val contract = SchemaContract.parse(ConfigFactory.parseString(fullContract)).get
    assert(contract.resolve("subscriber_id").contains("subscriber_id"))
    assert(contract.resolve("Subscriber ID ").contains("subscriber_id"))
    assert(contract.resolve("PLAN_HIOS_ID").contains("hios_id"))
    assert(contract.resolve("unknown_column").isEmpty)
  }

  test("positionalOrder sorts columns by declared position") {
    val c = ConfigFactory.parseString(
      """schema { version = "1.0", columns = [
        |  { name = "b", position = 1 },
        |  { name = "a", position = 0 }
        |]}""".stripMargin
    )
    assert(SchemaContract.parse(c).get.positionalOrder.map(_.name) == Seq("a", "b"))
  }

  test("positionalOrder throws when a column has no position") {
    val c = ConfigFactory.parseString(
      """schema { version = "1.0", columns = [
        |  { name = "a", position = 0 },
        |  { name = "b" }
        |]}""".stripMargin
    )
    intercept[IllegalArgumentException] {
      SchemaContract.parse(c).get.positionalOrder
    }
  }

  test("parse rejects empty columns") {
    val c = ConfigFactory.parseString("""schema { version = "1.0", columns = [] }""")
    intercept[IllegalArgumentException] { SchemaContract.parse(c) }
  }

  test("parse rejects duplicate column names") {
    val c = ConfigFactory.parseString(
      """schema { version = "1.0", columns = [{ name = "a" }, { name = "A" }] }"""
    )
    intercept[IllegalArgumentException] { SchemaContract.parse(c) }
  }

  test("parse rejects aliases owned by more than one column") {
    val c = ConfigFactory.parseString(
      """schema { version = "1.0", columns = [
        |  { name = "a", aliases = ["shared"] },
        |  { name = "b", aliases = ["shared"] }
        |]}""".stripMargin
    )
    intercept[IllegalArgumentException] { SchemaContract.parse(c) }
  }

  test("parse rejects duplicate positions") {
    val c = ConfigFactory.parseString(
      """schema { version = "1.0", columns = [
        |  { name = "a", position = 0 },
        |  { name = "b", position = 0 }
        |]}""".stripMargin
    )
    intercept[IllegalArgumentException] { SchemaContract.parse(c) }
  }

  test("parse rejects unknown policy action") {
    val c = ConfigFactory.parseString(
      """schema { version = "1.0", on_missing_column = "EXPLODE", columns = [{ name = "a" }] }"""
    )
    intercept[IllegalArgumentException] { SchemaContract.parse(c) }
  }

  test("PolicyAction.parse is case-insensitive") {
    assert(PolicyAction.parse("fail") == PolicyAction.Fail)
    assert(PolicyAction.parse("Warn") == PolicyAction.Warn)
    assert(PolicyAction.parse("IGNORE") == PolicyAction.Ignore)
  }
}
