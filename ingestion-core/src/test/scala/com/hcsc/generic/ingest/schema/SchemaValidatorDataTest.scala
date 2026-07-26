package com.hcsc.generic.ingest.schema

import com.hcsc.generic.ingest.transform.SharedSparkSession
import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite

class SchemaValidatorDataTest extends AnyFunSuite with SharedSparkSession {

  import spark.implicits._

  private def contract(hocon: String): SchemaContract =
    SchemaContract.parse(ConfigFactory.parseString(hocon)).get

  test("validateData passes when types match and non-nullable columns have no nulls") {
    val c = contract(
      """schema { version = "1.0", columns = [
        |  { name = "id", type = "string", nullable = false },
        |  { name = "amount", type = "int" }
        |]}""".stripMargin
    )
    val df = Seq(("a", 1), ("b", 2)).toDF("id", "amount")
    assert(SchemaValidator.validateData(df, c).isEmpty)
  }

  test("validateData detects a data-type change") {
    val c = contract(
      """schema { version = "1.0", columns = [{ name = "amount", type = "string" }] }"""
    )
    val df = Seq(1, 2).toDF("amount")
    val violations = SchemaValidator.validateData(df, c)
    assert(violations.map(_.kind) == Seq(ViolationKind.TypeChange))
    assert(violations.head.message.contains("amount"))
  }

  test("validateData detects nullability violations at runtime") {
    val c = contract(
      """schema { version = "1.0", columns = [{ name = "id", type = "string", nullable = false }] }"""
    )
    val df = Seq(Some("a"), None, None).toDF("id")
    val violations = SchemaValidator.validateData(df, c)
    assert(violations.map(_.kind) == Seq(ViolationKind.NullabilityViolation))
    assert(violations.head.message.contains("2 null value"))
  }

  test("validateData ignores contract columns absent from the DataFrame") {
    val c = contract(
      """schema { version = "1.0", columns = [
        |  { name = "id", type = "string" },
        |  { name = "not_here", type = "string", nullable = false }
        |]}""".stripMargin
    )
    val df = Seq("a").toDF("id")
    // Missing columns are a header-level violation, not a data-level one.
    assert(SchemaValidator.validateData(df, c).isEmpty)
  }

  test("validateData throws a clear error for an invalid contract type") {
    val c = contract(
      """schema { version = "1.0", columns = [{ name = "id", type = "not_a_type" }] }"""
    )
    val df = Seq("a").toDF("id")
    val ex = intercept[IllegalArgumentException] {
      SchemaValidator.validateData(df, c)
    }
    assert(ex.getMessage.contains("not_a_type"))
  }
}
