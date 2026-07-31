package com.hcsc.generic.ingest.schema

import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite

/** The required-column snapshot uses ';' and ':' as separators — names or
  * types containing them must round-trip, never shear the snapshot into
  * false BACKWARD-compatibility breaks. */
class SchemaVersionsTest extends AnyFunSuite {

  private def contract(cols: String): SchemaContract =
    SchemaContract.parse(ConfigFactory.parseString(
      s"""schema { version = "1.0", columns = [ $cols ] }""")).get

  test("plain snapshots round-trip") {
    val c = contract(
      """{ name = "member_id", type = "string" }, { name = "amount", type = "decimal(10,2)" }""")
    val parsed = SchemaVersions.parseRequiredSnapshot(SchemaVersions.requiredSnapshot(c))
    assert(parsed == Map("member_id" -> "string", "amount" -> "decimal(10,2)"))
  }

  test("separator characters in names and types round-trip instead of shearing") {
    val c = contract(
      """{ name = "weird;name", type = "string" }, { name = "colon:name", type = "struct<a:int>" }""")
    val parsed = SchemaVersions.parseRequiredSnapshot(SchemaVersions.requiredSnapshot(c))
    assert(parsed == Map("weird;name" -> "string", "colon:name" -> "struct<a:int>"),
      s"snapshot must round-trip exactly, got $parsed")
  }

  test("legacy unescaped snapshots still parse") {
    assert(SchemaVersions.parseRequiredSnapshot("member_id:string;hios_id:string") ==
      Map("member_id" -> "string", "hios_id" -> "string"))
  }

  test("percent signs survive the escaping round-trip") {
    val c = contract("""{ name = "pct%3Bcol", type = "string" }""")
    val parsed = SchemaVersions.parseRequiredSnapshot(SchemaVersions.requiredSnapshot(c))
    assert(parsed == Map("pct%3Bcol" -> "string"))
  }
}
