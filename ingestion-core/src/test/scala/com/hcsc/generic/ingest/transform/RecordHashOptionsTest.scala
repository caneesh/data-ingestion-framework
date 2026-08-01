package com.hcsc.generic.ingest.transform

import org.scalatest.funsuite.AnyFunSuite

class RecordHashOptionsTest extends AnyFunSuite with SharedSparkSession {

  import spark.implicits._

  private def hashOf(value: String, options: RecordHashOptions): String =
    RecordHash.stamp(Seq(Tuple1(value)).toDF("name"), None, options)
      .select(RecordHash.Column).collect().head.getString(0)

  test("recipe version embeds the active canonicalization options") {
    assert(RecordHash.recipeVersion(RecordHashOptions()) == "1")
    assert(RecordHash.recipeVersion(RecordHashOptions(trimValues = true)) == "1+trim")
    assert(RecordHash.recipeVersion(RecordHashOptions(uppercaseValues = true)) == "1+upper")
    assert(RecordHash.recipeVersion(RecordHashOptions(trimValues = true, uppercaseValues = true)) ==
      "1+trim+upper")
  }

  test("default recipe hashes values verbatim (wire-compatible with existing tables)") {
    assert(hashOf(" X ", RecordHashOptions()) != hashOf("x", RecordHashOptions()))
  }

  test("trim + uppercase canonicalization makes equivalent values hash identically") {
    val canonical = RecordHashOptions(trimValues = true, uppercaseValues = true)
    assert(hashOf(" x ", canonical) == hashOf("X", canonical))
    assert(hashOf(" x ", canonical) != hashOf("Y", canonical))
    // and the canonicalized recipe differs from the default recipe
    assert(hashOf("X", canonical) == hashOf("X", RecordHashOptions()),
      "an already-canonical value hashes the same under both recipes")
  }

  test("null stays distinct from empty and whitespace under canonicalization") {
    val canonical = RecordHashOptions(trimValues = true)
    val nullHash = RecordHash.stamp(Seq(Tuple1(null.asInstanceOf[String])).toDF("name"), None, canonical)
      .select(RecordHash.Column).collect().head.getString(0)
    assert(nullHash != hashOf("", canonical))
    assert(hashOf("   ", canonical) == hashOf("", canonical),
      "trim canonicalization deliberately equates whitespace-only and empty")
  }
}
