package com.hcsc.generic.ingest.transform

import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite

class PartitioningTest extends AnyFunSuite with SharedSparkSession {

  // ---------------------------------------------------------------------------
  // Partitioning.parse — no Spark needed
  // ---------------------------------------------------------------------------

  test("parse returns empty spec when no partitioning block exists") {
    val c = ConfigFactory.parseString("{}")
    val spec = Partitioning.parse(c)
    assert(spec.keys.isEmpty)
    assert(spec.expressions.isEmpty)
  }

  test("parse returns keys only when derive is absent") {
    val c = ConfigFactory.parseString("""partitioning { keys = ["year", "month"] }""")
    val spec = Partitioning.parse(c)
    assert(spec.keys == Seq("year", "month"))
    assert(spec.expressions.isEmpty)
  }

  test("parse derive with inline string expression") {
    val c = ConfigFactory.parseString(
      """partitioning {
        |  keys = ["dt"]
        |  derive { dt = "date_format(current_date(), 'yyyy-MM-dd')" }
        |}""".stripMargin)
    val spec = Partitioning.parse(c)
    assert(spec.keys == Seq("dt"))
    assert(spec.expressions("dt") == "date_format(current_date(), 'yyyy-MM-dd')")
  }

  test("parse derive with object kind=expr") {
    val c = ConfigFactory.parseString(
      """partitioning {
        |  keys = ["region"]
        |  derive { region { kind = expr, expr = "upper(region_raw)" } }
        |}""".stripMargin)
    val spec = Partitioning.parse(c)
    assert(spec.expressions("region") == "upper(region_raw)")
  }

  test("parse derive with object kind=literal produces single-quoted value") {
    val c = ConfigFactory.parseString(
      """partitioning {
        |  keys = ["src"]
        |  derive { src { kind = literal, value = "BATCH" } }
        |}""".stripMargin)
    val spec = Partitioning.parse(c)
    assert(spec.expressions("src") == "'BATCH'")
  }

  test("parse derive with kind=literal escapes single quotes in value") {
    val c = ConfigFactory.parseString(
      """partitioning {
        |  keys = ["lbl"]
        |  derive { lbl { kind = literal, value = "it's" } }
        |}""".stripMargin)
    val spec = Partitioning.parse(c)
    // single quote in value should become ''
    assert(spec.expressions("lbl") == "'it''s'")
  }

  test("parse throws IllegalArgumentException for unsupported kind") {
    val c = ConfigFactory.parseString(
      """partitioning {
        |  keys = ["x"]
        |  derive { x { kind = unknown_kind, value = "v" } }
        |}""".stripMargin)
    val ex = intercept[IllegalArgumentException] {
      Partitioning.parse(c)
    }
    assert(ex.getMessage.contains("unknown_kind"))
  }

  // ---------------------------------------------------------------------------
  // Partitioning.apply — Spark required
  // ---------------------------------------------------------------------------

  test("apply with derive expression adds new column to DataFrame") {
    import spark.implicits._
    val df = Seq(("Alice", 10), ("Bob", 20)).toDF("name", "value")
    val spec = PartitionSpec(keys = Seq("tag"), expressions = Map("tag" -> "'static'"))
    val result = Partitioning(df, spec)
    assert(result.columns.contains("tag"))
    val tags = result.select("tag").as[String].collect().toSet
    assert(tags == Set("static"))
  }

  test("apply leaves DataFrame unchanged when key already exists and no derive") {
    import spark.implicits._
    val df = Seq(("Alice", "US")).toDF("name", "country")
    val spec = PartitionSpec(keys = Seq("country"), expressions = Map.empty)
    val result = Partitioning(df, spec)
    assert(result.columns.toSeq == Seq("name", "country"))
  }

  test("apply with derive expression overwrites existing column value") {
    import spark.implicits._
    val df = Seq(("Alice", "us")).toDF("name", "country")
    // derive replaces whatever is in the column
    val spec = PartitionSpec(keys = Seq("country"), expressions = Map("country" -> "upper(country)"))
    val result = Partitioning(df, spec)
    val values = result.select("country").as[String].collect().toSet
    assert(values == Set("US"))
  }

  test("apply throws IllegalArgumentException when key missing with no derive") {
    import spark.implicits._
    val df = Seq(("Alice", 1)).toDF("name", "value")
    val spec = PartitionSpec(keys = Seq("missing_key"), expressions = Map.empty)
    val ex = intercept[IllegalArgumentException] {
      Partitioning(df, spec)
    }
    assert(ex.getMessage.contains("missing_key"))
  }

  test("apply is a no-op when spec has no keys") {
    import spark.implicits._
    val df = Seq(("Alice", 1)).toDF("name", "value")
    val spec = PartitionSpec(keys = Seq.empty, expressions = Map.empty)
    val result = Partitioning(df, spec)
    assert(result.columns.toSeq == Seq("name", "value"))
  }
}
