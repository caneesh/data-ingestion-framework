package com.hcsc.generic.ingest.transform

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

class CuratedTransformTest extends AnyFunSuite with SharedSparkSession {

  private def transform = new CuratedTransform(spark)

  // ---------------------------------------------------------------------------
  // castConfigured
  // ---------------------------------------------------------------------------

  test("castConfigured casts a configured column to the specified type") {
    import spark.implicits._
    val df = Seq(("123", "45.6")).toDF("id", "amount")
    val conf = ConfigFactory.parseString("""column_types { id = "int", amount = "double" }""")
    val result = transform.castConfigured(df, conf)
    assert(result.schema("id").dataType == IntegerType)
    assert(result.schema("amount").dataType == DoubleType)
  }

  test("castConfigured is case-insensitive for column name matching") {
    import spark.implicits._
    // Use Seq of String to avoid 1-tuple trailing-comma parse issues
    val df = Seq("99").toDF("MyCol")
    val conf = ConfigFactory.parseString("""column_types { mycol = "int" }""")
    val result = transform.castConfigured(df, conf)
    val field = result.schema.fields.find(_.name.equalsIgnoreCase("mycol"))
    assert(field.isDefined)
    assert(field.get.dataType == IntegerType)
  }

  test("castConfigured ignores column names not found in DataFrame") {
    import spark.implicits._
    val df = Seq("hello").toDF("name")
    val conf = ConfigFactory.parseString("""column_types { nonexistent = "int" }""")
    val result = transform.castConfigured(df, conf)
    assert(result.columns.toSeq == Seq("name"))
  }

  test("castConfigured returns original DataFrame when column_types absent") {
    import spark.implicits._
    val df = Seq(("a", 1)).toDF("name", "value")
    val conf = ConfigFactory.parseString("{}")
    val result = transform.castConfigured(df, conf)
    assert(result.columns.toSeq == Seq("name", "value"))
  }

  // ---------------------------------------------------------------------------
  // applyTransforms
  // ---------------------------------------------------------------------------

  test("applyTransforms derives a new column from an expression") {
    import spark.implicits._
    val df = Seq((10, 2)).toDF("a", "b")
    val conf = ConfigFactory.parseString(
      """transform.derive = [{name = "product", expr = "a * b"}]""")
    val result = transform.applyTransforms(df, conf)
    assert(result.columns.contains("product"))
    val products = result.select("product").as[Int].collect()
    assert(products.toSeq == Seq(20))
  }

  test("applyTransforms applies multiple derive steps in order") {
    import spark.implicits._
    val df = Seq(5).toDF("x")
    val conf = ConfigFactory.parseString(
      """transform.derive = [
        |  {name = "y", expr = "x + 1"},
        |  {name = "z", expr = "y * 2"}
        |]""".stripMargin)
    val result = transform.applyTransforms(df, conf)
    val row = result.select("z").as[Int].collect().head
    assert(row == 12) // (5+1)*2
  }

  test("applyTransforms is a no-op when transform.derive is absent") {
    import spark.implicits._
    val df = Seq(("a", 1)).toDF("name", "value")
    val conf = ConfigFactory.parseString("{}")
    val result = transform.applyTransforms(df, conf)
    assert(result.columns.toSeq == Seq("name", "value"))
  }

  // ---------------------------------------------------------------------------
  // ensureAudit
  // ---------------------------------------------------------------------------

  test("ensureAudit adds create_timestamp when absent") {
    import spark.implicits._
    val df = Seq("x").toDF("name")
    val result = transform.ensureAudit(df)
    assert(result.columns.contains("create_timestamp"))
  }

  test("ensureAudit adds last_modified_ts when absent") {
    import spark.implicits._
    val df = Seq("x").toDF("name")
    val result = transform.ensureAudit(df)
    assert(result.columns.contains("last_modified_ts"))
  }

  test("ensureAudit adds last_modified_op = 'I' when absent") {
    import spark.implicits._
    val df = Seq("x").toDF("name")
    val result = transform.ensureAudit(df)
    assert(result.columns.contains("last_modified_op"))
    val ops = result.select("last_modified_op").as[String].collect().toSet
    assert(ops == Set("I"))
  }

  test("ensureAudit preserves existing create_timestamp column") {
    import spark.implicits._
    val df = Seq(("x", "2020-01-01")).toDF("name", "create_timestamp")
    val result = transform.ensureAudit(df)
    val existing = result.select("create_timestamp").as[String].collect().head
    assert(existing == "2020-01-01")
  }

  test("ensureAudit preserves existing last_modified_ts column") {
    import spark.implicits._
    val df = Seq(("x", "2020-06-01")).toDF("name", "last_modified_ts")
    val result = transform.ensureAudit(df)
    assert(result.columns.count(_.equalsIgnoreCase("last_modified_ts")) == 1)
    val val1 = result.select("last_modified_ts").as[String].collect().head
    assert(val1 == "2020-06-01")
  }

  test("ensureAudit preserves existing last_modified_op column") {
    import spark.implicits._
    val df = Seq(("x", "D")).toDF("name", "last_modified_op")
    val result = transform.ensureAudit(df)
    val ops = result.select("last_modified_op").as[String].collect().toSet
    assert(ops == Set("D"))
  }

  // ---------------------------------------------------------------------------
  // normalizeKeys
  // ---------------------------------------------------------------------------

  test("normalizeKeys applies expression to matching column (case-insensitive)") {
    import spark.implicits._
    val df = Seq(" Alice ").toDF("Name")
    val conf = ConfigFactory.parseString("""merge.normalize { name = "trim(Name)" }""")
    val result = transform.normalizeKeys(df, conf)
    val names = result.select("Name").as[String].collect().toSet
    assert(names == Set("Alice"))
  }

  test("normalizeKeys is a no-op when merge.normalize absent") {
    import spark.implicits._
    val df = Seq("Alice").toDF("name")
    val conf = ConfigFactory.parseString("{}")
    val result = transform.normalizeKeys(df, conf)
    assert(result.columns.toSeq == Seq("name"))
  }

  // ---------------------------------------------------------------------------
  // filterNullKeys
  // ---------------------------------------------------------------------------

  test("filterNullKeys drops rows with null key values when drop=true") {
    import spark.implicits._
    val df = Seq(Some("Alice"), None).toDF("name")
    val result = transform.filterNullKeys(df, Seq("name"), drop = true, blanks = false)
    assert(result.count() == 1)
    assert(result.select("name").as[String].collect().head == "Alice")
  }

  test("filterNullKeys drops rows with blank key values when drop=true and blanks=true") {
    import spark.implicits._
    val df = Seq("Alice", "  ", "Bob", "").toDF("name")
    val result = transform.filterNullKeys(df, Seq("name"), drop = true, blanks = true)
    val names = result.select("name").as[String].collect().toSet
    assert(names == Set("Alice", "Bob"))
  }

  test("filterNullKeys keeps blank rows when blanks=false") {
    import spark.implicits._
    val df = Seq("Alice", "  ", "Bob").toDF("name")
    val result = transform.filterNullKeys(df, Seq("name"), drop = true, blanks = false)
    // Only true nulls should be dropped; blank strings are kept
    assert(result.count() == 3)
  }

  test("filterNullKeys is no-op when drop=false") {
    import spark.implicits._
    val df = Seq(Some("Alice"), None).toDF("name")
    val result = transform.filterNullKeys(df, Seq("name"), drop = false, blanks = false)
    assert(result.count() == 2)
  }

  test("filterNullKeys is no-op when keys list is empty") {
    import spark.implicits._
    val df = Seq(Some("Alice"), None).toDF("name")
    val result = transform.filterNullKeys(df, Seq.empty, drop = true, blanks = false)
    assert(result.count() == 2)
  }

  // ---------------------------------------------------------------------------
  // deduplicate
  // ---------------------------------------------------------------------------

  test("deduplicate drops exact duplicates using dropDuplicates when no orderBy cols") {
    import spark.implicits._
    val df = Seq(
      ("A", 1),
      ("A", 1),
      ("B", 2)
    ).toDF("key", "value")
    val result = transform.deduplicate(df, Seq("key", "value"), orderBy = Seq.empty)
    assert(result.count() == 2)
  }

  test("deduplicate keeps latest row by order_by column when present") {
    import spark.implicits._
    val df = Seq(
      ("A", 100, 1),
      ("A", 200, 2),   // higher ts => latest
      ("B", 300, 3)
    ).toDF("key", "amount", "ts")
    val result = transform.deduplicate(df, Seq("key"), orderBy = Seq("ts"))
    assert(result.count() == 2)
    val aRow = result.filter(result("key") === "A").select("amount").as[Int].collect().head
    assert(aRow == 200)
  }

  test("deduplicate breaks order_by ties deterministically via row_idx when present") {
    import spark.implicits._
    // Both A-rows tie on ts; row_idx must decide (later-read row wins),
    // making the winner stable across replays and partition layouts.
    val df = Seq(
      ("A", "first", 100, 1L),
      ("A", "second", 100, 2L),
      ("B", "only", 300, 3L)
    ).toDF("key", "payload", "ts", "row_idx")
    (1 to 3).foreach { _ =>
      val result = transform.deduplicate(df, Seq("key"), orderBy = Seq("ts"))
      val winner = result.filter(result("key") === "A").select("payload").as[String].collect().head
      assert(winner == "second", "the higher row_idx must win every time")
    }
  }

  test("deduplicate without a row_idx column keeps the historical behavior") {
    import spark.implicits._
    val df = Seq(("A", 100, 1), ("A", 200, 2), ("B", 300, 3)).toDF("key", "amount", "ts")
    assert(transform.deduplicate(df, Seq("key"), orderBy = Seq("ts")).count() == 2)
  }

  test("deduplicate falls back to dropDuplicates when orderBy column not in DataFrame") {
    import spark.implicits._
    val df = Seq(
      ("A", 1),
      ("A", 1),
      ("B", 2)
    ).toDF("key", "value")
    // orderBy col does not exist in the DataFrame
    val result = transform.deduplicate(df, Seq("key", "value"), orderBy = Seq("nonexistent_col"))
    assert(result.count() == 2)
  }

  test("deduplicate is no-op when keys list is empty") {
    import spark.implicits._
    val df = Seq(("A", 1), ("A", 1)).toDF("key", "value")
    val result = transform.deduplicate(df, Seq.empty, orderBy = Seq.empty)
    assert(result.count() == 2)
  }

  // ---------------------------------------------------------------------------
  // align
  // ---------------------------------------------------------------------------

  test("align pads missing columns with nulls") {
    import spark.implicits._
    val df = Seq("Alice").toDF("name")
    val schema = StructType(Seq(
      StructField("name", StringType),
      StructField("age", IntegerType)
    ))
    val result = transform.align(df, schema)
    assert(result.columns.toSeq == Seq("name", "age"))
    val ageNull = result.select("age").collect().head.isNullAt(0)
    assert(ageNull)
  }

  test("align reorders columns to match target schema") {
    import spark.implicits._
    val df = Seq(("Alice", 30)).toDF("name", "age")
    // target has age first
    val schema = StructType(Seq(
      StructField("age", IntegerType),
      StructField("name", StringType)
    ))
    val result = transform.align(df, schema)
    assert(result.columns.toSeq == Seq("age", "name"))
  }

  test("align casts columns to target types") {
    import spark.implicits._
    val df = Seq("42").toDF("age")
    val schema = StructType(Seq(StructField("age", IntegerType)))
    val result = transform.align(df, schema)
    assert(result.schema("age").dataType == IntegerType)
    assert(result.select("age").as[Int].collect().head == 42)
  }

  test("align is case-insensitive for column matching") {
    import spark.implicits._
    val df = Seq("Alice").toDF("NAME")
    val schema = StructType(Seq(StructField("name", StringType)))
    val result = transform.align(df, schema)
    assert(result.columns.map(_.toLowerCase).contains("name"))
    assert(result.count() == 1)
  }
}
