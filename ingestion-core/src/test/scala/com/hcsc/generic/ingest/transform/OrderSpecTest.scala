package com.hcsc.generic.ingest.transform

import org.scalatest.funsuite.AnyFunSuite

class OrderSpecLogicalTypeTest extends org.scalatest.funsuite.AnyFunSuite with SharedSparkSession {

  test("'as <type>' parses and orders by the LOGICAL type, storage untouched") {
    val spec = OrderSpec.parse("seq desc as bigint")
    assert(spec.compareAs.contains("bigint") && !spec.ascending)
    val s = spark
    import s.implicits._
    // Lexically "9" > "10"; numerically 10 > 9 — the cast must win
    val df = Seq(("a", "9"), ("b", "10")).toDF("id", "seq")
    val winner = df.orderBy(spec.toColumn("seq")).collect().head.getString(0)
    assert(winner == "b", "desc as bigint must rank 10 above 9 despite string storage")
    assert(df.schema("seq").dataType == org.apache.spark.sql.types.StringType,
      "the physical column type is untouched")
  }

  test("invalid or misplaced 'as' fails at parse time (HDR_020)") {
    val bad = intercept[IllegalArgumentException] { OrderSpec.parse("seq as not_a_type") }
    assert(bad.getMessage.contains("HDR_020"))
    val misplaced = intercept[IllegalArgumentException] { OrderSpec.parse("seq as bigint desc") }
    assert(misplaced.getMessage.contains("HDR_020"))
    // plain specs unchanged
    assert(OrderSpec.parse("seq desc nulls_first").compareAs.isEmpty)
  }
}

class OrderSpecTest extends AnyFunSuite with SharedSparkSession {

  test("bare names keep the historical default: descending, nulls last") {
    assert(OrderSpec.parse("modified_ts") == OrderSpec("modified_ts", ascending = false, nullsFirst = false))
  }

  test("direction and null-ordering tokens parse case-insensitively") {
    assert(OrderSpec.parse("seq ASC") == OrderSpec("seq", ascending = true, nullsFirst = false))
    assert(OrderSpec.parse("seq asc nulls_first") == OrderSpec("seq", ascending = true, nullsFirst = true))
    assert(OrderSpec.parse("  seq   DESC   NULLS_LAST ") == OrderSpec("seq", ascending = false, nullsFirst = false))
  }

  test("malformed specifications fail with HDR_020") {
    Seq("seq sideways", "seq asc desc", "seq nulls_first nulls_last").foreach { bad =>
      val e = intercept[IllegalArgumentException](OrderSpec.parse(bad))
      assert(e.getMessage.contains("HDR_020"), s"'$bad': ${e.getMessage}")
    }
  }

  test("deduplicate honors per-column direction: asc keeps the LOWEST value") {
    import spark.implicits._
    val transform = new CuratedTransform(spark)
    val df = Seq(("K1", 5, "high"), ("K1", 1, "low")).toDF("id", "seq", "tag")

    val descWinner = transform.deduplicate(df, Seq("id"), Seq("seq"))
      .select("tag").collect().head.getString(0)
    assert(descWinner == "high", "bare column keeps the legacy highest-wins")

    val ascWinner = transform.deduplicate(df, Seq("id"), Seq("seq asc"))
      .select("tag").collect().head.getString(0)
    assert(ascWinner == "low", "asc flips the winner deterministically")
  }

  test("deduplicate null ordering: nulls_first makes null values win") {
    import spark.implicits._
    val transform = new CuratedTransform(spark)
    val df = Seq(("K1", Some(5), "valued"), ("K1", None, "nullrow"))
      .toDF("id", "seq", "tag")

    assert(transform.deduplicate(df, Seq("id"), Seq("seq"))
      .select("tag").collect().head.getString(0) == "valued",
      "default nulls_last: the null row loses")
    assert(transform.deduplicate(df, Seq("id"), Seq("seq desc nulls_first"))
      .select("tag").collect().head.getString(0) == "nullrow")
  }

  test("a direction token on a missing column still fails HDR_019 by column name") {
    import spark.implicits._
    val transform = new CuratedTransform(spark)
    val df = Seq(("K1", 1)).toDF("id", "seq")
    val e = intercept[IllegalStateException] {
      transform.deduplicate(df, Seq("id"), Seq("not_there desc"))
    }
    assert(e.getMessage.contains("HDR_019") && e.getMessage.contains("not_there"))
  }
}
