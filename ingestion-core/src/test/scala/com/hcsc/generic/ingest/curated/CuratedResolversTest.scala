package com.hcsc.generic.ingest.curated

import com.hcsc.generic.ingest.transform.{CuratedTransform, SharedSparkSession}
import org.scalatest.funsuite.AnyFunSuite

class CuratedResolversTest extends AnyFunSuite with SharedSparkSession {

  import spark.implicits._

  private val transform = new CuratedTransform(spark)
  private val keys = new BusinessKeyResolver(transform)
  private val duplicates = new DuplicateResolver(transform)

  test("business keys resolve case-insensitively to actual column names") {
    val df = Seq(("K1", "v")).toDF("Claim_ID", "payload")
    assert(keys.resolve(df, Seq("claim_id")) == Seq("Claim_ID"))
  }

  test("missing business keys fail with CUR_001 naming the key") {
    val df = Seq(("K1", "v")).toDF("claim_id", "payload")
    val ex = intercept[IllegalArgumentException](keys.resolve(df, Seq("member_id")))
    assert(ex.getMessage.contains("CUR_001") && ex.getMessage.contains("member_id"))
  }

  test("null and blank keys are excluded before matching") {
    val df = Seq(("K1", "a"), (null.asInstanceOf[String], "b"), ("  ", "c"))
      .toDF("claim_id", "payload")
    val filtered = keys.filterInvalidKeys(df, Seq("claim_id"))
    assert(filtered.count() == 1)
  }

  test("duplicate resolution keeps the latest by ordering and counts removals") {
    val df = Seq(
      ("K1", 1, "old"), ("K1", 3, "new"), ("K1", 2, "mid"), ("K2", 1, "only"))
      .toDF("claim_id", "seq", "payload")
    val outcome = duplicates.resolve(df, Seq("claim_id"), Seq("seq"), countedInput = 4L)
    assert(outcome.duplicateRows == 2L)
    val winners = outcome.deduplicated.select("claim_id", "payload").collect()
      .map(r => r.getString(0) -> r.getString(1)).toMap
    assert(winners == Map("K1" -> "new", "K2" -> "only"))
  }

  test("no keys means no deduplication and a zero duplicate count") {
    val df = Seq(("K1", "a"), ("K1", "a")).toDF("claim_id", "payload")
    val outcome = duplicates.resolve(df, Seq.empty, Seq.empty, countedInput = 2L)
    assert(outcome.duplicateRows == 0L && outcome.deduplicated.count() == 2L)
  }

  test("unknown curated strategy kinds fail with CUR_003; TYPE2 is explicitly unimplemented") {
    val ex = intercept[IllegalArgumentException](CuratedStrategies.forKind(spark, "SCD9"))
    assert(ex.getMessage.contains("CUR_003"))
    val type2 = intercept[IllegalArgumentException](CuratedStrategies.forKind(spark, "TYPE2_MERGE"))
    assert(type2.getMessage.contains("not yet implemented"))
    assert(CuratedStrategies.all.forall(k => CuratedStrategies.forKind(spark, k).kind == k))
    assert(CuratedStrategies.forKind(spark, "MERGE").kind == CuratedStrategies.Type1Merge)
  }
}
