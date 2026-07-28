package com.hcsc.generic.ingest.raw

import com.hcsc.generic.ingest.transform.SharedSparkSession
import org.scalatest.funsuite.AnyFunSuite

import java.sql.Timestamp

class RawMetadataStamperTest extends AnyFunSuite with SharedSparkSession {

  import spark.implicits._

  private val stamper = new RawMetadataStamper
  private val context = BatchContext(
    feedName = "member",
    sourceSystem = "health_sherpa",
    runId = "run-1",
    batchId = "batch-1",
    rawFlag = "F",
    loadTimestamp = Timestamp.valueOf("2026-07-28 12:00:00"))

  test("stamps every required metadata column with the batch's values") {
    val stamped = stamper.stamp(Seq(("S1", "H1")).toDF("subscriber_id", "hios_id"), context)

    RawMetadataStamper.Reserved.foreach(c =>
      assert(stamped.columns.contains(c), s"missing stamped column $c"))

    val row = stamped.collect().head
    assert(row.getAs[String]("run_id") == "run-1")
    assert(row.getAs[String]("batch_id") == "batch-1")
    assert(row.getAs[Timestamp]("load_timestamp") == context.loadTimestamp)
    assert(row.getAs[String]("source_system") == "health_sherpa")
    assert(row.getAs[String]("feed_name") == "member")
    assert(row.getAs[String]("file_type") == "F")
    assert(row.getAs[String]("record_hash").length == 64)
    // Business data untouched
    assert(row.getAs[String]("subscriber_id") == "S1" && row.getAs[String]("hios_id") == "H1")
  }

  test("record_hash is stable across column order and sensitive to values") {
    def hashOf(df: org.apache.spark.sql.DataFrame): String =
      stamper.stamp(df, context).select("record_hash").collect().head.getString(0)

    val forward = hashOf(Seq(("S1", "H1")).toDF("subscriber_id", "hios_id"))
    val reordered = hashOf(Seq(("H1", "S1")).toDF("hios_id", "subscriber_id"))
    val changed = hashOf(Seq(("S1", "H2")).toDF("subscriber_id", "hios_id"))

    assert(forward == reordered, "hash must not depend on source column order")
    assert(forward != changed, "hash must change when a value changes")
  }

  test("record_hash distinguishes null from empty string and adjacent-field ambiguity") {
    def hashOf(values: (String, String)): String =
      stamper.stamp(Seq(values).toDF("a", "b"), context)
        .select("record_hash").collect().head.getString(0)

    assert(hashOf((null, "x")) != hashOf(("", "x")))
    assert(hashOf(("ab", "")) != hashOf(("a", "b")), "separator must prevent field-boundary collisions")
  }

  test("a connector-provided source_file column is kept, not overwritten") {
    val stamped = stamper.stamp(
      Seq(("S1", "/landing/member.csv")).toDF("subscriber_id", "source_file"), context)
    assert(stamped.select("source_file").collect().head.getString(0) == "/landing/member.csv")
  }

  test("reserved-name collisions fail fast with RAW_001") {
    val ex = intercept[IllegalArgumentException] {
      stamper.stamp(Seq(("S1", "x")).toDF("subscriber_id", "batch_id"), context)
    }
    assert(ex.getMessage.contains("RAW_001"))
    assert(ex.getMessage.contains("batch_id"))
  }

  test("deterministic batch ids repeat for equal inputs and differ otherwise") {
    val a = BatchContext.deterministicBatchId("run-1", Seq("f1.csv", "f2.csv"))
    val b = BatchContext.deterministicBatchId("run-1", Seq("f1.csv", "f2.csv"))
    val c = BatchContext.deterministicBatchId("run-1", Seq("f1.csv"))
    assert(a == b && a != c && a.length == 32)
  }

  test("unknown strategy kind is rejected with the available list") {
    val ex = intercept[IllegalArgumentException](RawWriteStrategies.forKind("MERGE"))
    assert(ex.getMessage.contains("RAW_003") && ex.getMessage.contains("APPEND_BATCH"))
    assert(RawWriteStrategies.all.forall(k => RawWriteStrategies.forKind(k).kind == k))
  }
}
