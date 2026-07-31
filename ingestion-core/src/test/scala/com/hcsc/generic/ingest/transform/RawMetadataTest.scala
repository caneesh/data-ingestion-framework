package com.hcsc.generic.ingest.transform

import org.scalatest.funsuite.AnyFunSuite

class RawMetadataTest extends AnyFunSuite with SharedSparkSession {

  test("add appends row_idx, load_timestamp, file_type, and file_id columns") {
    import spark.implicits._
    val df = Seq(("Alice", 1), ("Bob", 2)).toDF("name", "value")
    val result = RawMetadata.add(df, "CSV")

    val cols = result.columns.toSet
    assert(cols.contains("row_idx"))
    assert(cols.contains("load_timestamp"))
    assert(cols.contains("file_type"))
    assert(cols.contains("file_id"))
  }

  test("add sets file_type to the supplied rawFlag value") {
    import spark.implicits._
    val df = Seq(("x", 1)).toDF("name", "value")
    val result = RawMetadata.add(df, "MYTYPE")
    val fileTypes = result.select("file_type").as[String].collect().toSet
    assert(fileTypes == Set("MYTYPE"))
  }

  test("add adds source_file column when not already present") {
    import spark.implicits._
    val df = Seq(("x", 1)).toDF("name", "value")
    assert(!df.columns.contains("source_file"))
    val result = RawMetadata.add(df, "F")
    assert(result.columns.contains("source_file"))
  }

  test("add preserves existing source_file column without duplicating it") {
    import spark.implicits._
    val df = Seq(("x", "myfile.csv")).toDF("name", "source_file")
    val result = RawMetadata.add(df, "F")
    // column should appear exactly once
    assert(result.columns.count(_.equalsIgnoreCase("source_file")) == 1)
  }

  test("add preserves original data columns") {
    import spark.implicits._
    val df = Seq(("Alice", 42)).toDF("name", "age")
    val result = RawMetadata.add(df, "F")
    assert(result.columns.contains("name"))
    assert(result.columns.contains("age"))
  }

  test("add produces non-null file_id values") {
    import spark.implicits._
    val df = Seq(("x", 1)).toDF("name", "value")
    val result = RawMetadata.add(df, "F")
    val nullCount = result.filter(result("file_id").isNull).count()
    assert(nullCount == 0)
  }

  test("add row count is unchanged") {
    import spark.implicits._
    val df = Seq(("a", 1), ("b", 2), ("c", 3)).toDF("name", "value")
    val result = RawMetadata.add(df, "F")
    assert(result.count() == 3)
  }

  // ---------------------------------------------------------------------------
  // RAW_003: withColumn REPLACES silently — colliding source columns must
  // fail fast, never lose their data
  // ---------------------------------------------------------------------------

  test("a source column colliding with a stamped metadata column fails fast (RAW_003)") {
    import spark.implicits._
    val df = Seq(("x", "source-provided")).toDF("name", "load_timestamp")
    val e = intercept[IllegalStateException] { RawMetadata.add(df, "F") }
    assert(e.getMessage.contains("RAW_003"))
    assert(e.getMessage.contains("load_timestamp"))
  }

  test("a source run_id column collides with lineage stamping (RAW_003)") {
    import spark.implicits._
    val df = Seq(("x", "rogue")).toDF("name", "RUN_ID") // case-insensitive
    val e = intercept[IllegalStateException] {
      RawMetadata.add(df, "F", RawLineage(runId = "r1"))
    }
    assert(e.getMessage.contains("RAW_003"))
    assert(e.getMessage.toLowerCase.contains("run_id"))
  }

  test("a source record_hash column collides with hash stamping (RAW_003)") {
    import spark.implicits._
    val df = Seq(("x", "fake-hash")).toDF("name", "record_hash")
    val e = intercept[IllegalStateException] { RecordHash.stamp(df, None) }
    assert(e.getMessage.contains("RAW_003"))
    assert(e.getMessage.contains("record_hash"))
  }
}
