package com.hcsc.generic.ingest.pipeline

import com.hcsc.generic.ingest.transform.SharedSparkSession
import org.apache.log4j.Logger
import org.scalatest.funsuite.AnyFunSuite

/** Cross-run RAW idempotency: rows whose staged file already has RAW rows
  * from a PRIOR run (crash between append and completion) are excluded,
  * regardless of run_id. */
class RawIdempotencyTest extends AnyFunSuite with SharedSparkSession {

  import spark.implicits._
  private val logger = Logger.getLogger(getClass.getName)

  test("rows from already-loaded files are excluded; fresh files pass") {
    spark.sql("CREATE DATABASE IF NOT EXISTS raw_idem_db")
    // Explicit temp location: the in-memory catalog forgets tables between
    // JVM runs but the default warehouse directory persists.
    val location = java.nio.file.Files.createTempDirectory("raw-idem-t").toString
    Seq(("f-loaded", "run-old", "row1"), ("f-loaded", "run-old", "row2"))
      .toDF("file_id", "run_id", "payload")
      .write.mode("overwrite").option("path", location).saveAsTable("raw_idem_db.t")

    val incoming = Seq(
      ("f-loaded", "run-new", "row1"),   // crash-recovery replay of the same file
      ("f-loaded", "run-new", "row2"),
      ("f-fresh", "run-new", "row3")
    ).toDF("file_id", "run_id", "payload")

    val loaded = RawIdempotency.alreadyLoadedFileIds(spark, "raw_idem_db.t", incoming)
    assert(loaded == Seq("f-loaded"))

    val deduped = RawIdempotency.excludeLoadedFiles(spark, "raw_idem_db.t", incoming, logger)
    assert(deduped.select("file_id").distinct().collect().map(_.getString(0)).toSeq == Seq("f-fresh"))
  }

  test("no table, no file_id column, or no overlap means no filtering") {
    val incoming = Seq(("f1", "r", "x")).toDF("file_id", "run_id", "payload")
    assert(RawIdempotency.alreadyLoadedFileIds(spark, "raw_idem_db.absent", incoming).isEmpty)

    val noFileId = Seq(("r", "x")).toDF("run_id", "payload")
    assert(RawIdempotency.alreadyLoadedFileIds(spark, "raw_idem_db.t", noFileId).isEmpty)

    spark.sql("CREATE DATABASE IF NOT EXISTS raw_idem_db")
    val fresh = Seq(("f-never-seen", "r", "x")).toDF("file_id", "run_id", "payload")
    val out = RawIdempotency.excludeLoadedFiles(spark, "raw_idem_db.t", fresh, logger)
    assert(out.count() == 1)
  }
}
