package com.hcsc.generic.ingest.file

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

/** Local copy of the shared Spark session mixin for ingestion-file tests.
  * Duplicated (rather than using a test-jar) to keep the build simpler.
  */
trait SharedSparkSession extends BeforeAndAfterAll { this: Suite =>

  protected lazy val spark: SparkSession = SharedSparkSession.session

  override def afterAll(): Unit = {
    super.afterAll()
  }
}

object SharedSparkSession {
  val session: SparkSession = SparkSession.builder()
    .master("local[2]")
    .appName("ingestion-file-tests")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.caseSensitive", "false")
    .config("spark.sql.catalogImplementation", "in-memory")
    .getOrCreate()
}
