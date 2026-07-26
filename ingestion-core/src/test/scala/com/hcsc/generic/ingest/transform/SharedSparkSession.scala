package com.hcsc.generic.ingest.transform

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

/** Mixin that creates a single local SparkSession for all test suites that use it.
  * Uses a non-Hive session to keep tests fast and dependency-free.
  * Each suite using this trait shares the same session within a JVM.
  */
trait SharedSparkSession extends BeforeAndAfterAll { this: Suite =>

  protected lazy val spark: SparkSession = SharedSparkSession.session

  override def afterAll(): Unit = {
    // Do NOT stop the session here — other suites running in the same JVM share it.
    super.afterAll()
  }
}

object SharedSparkSession {
  val session: SparkSession = SparkSession.builder()
    .master("local[2]")
    .appName("ingestion-core-tests")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.caseSensitive", "false")
    // Disable Hive-specific features; tests should not need it.
    .config("spark.sql.catalogImplementation", "in-memory")
    .getOrCreate()
}
