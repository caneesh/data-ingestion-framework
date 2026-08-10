package com.hcsc.generic.ingest.transform

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

/** Mixin that creates a single local SparkSession for all test suites that use it.
  * Uses a non-Hive session to keep tests fast and dependency-free.
  * Each suite using this trait shares the same session within a JVM.
  */
trait SharedSparkSession extends BeforeAndAfterAll { this: Suite =>

  protected lazy val spark: SparkSession = SharedSparkSession.session

  /**
    * Removes a database's warehouse directory before a suite recreates its
    * tables.
    *
    * DROP TABLE clears the metastore entry but leaves the directory on disk,
    * and the in-memory catalog is rebuilt per JVM while the directory is
    * not — so the second run of a suite fails with LOCATION_ALREADY_EXISTS.
    * (The same trap as a dropped EXTERNAL table in production: the metadata
    * goes, the files stay.)
    *
    * Call from a suite's initializer before CREATE/INSERT.
    */
  protected def purgeWarehouseDb(database: String): Unit = {
    val warehouse = new java.io.File(
      new java.net.URI(spark.conf.get("spark.sql.warehouse.dir")).getPath, s"$database.db")
    def purge(f: java.io.File): Unit = {
      if (f.isDirectory) Option(f.listFiles()).foreach(_.foreach(purge))
      f.delete()
    }
    if (warehouse.exists()) purge(warehouse)
  }

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
