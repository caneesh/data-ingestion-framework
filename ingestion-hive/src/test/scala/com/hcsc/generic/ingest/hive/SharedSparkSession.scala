package com.hcsc.generic.ingest.hive

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

/** Local copy of the shared Spark session mixin for ingestion-hive tests.
  * Duplicated (rather than using a test-jar) to keep the build simpler.
  * In-memory catalog: partitioned datasource tables register their
  * partitions in the catalog, which is all the source needs.
  */
trait SharedSparkSession extends BeforeAndAfterAll { this: Suite =>

  protected lazy val spark: SparkSession = SharedSparkSession.session

  /**
    * Removes a database's warehouse directory before a suite recreates its
    * tables. DROP TABLE clears the metastore entry but leaves the directory
    * on disk, and the in-memory catalog is rebuilt per JVM while the
    * directory is not — so a second run fails with LOCATION_ALREADY_EXISTS.
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
    super.afterAll()
  }
}

object SharedSparkSession {
  val session: SparkSession = SparkSession.builder()
    .master("local[2]")
    .appName("ingestion-hive-tests")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.caseSensitive", "false")
    .config("spark.sql.catalogImplementation", "in-memory")
    .getOrCreate()
}
