package com.hcsc.generic.ingest.jdbc.watermark

import com.hcsc.generic.ingest.jdbc.WatermarkConfig
import com.hcsc.generic.ingest.watermark.{HiveWatermarkStore, InMemoryWatermarkStore}
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

/** `WatermarkStores.from` is the only JDBC-specific piece of the watermark
  * store machinery: it maps the `incremental.watermark_store` block onto a
  * store type that lives in core. */
class WatermarkStoresTest extends AnyFunSuite {

  private def cfg(storeType: String, database: Option[String]) = WatermarkConfig(
    "TIMESTAMP", Seq("wm"), Seq("TIMESTAMP"), "unused", None, storeType, database, "ingest_watermarks")

  // HiveWatermarkStore's constructor only validates identifiers; the session
  // is not touched until a method runs, so none is needed to test dispatch.
  private val noSession: SparkSession = null

  test("memory selects the shared in-memory store") {
    assert(WatermarkStores.from(cfg("memory", None), noSession) eq InMemoryWatermarkStore)
  }

  test("hive with a database selects a Hive store") {
    assert(WatermarkStores.from(cfg("hive", Some("ingest_audit")), noSession).isInstanceOf[HiveWatermarkStore])
  }

  test("hive without a database is rejected (JDBC_003)") {
    val ex = intercept[IllegalArgumentException] {
      WatermarkStores.from(cfg("hive", None), noSession)
    }
    assert(ex.getMessage.contains("JDBC_003"))
    assert(ex.getMessage.contains("database is required"))
  }

  test("an unknown store type is rejected (JDBC_003)") {
    val ex = intercept[IllegalArgumentException] {
      WatermarkStores.from(cfg("redis", None), noSession)
    }
    assert(ex.getMessage.contains("JDBC_003"))
    assert(ex.getMessage.contains("Unknown watermark_store.type"))
  }
}
