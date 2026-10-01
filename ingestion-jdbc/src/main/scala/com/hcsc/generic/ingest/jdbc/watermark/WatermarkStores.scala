package com.hcsc.generic.ingest.jdbc.watermark

import com.hcsc.generic.ingest.jdbc.WatermarkConfig
import com.hcsc.generic.ingest.watermark.{HiveWatermarkStore, InMemoryWatermarkStore, WatermarkStore}
import org.apache.spark.sql.SparkSession

/** Selects the watermark store a JDBC feed's `incremental.watermark_store`
  * block names. The store types themselves live in core
  * (`com.hcsc.generic.ingest.watermark`); only this config-driven dispatch
  * is JDBC-specific. */
object WatermarkStores {
  def from(cfg: WatermarkConfig, spark: SparkSession): WatermarkStore = cfg.storeType match {
    case "memory" => InMemoryWatermarkStore
    case "hive" =>
      val database = cfg.storeDatabase.getOrElse(
        throw new IllegalArgumentException("JDBC_003 incremental.watermark_store.database is required for the hive store"))
      new HiveWatermarkStore(spark, database, cfg.storeTable)
    case other =>
      throw new IllegalArgumentException(s"JDBC_003 Unknown watermark_store.type '$other'; expected hive or memory")
  }
}
