package com.hcsc.generic.ingest.source

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Extension point for sources that track an incremental position (JDBC
  * watermarks, offsets, ...). The pipeline invokes advanceWatermark ONLY
  * after the full run — RAW write, CURATED publish, file completion — has
  * succeeded, so a failed run never moves the position and replays are safe.
  */
trait WatermarkAdvancing { self: Source =>
  def advanceWatermark(
    spark: SparkSession,
    sourceConf: Config,
    entity: String,
    runId: String,
    accepted: DataFrame
  ): Unit
}
