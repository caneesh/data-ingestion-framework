package com.hcsc.generic.ingest.source

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Extension point for sources that can re-read exactly the window one run
  * extracted, given that run's recorded (lower, upper] bounds. The pipeline
  * uses it under `raw.mode = SOURCE`, where no RAW copy exists: a curated
  * replay (`--stage curated --run-id`, `--pending`, `--resume`) takes the
  * bounds from the run's `raw` ledger row and asks the source for that slice
  * again. Touches neither the watermark store nor any in-memory read window.
  *
  * Bounds are the serialized form the ledger carries (`window_start` /
  * `window_end`), so core needs no knowledge of the source's value type.
  */
trait WindowReplayable { self: Source =>
  def readWindow(spark: SparkSession, sourceConf: Config, lower: String, upper: String): DataFrame
}
