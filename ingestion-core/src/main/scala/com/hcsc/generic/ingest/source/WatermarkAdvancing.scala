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

  /** The extraction window observed by this run's read, serialized:
    * (lower bound, captured upper bound when one exists). The pipeline
    * stamps it onto RAW records as extract_start_ts / extract_end_ts.
    * Sources without window tracking return None. */
  def lastWindow(entity: String, runId: Option[String]): Option[(String, Option[String])] = None

  /** Invoked by the pipeline when a run FAILS: sources tracking in-memory
    * read state discard it so abandoned entries cannot accumulate or feed a
    * stale commit in a long-lived driver JVM. Default no-op. */
  def discardWindow(entity: String, runId: Option[String]): Unit = ()
}
