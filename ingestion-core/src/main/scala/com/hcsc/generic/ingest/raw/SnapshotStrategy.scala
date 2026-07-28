package com.hcsc.generic.ingest.raw

import com.hcsc.generic.ingest.transform.Partitioning
import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession, DataFrame}

/**
  * Snapshot loading: each run writes the complete source image into a
  * `snapshot_dt` partition (derived from the batch's load timestamp).
  * Re-running a snapshot for the same date replaces exactly that partition —
  * dynamic partition overwrite — and never touches earlier snapshots, so
  * history stays append-only at partition granularity. Readers consume the
  * latest snapshot_dt; retention (dropping old snapshots) is an offline
  * maintenance concern, deliberately not performed here.
  */
final class SnapshotStrategy extends RawWriteStrategy {
  private val logger = Logger.getLogger(getClass.getName)

  val kind: String = RawWriteStrategies.Snapshot

  def write(spark: SparkSession, stamped: DataFrame, target: RawTarget, context: BatchContext): RawWriteResult = {
    val snapshotDt = context.loadTimestamp.toLocalDateTime.toLocalDate.toString
    val withPartitions = Partitioning(stamped, target.partitions)
      .withColumn("snapshot_dt", org.apache.spark.sql.functions.lit(snapshotDt))
    val partitionKeys = target.partitions.keys :+ "snapshot_dt"
    val rows = withPartitions.count()

    if (!spark.catalog.tableExists(target.fullTable)) {
      var writer = withPartitions.write.format("orc").mode(SaveMode.Overwrite)
      target.path.foreach(p => writer = writer.option("path", p))
      writer.partitionBy(partitionKeys: _*).saveAsTable(target.fullTable)
    } else {
      val aligned = AppendBatchStrategy.alignToTarget(spark, withPartitions, target.fullTable, logger)
      // Dynamic partition overwrite: only partitions present in this batch
      // (this snapshot_dt) are replaced; all earlier snapshots survive.
      val overwriteModeKey = "spark.sql.sources.partitionOverwriteMode"
      val previous = spark.conf.getOption(overwriteModeKey)
      spark.conf.set(overwriteModeKey, "dynamic")
      try aligned.write.mode(SaveMode.Overwrite).insertInto(target.fullTable)
      finally previous match {
        case Some(v) => spark.conf.set(overwriteModeKey, v)
        case None    => spark.conf.unset(overwriteModeKey)
      }
    }
    logger.info(s"[RawWrite] snapshot_dt=$snapshotDt written to ${target.fullTable} ($rows rows)")
    RawWriteResult(target.fullTable, context.batchId, rows, detail = s"snapshot_dt=$snapshotDt")
  }
}
