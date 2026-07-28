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
    // UTC-derived so the same instant lands in the same partition regardless
    // of the host JVM's default timezone.
    val snapshotDt = context.loadTimestamp.toInstant
      .atZone(java.time.ZoneOffset.UTC).toLocalDate.toString
    val withPartitions = Partitioning(stamped, target.partitions)
      .withColumn("snapshot_dt", org.apache.spark.sql.functions.lit(snapshotDt))
    val partitionKeys = target.partitions.keys :+ "snapshot_dt"
    val rows = withPartitions.count()

    if (!spark.catalog.tableExists(target.fullTable)) {
      var writer = withPartitions.write.format("orc").mode(SaveMode.Overwrite)
      target.path.foreach(p => writer = writer.option("path", p))
      writer.partitionBy(partitionKeys: _*).saveAsTable(target.fullTable)
    } else {
      // Dynamic partition overwrite via a PER-WRITE option — never a session
      // conf mutation, which under concurrent feeds could revert to static
      // mid-write and turn partition replacement into whole-table overwrite.
      AppendBatchStrategy.alignToTarget(spark, withPartitions, target.fullTable, logger)
        .write.mode(SaveMode.Overwrite)
        .option("partitionOverwriteMode", "dynamic")
        .insertInto(target.fullTable)
    }
    logger.info(s"[RawWrite] snapshot_dt=$snapshotDt written to ${target.fullTable} ($rows rows)")
    RawWriteResult(target.fullTable, context.batchId, rows, detail = s"snapshot_dt=$snapshotDt")
  }
}
