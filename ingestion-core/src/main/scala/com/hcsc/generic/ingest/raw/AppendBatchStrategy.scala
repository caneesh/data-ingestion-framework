package com.hcsc.generic.ingest.raw

import com.hcsc.generic.ingest.transform.Partitioning
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.col

/**
  * Append-only batch loading (the Raw default): derived partition columns
  * are added, the table is created on first write (ORC, dynamic partitions),
  * and every later write appends aligned by target column order.
  */
final class AppendBatchStrategy extends RawWriteStrategy {
  private val logger = Logger.getLogger(getClass.getName)

  val kind: String = RawWriteStrategies.AppendBatch

  def write(spark: SparkSession, stamped: DataFrame, target: RawTarget, context: BatchContext): RawWriteResult = {
    val withPartitions = Partitioning(stamped, target.partitions)
    val rows = withPartitions.count()

    spark.sql("SET hive.exec.dynamic.partition=true")
    spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")

    if (spark.catalog.tableExists(target.fullTable)) {
      AppendBatchStrategy.alignToTarget(spark, withPartitions, target.fullTable, logger)
        .write.mode(SaveMode.Append).insertInto(target.fullTable)
    } else {
      var writer = withPartitions.write.format("orc").mode(SaveMode.Overwrite)
      target.path.foreach(p => writer = writer.option("path", p))
      if (target.partitions.keys.nonEmpty)
        writer.partitionBy(target.partitions.keys: _*).saveAsTable(target.fullTable)
      else
        writer.saveAsTable(target.fullTable)
    }
    RawWriteResult(target.fullTable, context.batchId, rows)
  }
}

object AppendBatchStrategy {
  /** insertInto is positional: select the frame in the target's column order.
    * Missing target columns fail; extra source columns are dropped loudly. */
  private[raw] def alignToTarget(
    spark: SparkSession,
    df: DataFrame,
    fullTable: String,
    logger: Logger
  ): DataFrame = {
    val targetColumns = spark.table(fullTable).columns
    val present = df.columns.map(_.toLowerCase).toSet
    val missing = targetColumns.filterNot(c => present.contains(c.toLowerCase))
    require(missing.isEmpty,
      s"RAW_002 batch is missing raw table columns: ${missing.mkString(", ")}")

    val targetSet = targetColumns.map(_.toLowerCase).toSet
    val extra = df.columns.filterNot(c => targetSet.contains(c.toLowerCase))
    if (extra.nonEmpty)
      logger.warn(s"[RawWrite] source columns not in $fullTable will be dropped: ${extra.mkString(", ")}")

    df.select(targetColumns.map(col): _*)
  }
}
