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

    if (spark.catalog.tableExists(target.fullTable)) {
      // Needed only when appending into pre-existing Hive-serde tables;
      // saved and restored so strict-mode protection is not silently lost
      // for everything else sharing the session.
      AppendBatchStrategy.withDynamicPartitions(spark) {
        AppendBatchStrategy.alignToTarget(spark, withPartitions, target.fullTable, logger)
          .write.mode(SaveMode.Append).insertInto(target.fullTable)
      }
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

  /** Applies session settings for the duration of `body`, restoring the
    * previous values afterwards — even on failure. The conf is still
    * session-global while `body` runs, so concurrent writers sharing one
    * SparkSession see the temporary values; the guarantee here is that
    * nothing LEAKS past the write. */
  private[raw] def withSessionConf[A](spark: SparkSession, settings: Seq[(String, String)])(body: => A): A = {
    val previous = settings.map { case (k, _) => k -> spark.conf.getOption(k) }
    settings.foreach { case (k, v) => spark.conf.set(k, v) }
    try body
    finally previous.foreach {
      case (k, Some(v)) => spark.conf.set(k, v)
      case (k, None)    => spark.conf.unset(k)
    }
  }

  /** Enables Hive dynamic partitioning for `body`, restoring the previous
    * session values afterwards. */
  private[raw] def withDynamicPartitions[A](spark: SparkSession)(body: => A): A =
    withSessionConf(spark, Seq(
      "hive.exec.dynamic.partition" -> "true",
      "hive.exec.dynamic.partition.mode" -> "nonstrict"))(body)

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
