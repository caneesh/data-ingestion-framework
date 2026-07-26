package com.hcsc.generic.ingest.sink

import com.hcsc.generic.ingest.config.ConfigUtils
import com.hcsc.generic.ingest.transform.Partitioning
import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.col

object HiveSink extends Sink {
  private val logger = Logger.getLogger(getClass.getName)

  override def sinkType: String = "hive"

  override def write(spark: SparkSession, df: DataFrame, sinkConf: Config): Unit = {
    val database = ConfigUtils.sqlIdentifier(sinkConf, "database")
    val table = ConfigUtils.sqlIdentifier(sinkConf, "table")
    val fullTable = s"$database.$table"
    val path = sinkConf.getString("path")
    val format = if (sinkConf.hasPath("format")) sinkConf.getString("format") else "orc"

    val partitionSpec = Partitioning.parse(sinkConf)
    val withPartitions = Partitioning(df, partitionSpec)

    spark.sql("SET hive.exec.dynamic.partition=true")
    spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")

    if (spark.catalog.tableExists(fullTable)) {
      val targetCols = spark.table(fullTable).columns
      val actual = withPartitions.columns.map(_.toLowerCase).toSet
      val missing = targetCols.filterNot(c => actual.contains(c.toLowerCase))
      require(missing.isEmpty, s"DataFrame is missing target columns: ${missing.mkString(",")}")

      val targetSet = targetCols.map(_.toLowerCase).toSet
      val extra = withPartitions.columns.filterNot(c => targetSet.contains(c.toLowerCase))
      if (extra.nonEmpty)
        logger.warn(s"[HiveSink] Source columns not in target $fullTable will be dropped: ${extra.mkString(",")}")

      withPartitions.select(targetCols.map(col): _*)
        .write
        .mode(SaveMode.Append)
        .insertInto(fullTable)
    } else {
      val writer = withPartitions.write
        .format(format)
        .mode(SaveMode.Overwrite)
        .option("path", path)

      if (partitionSpec.keys.nonEmpty)
        writer.partitionBy(partitionSpec.keys: _*).saveAsTable(fullTable)
      else
        writer.saveAsTable(fullTable)
    }
  }

  def register(): Unit = SinkRegistry.register(this)
}
