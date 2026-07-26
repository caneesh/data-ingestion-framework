package com.hcsc.generic.ingest.sink

import com.hcsc.generic.ingest.transform.{Partitioning, PartitionSpec}
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.col

final class HiveSink(spark: SparkSession, sinkConf: Config) {

  def write(df: DataFrame): Unit = {
    val database = sinkConf.getString("database")
    val table = sinkConf.getString("table")
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
}
