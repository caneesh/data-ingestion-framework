package com.hcsc.generic.ingest.sink

import com.hcsc.generic.ingest.config.ConfigUtils
import com.hcsc.generic.ingest.transform.{Partitioning, RawMetadata}
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
    // Optional: absent path = managed table in the Hive warehouse
    val path = ConfigUtils.optString(sinkConf, "path")
    val format = if (sinkConf.hasPath("format")) sinkConf.getString("format") else "orc"

    val partitionSpec = Partitioning.parse(sinkConf)
    val withPartitions = Partitioning(df, partitionSpec)

    spark.sql("SET hive.exec.dynamic.partition=true")
    spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")

    if (spark.catalog.tableExists(fullTable)) {
      val targetCols = spark.table(fullTable).columns
      val actual = withPartitions.columns.map(_.toLowerCase).toSet
      val missing = targetCols.filterNot(c => actual.contains(c.toLowerCase))
      if (missing.nonEmpty) {
        // A missing PARTITION column has one overwhelmingly likely cause: the
        // table's partition column and raw.partitioning.keys/derive disagree,
        // usually because the DDL and the feed config were updated out of
        // step. Naming the configured keys turns "add the column somehow"
        // into a one-line diff.
        val targetPartitions =
          try spark.catalog.listColumns(fullTable).collect()
            .filter(_.isPartition).map(_.name.toLowerCase).toSet
          catch { case scala.util.control.NonFatal(_) => Set.empty[String] }
        val missingPartitions = missing.filter(c => targetPartitions.contains(c.toLowerCase))
        val hint =
          if (missingPartitions.isEmpty) ""
          else s". ${missingPartitions.mkString(",")} " +
            (if (missingPartitions.length == 1) "is a PARTITION column of" else "are PARTITION columns of") +
            s" $fullTable, but raw.partitioning produces " +
            (if (partitionSpec.keys.isEmpty) "no partition columns"
             else s"[${partitionSpec.keys.mkString(",")}]") +
            " — the table DDL and the feed config disagree. Align " +
            "raw.partitioning.keys/derive with the table, or recreate the table to match"
        require(false, s"DataFrame is missing target columns: ${missing.mkString(",")}$hint")
      }

      val targetSet = targetCols.map(_.toLowerCase).toSet
      val extra = withPartitions.columns.filterNot(c => targetSet.contains(c.toLowerCase))
      // Framework metadata columns are never silently dropped: without them
      // (run_id above all) run traceability, the replay guard, raw-count
      // reconciliation and --resume all silently stop working.
      val (metaExtra, businessExtra) =
        extra.partition(c => RawMetadata.ColumnNames.contains(c.toLowerCase))
      if (businessExtra.nonEmpty)
        logger.warn(s"[HiveSink] Source columns not in target $fullTable will be dropped: ${businessExtra.mkString(",")}")

      val finalTargetCols =
        if (metaExtra.isEmpty) targetCols
        else ConfigUtils.optString(sinkConf, "metadata_columns").map(_.toUpperCase).getOrElse("FAIL") match {
          case "ALTER" =>
            val ddl = metaExtra.map(c => s"${c.toLowerCase} ${RawMetadata.sqlType(c)}").mkString(", ")
            logger.warn(s"[HiveSink] Adding framework metadata column(s) to $fullTable: $ddl")
            spark.sql(s"ALTER TABLE $fullTable ADD COLUMNS ($ddl)")
            spark.table(fullTable).columns
          case "WARN" =>
            logger.warn(s"[HiveSink] Framework metadata column(s) ${metaExtra.mkString(",")} are " +
              s"missing from $fullTable and will be DROPPED (raw.metadata_columns=WARN): run " +
              "traceability, replay guards, reconciliation and --resume are degraded for this table")
            targetCols
          case "FAIL" =>
            throw new IllegalStateException(
              s"RAW_001 Target table $fullTable is missing framework metadata column(s): " +
                s"${metaExtra.mkString(", ")}. Dropping them silently disables run traceability, " +
                "the replay guard, raw-count reconciliation and --resume. Add the columns " +
                "(ALTER TABLE ... ADD COLUMNS), or set raw.metadata_columns = ALTER to add them " +
                "automatically, or WARN to accept the degraded legacy behavior.")
          case other =>
            throw new IllegalArgumentException(s"raw.metadata_columns '$other' must be FAIL, ALTER or WARN")
        }

      withPartitions.select(finalTargetCols.map(com.hcsc.generic.ingest.schema.ColumnMapping.quotedCol): _*)
        .write
        .mode(SaveMode.Append)
        .insertInto(fullTable)
    } else {
      var writer = withPartitions.write
        .format(format)
        .mode(SaveMode.Overwrite)
      path.foreach(p => writer = writer.option("path", p))

      if (partitionSpec.keys.nonEmpty)
        writer.partitionBy(partitionSpec.keys: _*).saveAsTable(fullTable)
      else
        writer.saveAsTable(fullTable)
    }
  }

  def register(): Unit = SinkRegistry.register(this)
}
