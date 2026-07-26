package com.hcsc.generic.ingest.stage

import com.hcsc.generic.ingest.config.ConfigUtils
import com.hcsc.generic.ingest.model.Cli
import com.hcsc.generic.ingest.sink.HiveSink
import com.hcsc.generic.ingest.source.SourceRegistry
import com.hcsc.generic.ingest.transform.RawMetadata
import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

final class RawStageRunner(
  spark: SparkSession,
  feedConf: Config,
  cli: Cli,
  rawFlag: String,
  logger: Logger
) {
  def run(): DataFrame = {
    val rawConf = feedConf.getConfig("raw")

    cli.stage.toLowerCase match {
      case "curated" | "curated-only" | "c" =>
        readFromRaw(rawConf)

      case _ =>
        val sourceConf = feedConf.getConfig("source")
        val sourceType = ConfigUtils.optString(sourceConf, "type").getOrElse("file")
        val source = SourceRegistry.resolve(sourceType)

        val df = source.read(spark, sourceConf)
        val rawReady = RawMetadata.add(df, rawFlag)
        new HiveSink(spark, rawConf).write(rawReady)
        logger.info(s"[RawStageRunner] RAW completed rows=${rawReady.count()}")
        rawReady
    }
  }

  private def readFromRaw(rawConf: Config): DataFrame = {
    val rawTable = s"${rawConf.getString("database")}.${rawConf.getString("table")}"
    val ingestDt = cli.resumeIngestDt.getOrElse(
      throw new IllegalArgumentException("--resume-ingest-dt is required for curated-only")
    )
    val partitionCols = spark.catalog.listColumns(rawTable).collect().filter(_.isPartition)
    val hasFileType = partitionCols.exists(_.name.equalsIgnoreCase("file_type"))
    val predicate =
      if (hasFileType) s"ingest_dt='$ingestDt' AND file_type='$rawFlag'"
      else s"ingest_dt='$ingestDt'"

    val df = spark.table(rawTable).where(predicate)
    logger.info(s"[RawStageRunner] Read RAW slice $rawTable predicate=$predicate rows=${df.count()}")
    df
  }
}
