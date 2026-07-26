package com.hcsc.generic.ingest.stage

import com.hcsc.generic.ingest.config.ConfigUtils
import com.hcsc.generic.ingest.model.Cli
import com.hcsc.generic.ingest.schema.{SchemaContract, SchemaValidator, SchemaVersions}
import com.hcsc.generic.ingest.sink.SinkRegistry
import com.hcsc.generic.ingest.source.SourceRegistry
import com.hcsc.generic.ingest.transform.RawMetadata
import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.storage.StorageLevel

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
        val baseSourceConf = feedConf.getConfig("source")
        // The schema contract is declared at feed level; attach it to the
        // source config so connectors can resolve headers/aliases from it.
        val contract = SchemaContract.parse(feedConf).orElse(SchemaContract.parse(baseSourceConf))
        val sourceConf =
          if (feedConf.hasPath("schema")) baseSourceConf.withValue("schema", feedConf.getValue("schema"))
          else baseSourceConf

        val sourceType = ConfigUtils.optString(sourceConf, "type").getOrElse("file")
        val source = SourceRegistry.resolve(sourceType)
        val sinkType = ConfigUtils.optString(rawConf, "type").getOrElse("hive")
        val sink = SinkRegistry.resolve(sinkType)

        val df = source.read(spark, sourceConf)

        val database = ConfigUtils.sqlIdentifier(rawConf, "database")
        val table = ConfigUtils.sqlIdentifier(rawConf, "table")

        contract.foreach { c =>
          val violations = SchemaValidator.validateData(df, c) ++
            SchemaValidator.versionMismatch(SchemaVersions.stored(spark, database, table), c)
          SchemaValidator.enforce(violations, c.policies, logger)
        }

        val rawReady = RawMetadata.add(df, rawFlag).persist(StorageLevel.MEMORY_AND_DISK)
        sink.write(spark, rawReady, rawConf)
        contract.foreach(c => SchemaVersions.record(spark, database, table, c.version, logger))
        logger.info(s"[RawStageRunner] RAW completed rows=${rawReady.count()}")
        rawReady
    }
  }

  private def readFromRaw(rawConf: Config): DataFrame = {
    val database = ConfigUtils.sqlIdentifier(rawConf, "database")
    val table = ConfigUtils.sqlIdentifier(rawConf, "table")
    val rawTable = s"$database.$table"
    val ingestDt = cli.resumeIngestDt.getOrElse(
      throw new IllegalArgumentException("--resume-ingest-dt is required for curated-only")
    )
    val partitionCols = spark.catalog.listColumns(rawTable).collect().filter(_.isPartition)
    val hasFileType = partitionCols.exists(_.name.equalsIgnoreCase("file_type"))

    var df = spark.table(rawTable).filter(col("ingest_dt") === lit(ingestDt))
    if (hasFileType) df = df.filter(col("file_type") === lit(rawFlag))

    logger.info(s"[RawStageRunner] Read RAW slice $rawTable ingest_dt=$ingestDt fileTypeFilter=$hasFileType")
    df
  }
}
