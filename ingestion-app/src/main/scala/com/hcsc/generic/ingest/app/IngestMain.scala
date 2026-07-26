package com.hcsc.generic.ingest.app

import com.hcsc.generic.ingest.file.FileSource
import com.hcsc.generic.ingest.jdbc.JdbcSource
import com.hcsc.generic.ingest.kafka.KafkaSource
import com.hcsc.generic.ingest.model.{Cli, CliParser}
import com.hcsc.generic.ingest.pipeline.IngestPipeline
import com.hcsc.generic.ingest.sink.HiveSink
import com.hcsc.generic.ingest.stage.{CuratedStageRunner, RawStageRunner}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import java.io.File

object IngestMain {
  private val logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    registerConnectors()

    val cli = CliParser.parse(args)

    val baseConf: Config = cli.confPath match {
      case Some(path) => ConfigFactory.parseFile(new File(path)).resolve()
      case None       => ConfigFactory.load().resolve()
    }

    val feedConf = baseConf.getConfig(s"feeds.${cli.entity}")

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    try {
      spark.sqlContext.setConf("spark.sql.caseSensitive", "false")

      if (cli.validateOnly) {
        new IngestPipeline(spark, feedConf, cli, logger).validateOnly(cli.explainMapping)
      } else cli.stage.toLowerCase match {
        case "curated" | "curated-only" | "c" =>
          runCuratedOnly(spark, feedConf, cli)
        case _ =>
          new IngestPipeline(spark, feedConf, cli, logger).run()
      }
    } catch {
      case error: Throwable =>
        logger.error(s"Ingest failed entity=${cli.entity}", error)
        throw error
    } finally {
      spark.stop()
    }
  }

  /** Legacy replay path: rebuild curated from an existing RAW partition. */
  private def runCuratedOnly(spark: SparkSession, feedConf: Config, cli: Cli): Unit = {
    val curatedConf =
      if (feedConf.hasPath("curated")) Some(feedConf.getConfig("curated")) else None
    val rawFlag = cli.rawFlag.getOrElse(if (cli.mode == "FULL") "F" else "I")

    val rawDf = new RawStageRunner(
      spark = spark,
      feedConf = feedConf,
      cli = cli,
      rawFlag = rawFlag,
      logger = logger
    ).run()

    new CuratedStageRunner(
      spark = spark,
      curatedConf = curatedConf,
      logger = logger
    ).run(rawDf, cli.mode)
  }

  private def registerConnectors(): Unit = {
    FileSource.register()
    JdbcSource.register()
    KafkaSource.register()
    HiveSink.register()
  }
}
