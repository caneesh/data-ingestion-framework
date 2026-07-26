package com.hcsc.generic.ingest.app

import com.hcsc.generic.ingest.file.FileSource
import com.hcsc.generic.ingest.jdbc.JdbcSource
import com.hcsc.generic.ingest.kafka.KafkaSource
import com.hcsc.generic.ingest.model.CliParser
import com.hcsc.generic.ingest.stage.{CuratedStageRunner, RawStageRunner}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import java.io.File
import java.util.UUID

object IngestMain {
  private val logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    registerSources()

    val cli = CliParser.parse(args)
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    spark.sqlContext.setConf("spark.sql.caseSensitive", "false")

    val baseConf: Config = cli.confPath match {
      case Some(path) => ConfigFactory.parseFile(new File(path)).resolve()
      case None       => ConfigFactory.load().resolve()
    }

    val feedConf = baseConf.getConfig(s"feeds.${cli.entity}")
    val curatedConf =
      if (feedConf.hasPath("curated")) Some(feedConf.getConfig("curated")) else None

    val runId = UUID.randomUUID().toString
    val rawFlag = cli.rawFlag.getOrElse(if (cli.mode == "FULL") "F" else "I")

    try {
      logger.info(s"==== Ingest start entity=${cli.entity} mode=${cli.mode} runId=$runId ====")

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

      logger.info(s"==== Ingest success entity=${cli.entity} runId=$runId ====")
    } catch {
      case error: Throwable =>
        logger.error(s"Ingest failed entity=${cli.entity} runId=$runId", error)
        throw error
    } finally {
      spark.stop()
    }
  }

  private def registerSources(): Unit = {
    FileSource.register()
    JdbcSource.register()
    KafkaSource.register()
  }
}
