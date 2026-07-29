package com.hcsc.generic.ingest.app

import com.hcsc.generic.ingest.config.ConfigUtils
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
      // All framework-stamped timestamps (load_timestamp, create_timestamp,
      // last_modified_ts, ingest_dt derivations) are UTC unless a site
      // explicitly overrides the session zone.
      val sessionTz = ConfigUtils.optString(baseConf, "app.spark.session_time_zone").getOrElse("UTC")
      spark.sqlContext.setConf("spark.sql.session.timeZone", sessionTz)

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

  /** Legacy replay path: rebuild curated from an existing RAW partition.
    * Held under the same entity lock as full pipeline runs — this path
    * INSERT OVERWRITEs the real curated table. */
  private def runCuratedOnly(spark: SparkSession, feedConf: Config, cli: Cli): Unit = {
    val curatedConf =
      if (feedConf.hasPath("curated")) Some(feedConf.getConfig("curated")) else None
    val rawFlag = cli.rawFlag.getOrElse(if (cli.mode == "FULL") "F" else "I")
    val runId = cli.runId.getOrElse(java.util.UUID.randomUUID().toString)

    val lock = com.hcsc.generic.ingest.lock.LockService.fromConfig(spark, feedConf, logger)
    lock.foreach(_.acquire(cli.entity, runId))
    try {
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
    } finally lock.foreach { l =>
      try l.release(cli.entity, runId)
      catch { case e: Exception => logger.warn(s"Lock release failed: ${e.getMessage}") }
    }
  }

  private def registerConnectors(): Unit = {
    FileSource.register()
    JdbcSource.register()
    KafkaSource.register()
    HiveSink.register()
  }
}
