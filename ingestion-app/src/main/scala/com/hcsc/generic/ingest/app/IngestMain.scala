package com.hcsc.generic.ingest.app

import com.hcsc.generic.ingest.config.ConfigUtils
import com.hcsc.generic.ingest.file.FileSource
import com.hcsc.generic.ingest.jdbc.JdbcSource
import com.hcsc.generic.ingest.kafka.KafkaSource
import com.hcsc.generic.ingest.model.{Cli, CliParser}
import com.hcsc.generic.ingest.pipeline.IngestPipeline
import com.hcsc.generic.ingest.sink.HiveSink
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
          // Governed replay path: real run context, entity lock, ledger,
          // rejects and contract validation — never the watermark.
          new IngestPipeline(spark, feedConf, cli, logger).curatedReplay()
        case "retention" =>
          // Config-driven purge of raw partitions, reject/audit rows and
          // watermark history; honors --dry-run.
          new com.hcsc.generic.ingest.retention.RetentionService(spark, feedConf, logger)
            .run(dryRun = cli.dryRun)
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

  private def registerConnectors(): Unit = {
    FileSource.register()
    JdbcSource.register()
    KafkaSource.register()
    HiveSink.register()
  }
}
