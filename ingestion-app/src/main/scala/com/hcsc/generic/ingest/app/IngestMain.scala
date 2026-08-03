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

      // Scheduler safety: a DECOUPLED feed rejects the coupled invocation
      // so Control-M cannot double-process batches its curated job drains.
      com.hcsc.generic.ingest.config.ExecutionMode.validateInvocation(cli, feedConf)

      if (cli.validateOnly) {
        new IngestPipeline(spark, feedConf, cli, logger).validateOnly(cli.explainMapping)
      } else cli.stage.toLowerCase match {
        case "curated" | "curated-only" | "c" =>
          // Governed replay path: real run context, entity lock, ledger,
          // rejects and contract validation — never the watermark. With a
          // batch selector (--pending / --replay-*) the driver sequences
          // one governed replay per selected RAW batch.
          if (cli.batchSelector || cli.replaySourceSystem.isDefined)
            new com.hcsc.generic.ingest.pipeline.CuratedBatchDriver(spark, feedConf, cli, logger).run()
          else
            new IngestPipeline(spark, feedConf, cli, logger).curatedReplay()
        case "retention" =>
          // Config-driven purge of raw partitions, reject/audit rows and
          // watermark history; honors --dry-run. Runs under the entity lock
          // (a staged rewrite racing a live run's appends would discard
          // them) and records its outcome in the run ledger AFTER purging,
          // so the record of the purge survives it.
          runRetention(spark, feedConf, cli)
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

  private def runRetention(spark: SparkSession, feedConf: Config, cli: Cli): Unit = {
    import com.hcsc.generic.ingest.runtime.{RunContext, StageStatus}
    val ctx = RunContext(
      runId = cli.runId.getOrElse(java.util.UUID.randomUUID().toString),
      entity = cli.entity, mode = cli.mode,
      rawFlag = cli.rawFlag.getOrElse(""), dryRun = cli.dryRun)
    val audit = com.hcsc.generic.ingest.audit.AuditService(spark, feedConf)
    val lock = com.hcsc.generic.ingest.lock.RunLock.fromConfig(spark, feedConf, logger)
    val held = if (cli.dryRun) None else lock.map { l => l.acquire(ctx.entity, ctx.runId); l }
    try {
      val results = new com.hcsc.generic.ingest.retention.RetentionService(spark, feedConf, logger)
        .run(dryRun = cli.dryRun)
      audit.recordStage(ctx, "retention", StageStatus.Success,
        message = (if (cli.dryRun) "dry-run: " else "") +
          results.map { case (t, action, n) => s"$t $action count=$n" }.mkString("; "))
    } catch {
      case e: Throwable =>
        audit.recordStage(ctx, "retention", StageStatus.Failed, message = String.valueOf(e.getMessage))
        throw e
    } finally {
      held.foreach(l =>
        try l.release(ctx.entity, ctx.runId)
        catch { case e: Exception => logger.warn(s"[Retention] Lock release failed: ${e.getMessage}") })
    }
  }

  private def registerConnectors(): Unit = {
    FileSource.register()
    JdbcSource.register()
    KafkaSource.register()
    HiveSink.register()
  }
}
