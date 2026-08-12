package com.hcsc.generic.ingest.app

import com.hcsc.generic.ingest.config.ConfigUtils
import com.hcsc.generic.ingest.file.FileSource
import com.hcsc.generic.ingest.jdbc.JdbcSource
import com.hcsc.generic.ingest.kafka.KafkaSource
import com.hcsc.generic.ingest.model.{Cli, CliParser}
import com.hcsc.generic.ingest.pipeline.IngestPipeline
import com.hcsc.generic.ingest.sink.HiveSink
import com.typesafe.config.{Config, ConfigException, ConfigFactory, ConfigParseOptions}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import java.io.File

object IngestMain {
  private val logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    registerConnectors()

    val cli = CliParser.parse(args)

    val baseConf: Config = loadBaseConfig(cli.confPath)

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
        // Single hook covering every terminal failure: stage failures,
        // reconciliation mismatches and lock contention all reach here by
        // rethrowing. Best-effort by construction — the run has already
        // failed, and an unreachable webhook must not change how it failed
        // or mask the original exception.
        try {
          com.hcsc.generic.ingest.notify.NotificationService(feedConf, logger)
            .notifyFailure(cli.entity, cli.runId.getOrElse("<unassigned>"),
              cli.stage, String.valueOf(error.getMessage))
        } catch {
          case notifyError: Throwable =>
            logger.warn(s"[Notify] failure notification could not be sent: " +
              s"${notifyError.getMessage}")
        }
        throw error
    } finally {
      spark.stop()
    }
  }

  /**
    * Loads the base configuration, tolerating a BARE filename on either
    * entry point.
    *
    * HOCON resolves `include` relative to the including file's PARENT
    * directory. A bare filename — the natural form in a YARN container,
    * where `--files` drops everything in the working directory — has a null
    * parent (`new File("feed.conf").getParentFile == null`), so includes
    * fall back to the classpath and the run dies with
    * `ConfigException$IO: include was not found`.
    *
    * Both routes are absolutised: `--conf-path` directly, and the
    * `config.file` system property that `ConfigFactory.load()` honours.
    * Fixing only the former (as an earlier revision did) still left every
    * `-Dconfig.file=<basename>` deployment broken.
    *
    * `allowMissing(false)` is deliberate: the default silently yields an
    * EMPTY config for a mistyped path, and the operator then sees a
    * confusing "No configuration setting found for key 'feeds'" instead of
    * the actual problem, which is that the file was never shipped.
    */
  private[app] def loadBaseConfig(confPath: Option[String]): Config = confPath match {
    case Some(path) =>
      val file = new File(path).getAbsoluteFile
      if (!file.isFile)
        throw new IllegalArgumentException(
          s"CFG_018: config file not found: '$path' (resolved to '$file'). " +
            "In YARN cluster mode the file must be shipped with --files and " +
            "referenced by its basename, e.g. --files /p/feed.conf --conf-path ./feed.conf")
      logger.info(s"[Config] --conf-path='$path' resolved='$file' " +
        s"includes resolve against '${file.getParent}'")
      withIncludeDiagnostics(file.getParentFile) {
        ConfigFactory.parseFile(file, ConfigParseOptions.defaults().setAllowMissing(false)).resolve()
      }
    case None =>
      Option(System.getProperty("config.file")).map(_.trim).filter(_.nonEmpty).foreach { p =>
        val absolute = new File(p).getAbsolutePath
        if (absolute != p) {
          System.setProperty("config.file", absolute)
          ConfigFactory.invalidateCaches() // the property is read at load()
        }
      }
      val effective = Option(System.getProperty("config.file")).map(new File(_))
      logger.info(s"[Config] -Dconfig.file=${effective.map(_.toString).getOrElse("<unset>")} " +
        s"cwd='${workingDir.getAbsolutePath}'")
      withIncludeDiagnostics(effective.map(_.getParentFile).getOrElse(workingDir)) {
        ConfigFactory.load().resolve()
      }
  }

  private def workingDir: File = new File("").getAbsoluteFile

  /**
    * A failed `include` reports the missing NAME but never the directory it
    * searched, which makes the single most common deployment error — the
    * included schema file not shipped alongside the feed — near-undebuggable
    * from a YARN log. Restate it with the search directory and what is
    * actually there. Filenames only: no file contents are read, so nothing
    * secret can reach the log.
    */
  private def withIncludeDiagnostics[T](searchDir: File)(load: => T): T =
    try load
    catch {
      case e: ConfigException.IO =>
        val present = Option(searchDir).flatMap(d => Option(d.list())) match {
          case Some(names) if names.nonEmpty => names.sorted.mkString(", ")
          case Some(_)                       => "<empty directory>"
          case None                          => "<directory unreadable>"
        }
        throw new ConfigException.IO(e.origin(),
          s"${e.getMessage} | searched directory '${Option(searchDir).map(_.getPath).getOrElse("<none>")}' " +
            s"which contains: [$present]. An included file must be shipped too, e.g. " +
            "--files /p/feed.conf,/p/feed-schema.conf", e)
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
      // delete_count makes the purge volume queryable and trendable; the
      // per-table breakdown stays in the message, which cannot be aggregated.
      val purged = results.map { case (_, _, n) => n }.sum
      audit.recordStage(ctx, "retention", StageStatus.Success,
        counts = com.hcsc.generic.ingest.audit.StageCounts(deleteCount = purged),
        message = (if (cli.dryRun) "dry-run: " else "") +
          results.map { case (t, action, n) => s"$t $action count=$n" }.mkString("; "))
    } catch {
      case e: Throwable =>
        // Best-effort, same reasoning as the pipeline's stage handler: an
        // audit write that fails must not replace the failure it records.
        try audit.recordStage(ctx, "retention", StageStatus.Failed, message = String.valueOf(e.getMessage))
        catch {
          case audErr: Throwable =>
            logger.error("[Retention] Could not record FAILED in the ledger: " +
              s"${audErr.getMessage} — original failure follows and is rethrown", audErr)
            e.addSuppressed(audErr)
        }
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
