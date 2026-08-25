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

    // FIRST line in every driver log. The jar is scp'd to a server that is
    // not a checkout, so this is the only thing that can distinguish a
    // freshly built jar from the one already there — a distinction that has
    // cost several debugging rounds.
    logger.info(s"[Build] ingestion framework ${com.hcsc.generic.ingest.BuildInfo.summary}")

    val cli = CliParser.parse(args)

    val baseConf: Config = loadBaseConfig(cli.confPath, cli.overridePath)

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
        case "reconcile" =>
          // Independent SOURCE-vs-CURATED comparison (Tier 1 cardinality,
          // Tier 2 key sets). Scheduled separately from ingestion — it
          // issues real queries against the source system — and REPORTS by
          // default rather than failing: there is no batch to stop.
          runReconcile(spark, feedConf, cli)
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

        // Is retrying this worth a scheduler's time? Classified here, at the
        // one point every terminal failure reaches by rethrowing.
        val failure = com.hcsc.generic.ingest.runtime.FailureClass.classify(error)

        // THREE channels, because no single one works everywhere. The exit
        // code propagates through spark-submit in CLIENT mode only — in YARN
        // CLUSTER mode spark-submit reports the final APPLICATION state, not
        // the driver JVM's code — so the log marker and the notification
        // carry the same verdict for schedulers that cannot see the code.
        logger.error(s"[Outcome] class=${failure.name} exitCode=${failure.exitCode} " +
          s"retryable=${failure.retryable} entity=${cli.entity} " +
          s"runId=${cli.runId.getOrElse("<unassigned>")} stage=${cli.stage}")

        // Best-effort by construction — the run has already failed, and an
        // unreachable webhook must not change how it failed or mask the
        // original exception.
        try {
          com.hcsc.generic.ingest.notify.NotificationService(feedConf, logger)
            .notifyFailure(cli.entity, cli.runId.getOrElse("<unassigned>"),
              cli.stage, s"[${failure.name}] ${String.valueOf(error.getMessage)}")
        } catch {
          case notifyError: Throwable =>
            logger.warn(s"[Notify] failure notification could not be sent: " +
              s"${notifyError.getMessage}")
        }
        classifiedExit = failure.exitCode
        throw error
    } finally {
      spark.stop()
      // AFTER spark.stop(), so the context shuts down cleanly first. Only an
      // explicitly classified failure changes the code; Unclassified is 1,
      // exactly what an uncaught throw already produced.
      if (classifiedExit != 0 &&
          classifiedExit != com.hcsc.generic.ingest.runtime.FailureClass.Unclassified.exitCode)
        System.exit(classifiedExit)
    }
  }

  /** Set by the failure handler, read by the finally block after the Spark
    * context has stopped. */
  @volatile private var classifiedExit: Int = 0

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
  private[app] def loadBaseConfig(
    confPath: Option[String],
    overridePath: Option[String] = None
  ): Config = confPath match {
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
        val base = ConfigFactory.parseFile(
          file, ConfigParseOptions.defaults().setAllowMissing(false))
        // Merge BEFORE resolve: substitutions in either file must see the
        // combined view, and resolving the feed first would freeze values the
        // override is meant to replace.
        applyOverride(base, overridePath).resolve()
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
        applyOverride(ConfigFactory.load(), overridePath).resolve()
      }
  }

  /**
    * Applies the operational override layer, if one was supplied.
    *
    * WHY: a deployed feed config sits behind change control, but operational
    * values — a lock lease, a reject threshold, a webhook, a table name —
    * legitimately need to change faster than that process allows. An
    * override file is deployed on its own and WINS over the feed for every
    * path it declares.
    *
    * The risk is the mirror image of the benefit: a file that silently
    * changes behaviour is exactly the drift this project has spent weeks
    * chasing. So every overridden path is LOGGED BY NAME, and paths that
    * reduce a safety guarantee are logged as warnings. Values are never
    * logged — an override may carry a credential.
    */
  private[app] def applyOverride(base: Config, overridePath: Option[String]): Config =
    overridePath.map(_.trim).filter(_.nonEmpty) match {
      case None => base
      case Some(path) =>
        val file = new File(path).getAbsoluteFile
        if (!file.isFile)
          throw new IllegalArgumentException(
            s"CFG_019: override file not found: '$path' (resolved to '$file'). " +
              "It must be shipped like the feed config — --files /p/override.conf " +
              "--override-path ./override.conf — or the flag omitted entirely.")

        val overrideConf =
          ConfigFactory.parseFile(file, ConfigParseOptions.defaults().setAllowMissing(false))

        import scala.collection.JavaConverters._
        val paths = overrideConf.entrySet().asScala.map(_.getKey).toSeq.sorted
        if (paths.isEmpty)
          logger.warn(s"[Override] '$file' declares no values; the feed config applies unchanged")
        else {
          logger.warn(s"[Override] ${paths.size} value(s) from '$file' TAKE PRECEDENCE over the " +
            "feed config for this run:")
          paths.foreach { p =>
            // The config is deliberately UNRESOLVED here, and hasPath throws
            // on an unresolved substitution. This label is a courtesy; it
            // must never be the reason a run dies.
            val label = scala.util.Try(base.hasPath(p))
              .map(if (_) " (replaces the feed's value)" else " (new)")
              .getOrElse("")
            logger.warn(s"[Override]   $p$label")
          }
          // Named individually: silently weakening one of these through an
          // unversioned file is the failure mode worth shouting about.
          val safetyPaths = paths.filter(p => SafetyReducingOverrides.exists(p.contains))
          if (safetyPaths.nonEmpty)
            logger.warn(s"[Override] WARNING — these REDUCE a safety guarantee and are in effect " +
              s"for this run: ${safetyPaths.mkString(", ")}")
        }
        // The ledger must be able to say a run was overridden: the
        // structural fingerprint alone cannot see a changed VALUE.
        com.hcsc.generic.ingest.runtime.OverrideContext.record(paths, overrideDigest(overrideConf))
        overrideConf.withFallback(base)
    }

  /** Digest over the override's rendered content, so two different override
    * files touching the same paths are distinguishable in the ledger. Only
    * the digest is ever stored — the content may carry a credential. */
  private def overrideDigest(conf: Config): String = {
    import scala.collection.JavaConverters._
    val rendered = conf.entrySet().asScala.toSeq
      .map(e => e.getKey + "=" + e.getValue.render()).sorted.mkString("\n")
    java.security.MessageDigest.getInstance("SHA-256")
      .digest(rendered.getBytes(java.nio.charset.StandardCharsets.UTF_8))
      .take(8).map("%02x".format(_)).mkString
  }

  /** Override paths that trade a guarantee for convenience. Overriding them
    * is permitted — that is the point of the layer — but never quietly. */
  private val SafetyReducingOverrides = Seq(
    "allow_insecure_tls", "allow_insecure_http", "trustServerCertificate",
    "on_mismatch", "audit.enabled", "allow_unsafe_legacy_merge",
    "max_reject_percent", "max_reject_count", "confirm_complete_extract",
    "concurrency.lock", "metadata_columns", "force")

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
    // A purge can outlast lease_minutes — the SmartIQ feed runs a 30-minute
    // lease, and a sweep over raw partitions plus four audit tables is not
    // reliably shorter than that. Without renewal the lease lapses mid-purge,
    // another run legitimately claims the entity and appends rows, and the
    // staged INSERT OVERWRITE silently discards them. The pipeline has always
    // heartbeated for exactly this reason; retention was the one lock holder
    // that did not.
    val heartbeat = held.map { l =>
      val hb = new com.hcsc.generic.ingest.lock.LockHeartbeat(
        l, ctx.entity, ctx.runId, math.max(l.leaseMillis / 3, 1000L), logger)
      hb.start()
      hb
    }
    // Renewal failure is the case the heartbeat cannot fix: ownership may
    // already have passed to another run, so the purge must ABORT rather than
    // overwrite. This is a deliberate new failure mode — a retention job that
    // previously "succeeded" while eating another run's rows now fails loudly.
    val ownershipGuard: () => Unit = () =>
      if (heartbeat.exists(_.ownershipLost))
        throw new com.hcsc.generic.ingest.lock.PipelineLockException(
          "PIPE_001 lease ownership lost mid-retention; aborting BEFORE the purge overwrite. " +
            "Rows appended by whichever run now holds the entity would have been discarded. " +
            "Nothing was purged by this attempt; re-run retention once the entity is idle.")
    try {
      val results = new com.hcsc.generic.ingest.retention.RetentionService(
        spark, feedConf, logger, ownershipGuard).run(dryRun = cli.dryRun)
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
      heartbeat.foreach(hb => try hb.stop() catch { case _: Exception => () })
      held.foreach(l =>
        try l.release(ctx.entity, ctx.runId)
        catch { case e: Exception => logger.warn(s"[Retention] Lock release failed: ${e.getMessage}") })
    }
  }

  private def runReconcile(spark: SparkSession, feedConf: Config, cli: Cli): Unit = {
    import com.hcsc.generic.ingest.runtime.{RunContext, StageStatus}
    val ctx = RunContext(
      runId = cli.runId.getOrElse(java.util.UUID.randomUUID().toString),
      entity = cli.entity, mode = cli.mode,
      rawFlag = cli.rawFlag.getOrElse(""), dryRun = cli.dryRun)
    val audit = com.hcsc.generic.ingest.audit.AuditService(spark, feedConf)
    val service = new com.hcsc.generic.ingest.jdbc.reconcile
      .SourceReconciliationService(spark, feedConf, logger)

    // The entity lock keeps the comparison from reading a half-written
    // curated table, and the HEARTBEAT keeps a long comparison from losing
    // the lease under itself — the exact defect retention shipped with.
    val lock = com.hcsc.generic.ingest.lock.RunLock.fromConfig(spark, feedConf, logger)
    val held = if (cli.dryRun) None else lock.map { l => l.acquire(ctx.entity, ctx.runId); l }
    val heartbeat = held.map { l =>
      val hb = new com.hcsc.generic.ingest.lock.LockHeartbeat(
        l, ctx.entity, ctx.runId, math.max(l.leaseMillis / 3, 1000L), logger)
      hb.start()
      hb
    }
    try {
      val checks = service.run()
      if (checks.isEmpty) {
        logger.warn("[Reconcile] no checks were produced — nothing to compare")
        return
      }
      // Recorded in the SAME table as the in-run checks, so one query
      // answers "is this feed consistent" across both kinds.
      audit.recordReconciliation(ctx, checks.map(c => (c.name, c.expected, c.actual, c.passed)))

      val failed = checks.filterNot(_.passed)
      val summary = checks
        .map(c => s"${c.name}: expected=${c.expected} actual=${c.actual} passed=${c.passed}")
        .mkString("; ")
      audit.recordStage(ctx, "reconcile",
        if (failed.isEmpty) StageStatus.Success else StageStatus.Failed, message = summary)

      if (failed.isEmpty) logger.info(s"[Reconcile] all checks passed — $summary")
      else {
        logger.error(s"[Reconcile] MISMATCH — $summary")
        // Best-effort alert on the channel failures already use; the
        // finding is worthless if nobody sees it before the consumer does.
        try {
          com.hcsc.generic.ingest.notify.NotificationService(feedConf, logger)
            .notifyFailure(ctx.entity, ctx.runId, "reconcile", summary)
        } catch {
          case e: Throwable => logger.warn(s"[Notify] reconcile alert not sent: ${e.getMessage}")
        }
        if (service.onMismatch == "FAIL")
          throw new IllegalStateException(s"Source reconciliation failed: $summary")
      }
    } catch {
      case e: Throwable =>
        try audit.recordStage(ctx, "reconcile", StageStatus.Failed,
          message = String.valueOf(e.getMessage))
        catch { case audErr: Throwable => e.addSuppressed(audErr) }
        throw e
    } finally {
      heartbeat.foreach(hb => try hb.stop() catch { case _: Exception => () })
      held.foreach(l =>
        try l.release(ctx.entity, ctx.runId)
        catch { case e: Exception => logger.warn(s"[Reconcile] Lock release failed: ${e.getMessage}") })
    }
  }

  private def registerConnectors(): Unit = {
    FileSource.register()
    JdbcSource.register()
    KafkaSource.register()
    HiveSink.register()
  }
}
