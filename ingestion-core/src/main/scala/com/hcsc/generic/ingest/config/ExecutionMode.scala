package com.hcsc.generic.ingest.config

import com.hcsc.generic.ingest.model.Cli
import com.typesafe.config.Config

/**
  * Operator-facing execution declaration — how Raw and Curated run for
  * this feed (scheduler-driven, e.g. Control-M):
  *
  * ingestion { execution = COUPLED | DECOUPLED }   # default COUPLED
  *
  * COUPLED   — one process: `--stage all` runs raw then curated in a single
  *             job; the watermark advances after curated success (default).
  * DECOUPLED — two independent jobs: `--stage raw` (advances the watermark
  *             at raw success) and `--stage curated --pending` (drains the
  *             ledger checkpoint). The declaration exists so a scheduler
  *             cannot silently double-process a feed: a DECOUPLED feed
  *             REJECTS the coupled `--stage all` invocation.
  *
  * The declaration validates configuration consistency (CFG_016) and
  * invocation consistency; it adds no third behavior — both shapes run on
  * the same machinery.
  */
object ExecutionMode {

  val Coupled = "COUPLED"
  val Decoupled = "DECOUPLED"

  def declared(feedConf: Config): String = {
    val v = ConfigUtils.optConfig(feedConf, "ingestion")
      .flatMap(i => ConfigUtils.optString(i, "execution"))
      .getOrElse(Coupled).toUpperCase
    require(Seq(Coupled, Decoupled).contains(v),
      s"CFG_016 ingestion.execution '$v' must be COUPLED or DECOUPLED")
    v
  }

  /** Config-static consistency (called from FeedCompatibilityValidator). */
  def configProblems(feedConf: Config): Seq[String] = {
    val problems = Seq.newBuilder[String]
    val mode = try declared(feedConf) catch {
      case e: IllegalArgumentException => problems += e.getMessage; Coupled
    }
    if (mode == Decoupled) {
      val source = ConfigUtils.optConfig(feedConf, "source")
      val positional = source.exists(s =>
        s.hasPath("incremental") ||
          ConfigUtils.optConfig(s, "offsets").exists(o =>
            ConfigUtils.optBoolean(o, "track").getOrElse(false)))
      val advanceAfter = ConfigUtils.optConfig(feedConf, "watermark")
        .flatMap(w => ConfigUtils.optString(w, "advance_after"))
        .getOrElse("CURATED").toUpperCase
      if (positional && advanceAfter != "RAW")
        problems += "CFG_016 ingestion.execution = DECOUPLED requires watermark.advance_after " +
          "= RAW for incremental sources: the curated job never touches the watermark, so a " +
          "CURATED-gated advance would re-read the same window forever"
      val auditOff = ConfigUtils.optConfig(feedConf, "audit")
        .flatMap(a => ConfigUtils.optBoolean(a, "enabled")).contains(false)
      if (auditOff)
        problems += "CFG_016 ingestion.execution = DECOUPLED requires the run ledger " +
          "(audit.database): the ledger is the curated job's batch checkpoint"
    }
    problems.result()
  }

  /** Invocation consistency, called by the entry point before dispatch. */
  def validateInvocation(cli: Cli, feedConf: Config): Unit = {
    val mode = declared(feedConf)
    val stage = cli.stage.toLowerCase
    if (mode == Decoupled && stage == "all" && !cli.validateOnly)
      throw new IllegalArgumentException(
        "CFG_016 this feed declares ingestion.execution = DECOUPLED: invoke the two jobs " +
          "separately (--stage raw, then --stage curated --pending) — running --stage all " +
          "would double-process batches the scheduled curated job also drains")
  }
}
