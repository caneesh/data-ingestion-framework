package com.hcsc.generic.ingest.model

case class Cli(
  entity: String = "",
  mode: String = "FULL",
  confPath: Option[String] = None,
  /** Operational override layer. Every value here WINS over the same path in
    * the feed config, so a lease, a threshold or a table name can change
    * without reissuing the feed through change control. */
  overridePath: Option[String] = None,
  rawFlag: Option[String] = None,
  stage: String = "all",
  resumeIngestDt: Option[String] = None,
  runId: Option[String] = None,
  resume: Boolean = false,
  fileId: Option[String] = None,
  dryRun: Boolean = false,
  forceReprocess: Boolean = false,
  validateOnly: Boolean = false,
  explainMapping: Boolean = false,
  // Decoupled curated batch selectors (--stage curated only)
  pending: Boolean = false,
  replayLast: Option[Int] = None,
  replayFrom: Option[String] = None,
  replayTo: Option[String] = None,
  replayFailed: Boolean = false,
  replaySourceSystem: Option[String] = None
) {
  /** True when any decoupled batch selector is present. */
  def batchSelector: Boolean =
    pending || replayLast.isDefined || replayFrom.isDefined || replayTo.isDefined || replayFailed
}

object CliParser {
  def parse(args: Array[String]): Cli = {
    var cli = Cli()
    var index = 0

    def value(flag: String): String = {
      require(index + 1 < args.length, s"Missing value for $flag")
      args(index + 1)
    }

    while (index < args.length) {
      args(index) match {
        case "--entity" =>
          cli = cli.copy(entity = value("--entity"))
          index += 2
        case "--mode" =>
          cli = cli.copy(mode = value("--mode"))
          index += 2
        case "--conf-path" =>
          cli = cli.copy(confPath = Some(value("--conf-path")))
          index += 2
        case "--override-path" =>
          cli = cli.copy(overridePath = Some(value("--override-path")))
          index += 2
        case "--raw-flag" =>
          cli = cli.copy(rawFlag = Some(value("--raw-flag")))
          index += 2
        case "--stage" =>
          cli = cli.copy(stage = value("--stage"))
          index += 2
        case "--resume-ingest-dt" =>
          cli = cli.copy(resumeIngestDt = Some(value("--resume-ingest-dt")))
          index += 2
        case "--run-id" =>
          cli = cli.copy(runId = Some(value("--run-id")))
          index += 2
        case "--file-id" =>
          cli = cli.copy(fileId = Some(value("--file-id")))
          index += 2
        case "--resume" =>
          cli = cli.copy(resume = true)
          index += 1
        case "--dry-run" =>
          cli = cli.copy(dryRun = true)
          index += 1
        case "--force-reprocess" =>
          cli = cli.copy(forceReprocess = true)
          index += 1
        case "--validate-only" =>
          cli = cli.copy(validateOnly = true)
          index += 1
        case "--explain-mapping" =>
          cli = cli.copy(explainMapping = true)
          index += 1
        case "--pending" =>
          cli = cli.copy(pending = true)
          index += 1
        case "--replay-last" =>
          cli = cli.copy(replayLast = Some(value("--replay-last").toInt))
          index += 2
        case "--replay-from" =>
          cli = cli.copy(replayFrom = Some(value("--replay-from")))
          index += 2
        case "--replay-to" =>
          cli = cli.copy(replayTo = Some(value("--replay-to")))
          index += 2
        case "--replay-failed" =>
          cli = cli.copy(replayFailed = true)
          index += 1
        case "--replay-source-system" =>
          cli = cli.copy(replaySourceSystem = Some(value("--replay-source-system")))
          index += 2
        case unknown =>
          throw new IllegalArgumentException(s"Unknown argument: $unknown")
      }
    }

    require(cli.entity.nonEmpty, "--entity is required")
    val mode = cli.mode.toUpperCase
    require(mode == "FULL" || mode == "INCR", "--mode must be FULL or INCR")
    val validStages =
      Set("all", "raw", "curated", "curated-only", "c", "retention", "reconcile")
    require(
      validStages.contains(cli.stage.toLowerCase),
      s"--stage must be one of: ${validStages.mkString(", ")}"
    )
    require(
      !cli.resume || cli.runId.isDefined,
      "--resume requires --run-id of the run to resume"
    )
    // run ids reach staging-table identifiers and audit records: constrain
    // them at the boundary instead of trusting every downstream consumer.
    cli.runId.foreach { r =>
      require(
        r.matches("[A-Za-z0-9_-]{1,128}"),
        "--run-id may contain only letters, digits, underscore and hyphen (max 128 chars)"
      )
    }

    // Decoupled curated batch selectors: --stage curated only, mutually
    // exclusive with each other and with the single-shot selectors;
    // --replay-source-system is a FILTER composable with --pending or a
    // date range (or standing alone = pending for that system).
    val curatedStage = Seq("curated", "curated-only", "c").contains(cli.stage.toLowerCase)
    val exclusive = Seq(
      cli.pending -> "--pending",
      cli.replayLast.isDefined -> "--replay-last",
      (cli.replayFrom.isDefined || cli.replayTo.isDefined) -> "--replay-from/--replay-to",
      cli.replayFailed -> "--replay-failed"
    ).filter(_._1).map(_._2)
    require(exclusive.size <= 1,
      s"batch selectors are mutually exclusive; got: ${exclusive.mkString(", ")}")
    if (cli.batchSelector || cli.replaySourceSystem.isDefined) {
      require(curatedStage,
        "batch selectors (--pending, --replay-*) require --stage curated")
      require(cli.runId.isEmpty && cli.resumeIngestDt.isEmpty,
        "batch selectors cannot be combined with --run-id or --resume-ingest-dt")
      // A dry-run replay records SUCCESS while publishing NOTHING — under a
      // selector that would permanently checkpoint unpublished batches.
      require(!cli.dryRun,
        "batch selectors cannot be combined with --dry-run: a dry-run replay would " +
          "checkpoint batches as curated-done without publishing them")
    }
    cli.replayLast.foreach(n => require(n > 0, "--replay-last must be a positive integer"))
    Seq(cli.replayFrom -> "--replay-from", cli.replayTo -> "--replay-to").foreach {
      case (Some(d), flag) => require(d.matches("\\d{4}-\\d{2}-\\d{2}"),
        s"$flag must be a yyyy-MM-dd date, got '$d'")
      case _ => ()
    }
    for (f <- cli.replayFrom; t <- cli.replayTo)
      require(f <= t, s"--replay-from ($f) must not be after --replay-to ($t)")
    cli.copy(mode = mode)
  }
}
