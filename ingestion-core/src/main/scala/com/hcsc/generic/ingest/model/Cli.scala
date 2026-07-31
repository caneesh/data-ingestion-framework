package com.hcsc.generic.ingest.model

case class Cli(
  entity: String = "",
  mode: String = "FULL",
  confPath: Option[String] = None,
  rawFlag: Option[String] = None,
  stage: String = "all",
  resumeIngestDt: Option[String] = None,
  runId: Option[String] = None,
  resume: Boolean = false,
  fileId: Option[String] = None,
  dryRun: Boolean = false,
  forceReprocess: Boolean = false,
  validateOnly: Boolean = false,
  explainMapping: Boolean = false
)

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
        case unknown =>
          throw new IllegalArgumentException(s"Unknown argument: $unknown")
      }
    }

    require(cli.entity.nonEmpty, "--entity is required")
    val mode = cli.mode.toUpperCase
    require(mode == "FULL" || mode == "INCR", "--mode must be FULL or INCR")
    val validStages = Set("all", "raw", "curated", "curated-only", "c", "retention")
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
    cli.copy(mode = mode)
  }
}
