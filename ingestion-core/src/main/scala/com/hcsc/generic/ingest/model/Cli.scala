package com.hcsc.generic.ingest.model

case class Cli(
  entity: String = "",
  mode: String = "FULL",
  confPath: Option[String] = None,
  rawFlag: Option[String] = None,
  stage: String = "all",
  resumeIngestDt: Option[String] = None
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
        case unknown =>
          throw new IllegalArgumentException(s"Unknown argument: $unknown")
      }
    }

    require(cli.entity.nonEmpty, "--entity is required")
    val mode = cli.mode.toUpperCase
    require(mode == "FULL" || mode == "INCR", "--mode must be FULL or INCR")
    val validStages = Set("all", "raw", "curated", "curated-only", "c")
    require(
      validStages.contains(cli.stage.toLowerCase),
      s"--stage must be one of: ${validStages.mkString(", ")}"
    )
    cli.copy(mode = mode)
  }
}
