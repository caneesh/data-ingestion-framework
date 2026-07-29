package com.hcsc.generic.ingest.confgen

import com.hcsc.generic.ingest.confgen.build.ConfigAssembler
import com.hcsc.generic.ingest.confgen.draft.Drafts
import com.hcsc.generic.ingest.confgen.flow.QuestionFlowRegistry
import com.hcsc.generic.ingest.confgen.io.{AnsiConsole, ConsoleIO}
import com.hcsc.generic.ingest.confgen.model.Answers
import com.hcsc.generic.ingest.confgen.render.ConfigRenderers
import com.hcsc.generic.ingest.confgen.summary.ConfigSummary
import com.hcsc.generic.ingest.confgen.validate.DryRunValidator
import com.hcsc.generic.ingest.confgen.wizard.Wizard

import java.nio.file.{Files, Paths}

/**
  * Interactive configuration generator.
  *
  * {{{
  * java -cp ingestion-app-...-jar-with-dependencies.jar \
  *   com.hcsc.generic.ingest.confgen.ConfigGeneratorMain \
  *   [--source-type jdbc|file|kafka] [--output-dir DIR] [--formats hocon,yaml,json]
  *   [--draft FILE]        resume (and keep auto-saving) this draft
  *   [--answers FILE]      non-interactive: pre-load answers, fail if any are missing
  *   [--connect-test]      probe database connectivity after generation (JDBC)
  *   [--no-color]
  * }}}
  *
  * Exit codes: 0 generated, 1 validation failed, 2 bad usage.
  */
object ConfigGeneratorMain {

  case class Options(
    sourceType: Option[String] = None,
    outputDir: String = "generated-configs",
    formats: Seq[String] = Seq("hocon"),
    draft: Option[String] = None,
    answersFile: Option[String] = None,
    connectTest: Boolean = false,
    noColor: Boolean = false
  )

  def main(args: Array[String]): Unit = {
    val options = parseArgs(args)
    val console: ConsoleIO = new AnsiConsole(if (options.noColor) Some(false) else None)
    System.exit(run(options, console))
  }

  /** Testable entry point (no System.exit, injectable console). */
  def run(options: Options, console: ConsoleIO): Int = {
    QuestionFlowRegistry.registerDefaults()
    options.formats.foreach(ConfigRenderers.extensionFor) // fail fast on typos

    // Resume order: draft > answers file > empty session.
    val (initialType, answers) = options.draft.filter(p => Files.exists(Paths.get(p))) match {
      case Some(path) =>
        val draft = Drafts.load(path)
        console.info(s"Resuming draft $path (${draft.answers.ordered.size} answers)")
        (Some(draft.sourceType), draft.answers)
      case None =>
        options.answersFile match {
          case Some(path) =>
            val draft = Drafts.load(path)
            (Some(draft.sourceType), draft.answers)
          case None => (None, new Answers)
        }
    }

    val sourceType = options.sourceType.orElse(initialType).getOrElse {
      val available = QuestionFlowRegistry.availableNames
      var chosen = ""
      while (chosen.isEmpty) {
        val raw = console.readLine(s"Source type (${available.mkString("/")}): ").trim.toLowerCase
        if (available.contains(raw)) chosen = raw
        else console.error(s"'$raw' is not one of ${available.mkString(", ")}")
      }
      chosen
    }
    val flow = QuestionFlowRegistry.resolve(sourceType)

    console.header(s"=== Configuration generator: $sourceType feed ===")
    console.info("Answer '?' for help on any question, ':save' to save a draft and exit.")

    val completed =
      if (options.answersFile.isDefined && options.draft.isEmpty)
        nonInteractive(flow, answers, console) match {
          case Left(missing) =>
            console.error(s"Answers file is missing required answers: ${missing.mkString(", ")}")
            return 2
          case Right(a) => a
        }
      else
        new Wizard(console, options.draft).run(sourceType, flow.questions, answers)

    // A ':save' mid-session leaves required questions unanswered — that is a
    // saved draft, not a generation run.
    if (findMissing(flow, completed).nonEmpty) return 0

    // JDBC schema introspection: generate the column mapping from the source
    // table's metadata. An explicitly supplied schema.columns answer wins.
    if (sourceType == "jdbc" && completed.isTrue("_schema.use_contract") &&
        completed.isTrue("_schema.introspect") && !completed.contains("schema.columns")) {
      val draftFeed = ConfigAssembler.assemble(flow, completed)
      com.hcsc.generic.ingest.confgen.introspect.JdbcSchemaIntrospector
        .introspect(draftFeed, console) match {
        case Right(columns) =>
          completed.put("schema.columns",
            com.hcsc.generic.ingest.confgen.model.AnswerValue.Blocks(columns))
        case Left(problem) =>
          console.error(s"Schema introspection failed: $problem")
          console.error("Re-run answering 'n' to introspection and supply the columns " +
            "inline or via @file, or fix the connectivity/table name and retry.")
          return 1
      }
    }

    val feed = ConfigAssembler.assemble(flow, completed)
    val report = DryRunValidator.validate(feed)

    report.warnings.foreach(w => console.warn(s"WARN: $w"))
    if (!report.ok) {
      console.error("Configuration failed validation:")
      report.errors.foreach(e => console.error(s"  - $e"))
      return 1
    }

    report.sqlPreview.foreach { sql =>
      console.header("\n--- Extraction SQL preview ---")
      console.println(sql)
    }

    if (options.connectTest && sourceType == "jdbc")
      DryRunValidator.connectivityTest(feed) match {
        case Right(message) => console.success(s"Connectivity: $message")
        case Left(problem) => console.warn(s"Connectivity: $problem")
      }

    val entity = feed.getString("entity")
    // The wizard validates entity interactively, but answers/draft files
    // bypass per-question validation — and entity becomes a file name and
    // an include(...) string, so it must be re-checked unconditionally.
    if (!entity.matches("[A-Za-z][A-Za-z0-9_-]*")) {
      console.error(s"entity '$entity' is not a valid identifier " +
        "(letters, digits, underscore, hyphen; must start with a letter) — " +
        "fix it in the answers/draft file")
      return 2
    }
    val wrapped = ConfigAssembler.wrapAsFeeds(feed)
    Files.createDirectories(Paths.get(options.outputDir))
    val utf8 = java.nio.charset.StandardCharsets.UTF_8
    val writeProblems = scala.collection.mutable.ArrayBuffer.empty[String]
    options.formats.foreach {
      case "hocon" =>
        // HOCON output is split for maintainability: the (potentially very
        // large) schema contract goes to its own file, included from the
        // main feed file. The runtime resolves the include natively.
        val schemaFile = com.hcsc.generic.ingest.confgen.render.HoconFeedWriter
          .renderSchema(entity, feed)
          .map { content =>
            val name = com.hcsc.generic.ingest.confgen.render.HoconFeedWriter.schemaFileName(entity)
            val path = Paths.get(options.outputDir, name)
            Files.write(path, content.getBytes(utf8))
            console.success(s"Wrote $path (schema contract / mapping document)")
            name
          }
        val mainPath = Paths.get(options.outputDir, s"$entity.conf")
        Files.write(mainPath, com.hcsc.generic.ingest.confgen.render.HoconFeedWriter
          .renderMain(entity, feed, schemaFile).getBytes(utf8))
        console.success(s"Wrote $mainPath")

        // Self-check: the split files must re-parse to EXACTLY the assembled
        // configuration the dry-run validated.
        val reparsed = com.typesafe.config.ConfigFactory.parseFile(mainPath.toFile)
        if (ConfigRenderers.render(reparsed, "json") != ConfigRenderers.render(wrapped, "json"))
          writeProblems += s"$mainPath did not round-trip to the validated configuration " +
            "(generator bug — do not deploy these files)"

      case format =>
        val path = Paths.get(options.outputDir, s"$entity.${ConfigRenderers.extensionFor(format)}")
        Files.write(path, ConfigRenderers.render(wrapped, format).getBytes(utf8))
        console.success(s"Wrote $path")
    }
    if (writeProblems.nonEmpty) {
      writeProblems.foreach(p => console.error(p))
      return 1
    }

    console.header("\n--- Configuration summary (secrets masked) ---")
    console.println(ConfigRenderers.render(ConfigSummary.masked(wrapped), "hocon"))
    0
  }

  /** Non-interactive mode: every applicable required question must already be
    * answered; nothing is prompted. */
  private def nonInteractive(
    flow: com.hcsc.generic.ingest.confgen.flow.SourceQuestionFlow,
    answers: Answers,
    console: ConsoleIO
  ): Either[Seq[String], Answers] = {
    // Apply defaults for unanswered optional/defaulted questions.
    flow.questions.foreach { q =>
      if (q.appliesWhen(answers) && !answers.contains(q.id))
        q.defaultFrom(answers).orElse(q.default).filter(_.nonEmpty).foreach { d =>
          q.validate(d) match {
            case Right(v) => answers.put(q.id, com.hcsc.generic.ingest.confgen.model.AnswerValue.Scalar(v))
            case Left(problem) => console.warn(s"Default for ${q.id} rejected: $problem")
          }
        }
    }
    val missing = findMissing(flow, answers)
    if (missing.nonEmpty) Left(missing) else Right(answers)
  }

  private def findMissing(
    flow: com.hcsc.generic.ingest.confgen.flow.SourceQuestionFlow,
    answers: Answers
  ): Seq[String] =
    flow.questions
      .filter(q => q.required && q.appliesWhen(answers) && !answers.contains(q.id) &&
        q.defaultFrom(answers).orElse(q.default).isEmpty)
      .map(_.id)

  private[confgen] def parseArgs(args: Array[String]): Options = {
    var options = Options()
    var index = 0
    def value(flag: String): String = {
      require(index + 1 < args.length, s"Missing value for $flag")
      args(index + 1)
    }
    while (index < args.length) {
      args(index) match {
        case "--source-type" => options = options.copy(sourceType = Some(value("--source-type").toLowerCase)); index += 2
        case "--output-dir" => options = options.copy(outputDir = value("--output-dir")); index += 2
        case "--formats" =>
          options = options.copy(formats = value("--formats").split(",").map(_.trim.toLowerCase).filter(_.nonEmpty).toSeq)
          index += 2
        case "--draft" => options = options.copy(draft = Some(value("--draft"))); index += 2
        case "--answers" => options = options.copy(answersFile = Some(value("--answers"))); index += 2
        case "--connect-test" => options = options.copy(connectTest = true); index += 1
        case "--no-color" => options = options.copy(noColor = true); index += 1
        case unknown => throw new IllegalArgumentException(s"Unknown argument: $unknown")
      }
    }
    options
  }
}
