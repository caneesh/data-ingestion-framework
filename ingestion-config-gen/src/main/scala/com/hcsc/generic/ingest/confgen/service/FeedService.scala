package com.hcsc.generic.ingest.confgen.service

import com.hcsc.generic.ingest.confgen.build.ConfigAssembler
import com.hcsc.generic.ingest.confgen.flow.QuestionFlowRegistry
import com.hcsc.generic.ingest.confgen.model.{Answers, AnswerValue}
import com.hcsc.generic.ingest.confgen.render.{ConfigRenderers, HoconFeedWriter}
import com.hcsc.generic.ingest.confgen.validate.DryRunValidator
import com.typesafe.config.{Config, ConfigFactory}

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

/**
  * A stored feed, in the form an application would persist.
  *
  * Deliberately NOT a typed schema of every framework option. The settings
  * are the same ordered answer set the generator already round-trips through
  * drafts, so this model gains a store without gaining a second definition
  * of what a feed is — which would then have to be kept in step with
  * `CONFIGURATION_MODEL.md` by hand, and wouldn't be.
  *
  * Ordering is significant: it drives the generated layout, so a rendered
  * feed is stable across renders and diffs cleanly between versions.
  */
final case class FeedDefinition(
  entity: String,
  sourceType: String,
  settings: Seq[(String, AnswerValue)]
)

/**
  * The outcome of validating a feed, in the shape an API returns.
  *
  * `errors` block a save; `warnings` do not. `sqlPreview` is the statement a
  * JDBC feed would actually issue — the single most useful thing to show
  * someone authoring an extract, and the reason validation belongs at
  * authoring time rather than at 3am.
  */
final case class ValidationReport(
  errors: Seq[String],
  warnings: Seq[String],
  sqlPreview: Option[String]
) {
  def ok: Boolean = errors.isEmpty
}

/**
  * Headless feed API: render, validate, fingerprint, write.
  *
  * This is the seam a control-plane application builds on (see
  * `docs/architecture/CONTROL_PLANE_DESIGN.md`). It has no terminal, no
  * prompts, no draft files and no `System.exit` — every operation is a
  * function from values to values, so it is callable from a service, a
  * test, or CI validating every feed definition in a repository.
  *
  * WHAT THIS DELIBERATELY DOES NOT DO: define validation rules. Every check
  * comes from the framework's own parsers via [[DryRunValidator]], so a feed
  * this accepts is a feed the pipeline accepts. A control plane that grew
  * its own rules could bless a feed the pipeline then rejects, which is
  * worse than no validation at all.
  */
object FeedService {

  /**
    * Makes the source flows available. Idempotent, and called automatically
    * by every entry point below — a headless caller must not have to know
    * that a registry needs priming, which is the kind of setup step that
    * works in a test and fails in a service.
    */
  private def ensureFlows(): Unit = QuestionFlowRegistry.registerDefaults()

  /**
    * Stored definition -> feed configuration, UNWRAPPED (no `feeds.<entity>`
    * nesting). This is the form both [[validate]] and the pipeline's own
    * `FeedCompatibilityValidator` expect.
    */
  def render(definition: FeedDefinition): Config = {
    ensureFlows()
    val answers = new Answers
    definition.settings.foreach { case (id, value) => answers.put(id, value) }
    ConfigAssembler.assemble(QuestionFlowRegistry.resolve(definition.sourceType), answers)
  }

  /**
    * Validates through the framework's own parsers.
    *
    * Delegates to [[DryRunValidator]] alone, which ALREADY invokes
    * `FeedCompatibilityValidator` internally for the CFG_* cross-section
    * matrix. Calling both here would report every CFG_* twice — a detail
    * worth stating because the obvious implementation is wrong.
    */
  def validate(feed: Config): ValidationReport = {
    val r = DryRunValidator.validate(feed)
    ValidationReport(r.errors, r.warnings, r.sqlPreview)
  }

  /** Convenience: the usual authoring round trip in one call. */
  def renderAndValidate(definition: FeedDefinition): (Config, ValidationReport) = {
    val feed = render(definition)
    (feed, validate(feed))
  }

  /**
    * Stable digest of the feed's KEY STRUCTURE — the same recipe the run
    * ledger stamps into `config_fingerprint`, so a stored version can be
    * matched against the runs produced under it.
    *
    * KNOWN LIMITATION, inherited deliberately rather than diverging: values
    * are not hashed, so two versions differing only in a value (a lease, a
    * threshold) produce the SAME fingerprint. The ledger closes this for
    * overrides with an `+ovr:` suffix; a control plane storing versions
    * needs its own version identifier and must not rely on this alone.
    */
  def fingerprint(feed: Config): String =
    com.hcsc.generic.ingest.audit.AuditService.fingerprint(feed)

  /**
    * Writes the deployable artifacts and returns their paths: the feed file
    * and, when the definition carries a schema contract, the schema file it
    * `include`s by relative name (so both must land in one directory).
    *
    * REFUSES TO WRITE A FILE THAT DOES NOT RE-PARSE to the configuration
    * that was validated. Rendering and parsing are separate code paths, and
    * a control plane that validates X while shipping Y would be actively
    * harmful — worse than not validating, because the report says it is
    * safe. The generator already self-checks this; a service needs it more,
    * not less.
    */
  def write(entity: String, feed: Config, targetDir: Path): Seq[Path] = {
    Files.createDirectories(targetDir)
    val schemaBody = HoconFeedWriter.renderSchema(entity, feed)
    val schemaName = schemaBody.map(_ => HoconFeedWriter.schemaFileName(entity))

    val written = schemaBody.zip(schemaName).map { case (body, name) =>
      val p = targetDir.resolve(name)
      Files.write(p, body.getBytes(StandardCharsets.UTF_8))
      p
    }.toSeq

    val mainPath = targetDir.resolve(s"$entity.conf")
    Files.write(mainPath,
      HoconFeedWriter.renderMain(entity, feed, schemaName).getBytes(StandardCharsets.UTF_8))

    val expected = ConfigAssembler.wrapAsFeeds(feed)
    val reparsed = ConfigFactory.parseFile(mainPath.toFile)
    if (ConfigRenderers.render(reparsed, "json") != ConfigRenderers.render(expected, "json"))
      throw new IllegalStateException(
        s"CFG_020 rendered feed at $mainPath does not re-parse to the configuration that was " +
          "validated. The artifact was written but MUST NOT be deployed — validating one " +
          "configuration and shipping another is worse than not validating. This indicates a " +
          "renderer defect, not a bad feed definition.")

    written :+ mainPath
  }
}
