package com.hcsc.generic.ingest.confgen.service

import com.hcsc.generic.ingest.confgen.build.ConfigAssembler
import com.hcsc.generic.ingest.confgen.flow.JdbcQuestionFlow
import com.hcsc.generic.ingest.confgen.io.ScriptedConsole
import com.hcsc.generic.ingest.confgen.model.{Answers, AnswerValue}
import com.hcsc.generic.ingest.confgen.wizard.Wizard
import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Path}

/**
  * The headless feed API a control-plane application builds on
  * (docs/architecture/CONTROL_PLANE_DESIGN.md).
  *
  * The property that makes the seam worth anything is FIDELITY: a feed
  * rendered from a stored definition, with no terminal involved, must be
  * byte-identical to what the interactive generator produces from the same
  * answers. If those two paths can drift, the wizard's test suite stops
  * being evidence about the service, and the service is validating
  * something nobody has ever run.
  */
class FeedServiceTest extends AnyFunSuite {

  /** The wizard session used as the reference — the same script the
    * generator's own end-to-end test drives. */
  private val jdbcScript = Seq(
    "claims_feed", "INCR",
    "jdbc:h2:mem:svc_e2e;DB_CLOSE_DELAY=-1", "generic", "org.h2.Driver",
    "SQL_PASSWORD", "sa", "sysprop", "confgen.svc.pwd",
    "INCREMENTAL", "d_claims", "claim_id, state, modified_ts", "state = 'IL'",
    "n", "NUMERIC", "claim_id", "0", "", "memory", "", "", "n", "", "NONE",
    "claims_raw", "", "hdfs:///tmp/raw/claims", "", "",
    "y", "claims_curated", "", "hdfs:///tmp/cur/claims", "",
    "claim_id", "modified_ts", "", "", "seq desc as bigint", "")

  private def wizardAnswers(script: Seq[String] = jdbcScript): Answers = {
    System.setProperty("confgen.svc.pwd", "s3cret")
    com.hcsc.generic.ingest.confgen.flow.QuestionFlowRegistry.registerDefaults()
    new Wizard(new ScriptedConsole(script))
      .run(JdbcQuestionFlow.sourceType, JdbcQuestionFlow.questions, new Answers)
  }

  /** A stored record, exactly as an application would persist it. */
  private def definitionFrom(answers: Answers, entity: String = "claims_feed") =
    FeedDefinition(entity, "jdbc", answers.ordered)

  private def json(c: com.typesafe.config.Config) =
    com.hcsc.generic.ingest.confgen.render.ConfigRenderers.render(c, "json")

  // ---- fidelity --------------------------------------------------------------

  test("a stored definition renders to EXACTLY what the interactive generator produces") {
    val answers = wizardAnswers()
    val viaWizard = ConfigAssembler.assemble(JdbcQuestionFlow, answers)
    val viaService = FeedService.render(definitionFrom(answers))
    assert(json(viaService) == json(viaWizard),
      "the headless path must not be a second implementation — if these diverge, the " +
        "generator's test suite is no longer evidence about the service")
  }

  test("rendering is deterministic: the same definition twice gives the same config") {
    // A control plane diffs versions and fingerprints them. Both are
    // meaningless if rendering is not reproducible.
    val d = definitionFrom(wizardAnswers())
    assert(json(FeedService.render(d)) == json(FeedService.render(d)))
  }

  test("the caller never has to prime a registry") {
    // The flows live in a process-global registry that the CLI happens to
    // populate at startup. A service that forgot would fail at runtime with
    // an unknown-source-type error, so the API primes it itself.
    val d = definitionFrom(wizardAnswers())
    assert(FeedService.render(d).getString("entity") == "claims_feed")
  }

  // ---- validation ------------------------------------------------------------

  test("a good feed validates clean and previews the SQL it would issue") {
    val (feed, report) = FeedService.renderAndValidate(definitionFrom(wizardAnswers()))
    assert(report.ok, report.errors.mkString("; "))
    assert(feed.getString("entity") == "claims_feed")
    // The preview is the point of validating at authoring time: it shows the
    // author the statement the source will actually receive.
    assert(report.sqlPreview.exists(_.contains("state = 'IL'")), report.sqlPreview.toString)
  }

  test("a broken feed reports errors and is not ok") {
    val broken = ConfigFactory.parseString("""entity = "x", source { type = "jdbc" }""")
    val report = FeedService.validate(broken)
    assert(!report.ok)
    assert(report.errors.nonEmpty)
  }

  test("CFG_* cross-section errors are reported ONCE, not twice") {
    // DryRunValidator already calls FeedCompatibilityValidator internally.
    // The obvious implementation of this API — call both and concatenate —
    // duplicates every CFG_* line in the operator's face. Pinned because the
    // mistake is invisible on a feed that happens to have zero CFG errors.
    val feed = ConfigFactory.parseString(
      """entity = "x"
        |source { type = "file", extraction { mode = "INCREMENTAL" } }
        |raw { database = "d", table = "t", path = "/p" }
        |""".stripMargin)
    val errors = FeedService.validate(feed).errors
    assert(errors.distinct.size == errors.size,
      s"duplicate validation lines: ${errors.mkString(" | ")}")
  }

  // ---- fingerprint -----------------------------------------------------------

  test("fingerprint uses the ledger's recipe so a version can be matched to its runs") {
    val feed = FeedService.render(definitionFrom(wizardAnswers()))
    val fp = FeedService.fingerprint(feed)
    assert(fp.startsWith("v1:"),
      s"must stay the versioned recipe config_fingerprint stamps, got '$fp'")
    assert(FeedService.fingerprint(feed) == fp, "stable across calls")
  }

  test("KNOWN LIMITATION: a value-only change does NOT change the fingerprint") {
    // Documented, not desirable. The digest hashes key STRUCTURE, so two
    // stored versions differing only in a value collide. A control plane
    // must carry its own version identifier and must never use this as the
    // change detector — pinned so the limitation is discovered here rather
    // than by an auditor.
    val a = FeedService.render(definitionFrom(wizardAnswers()))
    val changed = jdbcScript.updated(jdbcScript.indexOf("state = 'IL'"), "state = 'TX'")
    val b = FeedService.render(definitionFrom(wizardAnswers(changed)))

    assert(a.getString("source.where") != b.getString("source.where"), "the feeds do differ")
    assert(FeedService.fingerprint(a) == FeedService.fingerprint(b),
      "if this ever starts failing the recipe changed — update CONTROL_PLANE_DESIGN.md §7")
  }

  // ---- write -----------------------------------------------------------------

  test("write emits artifacts that re-parse to the configuration that was validated") {
    val dir = Files.createTempDirectory("feed-service-write")
    try {
      val feed = FeedService.render(definitionFrom(wizardAnswers()))
      val paths = FeedService.write("claims_feed", feed, dir)

      val main = paths.last
      assert(Files.exists(main) && main.getFileName.toString == "claims_feed.conf")

      // The guarantee the service sells: what was validated is what shipped.
      val reparsed = ConfigFactory.parseFile(main.toFile)
      assert(json(reparsed) == json(ConfigAssembler.wrapAsFeeds(feed)))
      assert(reparsed.getString("feeds.claims_feed.entity") == "claims_feed")
    } finally deleteTree(dir)
  }

  test("write creates the target directory rather than failing on a fresh deployment") {
    val root = Files.createTempDirectory("feed-service-mkdir")
    try {
      val nested = root.resolve("a/b/c")
      val feed = FeedService.render(definitionFrom(wizardAnswers()))
      assert(FeedService.write("claims_feed", feed, nested).forall(Files.exists(_)))
    } finally deleteTree(root)
  }

  private def deleteTree(dir: Path): Unit = {
    import scala.collection.JavaConverters._
    if (Files.exists(dir))
      Files.walk(dir).iterator().asScala.toSeq.reverse.foreach(p => Files.deleteIfExists(p))
  }
}
