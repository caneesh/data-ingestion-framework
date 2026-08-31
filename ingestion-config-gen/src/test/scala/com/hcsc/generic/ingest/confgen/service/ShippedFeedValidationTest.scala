package com.hcsc.generic.ingest.confgen.service

import com.typesafe.config.{ConfigFactory, ConfigParseOptions}
import org.scalatest.funsuite.AnyFunSuite

import java.io.File

/**
  * Every feed configuration this repository ships must pass the same
  * validation an application would run before saving it.
  *
  * This is the control plane's first phase (feed-lint in CI) applied to the
  * feeds we already have, and it exists because writing it immediately
  * found a real defect: `DryRunValidator` required `raw.path`, which
  * `HiveSink` documents as OPTIONAL ("absent path = managed table in the
  * warehouse") and which NONE of the shipped feeds declare — including the
  * two reference templates and the SmartIQ feeds that run in production.
  *
  * The rule had been written against wizard output, which always supplies a
  * path, so no test ever pointed it at a hand-authored feed. The direction
  * of that failure matters: a validator that rejects feeds the pipeline
  * accepts is not merely noisy, it makes the whole promise unusable —
  * turning validation on would have failed the build for every feed here.
  *
  * These feeds are hand-authored, which also demonstrates the import path
  * for an existing deployment: validate / fingerprint / write all operate
  * on a parsed `Config`, so only `render` needs a wizard-shaped definition.
  */
class ShippedFeedValidationTest extends AnyFunSuite {

  /** Repo root: tests run with the module directory as CWD. */
  private val repoRoot = new File("..").getCanonicalFile

  private def feedAt(relativePath: String, entity: String) = {
    val file = new File(repoRoot, relativePath)
    assert(file.isFile, s"shipped feed not found: $file")
    // resolve() so ${?ENV} placeholders collapse exactly as at runtime.
    ConfigFactory.parseFile(file, ConfigParseOptions.defaults().setAllowMissing(false))
      .resolve().getConfig(s"feeds.$entity")
  }

  private val shipped = Seq(
    ("docs/examples/smartiq_pdp/params/feed-smartiq-pdp.conf", "smartiq_pdp"),
    ("docs/examples/smartiq_pdp/lower-env/params/feed-smartiq-pdp-e2e.conf", "smartiq_pdp_e2e"))

  shipped.foreach { case (path, entity) =>
    test(s"shipped feed validates: $entity") {
      val report = FeedService.validate(feedAt(path, entity))
      assert(report.ok,
        s"$path would be rejected by an application that validates on save: " +
          report.errors.mkString("; "))
    }
  }

  test("a managed-table feed (no raw.path) is valid — absent path means the Hive warehouse") {
    // Pins the fix directly, independent of the shipped files, so the rule
    // cannot regress if those feeds later gain an explicit path.
    val managed = ConfigFactory.parseString(
      """entity = "m"
        |source { type = "file", path = "/landing/*.csv", format = "csv" }
        |raw { database = "d", table = "t" }
        |""".stripMargin)
    val report = FeedService.validate(managed)
    assert(!report.errors.exists(_.contains("raw.path")),
      s"raw.path must not be required: ${report.errors.mkString("; ")}")
  }

  test("the SQL a JDBC feed would issue is previewable straight from the shipped config") {
    // The authoring feature that makes validation worth adopting: the
    // extract author sees the exact statement the source receives.
    val report = FeedService.validate(feedAt(shipped.head._1, shipped.head._2))
    val sql = report.sqlPreview.getOrElse(
      fail("a JDBC feed must preview its extraction SQL"))
    assert(sql.contains("dbo.SmartIQ_PDP"), sql)
    assert(sql.toUpperCase.contains("WHERE"), s"the incremental window must be shown: $sql")
  }

  test("hand-authored feeds still fingerprint, so an imported version is joinable to its runs") {
    val fp = FeedService.fingerprint(feedAt(shipped.head._1, shipped.head._2))
    assert(fp.startsWith("v1:"), fp)
  }
}
