package com.hcsc.generic.ingest.app

import com.hcsc.generic.ingest.runtime.OverrideContext
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

/**
  * The operational override layer.
  *
  * A deployed feed config sits behind change control, but operational values
  * — a lock lease, a reject threshold, a webhook — legitimately need to
  * change faster than that process allows. `--override-path` supplies a
  * second file whose values WIN over the feed for every path it declares.
  *
  * The whole feature is one property: **the override always wins**. These
  * tests pin it at the boundaries where "always" usually breaks — nested
  * blocks, lists, substitutions, and values the feed does not declare at all
  * — because a layer that wins in the simple case and silently loses in a
  * nested one is worse than no layer, and would be found in production.
  */
class ConfigOverrideSpec extends AnyFunSuite with BeforeAndAfterEach {

  private val feedName = "ovr-spec-feed.conf"
  private val overrideName = "ovr-spec-override.conf"
  private val cwd: Path = Paths.get("").toAbsolutePath

  override def beforeEach(): Unit = {
    super.beforeEach()
    OverrideContext.reset()
    write(feedName,
      """feeds.spec_entity {
        |  database = "feed_db"
        |  concurrency.lease_minutes = 30
        |  quality.max_reject_percent = 1.0
        |  columns = ["a", "b"]
        |  notify.webhook = "https://feed.example/hook"
        |}
        |""".stripMargin)
  }

  override def afterEach(): Unit = {
    OverrideContext.reset()
    Files.deleteIfExists(cwd.resolve(feedName))
    Files.deleteIfExists(cwd.resolve(overrideName))
    super.afterEach()
  }

  private def write(name: String, body: String): Unit =
    Files.write(cwd.resolve(name), body.getBytes(StandardCharsets.UTF_8))

  private def load(withOverride: Boolean) =
    IngestMain.loadBaseConfig(Some(feedName), if (withOverride) Some(overrideName) else None)

  // ---- the core guarantee ---------------------------------------------------

  test("an override value beats the feed's value for the same path") {
    write(overrideName, """feeds.spec_entity.concurrency.lease_minutes = 120""")
    val conf = load(withOverride = true)
    assert(conf.getInt("feeds.spec_entity.concurrency.lease_minutes") == 120,
      "the point of the layer: the override wins, no exceptions")
  }

  test("overriding one key in a block leaves the block's other keys intact") {
    // HOCON objects MERGE rather than replace, which is what makes a
    // one-line override safe: it cannot silently delete its siblings.
    write(overrideName, """feeds.spec_entity.concurrency.lease_minutes = 120""")
    val conf = load(withOverride = true)
    assert(conf.getString("feeds.spec_entity.database") == "feed_db")
    assert(conf.getDouble("feeds.spec_entity.quality.max_reject_percent") == 1.0)
  }

  test("a LIST is replaced wholesale, not appended to") {
    // Worth pinning explicitly: an operator overriding a column list expects
    // exactly what they wrote, not their list concatenated with the feed's.
    write(overrideName, """feeds.spec_entity.columns = ["x"]""")
    import scala.collection.JavaConverters._
    assert(load(withOverride = true).getStringList("feeds.spec_entity.columns").asScala == Seq("x"))
  }

  test("an override may introduce a path the feed never declared") {
    write(overrideName, """feeds.spec_entity.notify.command = "/usr/local/bin/page-oncall"""")
    val conf = load(withOverride = true)
    assert(conf.getString("feeds.spec_entity.notify.command") == "/usr/local/bin/page-oncall")
    assert(conf.getString("feeds.spec_entity.notify.webhook") == "https://feed.example/hook")
  }

  test("a nested value overridden several levels deep still wins") {
    write(overrideName, """feeds.spec_entity.quality.max_reject_percent = 25.0""")
    assert(load(withOverride = true).getDouble("feeds.spec_entity.quality.max_reject_percent") == 25.0)
  }

  // ---- no override, no change -----------------------------------------------

  test("without the flag the feed config is returned exactly as before") {
    val conf = load(withOverride = false)
    assert(conf.getInt("feeds.spec_entity.concurrency.lease_minutes") == 30)
    assert(OverrideContext.digest.isEmpty, "nothing to record when nothing was overridden")
  }

  test("an empty override file changes nothing and is not silently treated as one") {
    write(overrideName, "# operators sometimes ship the template unedited\n")
    assert(load(withOverride = true).getInt("feeds.spec_entity.concurrency.lease_minutes") == 30)
  }

  test("a blank --override-path is treated as absent, not as a missing file") {
    // Wrapper scripts expand unset variables to empty strings; that must not
    // become a hard failure.
    assert(IngestMain.loadBaseConfig(Some(feedName), Some("   "))
      .getInt("feeds.spec_entity.concurrency.lease_minutes") == 30)
  }

  // ---- failure is loud ------------------------------------------------------

  test("a named-but-missing override file fails the run with CFG_019") {
    // Fail-closed: an operator who asked for an override and silently did not
    // get one is the worst outcome — the run would look normal and behave
    // under the settings they meant to change.
    val thrown = intercept[Exception](
      IngestMain.loadBaseConfig(Some(feedName), Some("no-such-override.conf")))
    assert(thrown.getMessage.contains("CFG_019"))
    assert(thrown.getMessage.contains("--override-path"),
      "the message must name the flag and how to ship the file")
  }

  // ---- substitution across the merged view ----------------------------------

  test("a feed substitution reads the OVERRIDDEN value, not the feed's own") {
    // This is why the merge happens before resolve(): resolving the feed
    // first would freeze `${...}` against values the override replaces.
    write(feedName,
      """feeds.spec_entity.database = "feed_db"
        |feeds.spec_entity.staging_database = ${feeds.spec_entity.database}"_stg"
        |""".stripMargin)
    write(overrideName, """feeds.spec_entity.database = "override_db"""")
    val conf = load(withOverride = true)
    assert(conf.getString("feeds.spec_entity.staging_database") == "override_db_stg",
      "the substitution must see the merged view")
  }

  test("an override may itself substitute from the feed") {
    write(overrideName,
      """feeds.spec_entity.archive_database = ${feeds.spec_entity.database}"_archive"""")
    assert(load(withOverride = true).getString("feeds.spec_entity.archive_database") == "feed_db_archive")
  }

  test("overriding a path whose feed value is itself a substitution still works") {
    // The merge happens on UNRESOLVED configs, where inspecting a path that
    // holds a `${...}` throws. The override diagnostics must not turn that
    // into a failed run.
    write(feedName,
      "feeds.spec_entity.database = \"feed_db\"\n" +
        "feeds.spec_entity.staging_database = ${feeds.spec_entity.database}\"_stg\"\n")
    write(overrideName, "feeds.spec_entity.staging_database = \"explicit_stg\"")
    assert(load(withOverride = true).getString("feeds.spec_entity.staging_database") == "explicit_stg")
  }

  // ---- auditability ---------------------------------------------------------

  test("the override is recorded so the ledger can distinguish the run") {
    write(overrideName, """feeds.spec_entity.concurrency.lease_minutes = 120""")
    load(withOverride = true)
    assert(OverrideContext.digest.isDefined)
    assert(OverrideContext.paths.contains("feeds.spec_entity.concurrency.lease_minutes"),
      "which paths were overridden must be recoverable — by NAME")
  }

  test("two different override files produce different digests") {
    write(overrideName, """feeds.spec_entity.concurrency.lease_minutes = 120""")
    load(withOverride = true)
    val first = OverrideContext.digest
    OverrideContext.reset()
    // Same PATH, different VALUE: the structural config fingerprint cannot
    // tell these apart, which is exactly the gap this digest closes.
    write(overrideName, """feeds.spec_entity.concurrency.lease_minutes = 240""")
    load(withOverride = true)
    assert(OverrideContext.digest != first)
  }

  test("the recorded digest carries no values, only a hash") {
    write(overrideName, """feeds.spec_entity.jdbc.password = "s3cr3t-not-for-the-ledger"""")
    load(withOverride = true)
    val recorded = OverrideContext.digest.getOrElse("") + OverrideContext.paths.mkString(",")
    assert(!recorded.contains("s3cr3t"),
      "an override may carry a credential; only key names and a digest may be kept")
  }

  test("the fingerprint suffix is empty without an override and marked with one") {
    assert(OverrideContext.fingerprintSuffix.isEmpty)
    write(overrideName, """feeds.spec_entity.concurrency.lease_minutes = 120""")
    load(withOverride = true)
    assert(OverrideContext.fingerprintSuffix.startsWith("+ovr:"),
      "the ledger's config_fingerprint must visibly differ for an overridden run")
  }
}
