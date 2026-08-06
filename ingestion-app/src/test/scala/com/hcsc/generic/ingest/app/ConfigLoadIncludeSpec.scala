package com.hcsc.generic.ingest.app

import com.typesafe.config.ConfigFactory
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

/**
  * Regression cover for `ConfigException$IO: include was not found`.
  *
  * In YARN cluster mode `--files` drops every shipped file in the driver
  * container's WORKING DIRECTORY, so the natural way to name the config is
  * a BARE filename. HOCON resolves `include` relative to the including
  * file's PARENT directory, and `new File("feed.conf").getParentFile` is
  * null — so a bare name sends the include to the classpath, where it does
  * not exist, and the run dies before Spark does anything.
  *
  * These tests write the two files into the JVM's actual working directory
  * (the only way to make a bare filename meaningful) and drive both entry
  * points the operators use: `--conf-path` and `-Dconfig.file`. An earlier
  * fix covered only the first, which is why the failure survived it.
  */
class ConfigLoadIncludeSpec extends AnyFunSuite with BeforeAndAfterEach {

  private val feedName = "cli-spec-feed.conf"
  private val schemaName = "cli-spec-schema.conf"
  private val cwd: Path = Paths.get("").toAbsolutePath
  private var savedConfigFile: Option[String] = None

  override def beforeEach(): Unit = {
    super.beforeEach()
    savedConfigFile = Option(System.getProperty("config.file"))
    write(schemaName,
      """feeds.spec_entity.columns = ["a", "b"]
        |""".stripMargin)
    write(feedName,
      s"""include required("$schemaName")
         |feeds.spec_entity.database = "spec_db"
         |""".stripMargin)
  }

  override def afterEach(): Unit = {
    savedConfigFile match {
      case Some(v) => System.setProperty("config.file", v)
      case None    => System.clearProperty("config.file")
    }
    ConfigFactory.invalidateCaches()
    Files.deleteIfExists(cwd.resolve(feedName))
    Files.deleteIfExists(cwd.resolve(schemaName))
    super.afterEach()
  }

  private def write(name: String, body: String): Unit =
    Files.write(cwd.resolve(name), body.getBytes(StandardCharsets.UTF_8))

  private def assertBothFilesMerged(conf: com.typesafe.config.Config): Unit = {
    import scala.collection.JavaConverters._
    assert(conf.getString("feeds.spec_entity.database") == "spec_db",
      "the feed config itself must be loaded")
    assert(conf.getStringList("feeds.spec_entity.columns").asScala == Seq("a", "b"),
      "the INCLUDED schema file must be merged in, not silently dropped")
  }

  test("--conf-path with a bare filename resolves the include (YARN container shape)") {
    assertBothFilesMerged(IngestMain.loadBaseConfig(Some(feedName)))
  }

  test("--conf-path with an explicit relative './name' resolves the include") {
    assertBothFilesMerged(IngestMain.loadBaseConfig(Some(s"./$feedName")))
  }

  test("--conf-path with an absolute path resolves the include") {
    assertBothFilesMerged(
      IngestMain.loadBaseConfig(Some(cwd.resolve(feedName).toString)))
  }

  test("-Dconfig.file with a bare filename resolves the include") {
    // The exact shape that still failed after the --conf-path-only fix.
    System.setProperty("config.file", feedName)
    ConfigFactory.invalidateCaches()
    assertBothFilesMerged(IngestMain.loadBaseConfig(None))
  }

  test("-Dconfig.file with a bare filename is absolutised in place") {
    System.setProperty("config.file", feedName)
    ConfigFactory.invalidateCaches()
    IngestMain.loadBaseConfig(None)
    val effective = System.getProperty("config.file")
    assert(effective == cwd.resolve(feedName).toString,
      s"config.file must be rewritten to an absolute path, was '$effective'")
  }

  test("-Dconfig.file already absolute is left untouched") {
    val absolute = cwd.resolve(feedName).toString
    System.setProperty("config.file", absolute)
    ConfigFactory.invalidateCaches()
    assertBothFilesMerged(IngestMain.loadBaseConfig(None))
    assert(System.getProperty("config.file") == absolute)
  }

  test("a missing INCLUDE names the searched directory and its contents") {
    // The failure mode that cost two debugging rounds: the stock message
    // says only "include was not found", never where it looked.
    Files.deleteIfExists(cwd.resolve(schemaName))
    val thrown = intercept[Exception](IngestMain.loadBaseConfig(Some(feedName)))
    val msg = String.valueOf(thrown.getMessage)
    assert(msg.contains(schemaName), s"must name the missing include: $msg")
    assert(msg.contains(cwd.toString), s"must name the directory searched: $msg")
    assert(msg.contains(feedName),
      s"must list what IS present so the operator sees the schema is absent: $msg")
    assert(msg.contains("--files"), s"must point at the fix: $msg")
  }

  test("a missing config file still fails loudly rather than silently loading nothing") {
    val missing = cwd.resolve("no-such-feed-config.conf").toString
    assert(!Files.exists(Paths.get(missing)))
    val thrown = intercept[Exception](IngestMain.loadBaseConfig(Some(missing)))
    assert(Option(thrown.getMessage).exists(_.contains("no-such-feed-config")),
      s"the error must name the missing file, got: ${thrown.getMessage}")
  }
}
