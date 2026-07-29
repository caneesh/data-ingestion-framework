package com.hcsc.generic.ingest.confgen.render

import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import org.scalatest.funsuite.AnyFunSuite

/** Split HOCON output: the schema contract lives in its own file, included
  * with `include required(...)`, and the pair must re-parse to exactly the
  * assembled configuration. */
class HoconFeedWriterTest extends AnyFunSuite {

  private val feed = ConfigFactory.parseString(
    """
      |entity = member
      |mode = FULL
      |source { type = jdbc, url = "jdbc:h2:mem:x", table = members }
      |schema {
      |  version = "1.0"
      |  columns = [
      |    { name = subscriber_id, type = string, nullable = false },
      |    { name = amount, type = int }
      |  ]
      |}
      |raw { database = r, table = member }
      |curated { enabled = false }
      |audit { database = a }
      |""".stripMargin)

  test("split output round-trips through include required(...)") {
    val dir = Files.createTempDirectory("writer-test")
    val schemaName = HoconFeedWriter.schemaFileName("member")
    Files.write(dir.resolve(schemaName),
      HoconFeedWriter.renderSchema("member", feed).get.getBytes(UTF_8))
    val main = dir.resolve("member.conf")
    Files.write(main,
      HoconFeedWriter.renderMain("member", feed, Some(schemaName)).getBytes(UTF_8))

    val reparsed = ConfigFactory.parseFile(main.toFile)
    val wrapped = ConfigFactory.empty().withValue("feeds.member", feed.root())
    assert(reparsed.root().render(ConfigRenderOptions.concise()) ==
      wrapped.root().render(ConfigRenderOptions.concise()),
      "split files must re-parse to exactly the assembled configuration")
    assert(reparsed.getConfigList("feeds.member.schema.columns").size() == 2)
  }

  test("main file declares the include and keeps a logical section order") {
    val text = HoconFeedWriter.renderMain("member", feed, Some("member-schema.conf"))
    assert(text.contains("include required(\"member-schema.conf\")"))
    val iSource = text.indexOf("source {")
    val iRaw = text.indexOf("raw {")
    val iCurated = text.indexOf("curated {")
    assert(iSource >= 0 && iRaw > iSource && iCurated > iRaw,
      s"expected source < raw < curated ordering in:\n$text")
    assert(!text.contains("schema {"), "the contract must live only in the schema file")
  }

  test("a deleted schema file fails the parse loudly (include required)") {
    val dir = Files.createTempDirectory("writer-test2")
    val main = dir.resolve("member.conf")
    Files.write(main,
      HoconFeedWriter.renderMain("member", feed, Some("member-schema.conf")).getBytes(UTF_8))
    intercept[com.typesafe.config.ConfigException] {
      ConfigFactory.parseFile(main.toFile)
    }
  }

  test("feeds without a contract render as a single self-contained file") {
    val noSchema = feed.withoutPath("schema")
    assert(HoconFeedWriter.renderSchema("member", noSchema).isEmpty)
    val text = HoconFeedWriter.renderMain("member", noSchema, None)
    assert(!text.contains("include"))
    val reparsed = ConfigFactory.parseString(text)
    assert(reparsed.getString("feeds.member.source.table") == "members")
  }

  test("schema file carries a maintenance header and the full contract") {
    val content = HoconFeedWriter.renderSchema("member", feed).get
    assert(content.startsWith("# Schema contract"))
    assert(content.contains("subscriber_id"))
    val reparsed = ConfigFactory.parseString(content)
    assert(reparsed.getString("schema.version") == "1.0")
  }
}
