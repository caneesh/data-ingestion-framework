package com.hcsc.generic.ingest.confgen

import com.hcsc.generic.ingest.confgen.draft.Drafts
import com.hcsc.generic.ingest.confgen.io.ScriptedConsole
import com.hcsc.generic.ingest.confgen.model.{Answers, AnswerValue}
import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Paths}

/** Non-interactive generation from an answers file, multi-format output and
  * exit codes. */
class MainNonInteractiveTest extends AnyFunSuite {

  private def jdbcAnswers(): Answers = {
    val a = new Answers
    Seq(
      "entity" -> "orders_feed",
      "mode" -> "FULL",
      "source.url" -> "jdbc:h2:mem:main_e2e",
      "source.dialect" -> "generic",
      "source.driver" -> "org.h2.Driver",
      "source.auth.type" -> "SQL_PASSWORD",
      "source.auth.user" -> "sa",
      "_auth.secret.provider" -> "inline",
      "_auth.secret.value" -> "local-only",
      "source.mode" -> "FULL_TABLE",
      "source.table" -> "d_orders",
      "_perf.partitioning" -> "NONE",
      "_audit.enabled" -> "false",
      "raw.database" -> "orders_raw",
      "raw.path" -> "/tmp/raw/orders",
      "curated.enabled" -> "false"
    ).foreach { case (k, v) => a.put(k, AnswerValue.Scalar(v)) }
    a
  }

  test("answers file generates HOCON, JSON and YAML that agree with each other") {
    val dir = Files.createTempDirectory("confgen-out")
    val answersFile = Files.createTempFile("answers", ".json").toString
    Drafts.save(answersFile, "jdbc", jdbcAnswers())

    val console = new ScriptedConsole(Seq.empty)
    val exit = ConfigGeneratorMain.run(
      ConfigGeneratorMain.Options(
        outputDir = dir.toString,
        formats = Seq("hocon", "json", "yaml"),
        answersFile = Some(answersFile)),
      console)
    assert(exit == 0, console.output)

    val conf = Paths.get(dir.toString, "orders_feed.conf")
    val json = Paths.get(dir.toString, "orders_feed.json")
    val yaml = Paths.get(dir.toString, "orders_feed.yaml")
    Seq(conf, json, yaml).foreach(p => assert(Files.exists(p), s"missing $p"))

    // HOCON and JSON parse back to the same feed configuration.
    val fromHocon = ConfigFactory.parseString(new String(Files.readAllBytes(conf), "UTF-8"))
    val fromJson = ConfigFactory.parseString(new String(Files.readAllBytes(json), "UTF-8"))
    assert(fromHocon == fromJson)
    assert(fromHocon.getString("feeds.orders_feed.source.table") == "d_orders")
    // Defaults applied for unanswered questions (retry, fetchsize, raw table name).
    assert(fromHocon.getInt("feeds.orders_feed.source.retry.max_attempts") == 3)
    assert(fromHocon.getString("feeds.orders_feed.raw.table") == "orders_feed")

    // The summary is printed with the inline secret masked.
    assert(console.output.contains("secrets masked"))
    assert(!console.output.contains("local-only"))
    assert(console.output.contains("Extraction SQL preview"))
  }

  test("missing required answers fail with exit code 2 and name the gaps") {
    val incomplete = jdbcAnswers()
    incomplete.remove("source.url")
    val answersFile = Files.createTempFile("answers-bad", ".json").toString
    Drafts.save(answersFile, "jdbc", incomplete)

    val console = new ScriptedConsole(Seq.empty)
    val exit = ConfigGeneratorMain.run(
      ConfigGeneratorMain.Options(
        outputDir = Files.createTempDirectory("confgen-out2").toString,
        answersFile = Some(answersFile)),
      console)
    assert(exit == 2)
    assert(console.output.contains("source.url"))
  }

  test("a feed that fails dry-run validation exits 1 with the parser's error") {
    val broken = jdbcAnswers()
    // Answers files may set config paths directly; a partially configured
    // STATIC_RANGE (column without bounds) is rejected by the JDBC parser.
    broken.put("source.partition_strategy", AnswerValue.Scalar("STATIC_RANGE"))
    broken.put("source.partitionColumn", AnswerValue.Scalar("order_id"))
    val answersFile = Files.createTempFile("answers-broken", ".json").toString
    Drafts.save(answersFile, "jdbc", broken)

    val console = new ScriptedConsole(Seq.empty)
    val exit = ConfigGeneratorMain.run(
      ConfigGeneratorMain.Options(
        outputDir = Files.createTempDirectory("confgen-out3").toString,
        answersFile = Some(answersFile)),
      console)
    assert(exit == 1)
    assert(console.output.contains("JDBC_003"))
  }

  test("argument parser handles every flag and rejects unknowns") {
    val options = ConfigGeneratorMain.parseArgs(Array(
      "--source-type", "JDBC", "--output-dir", "/tmp/o",
      "--formats", "hocon, yaml", "--draft", "/tmp/d.json",
      "--connect-test", "--no-color"))
    assert(options.sourceType.contains("jdbc"))
    assert(options.formats == Seq("hocon", "yaml"))
    assert(options.connectTest && options.noColor)

    intercept[IllegalArgumentException] {
      ConfigGeneratorMain.parseArgs(Array("--bogus"))
    }
  }
}
