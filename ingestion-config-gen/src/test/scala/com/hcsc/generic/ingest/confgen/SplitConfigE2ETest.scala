package com.hcsc.generic.ingest.confgen

import com.hcsc.generic.ingest.confgen.draft.Drafts
import com.hcsc.generic.ingest.confgen.flow.JdbcQuestionFlow
import com.hcsc.generic.ingest.confgen.io.ScriptedConsole
import com.hcsc.generic.ingest.confgen.model.{Answers, AnswerValue}
import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths}

/** End-to-end split output: a JDBC feed with a schema contract (mapping
  * imported from a file, as a wide table would be) generates a main feed
  * file plus a separate schema file that re-parse to one configuration. */
class SplitConfigE2ETest extends AnyFunSuite {

  test("JDBC flow offers the schema contract, off by default") {
    val questions = JdbcQuestionFlow.questions
    assert(questions.exists(_.id == "schema.columns"))
    val gate = questions.find(_.id == "_schema.use_contract").get
    assert(gate.default.contains("false"),
      "JDBC contract must be opt-in so existing answers files keep working")
  }

  test("generated JDBC feed splits the mapping into <entity>-schema.conf") {
    // The mapping document arrives as a file, as a 350-column table's would.
    val columnsFile = Files.createTempFile("columns", ".json")
    Files.write(columnsFile,
      """[
        |  { "name": "claim_id", "type": "string", "nullable": false },
        |  { "name": "amount", "type": "int" },
        |  { "name": "modified_ts", "type": "timestamp" }
        |]""".stripMargin.getBytes(UTF_8))

    val answers = new Answers
    Seq(
      "entity" -> "claims_split",
      "mode" -> "FULL",
      "source.url" -> "jdbc:h2:mem:split_e2e",
      "source.dialect" -> "generic",
      "source.driver" -> "org.h2.Driver",
      "source.auth.type" -> "SQL_PASSWORD",
      "source.auth.user" -> "sa",
      "_auth.secret.provider" -> "env",
      "_auth.secret.key" -> "DB_PASSWORD",
      "source.mode" -> "FULL_TABLE",
      "source.table" -> "claims",
      "_schema.use_contract" -> "true",
      "schema.columns" -> s"@${columnsFile.toAbsolutePath}",
      "_perf.partitioning" -> "NONE",
      "_audit.enabled" -> "false",
      "raw.database" -> "claims_raw",
      "raw.path" -> "/tmp/raw/claims",
      "curated.enabled" -> "false"
    ).foreach { case (k, v) => answers.put(k, AnswerValue.Scalar(v)) }

    val answersFile = Files.createTempFile("answers-split", ".json").toString
    Drafts.save(answersFile, "jdbc", answers)

    val dir = Files.createTempDirectory("confgen-split")
    val console = new ScriptedConsole(Seq.empty)
    val exit = ConfigGeneratorMain.run(
      ConfigGeneratorMain.Options(
        outputDir = dir.toString,
        formats = Seq("hocon", "json"),
        answersFile = Some(answersFile)),
      console)
    assert(exit == 0, console.output)

    val main = Paths.get(dir.toString, "claims_split.conf")
    val schema = Paths.get(dir.toString, "claims_split-schema.conf")
    assert(Files.exists(main))
    assert(Files.exists(schema), "the mapping document must be written as its own file")

    // The main file includes the schema file; together they re-parse to the
    // same configuration as the single-file JSON output.
    val mainText = new String(Files.readAllBytes(main), UTF_8)
    assert(mainText.contains("include required(\"claims_split-schema.conf\")"))
    assert(!mainText.contains("claim_id"), "columns must not be duplicated in the main file")

    val fromSplit = ConfigFactory.parseFile(main.toFile)
    val fromJson = ConfigFactory.parseString(
      new String(Files.readAllBytes(Paths.get(dir.toString, "claims_split.json")), UTF_8))
    assert(fromSplit == fromJson)
    assert(fromSplit.getConfigList("feeds.claims_split.schema.columns").size() == 3)
    assert(fromSplit.getString("feeds.claims_split.schema.version") == "1.0")
  }
}
