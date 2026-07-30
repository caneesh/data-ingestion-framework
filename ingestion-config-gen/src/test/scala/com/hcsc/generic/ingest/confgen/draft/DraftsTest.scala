package com.hcsc.generic.ingest.confgen.draft

import com.hcsc.generic.ingest.confgen.flow.JdbcQuestionFlow
import com.hcsc.generic.ingest.confgen.io.ScriptedConsole
import com.hcsc.generic.ingest.confgen.model.{Answers, AnswerValue}
import com.hcsc.generic.ingest.confgen.wizard.Wizard
import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.Files

/** Draft persistence and mid-session resume. */
class DraftsTest extends AnyFunSuite {

  test("drafts round-trip scalars, item lists and structured blocks in order") {
    val answers = new Answers
    answers.put("entity", AnswerValue.Scalar("claims"))
    answers.put("source.columns", AnswerValue.Items(Seq("a", "b")))
    answers.put("schema.columns", AnswerValue.Blocks(Seq(
      ConfigFactory.parseString("""name = "id", type = "string", required = true"""))))

    val path = Files.createTempFile("draft", ".json").toString
    Drafts.save(path, "jdbc", answers)
    val loaded = Drafts.load(path)

    assert(loaded.sourceType == "jdbc")
    assert(loaded.answers.ordered.map(_._1) == Seq("entity", "source.columns", "schema.columns"))
    assert(loaded.answers.scalar("entity").contains("claims"))
    assert(loaded.answers.items("source.columns") == Seq("a", "b"))
    loaded.answers.get("schema.columns") match {
      case Some(AnswerValue.Blocks(blocks)) => assert(blocks.head.getString("name") == "id")
      case other => fail(s"expected Blocks, got $other")
    }
  }

  test("':save' persists a draft mid-session and the session resumes after it") {
    val draftPath = Files.createTempFile("session", ".json").toString

    // First session: answer three questions, then save and exit.
    val first = new ScriptedConsole(Seq("claims_feed", "INCR",
      "jdbc:h2:mem:draft_resume", ":save"))
    new Wizard(first, Some(draftPath))
      .run("jdbc", JdbcQuestionFlow.questions, new Answers)
    assert(first.output.contains("Draft saved"))

    val draft = Drafts.load(draftPath)
    assert(draft.answers.scalar("source.url").contains("jdbc:h2:mem:draft_resume"))
    assert(!draft.answers.contains("source.auth.type"))

    // Second session resumes: already-answered questions are not asked again.
    val second = new ScriptedConsole(Seq(":save"))
    new Wizard(second, Some(draftPath))
      .run("jdbc", JdbcQuestionFlow.questions, draft.answers)
    assert(!second.output.contains("Feed entity name"))
    // The next unanswered question is the dialect (Source group continues).
    assert(second.transcript.exists(_.contains("SQL dialect")))
  }

  test("loading a missing draft fails clearly") {
    val ex = intercept[IllegalArgumentException] { Drafts.load("/no/such/draft.json") }
    assert(ex.getMessage.contains("not found"))
  }

  test("secret answers are excluded from saved drafts") {
    import com.hcsc.generic.ingest.confgen.model.{Answers, AnswerValue}
    val answers = new Answers
    answers.put("entity", AnswerValue.Scalar("orders"))
    answers.put("_auth.secret.value", AnswerValue.Scalar("super-secret"))

    val path = java.nio.file.Files.createTempFile("draft-secrets", ".json").toString
    Drafts.save(path, "jdbc", answers, excludeIds = Set("_auth.secret.value"))

    val content = new String(java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(path)), "UTF-8")
    assert(!content.contains("super-secret"), "drafts must never persist secret values")
    val reloaded = Drafts.load(path)
    assert(reloaded.answers.scalar("entity").contains("orders"))
    assert(!reloaded.answers.contains("_auth.secret.value"),
      "a resumed session must re-ask the secret")
  }
}
