package com.hcsc.generic.ingest.confgen.flow

import com.hcsc.generic.ingest.confgen.build.ConfigAssembler
import com.hcsc.generic.ingest.confgen.model.{Answers, AnswerValue}
import com.hcsc.generic.ingest.confgen.validate.DryRunValidator
import org.scalatest.funsuite.AnyFunSuite

/** Conjur secret provider wiring in the generator: questions, assembled
  * reference form and dry-run validation with placeholders. */
class ConjurWiringTest extends AnyFunSuite {

  private def answers(pairs: (String, String)*): Answers = {
    val a = new Answers
    pairs.foreach { case (k, v) => a.put(k, AnswerValue.Scalar(v)) }
    a
  }

  test("conjur questions appear only when the conjur provider is selected") {
    val conjur = answers("source.auth.type" -> "SQL_PASSWORD",
      "_auth.secret.provider" -> "conjur")
    val ids = JdbcQuestionFlow.questions.filter(_.appliesWhen(conjur)).map(_.id)
    Seq("_auth.secret.conjur_url", "_auth.secret.account", "_auth.secret.host_id",
      "_auth.secret.api_key_env", "_auth.secret.variable").foreach(id =>
      assert(ids.contains(id), s"missing $id"))
    assert(!ids.contains("_auth.secret.key"))

    val env = answers("source.auth.type" -> "SQL_PASSWORD", "_auth.secret.provider" -> "env")
    val envIds = JdbcQuestionFlow.questions.filter(_.appliesWhen(env)).map(_.id)
    assert(!envIds.contains("_auth.secret.conjur_url"))
  }

  test("assembler emits the conjur reference with the api key as an env reference") {
    val a = answers(
      "entity" -> "claims_feed",
      "mode" -> "FULL",
      "source.url" -> "jdbc:sqlserver://srv:1433;databaseName=Claims",
      "source.auth.type" -> "SQL_PASSWORD",
      "source.auth.user" -> "svc",
      "_auth.secret.provider" -> "conjur",
      "_auth.secret.conjur_url" -> "https://conjur.example.com",
      "_auth.secret.account" -> "myorg",
      "_auth.secret.host_id" -> "data/ingestion/spark",
      "_auth.secret.api_key_env" -> "CONJUR_API_KEY",
      "_auth.secret.variable" -> "applications/ingestion/db/password",
      "source.mode" -> "FULL_TABLE",
      "source.table" -> "dbo.claims",
      "_perf.partitioning" -> "NONE",
      "raw.database" -> "claims_raw",
      "raw.table" -> "claims_feed",
      "raw.path" -> "/tmp/raw/claims")

    val feed = ConfigAssembler.assemble(JdbcQuestionFlow, a)
    val password = feed.getConfig("source.auth.password")
    assert(password.getString("provider") == "conjur")
    assert(password.getString("url") == "https://conjur.example.com")
    assert(password.getString("account") == "myorg")
    assert(password.getString("host_id") == "data/ingestion/spark")
    assert(password.getString("variable") == "applications/ingestion/db/password")
    assert(password.getString("api_key.provider") == "env")
    assert(password.getString("api_key.key") == "CONJUR_API_KEY")

    // Dry-run validates structurally (placeholder substitution for the
    // conjur reference) even though no Conjur server is reachable here.
    val report = DryRunValidator.validate(feed)
    assert(report.ok, report.errors.mkString("; "))
    assert(report.warnings.exists(w => w.contains("password") && w.contains("did not resolve")))
  }
}
