package com.hcsc.generic.ingest.confgen.flow

import com.hcsc.generic.ingest.confgen.model.{Answers, AnswerValue}
import org.scalatest.funsuite.AnyFunSuite

/** Rule-driven questionnaire: only applicable questions show, driven by
  * previous answers. */
class FlowBranchingTest extends AnyFunSuite {

  private def answers(pairs: (String, String)*): Answers = {
    val a = new Answers
    pairs.foreach { case (k, v) => a.put(k, AnswerValue.Scalar(v)) }
    a
  }

  private def applicableIds(flow: SourceQuestionFlow, a: Answers): Seq[String] =
    flow.questions.filter(_.appliesWhen(a)).map(_.id)

  test("registry resolves built-in flows and rejects unknown types") {
    QuestionFlowRegistry.registerDefaults()
    assert(QuestionFlowRegistry.resolve("JDBC") eq JdbcQuestionFlow)
    assert(QuestionFlowRegistry.availableNames.contains("kafka"))
    val ex = intercept[IllegalArgumentException] { QuestionFlowRegistry.resolve("mainframe") }
    assert(ex.getMessage.contains("available"))
  }

  test("watermark questions appear only for INCREMENTAL mode") {
    val full = answers("source.mode" -> "FULL_TABLE")
    assert(!applicableIds(JdbcQuestionFlow, full).exists(_.startsWith("source.incremental")))

    val incr = answers("source.mode" -> "INCREMENTAL")
    val ids = applicableIds(JdbcQuestionFlow, incr)
    assert(ids.contains("source.incremental.watermark_type"))
    assert(ids.contains("source.incremental.initial_value"))
  }

  test("composite column types asked only for COMPOSITE; string ack only for STRING") {
    val composite = answers("source.mode" -> "INCREMENTAL",
      "source.incremental.watermark_type" -> "COMPOSITE")
    assert(applicableIds(JdbcQuestionFlow, composite).contains("source.incremental.column_types"))
    assert(!applicableIds(JdbcQuestionFlow, composite).contains("_watermark.string_ack"))

    val string = answers("source.mode" -> "INCREMENTAL",
      "source.incremental.watermark_type" -> "STRING")
    assert(applicableIds(JdbcQuestionFlow, string).contains("_watermark.string_ack"))
    // Overlap is meaningless for lexicographic STRING watermarks
    assert(!applicableIds(JdbcQuestionFlow, string).contains("source.incremental.overlap"))
  }

  test("auth branching: each mode asks only for its own credentials") {
    val managed = answers("source.auth.type" -> "MANAGED_IDENTITY")
    val managedIds = applicableIds(JdbcQuestionFlow, managed)
    assert(!managedIds.contains("source.auth.user"))
    assert(!managedIds.contains("_auth.secret.provider"))

    val sp = answers("source.auth.type" -> "ENTRA_SERVICE_PRINCIPAL",
      "_auth.secret.provider" -> "azure_keyvault")
    val spIds = applicableIds(JdbcQuestionFlow, sp)
    assert(spIds.contains("source.auth.client_id"))
    assert(spIds.contains("_auth.secret.vault_url"))
    assert(!spIds.contains("_auth.secret.key"))
    assert(JdbcQuestionFlow.secretField(sp).contains("client_secret"))

    val token = answers("source.auth.type" -> "ACCESS_TOKEN")
    assert(JdbcQuestionFlow.secretField(token).contains("token"))
  }

  test("partitioning questions follow the chosen strategy") {
    val static = answers("_perf.partitioning" -> "STATIC_RANGE")
    val staticIds = applicableIds(JdbcQuestionFlow, static)
    assert(staticIds.contains("source.lowerBound"))
    assert(!staticIds.contains("source.partition_predicates"))

    val preds = answers("_perf.partitioning" -> "PREDICATES")
    val predIds = applicableIds(JdbcQuestionFlow, preds)
    assert(predIds.contains("source.partition_predicates"))
    assert(!predIds.contains("source.partitionColumn"))
  }

  test("file flow: managed folders vs static glob, contract sub-questions") {
    val managed = answers("_file.managed" -> "true")
    val ids = applicableIds(FileQuestionFlow, managed)
    assert(ids.contains("source.folders.landing"))
    assert(!ids.contains("source.path"))

    val glob = answers("_file.managed" -> "false")
    assert(applicableIds(FileQuestionFlow, glob).contains("source.path"))

    val noContract = answers("_schema.use_contract" -> "false")
    assert(!applicableIds(FileQuestionFlow, noContract).contains("schema.columns"))

    val customPolicies = answers("_schema.use_contract" -> "true", "_schema.policy_defaults" -> "false")
    assert(applicableIds(FileQuestionFlow, customPolicies).contains("schema.on_missing_column"))
  }

  test("kafka flow: SASL questions only for SASL protocols, schema only for json") {
    val plain = answers("source.kafka.security.protocol" -> "PLAINTEXT")
    assert(!applicableIds(KafkaQuestionFlow, plain).contains("source.kafka.sasl.mechanism"))

    val sasl = answers("source.kafka.security.protocol" -> "SASL_SSL")
    assert(applicableIds(KafkaQuestionFlow, sasl).contains("source.kafka.sasl.jaas.config"))

    val json = answers("source.value.format" -> "json")
    assert(applicableIds(KafkaQuestionFlow, json).contains("source.value.schema"))
  }

  test("STRING watermark acknowledgement rejects a 'no' answer") {
    val q = JdbcQuestionFlow.questions.find(_.id == "_watermark.string_ack").get
    assert(q.validate("false").isLeft)
    assert(q.validate("true") == Right("true"))
  }

  test("defaults come from earlier answers where sensible") {
    val a = answers("entity" -> "member_feed")
    val rawTable = CommonQuestions.destination.find(_.id == "raw.table").get
    assert(rawTable.defaultFrom(a).contains("member_feed"))
  }
}
