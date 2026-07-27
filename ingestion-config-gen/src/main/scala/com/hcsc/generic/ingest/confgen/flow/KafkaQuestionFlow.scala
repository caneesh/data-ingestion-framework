package com.hcsc.generic.ingest.confgen.flow

import com.hcsc.generic.ingest.confgen.model.{Answers, Question, QuestionGroups => G, QuestionKind => K, Validators => V}

import java.nio.file.{Files, Paths}

/** Kafka feed questionnaire (batch read: bounded offsets). */
object KafkaQuestionFlow extends SourceQuestionFlow {

  override val sourceType: String = "kafka"

  private def secured(a: Answers) = !a.is("source.kafka.security.protocol", "PLAINTEXT")
  private def json(a: Answers) = a.is("source.value.format", "json")

  /** value.schema may be typed inline or referenced as @file. */
  private val schemaOrFile: String => Either[String, String] = v => {
    val loaded: Either[String, String] =
      if (v.startsWith("@")) {
        val p = Paths.get(v.drop(1).trim)
        if (!Files.exists(p)) Left(s"Schema file not found: ${v.drop(1)}")
        else Right(new String(Files.readAllBytes(p), java.nio.charset.StandardCharsets.UTF_8))
      } else Right(v)
    loaded.right.flatMap { raw =>
      if (raw.trim.startsWith("{")) Right(raw.trim)
      else Left("value.schema must be a Spark StructType JSON document (starts with '{')")
    }
  }

  override def questions: Seq[Question] =
    CommonQuestions.general ++ source ++ extraction ++
      CommonQuestions.audit ++ CommonQuestions.destination

  private def source = Seq(
    Question("source.bootstrap.servers", G.Source, "Bootstrap servers",
      help = "Comma-separated host:port list, e.g. kafka1:9092,kafka2:9092",
      validate = V.nonEmpty),
    Question("source.topic", G.Source, "Topic", validate = V.nonEmpty),
    Question("source.startingOffsets", G.Source, "Starting offsets",
      help = "earliest, latest, or a JSON offset map.",
      default = Some("earliest"), validate = V.nonEmpty),
    Question("source.endingOffsets", G.Source, "Ending offsets",
      default = Some("latest"), validate = V.nonEmpty),
    Question("source.kafka.security.protocol", G.Source, "Security protocol",
      kind = K.Choice(Seq("PLAINTEXT", "SSL", "SASL_SSL", "SASL_PLAINTEXT")),
      default = Some("PLAINTEXT"),
      validate = V.oneOf(Seq("PLAINTEXT", "SSL", "SASL_SSL", "SASL_PLAINTEXT"))),
    Question("source.kafka.sasl.mechanism", G.Source, "SASL mechanism",
      kind = K.Choice(Seq("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "GSSAPI")),
      default = Some("PLAIN"),
      validate = V.oneOf(Seq("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "GSSAPI")),
      appliesWhen = a => a.is("source.kafka.security.protocol", "SASL_SSL") ||
        a.is("source.kafka.security.protocol", "SASL_PLAINTEXT")),
    Question("source.kafka.sasl.jaas.config", G.Source, "SASL JAAS configuration",
      help = "Full JAAS line; masked in summaries. Prefer ${?ENV_VAR} substitution over literals.",
      secret = true, validate = V.nonEmpty,
      appliesWhen = a => secured(a) && (a.is("source.kafka.security.protocol", "SASL_SSL") ||
        a.is("source.kafka.security.protocol", "SASL_PLAINTEXT")))
  )

  private def extraction = Seq(
    Question("source.value.format", G.Extraction, "Message value format",
      kind = K.Choice(Seq("string", "json")), default = Some("string"),
      validate = V.oneOf(Seq("string", "json"))),
    Question("source.value.schema", G.Extraction, "Value schema (Spark StructType JSON, or @file)",
      validate = schemaOrFile, appliesWhen = json)
  )
}
