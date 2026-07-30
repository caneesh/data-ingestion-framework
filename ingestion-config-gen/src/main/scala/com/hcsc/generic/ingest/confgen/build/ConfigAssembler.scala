package com.hcsc.generic.ingest.confgen.build

import com.hcsc.generic.ingest.confgen.flow.SourceQuestionFlow
import com.hcsc.generic.ingest.confgen.model.{Answers, AnswerValue, Question, QuestionKind}
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}

import scala.collection.JavaConverters._

/**
  * Assembles the collected answers into a feed configuration `Config` — the
  * exact structure `IngestPipeline` / `JdbcSourceConfig.parse` /
  * `SchemaContract.parse` consume today, so generated feeds are backward
  * compatible by construction.
  *
  * Question ids are HOCON paths relative to the feed root; ids starting with
  * `_` are wizard metadata handled by source-type post-processing below.
  */
object ConfigAssembler {

  def assemble(flow: SourceQuestionFlow, answers: Answers): Config = {
    val byId: Map[String, Question] = flow.questions.map(q => q.id -> q).toMap
    var config = ConfigFactory.empty()

    answers.ordered.foreach { case (id, value) =>
      if (!id.startsWith("_")) typedValue(byId.get(id), value).foreach { v =>
        config = config.withValue(quotePath(id), ConfigValueFactory.fromAnyRef(v))
      }
    }

    config = config.withValue("source.type", ConfigValueFactory.fromAnyRef(flow.sourceType))
    // The pipeline requires the run ledger unless a feed opts out EXPLICITLY;
    // a wizard session that declined audit must therefore say so in config.
    if (!answers.isTrue("_audit.enabled") && !config.hasPath("audit"))
      config = config.withValue("audit.enabled", ConfigValueFactory.fromAnyRef(false))
    config = derivePartitionKeys(config)
    config = flow.sourceType match {
      case "jdbc" => jdbcPostProcess(config, answers)
      case "file" => filePostProcess(config, answers)
      case _ => config
    }
    config
  }

  /** Blank scalars are skipped (optional questions answered with their blank
    * default); ints/bools/lists are emitted with their native JSON types.
    * Scalar answers to list questions (defaults, answers files) are parsed
    * through CollectionInput, exactly as the wizard would have. */
  private def typedValue(question: Option[Question], value: AnswerValue): Option[Object] =
    value match {
      case AnswerValue.Scalar(v) if v.isEmpty => None
      case AnswerValue.Scalar(v) =>
        question.map(_.kind) match {
          case Some(QuestionKind.IntKind) => Some(java.lang.Long.valueOf(v.toLong))
          case Some(QuestionKind.BoolKind) => Some(java.lang.Boolean.valueOf(v.toBoolean))
          case Some(QuestionKind.ListKind) =>
            typedValue(question, listAnswer(v))
          case Some(QuestionKind.BlockList(wrap)) =>
            typedValue(question, listAnswer(v) match {
              case AnswerValue.Items(items) => AnswerValue.Blocks(items.map(wrap))
              case other => other
            })
          case _ => Some(v)
        }
      case AnswerValue.Items(items) if items.isEmpty => None
      case AnswerValue.Items(items) => Some(items.asJava)
      case AnswerValue.Blocks(blocks) if blocks.isEmpty => None
      case AnswerValue.Blocks(blocks) => Some(blocks.map(_.root().unwrapped()).asJava)
    }

  private def listAnswer(raw: String): AnswerValue =
    com.hcsc.generic.ingest.confgen.input.CollectionInput.parse(raw) match {
      case Right(parsed) => parsed.value
      case Left(problem) => throw new IllegalArgumentException(
        s"List answer '$raw' could not be parsed: $problem")
    }

  /** Question ids may contain segments that are not bare HOCON path tokens
    * (none do today); quote defensively segment by segment. */
  private def quotePath(id: String): String =
    id.split('.').map { seg =>
      if (seg.matches("[a-zA-Z0-9_-]+")) seg else "\"" + seg + "\""
    }.mkString(".")

  /** ingest_dt partition keys get the standard run-date derivation. */
  private def derivePartitionKeys(config: Config): Config = {
    val keys =
      if (config.hasPath("raw.partitioning.keys"))
        config.getStringList("raw.partitioning.keys").asScala.toSeq
      else Seq.empty
    if (!keys.contains("ingest_dt")) config
    else config.withValue(
      "raw.partitioning.derive.ingest_dt",
      ConfigValueFactory.fromMap(Map(
        "kind" -> "expr",
        "expr" -> "date_format(current_timestamp(), 'yyyy-MM-dd')").asJava))
  }

  // ---------------------------------------------------------------------------
  // JDBC
  // ---------------------------------------------------------------------------

  private def jdbcPostProcess(config0: Config, answers: Answers): Config = {
    var config = config0

    // Partition strategy (NONE = single-connection read, no strategy emitted).
    answers.scalar("_perf.partitioning").filterNot(_.equalsIgnoreCase("NONE")).foreach { s =>
      config = config.withValue("source.partition_strategy", ConfigValueFactory.fromAnyRef(s))
    }

    // The credential's secret reference lands on the field the chosen auth
    // mode expects (password / client_secret / token).
    for {
      field <- com.hcsc.generic.ingest.confgen.flow.JdbcQuestionFlow.secretField(answers)
      provider <- answers.scalar("_auth.secret.provider")
    } {
      val ref = new java.util.LinkedHashMap[String, Object]()
      ref.put("provider", provider)
      def copy(metaKey: String, refKey: String): Unit =
        answers.scalar(s"_auth.secret.$metaKey").foreach(v => ref.put(refKey, v))
      provider match {
        case "env" | "sysprop" => copy("key", "key")
        case "file" => copy("path", "path")
        case "inline" => copy("value", "value")
        case "cyberark" =>
          copy("url", "url"); copy("app_id", "app_id"); copy("safe", "safe"); copy("object", "object")
        case "azure_keyvault" =>
          copy("vault_url", "vault_url"); copy("secret_name", "secret_name")
        case "conjur" =>
          copy("conjur_url", "url"); copy("account", "account")
          copy("host_id", "host_id"); copy("variable", "variable")
          // The host API key is itself a secret: reference it, never store it.
          answers.scalar("_auth.secret.api_key_env").foreach { envKey =>
            val apiKeyRef = new java.util.LinkedHashMap[String, Object]()
            apiKeyRef.put("provider", "env")
            apiKeyRef.put("key", envKey)
            ref.put("api_key", apiKeyRef)
          }
        case _ =>
      }
      config = config.withValue(s"source.auth.$field", ConfigValueFactory.fromMap(ref))
    }

    // STRING watermarks carry the explicit lexicographic-ordering approval.
    if (answers.is("source.incremental.watermark_type", "STRING") &&
        answers.isTrue("_watermark.string_ack"))
      config = config.withValue(
        "source.incremental.allow_string_watermark", ConfigValueFactory.fromAnyRef(true))

    config
  }

  // ---------------------------------------------------------------------------
  // File
  // ---------------------------------------------------------------------------

  private def filePostProcess(config0: Config, answers: Answers): Config = {
    var config = config0

    if (answers.isTrue("_idem.enabled")) {
      config = config.withValue("idempotency.database",
        ConfigValueFactory.fromAnyRef(answers.scalar("audit.database").getOrElse("ingest_audit")))
      config = config.withValue("idempotency.registry_table",
        ConfigValueFactory.fromAnyRef("ingest_file_registry"))
    }

    if (answers.isTrue("_rejects.enabled")) {
      config = config.withValue("rejects.database",
        ConfigValueFactory.fromAnyRef(answers.scalar("audit.database").getOrElse("ingest_audit")))
      config = config.withValue("rejects.table", ConfigValueFactory.fromAnyRef("ingest_rejects"))
    }

    // Contract feeds with managed folders quarantine header failures.
    if (answers.isTrue("_schema.use_contract") && answers.isTrue("_file.managed"))
      config = config.withValue("schema.header_validation.quarantine_on_failure",
        ConfigValueFactory.fromAnyRef(true))

    config
  }

  /** Wraps a feed config for file output: `feeds.<entity> { ... }`. */
  def wrapAsFeeds(feed: Config): Config = {
    val entity = feed.getString("entity")
    ConfigFactory.empty().withValue(s"feeds.${quotePath(entity)}", feed.root())
  }
}
