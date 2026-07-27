package com.hcsc.generic.ingest.confgen.flow

import com.hcsc.generic.ingest.confgen.model.{Answers, Question, QuestionGroups => G, QuestionKind => K, Validators => V}

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._

/**
  * Plugin contract for a source type's question flow. Mirrors the framework's
  * registry pattern (SourceRegistry, DialectRegistry, ...): new source types
  * register their flow and the wizard picks them up automatically.
  */
trait SourceQuestionFlow {
  def sourceType: String
  def questions: Seq[Question]
}

object QuestionFlowRegistry {
  private val flows = new ConcurrentHashMap[String, SourceQuestionFlow]()

  def register(flow: SourceQuestionFlow): Unit =
    flows.put(flow.sourceType.toLowerCase, flow)

  def resolve(sourceType: String): SourceQuestionFlow =
    Option(flows.get(sourceType.toLowerCase)).getOrElse(
      throw new IllegalArgumentException(
        s"No question flow registered for source type '$sourceType'; available: ${availableNames.mkString(", ")}"))

  def availableNames: Seq[String] = flows.keySet().asScala.toSeq.sorted

  /** Built-in flows; idempotent. */
  def registerDefaults(): Unit = {
    register(JdbcQuestionFlow)
    register(FileQuestionFlow)
    register(KafkaQuestionFlow)
  }
}

/** Question groups shared by every source type: feed identity, audit and the
  * raw/curated destination. */
object CommonQuestions {

  def general: Seq[Question] = Seq(
    Question("entity", G.General, "Feed entity name",
      help = "Unique feed identifier; becomes feeds.<entity> and the default table name.",
      validate = V.identifier),
    Question("mode", G.General, "Pipeline mode",
      help = "FULL reloads everything each run; INCR appends/merges deltas.",
      kind = K.Choice(Seq("FULL", "INCR")), default = Some("FULL"),
      validate = V.oneOf(Seq("FULL", "INCR")))
  )

  def audit: Seq[Question] = Seq(
    Question("_audit.enabled", G.Audit, "Enable audit and reconciliation?",
      help = "Records per-run counts, file lineage and reconciliation checks in Hive audit tables.",
      kind = K.BoolKind, default = Some("true")),
    Question("audit.database", G.Audit, "Audit database",
      kind = K.Text, default = Some("ingest_audit"), validate = V.identifier,
      appliesWhen = _.isTrue("_audit.enabled")),
    Question("audit.reconciliation.on_mismatch", G.Audit, "On reconciliation mismatch",
      help = "WARN logs and continues; FAIL aborts the run before publish.",
      kind = K.Choice(Seq("WARN", "FAIL")), default = Some("WARN"),
      validate = V.oneOf(Seq("WARN", "FAIL")),
      appliesWhen = _.isTrue("_audit.enabled"))
  )

  def destination: Seq[Question] = Seq(
    Question("raw.database", G.Destination, "RAW database", validate = V.identifier),
    Question("raw.table", G.Destination, "RAW table",
      help = "Defaults to the entity name.", validate = V.identifier,
      defaultFrom = _.scalar("entity")),
    Question("raw.path", G.Destination, "RAW storage path",
      help = "e.g. hdfs:///data/warehouse/<domain>/raw/<entity>", validate = V.nonEmpty),
    Question("raw.format", G.Destination, "RAW file format",
      kind = K.Choice(Seq("orc", "parquet")), default = Some("orc"),
      validate = V.oneOf(Seq("orc", "parquet"))),
    Question("raw.partitioning.keys", G.Destination, "RAW partition keys",
      help = "ingest_dt gets a run-date derivation automatically; @file, CSV or JSON accepted.",
      kind = K.ListKind, default = Some("ingest_dt"), required = false),

    Question("curated.enabled", G.Destination, "Build a curated table?",
      kind = K.BoolKind, default = Some("true")),
    Question("curated.database", G.Destination, "Curated database",
      validate = V.identifier, appliesWhen = _.isTrue("curated.enabled")),
    Question("curated.table", G.Destination, "Curated table",
      validate = V.identifier, defaultFrom = _.scalar("entity"),
      appliesWhen = _.isTrue("curated.enabled")),
    Question("curated.path", G.Destination, "Curated storage path",
      validate = V.nonEmpty, appliesWhen = _.isTrue("curated.enabled")),
    Question("curated.format", G.Destination, "Curated file format",
      kind = K.Choice(Seq("orc", "parquet")), default = Some("orc"),
      validate = V.oneOf(Seq("orc", "parquet")), appliesWhen = _.isTrue("curated.enabled")),
    Question("curated.merge.keys", G.Destination, "Merge keys (primary keys)",
      help = "Empty = append-only. With keys, INCR runs merge/upsert on them. @file, CSV or JSON accepted.",
      kind = K.ListKind, required = false, appliesWhen = _.isTrue("curated.enabled")),
    Question("curated.dedup.order_by", G.Destination, "Dedup order-by columns",
      help = "Latest record wins within a merge key, ordered by these columns (descending).",
      kind = K.ListKind, required = false,
      appliesWhen = a => a.isTrue("curated.enabled") && a.items("curated.merge.keys").nonEmpty)
  )
}
