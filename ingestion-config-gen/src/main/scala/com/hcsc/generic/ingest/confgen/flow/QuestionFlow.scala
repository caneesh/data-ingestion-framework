package com.hcsc.generic.ingest.confgen.flow

import com.hcsc.generic.ingest.confgen.model.{Answers, Question, QuestionGroups => G, QuestionKind => K, Validators => V}
import com.typesafe.config.{Config, ConfigFactory}

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

  private val Policies = Seq("FAIL", "WARN", "IGNORE")
  private val Strategies = Seq("STRICT_NAME", "NAME_WITH_ALIASES", "NAME_ALIAS_POSITION_FALLBACK")

  /** Plain CSV items become minimal column contracts. */
  private[confgen] def plainColumn(name: String): Config =
    ConfigFactory.parseString(
      s"""name = "${name.trim}"
         |type = "string"
         |nullable = true
         |required = true""".stripMargin)

  /**
    * Schema-contract (source-to-target mapping) questions, shared by the
    * file and JDBC flows. The generated contract is written to its own
    * `<entity>-schema.conf` file so a wide mapping (hundreds of columns)
    * stays maintainable without burying the rest of the feed config.
    */
  def schemaContract(gateDefault: String, gateHelp: String, introspectable: Boolean = false): Seq[Question] = {
    def contract(a: Answers) = a.isTrue("_schema.use_contract")
    def manualColumns(a: Answers) =
      contract(a) && !(introspectable && a.isTrue("_schema.introspect"))
    val introspect =
      if (!introspectable) Seq.empty
      else Seq(
        Question("_schema.introspect", G.Extraction,
          "Generate the column mapping by introspecting the source table?",
          help = "Connects to the database at generation time and emits one contract column " +
            "per source column (name, type, nullability, position) via " +
            "DatabaseMetaData/INFORMATION_SCHEMA — ideal for wide tables. The result is " +
            "written to <entity>-schema.conf for review. Answer n to type or @file the " +
            "columns instead.",
          kind = K.BoolKind, default = Some("true"), appliesWhen = contract))
    Seq(
      Question("_schema.use_contract", G.Extraction, "Define a schema contract?",
        help = gateHelp, kind = K.BoolKind, default = Some(gateDefault))) ++
    introspect ++
    Seq(
      Question("schema.columns", G.Extraction, "Contract columns",
        help = "CSV of names (typed string/required), or JSON objects with " +
          "name/type/nullable/required/aliases/position — inline or @file. For wide " +
          "tables keep the mapping in a file and answer @/path/columns.json; the " +
          "generator writes the contract to <entity>-schema.conf.",
        kind = K.BlockList(plainColumn), appliesWhen = manualColumns, validate = V.nonEmpty),
      Question("schema.version", G.Extraction, "Contract version",
        default = Some("1.0"), appliesWhen = contract),
      Question("schema.header_validation.strategy", G.Extraction, "Header matching strategy",
        help = "STRICT_NAME: canonical names only. NAME_WITH_ALIASES: plus approved aliases. " +
          "NAME_ALIAS_POSITION_FALLBACK: adds guarded positional fallback (stable-order vendors only).",
        kind = K.Choice(Strategies), default = Some("NAME_WITH_ALIASES"),
        validate = V.oneOf(Strategies), appliesWhen = contract),
      Question("_schema.policy_defaults", G.Extraction,
        "Accept recommended drift policies (missing=FAIL, extra=WARN, type=FAIL, duplicate=FAIL)?",
        kind = K.BoolKind, default = Some("true"), appliesWhen = contract),
      Question("schema.on_missing_column", G.Extraction, "Policy: missing column",
        kind = K.Choice(Policies), default = Some("FAIL"), validate = V.oneOf(Policies),
        appliesWhen = a => contract(a) && !a.isTrue("_schema.policy_defaults")),
      Question("schema.on_extra_column", G.Extraction, "Policy: extra column",
        kind = K.Choice(Policies), default = Some("WARN"), validate = V.oneOf(Policies),
        appliesWhen = a => contract(a) && !a.isTrue("_schema.policy_defaults")),
      Question("schema.on_type_change", G.Extraction, "Policy: type change",
        kind = K.Choice(Policies), default = Some("FAIL"), validate = V.oneOf(Policies),
        appliesWhen = a => contract(a) && !a.isTrue("_schema.policy_defaults")),
      Question("schema.on_duplicate_header", G.Extraction, "Policy: duplicate header",
        kind = K.Choice(Policies), default = Some("FAIL"), validate = V.oneOf(Policies),
        appliesWhen = a => contract(a) && !a.isTrue("_schema.policy_defaults"))
    )
  }

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
    // Keyed merges REQUIRE freshness (CUR_008 fails closed at run time
    // otherwise): the wizard produces safe configs by construction; legacy
    // last-write-wins remains a deliberate hand-edit, never generated.
    Question("curated.merge.freshness.column", G.Destination, "Freshness column",
      help = "SOURCE-provided version column that decides merge winners (highest wins). " +
        "Required for keyed feeds: without it the incremental merge fails closed (CUR_008).",
      validate = V.nonEmpty,
      appliesWhen = a => a.isTrue("curated.enabled") && a.items("curated.merge.keys").nonEmpty),
    Question("curated.merge.freshness.compare_as", G.Destination, "Freshness comparison type",
      help = "LOGICAL type for comparisons only (timestamp, bigint, decimal(10,2), ...). " +
        "Storage keeps its physical type — use for all-string layouts. Empty = compare as stored.",
      required = false,
      appliesWhen = a => a.isTrue("curated.enabled") && a.items("curated.merge.keys").nonEmpty),
    Question("curated.merge.freshness.compare_format", G.Destination, "Freshness datetime format",
      help = "Datetime parse pattern (e.g. M/d/yyyy) when the string value needs parsing " +
        "rather than a bare cast. Empty = none. Unparseable values fail (CUR_008).",
      required = false,
      appliesWhen = a => a.isTrue("curated.enabled") && a.items("curated.merge.keys").nonEmpty),
    Question("curated.merge.freshness.tie_breakers", G.Destination, "Freshness tie-breakers",
      help = "Columns breaking equal-freshness ties. Each entry: 'col [asc|desc] " +
        "[nulls_first|nulls_last] [as <type>]' — 'as bigint' compares numeric strings numerically.",
      kind = K.ListKind, required = false,
      appliesWhen = a => a.isTrue("curated.enabled") && a.items("curated.merge.keys").nonEmpty),
    Question("curated.dedup.order_by", G.Destination, "Extra dedup order-by columns",
      help = "ADDITIONAL ordering after freshness + tie-breakers (same syntax, incl. 'as <type>').",
      kind = K.ListKind, required = false,
      appliesWhen = a => a.isTrue("curated.enabled") && a.items("curated.merge.keys").nonEmpty)
  )
}
