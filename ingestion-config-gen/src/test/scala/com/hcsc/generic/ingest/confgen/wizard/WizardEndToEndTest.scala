package com.hcsc.generic.ingest.confgen.wizard

import com.hcsc.generic.ingest.confgen.build.ConfigAssembler
import com.hcsc.generic.ingest.confgen.flow.{FileQuestionFlow, JdbcQuestionFlow, KafkaQuestionFlow}
import com.hcsc.generic.ingest.confgen.io.ScriptedConsole
import com.hcsc.generic.ingest.confgen.model.Answers
import com.hcsc.generic.ingest.confgen.validate.DryRunValidator
import com.hcsc.generic.ingest.jdbc.JdbcSourceConfig
import com.hcsc.generic.ingest.schema.SchemaContract
import org.scalatest.funsuite.AnyFunSuite

/**
  * Scripted end-to-end wizard sessions for all three source types: every
  * generated configuration must pass the framework's own parsers.
  */
class WizardEndToEndTest extends AnyFunSuite {

  private def runFlow(flow: com.hcsc.generic.ingest.confgen.flow.SourceQuestionFlow,
                      script: Seq[String]): (Answers, ScriptedConsole) = {
    val console = new ScriptedConsole(script)
    val answers = new Wizard(console).run(flow.sourceType, flow.questions, new Answers)
    (answers, console)
  }

  // ---------------------------------------------------------------------------

  private val jdbcScript = Seq(
    "claims_feed",       // entity
    "INCR",              // mode
    "jdbc:h2:mem:confgen_e2e;DB_CLOSE_DELAY=-1", // url
    "generic",           // dialect (h2 has no inferable prefix)
    "org.h2.Driver",     // driver
    "SQL_PASSWORD",      // auth type
    "sa",                // user
    "sysprop",           // secret provider
    "confgen.e2e.pwd",   // secret key
    "INCREMENTAL",       // extraction mode
    "d_claims",          // table
    "claim_id, state, modified_ts", // columns
    "state = 'IL'",      // where
    "n",                 // schema contract? (opt-in for JDBC)
    "NUMERIC",           // watermark type
    "claim_id",          // watermark columns
    "0",                 // initial value
    "",                  // overlap (blank)
    "memory",            // watermark store
    "",                  // retry attempts (default 3)
    "",                  // backoff (default 2000)
    "n",                 // audit
    "",                  // fetchsize (default)
    "NONE",              // partitioning
    "claims_raw",        // raw db
    "",                  // raw table (default = entity)
    "hdfs:///tmp/raw/claims", // raw path
    "",                  // raw format (orc)
    "",                  // partition keys (default ingest_dt)
    "y",                 // curated?
    "claims_curated",    // curated db
    "",                  // curated table (default = entity)
    "hdfs:///tmp/cur/claims", // curated path
    "",                  // curated format
    "claim_id",          // merge keys
    "modified_ts",       // freshness column (required for keyed feeds, CUR_008)
    "",                  // freshness compare_as (empty = compare as stored)
    "",                  // freshness compare_format
    "seq desc as bigint", // freshness tie-breakers (logical-type syntax)
    ""                   // extra dedup order by
  )

  test("JDBC incremental session: generated feed passes JdbcSourceConfig.parse") {
    System.setProperty("confgen.e2e.pwd", "s3cret")
    val (answers, _) = runFlow(JdbcQuestionFlow, jdbcScript)
    val feed = ConfigAssembler.assemble(JdbcQuestionFlow, answers)

    // The generated source block is accepted by the framework's own parser.
    val cfg = JdbcSourceConfig.parse(feed.getConfig("source"))
    assert(cfg.mode == "INCREMENTAL")
    assert(cfg.password.contains("s3cret"))
    assert(cfg.watermark.get.watermarkType == "NUMERIC")
    assert(cfg.watermark.get.storeType == "memory")
    assert(cfg.columns == Seq("claim_id", "state", "modified_ts"))
    assert(cfg.retry.maxAttempts == 3 && cfg.retry.backoffMs == 2000L)
    assert(cfg.fetchSize == 1000)

    // Feed-level wiring
    assert(feed.getString("entity") == "claims_feed")
    assert(feed.getString("raw.table") == "claims_feed")
    assert(feed.getString("curated.table") == "claims_feed")
    assert(feed.getStringList("curated.merge.keys").get(0) == "claim_id")
    // Keyed feeds are generated SAFE by construction: freshness present
    // (CUR_008 would fail closed without it), logical-type tie-breaker kept
    assert(feed.getString("curated.merge.freshness.column") == "modified_ts")
    assert(feed.getStringList("curated.merge.freshness.tie_breakers").get(0) == "seq desc as bigint")
    assert(feed.getString("raw.partitioning.derive.ingest_dt.expr").contains("current_timestamp"))
    // Meta answers never leak into the configuration
    assert(!feed.root().keySet().contains("_perf"))
    assert(!feed.hasPath("source.partition_strategy")) // NONE emits nothing

    // Dry-run validation is green and previews the first extraction window.
    val report = DryRunValidator.validate(feed)
    assert(report.ok, report.errors.mkString("; "))
    val sql = report.sqlPreview.get
    assert(sql.contains("\"claim_id\" > 0"), sql)
    assert(sql.contains("state = 'IL'"), sql)
  }

  test("invalid answers re-prompt with the validation problem") {
    val script = Seq("9bad entity", "claims_ok", "FULL") ++ jdbcScript.drop(2)
    val (answers, console) = runFlow(JdbcQuestionFlow, script)
    assert(answers.scalar("entity").contains("claims_ok"))
    assert(console.output.contains("not a valid identifier"))
  }

  test("'?' shows help text and re-asks") {
    val script = Seq("?", "claims_help") ++ jdbcScript.drop(1)
    val (answers, console) = runFlow(JdbcQuestionFlow, script)
    assert(answers.scalar("entity").contains("claims_help"))
    assert(console.output.contains("feeds.<entity>"))
  }

  test("progress indicator shows numbered group headers") {
    val (_, console) = runFlow(JdbcQuestionFlow, jdbcScript)
    assert(console.output.contains("[1/9] General"))
    assert(console.output.contains("[5/9] Watermark"))
    assert(console.output.contains("[9/9] Destination"))
  }

  // ---------------------------------------------------------------------------

  private val fileScript = Seq(
    "member_feed",       // entity
    "FULL",              // mode
    "n",                 // managed folders?
    "hdfs:///data/incoming/member/*.csv", // glob
    "csv", "y", ",",     // format, header, delimiter
    "n", "n",            // multiline, trailer
    "n",                 // pre-read validation
    "y",                 // schema contract?
    """[{"name": "subscriber_id", "type": "string", "nullable": false, "required": true,
        "aliases": ["subscriber id"]},
       {"name": "hios_id", "type": "string", "nullable": false, "required": true}]""",
    "",                  // version (1.0)
    "",                  // strategy (NAME_WITH_ALIASES)
    "y",                 // recommended policies
    "n",                 // audit
    "y", "",             // idempotency + SKIP policy
    "y", "", "",         // rejects + contract nullability + 5.0 percent
    "member_raw", "", "hdfs:///tmp/raw/member", "", "",
    "n"                  // curated disabled
  )

  test("file session: schema contract parses and governance defaults land") {
    val (answers, console) = runFlow(FileQuestionFlow, fileScript)
    val feed = ConfigAssembler.assemble(FileQuestionFlow, answers)

    assert(feed.getString("source.type") == "file")
    assert(feed.getString("source.path").endsWith("*.csv"))

    val contract = SchemaContract.parse(
      feed.getConfig("source").withValue("schema", feed.getValue("schema"))).get
    assert(contract.columns.map(_.name) == Seq("subscriber_id", "hios_id"))
    assert(contract.columns.head.aliases.nonEmpty)

    assert(feed.getString("idempotency.duplicate_policy") == "SKIP")
    assert(feed.getString("idempotency.registry_table") == "ingest_file_registry")
    assert(feed.getBoolean("rejects.use_contract_nullability"))
    assert(feed.getString("rejects.max_reject_percent") == "5.0")
    assert(!feed.hasPath("curated.database"))
    assert(feed.getBoolean("curated.enabled") == false)

    // Structured input echoed a preview to the operator
    assert(console.output.contains("subscriber_id"))

    val report = DryRunValidator.validate(feed)
    assert(report.ok, report.errors.mkString("; "))
  }

  test("file session: CSV column names become minimal contract columns") {
    val script = fileScript.updated(11, "subscriber_id, hios_id, middle_name")
    val (answers, _) = runFlow(FileQuestionFlow, script)
    val feed = ConfigAssembler.assemble(FileQuestionFlow, answers)
    val contract = SchemaContract.parse(
      feed.getConfig("source").withValue("schema", feed.getValue("schema"))).get
    assert(contract.columns.map(_.name) == Seq("subscriber_id", "hios_id", "middle_name"))
  }

  // ---------------------------------------------------------------------------

  private val kafkaScript = Seq(
    "events_feed", "FULL",
    "kafka1:9092,kafka2:9092", "user-events",
    "", "",              // offsets defaults
    "SASL_SSL", "PLAIN",
    """org.apache.kafka.common.security.plain.PlainLoginModule required username="svc" password="pw";""",
    "json",
    """{"type":"struct","fields":[{"name":"event_id","type":"string","nullable":true,"metadata":{}}]}""",
    "n",                 // audit
    "events_raw", "", "hdfs:///tmp/raw/events", "", "",
    "n"                  // curated
  )

  test("kafka session: nested keys and schema validation") {
    val (answers, _) = runFlow(KafkaQuestionFlow, kafkaScript)
    val feed = ConfigAssembler.assemble(KafkaQuestionFlow, answers)

    // Emitted nested so KafkaSource's getString("bootstrap.servers") resolves.
    assert(feed.getString("source.bootstrap.servers") == "kafka1:9092,kafka2:9092")
    assert(feed.getString("source.kafka.security.protocol") == "SASL_SSL")
    assert(feed.getString("source.value.format") == "json")

    val report = DryRunValidator.validate(feed)
    assert(report.ok, report.errors.mkString("; "))
  }

  test("kafka json format without a schema fails dry-run validation") {
    val (answers, _) = runFlow(KafkaQuestionFlow, kafkaScript)
    val feed = ConfigAssembler.assemble(KafkaQuestionFlow, answers)
    val broken = feed.withoutPath("source.value.schema")
    val report = DryRunValidator.validate(broken)
    assert(report.errors.exists(_.contains("value.schema")))
  }
}
