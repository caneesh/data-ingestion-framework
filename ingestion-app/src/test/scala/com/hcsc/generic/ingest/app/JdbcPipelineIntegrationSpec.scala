package com.hcsc.generic.ingest.app

import com.hcsc.generic.ingest.jdbc.JdbcSource
import com.hcsc.generic.ingest.jdbc.watermark.InMemoryWatermarkStore
import com.hcsc.generic.ingest.model.Cli
import com.hcsc.generic.ingest.pipeline.IngestPipeline
import com.hcsc.generic.ingest.publish.PublishValidationException
import com.hcsc.generic.ingest.sink.HiveSink
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Path => JPath}
import java.sql.DriverManager

/**
  * End-to-end JDBC ingestion through the full pipeline: H2 source ->
  * incremental watermark read -> RAW Hive table -> transactional CURATED
  * publish -> watermark advance ONLY after success.
  */
class JdbcPipelineIntegrationSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var tempDir: JPath = _
  private var spark: SparkSession = _
  private val logger = Logger.getLogger(getClass.getName)

  private val h2Url = "jdbc:h2:mem:jdbc_pipeline;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false"

  private def h2(statements: String*): Unit = {
    Class.forName("org.h2.Driver")
    val conn = DriverManager.getConnection(h2Url, "sa", "")
    try {
      val stmt = conn.createStatement()
      try statements.foreach(stmt.execute)
      finally stmt.close()
    } finally conn.close()
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    tempDir = Files.createTempDirectory("jdbc-pipeline-")
    System.setProperty("derby.stream.error.file",
      tempDir.resolve("derby.log").toAbsolutePath.toString)

    spark = SparkSession.builder()
      .master("local[2]")
      .appName("jdbc-pipeline-test")
      .config("spark.sql.warehouse.dir", tempDir.resolve("warehouse").toAbsolutePath.toString)
      .config("javax.jdo.option.ConnectionURL",
        s"jdbc:derby:;databaseName=${tempDir.resolve("metastore_db").toAbsolutePath};create=true")
      .config("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver")
      .config("datanucleus.schema.autoCreateTables", "true")
      .config("hive.metastore.schema.verification", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.caseSensitive", "false")
      .enableHiveSupport()
      .getOrCreate()

    JdbcSource.register()
    HiveSink.register()
    Seq("j_raw", "j_curated").foreach(db => spark.sql(s"CREATE DATABASE IF NOT EXISTS $db"))

    h2(
      "DROP TABLE IF EXISTS claims",
      """CREATE TABLE claims (
        |  claim_id VARCHAR(20),
        |  amount INT,
        |  modified_ts TIMESTAMP
        |)""".stripMargin,
      "INSERT INTO claims VALUES ('C001', 100, '2026-01-01 09:00:00')",
      "INSERT INTO claims VALUES ('C002', 200, '2026-01-02 09:00:00')"
    )
    InMemoryWatermarkStore.clear()
  }

  override def afterAll(): Unit = {
    try { if (spark != null) spark.stop() }
    finally {
      import scala.collection.JavaConverters._
      Files.walk(tempDir).iterator().asScala.toSeq.reverse.foreach(p => Files.deleteIfExists(p))
    }
    super.afterAll()
  }

  private def feedConf(extraCurated: String = ""): Config = ConfigFactory.parseString(
    s"""
       |source {
       |  type = "jdbc"
       |  url = "$h2Url"
       |  dialect = "generic"
       |  driver = "org.h2.Driver"
       |  auth { user = "sa", password = { provider = "inline", value = "" } }
       |  mode = "INCREMENTAL"
       |  table = "claims"
       |  health_check { enabled = false }
       |  incremental {
       |    watermark_type = "TIMESTAMP"
       |    watermark_columns = ["modified_ts"]
       |    initial_value = "1900-01-01 00:00:00"
       |    watermark_store { type = "memory" }
       |  }
       |}
       |audit { enabled = false } # explicit opt-out (the ledger is mandatory otherwise)
       |raw {
       |  database = j_raw
       |  table = claims
       |  path = "${tempDir.resolve("raw_claims").toAbsolutePath}"
       |  format = parquet
       |  record_hash = true
       |}
       |curated {
       |  enabled = true
       |  database = j_curated
       |  table = claims
       |  path = "${tempDir.resolve("curated_claims").toAbsolutePath}"
       |  format = parquet
       |  merge { keys = ["claim_id"] }
       |  $extraCurated
       |}
    """.stripMargin)

  private val entity = "claims_feed"

  test("incremental JDBC feed loads, publishes, and advances the watermark") {
    new IngestPipeline(spark, feedConf(), Cli(entity = entity, mode = "FULL", runId = Some("jrun-1")), logger).run()

    assert(spark.table("j_raw.claims").count() == 2)
    assert(spark.table("j_curated.claims").count() == 2)
    assert(InMemoryWatermarkStore.latest(entity).get.values.head.startsWith("2026-01-02 09:00:00"))

    // New source row -> second run ingests only the delta
    h2("INSERT INTO claims VALUES ('C003', 300, '2026-01-03 09:00:00')")
    new IngestPipeline(spark, feedConf(), Cli(entity = entity, mode = "FULL", runId = Some("jrun-2")), logger).run()

    import org.apache.spark.sql.functions.col
    assert(spark.table("j_raw.claims").count() == 3)
    assert(spark.table("j_raw.claims").filter(col("run_id") === "jrun-2").count() == 1)
    assert(InMemoryWatermarkStore.latest(entity).get.values.head.startsWith("2026-01-03 09:00:00"))

    // RAW rows carry the extraction window and source identity (lineage)
    val meta = spark.table("j_raw.claims").filter(col("run_id") === "jrun-2")
      .select("extract_start_ts", "extract_end_ts", "source_table").collect().head
    assert(meta.getString(0).startsWith("2026-01-02 09:00:00"), "extract_start_ts = previous watermark")
    assert(meta.getString(1).startsWith("2026-01-03 09:00:00"), "extract_end_ts = captured upper")
    assert(meta.getString(2) == "claims")

    // raw.record_hash = true: every RAW row carries the business fingerprint
    val hash = spark.table("j_raw.claims").select("record_hash").collect().map(_.getString(0))
    assert(hash.forall(h => h != null && h.length == 64), "record_hash must be a SHA-256 hex string")
  }

  test("configuration compatibility (CFG) failures stop the run before extraction") {
    // Incremental JDBC into a keyless state-deriving curated layer would
    // keep only the latest window (CFG_009).
    val keyless = feedConf().withValue("curated.merge.keys",
      com.typesafe.config.ConfigValueFactory.fromIterable(java.util.Collections.emptyList[String]()))
    val e = intercept[IllegalStateException] {
      new IngestPipeline(spark, keyless,
        Cli(entity = entity, mode = "FULL", runId = Some("jrun-cfg")), logger).run()
    }
    assert(e.getMessage.contains("CFG_009"))
  }

  test("failed publish does not advance the watermark; replay picks the rows up again") {
    h2("INSERT INTO claims VALUES ('C004', 400, '2026-01-04 09:00:00')")
    val before = InMemoryWatermarkStore.latest(entity).get

    // Publish validation rejects everything -> curated fails after RAW
    val failing = feedConf(
      """publish { validation_query = "SELECT * FROM {table} WHERE claim_id = 'C004'" }""")
    intercept[PublishValidationException] {
      new IngestPipeline(spark, failing, Cli(entity = entity, mode = "FULL", runId = Some("jrun-3")), logger).run()
    }

    // Watermark untouched by the failed run
    assert(InMemoryWatermarkStore.latest(entity).get == before)

    // Replay with fixed config: same row extracted again (watermark never
    // moved), published, and only now does the watermark advance
    new IngestPipeline(spark, feedConf(), Cli(entity = entity, mode = "FULL", runId = Some("jrun-4")), logger).run()
    assert(InMemoryWatermarkStore.latest(entity).get.values.head.startsWith("2026-01-04 09:00:00"))
    assert(spark.table("j_curated.claims").count() == 1)

    // Window idempotency: jrun-3 already appended C004 to RAW before its
    // publish failed; jrun-4 (a NEW run id, no --resume) re-extracts the
    // identical bounded window and must NOT append the same rows twice.
    import org.apache.spark.sql.functions.col
    assert(spark.table("j_raw.claims").filter(col("claim_id") === "C004").count() == 1,
      "rerunning a failed window under a new run id must not duplicate RAW")
  }

  test("the run ledger is mandatory unless a feed opts out explicitly") {
    val noAudit = feedConf().withoutPath("audit")
    val e = intercept[IllegalStateException] {
      new IngestPipeline(spark, noAudit,
        Cli(entity = entity, mode = "FULL", runId = Some("jrun-ledger")), logger).run()
    }
    assert(e.getMessage.contains("PIPE_002"))
  }

  test("--stage raw does not advance the watermark; advance_after=RAW opts in") {
    h2("INSERT INTO claims VALUES ('C005', 500, '2026-01-05 09:00:00')")
    val before = InMemoryWatermarkStore.latest(entity).get

    // RAW-only run: the source window must NOT be burned, because curated
    // never processed it.
    new IngestPipeline(spark, feedConf(),
      Cli(entity = entity, mode = "FULL", runId = Some("jrun-5"), stage = "raw"), logger).run()

    import org.apache.spark.sql.functions.col
    assert(spark.table("j_raw.claims").filter(col("run_id") === "jrun-5").count() == 1,
      "raw must still be loaded by a --stage raw run")
    assert(InMemoryWatermarkStore.latest(entity).get == before,
      "--stage raw must not advance the watermark: curated never saw the window")

    // Declared raw-only topology: the same raw-only run advances once the
    // feed opts in explicitly.
    val rawOnly = ConfigFactory.parseString("""watermark { advance_after = "RAW" }""")
      .withFallback(feedConf())
    new IngestPipeline(spark, rawOnly,
      Cli(entity = entity, mode = "FULL", runId = Some("jrun-6"), stage = "raw"), logger).run()
    assert(InMemoryWatermarkStore.latest(entity).get.values.head.startsWith("2026-01-05 09:00:00"))
  }

  test("a drifted retry window appends (documented duplicate) rather than losing the delta") {
    // Fix 5 semantics: when the captured upper DRIFTS between a failed
    // attempt and its un-resumed retry (new source rows arrived), the
    // window guard cannot match — the retry must append the full window
    // (duplicating the failed attempt's overlap in RAW, deduplicated by the
    // curated merge) instead of skipping and permanently losing the delta.
    import org.apache.spark.sql.functions.col
    h2("INSERT INTO claims VALUES ('C006', 600, '2026-01-06 09:00:00')")
    val failing = feedConf(
      """publish { validation_query = "SELECT * FROM {table} WHERE claim_id = 'C006'" }""")
    intercept[PublishValidationException] {
      new IngestPipeline(spark, failing,
        Cli(entity = entity, mode = "FULL", runId = Some("jrun-7")), logger).run()
    }
    assert(spark.table("j_raw.claims").filter(col("claim_id") === "C006").count() == 1)

    // New row lands between the failed attempt and the retry: upper drifts
    h2("INSERT INTO claims VALUES ('C007', 700, '2026-01-07 09:00:00')")
    new IngestPipeline(spark, feedConf(),
      Cli(entity = entity, mode = "FULL", runId = Some("jrun-8")), logger).run()

    assert(spark.table("j_raw.claims").filter(col("claim_id") === "C007").count() == 1,
      "the drift delta must be captured")
    assert(spark.table("j_raw.claims").filter(col("claim_id") === "C006").count() == 2,
      "the overlap range duplicates in RAW by design (curated dedups by key)")
    assert(spark.table("j_curated.claims").filter(col("claim_id").isin("C006", "C007")).count() == 2,
      "curated holds exactly one row per key despite the RAW duplicate")
    assert(InMemoryWatermarkStore.latest(entity).get.values.head.startsWith("2026-01-07 09:00:00"))
  }

  /** Audited variant of feedConf for the HOLD/continuity tests: real ledger
    * (the recovery signal and the continuity check both live there), own
    * raw/curated tables, near-zero lock settle so the suite stays fast. */
  private def auditedConf(suffix: String, rejectsBlock: String = ""): Config =
    ConfigFactory.parseString(
      s"""
         |audit { enabled = true, database = "j_audit" }
         |concurrency { settle_ms = 1 }
         |raw {
         |  database = j_raw
         |  table = claims_$suffix
         |  path = "${tempDir.resolve(s"raw_claims_$suffix").toAbsolutePath}"
         |  format = parquet
         |  record_hash = true
         |}
         |curated {
         |  database = j_curated
         |  table = claims_$suffix
         |  path = "${tempDir.resolve(s"curated_claims_$suffix").toAbsolutePath}"
         |}
         |$rejectsBlock
       """.stripMargin).withFallback(feedConf())

  test("raw.lineage_extended stamps source lineage and nulls the constant JDBC file_id") {
    import org.apache.spark.sql.functions.col
    val extended = ConfigFactory.parseString("""raw { lineage_extended = true }""")
      .withFallback(auditedConf("lineage"))
    new IngestPipeline(spark, extended,
      Cli(entity = "claims_lineage", mode = "FULL", runId = Some("lin-1")), logger).run()

    val row = spark.table("j_raw.claims_lineage").filter(col("claim_id") === "C001")
      .select("source_modified_ts", "source_operation", "source_primary_key", "file_id")
      .collect().head
    assert(row.getString(0) != null && row.getString(0).startsWith("2026-01-01 09:00:00"),
      "source_modified_ts copies the designated watermark column")
    assert(row.getString(1) == "I")
    assert(row.getString(2) == "C001", "source_primary_key derives from merge keys")
    assert(row.getString(3) == null, "JDBC rows carry no constant file_id under extended lineage")
  }

  test("ingestion.pattern=BACKFILL publishes but never commits the watermark") {
    val backfill = ConfigFactory.parseString("""ingestion { pattern = "BACKFILL" }""")
      .withFallback(auditedConf("backfill"))
    new IngestPipeline(spark, backfill,
      Cli(entity = "claims_backfill", mode = "FULL", runId = Some("bkf-1")), logger).run()

    assert(spark.table("j_curated.claims_backfill").count() > 0,
      "a backfill still publishes raw and curated")
    assert(InMemoryWatermarkStore.latest("claims_backfill").isEmpty,
      "BACKFILL must not seed/advance the live watermark")
  }

  test("on_reject_watermark=HOLD keeps the window open; the fixed rerun recovers the rejects") {
    import org.apache.spark.sql.functions.col
    val holdEntity = "claims_hold"
    val holdRejects = (rules: String) =>
      s"""rejects {
         |  database = "j_audit"
         |  table = "ingest_rejects_hold"
         |  on_reject_watermark = "HOLD"
         |  $rules
         |}""".stripMargin

    // Run 1: one row trips the reject rule -> run SUCCEEDS but the
    // watermark is HELD, so the window is not burned over the reject.
    new IngestPipeline(spark,
      auditedConf("hold", holdRejects(
        """rules = [ { name = "too_big", condition = "amount >= 700",
          |  error_code = "R900", category = "TEST", message = "amount cap" } ]""".stripMargin)),
      Cli(entity = holdEntity, mode = "FULL", runId = Some("hrun-1")), logger).run()

    assert(spark.table("j_audit.ingest_rejects_hold").filter(col("run_id") === "hrun-1").count() == 1)
    assert(spark.table("j_raw.claims_hold").filter(col("claim_id") === "C007").count() == 0,
      "the rejected row must not be in RAW")
    assert(InMemoryWatermarkStore.latest(holdEntity).isEmpty,
      "HOLD with rejected rows must leave the watermark untouched")

    // Run 2: reject rule fixed (removed). The same window is re-extracted;
    // the held-window exemption re-appends instead of window-skipping, so
    // the previously rejected row is RECOVERED, and only now does the
    // watermark advance.
    new IngestPipeline(spark, auditedConf("hold", holdRejects("")),
      Cli(entity = holdEntity, mode = "FULL", runId = Some("hrun-2")), logger).run()

    assert(spark.table("j_raw.claims_hold").filter(col("claim_id") === "C007").count() == 1,
      "the previously rejected row must be recovered into RAW")
    assert(spark.table("j_raw.claims_hold").filter(col("claim_id") === "C001").count() == 2,
      "previously accepted rows re-append as documented at-least-once duplicates")
    assert(spark.table("j_curated.claims_hold").count() ==
      spark.table("j_curated.claims_hold").select("claim_id").distinct().count(),
      "curated dedups the recovery duplicates")
    assert(spark.table("j_curated.claims_hold").filter(col("claim_id") === "C007").count() == 1)
    assert(InMemoryWatermarkStore.latest(holdEntity).get.values.head.startsWith("2026-01-07 09:00:00"))

    // The continuity check saw run 2 start where run 1 started (held) and passed
    val continuity = spark.table("j_audit.ingest_reconciliation")
      .filter(col("entity") === holdEntity && col("check_name") === "watermark_continuity" &&
        col("run_id") === "hrun-2")
      .select("passed").collect()
    assert(continuity.nonEmpty && continuity.forall(_.getBoolean(0)),
      "a held window rerun is a legal continuity link")
  }

  test("default ADVANCE still burns the window over rejects (historical behavior)") {
    import org.apache.spark.sql.functions.col
    val advEntity = "claims_adv"
    new IngestPipeline(spark,
      auditedConf("adv",
        """rejects {
          |  database = "j_audit"
          |  table = "ingest_rejects_adv"
          |  rules = [ { name = "too_big", condition = "amount >= 700",
          |    error_code = "R900", category = "TEST", message = "amount cap" } ]
          |}""".stripMargin),
      Cli(entity = advEntity, mode = "FULL", runId = Some("arun-1")), logger).run()

    assert(spark.table("j_audit.ingest_rejects_adv").count() == 1)
    assert(InMemoryWatermarkStore.latest(advEntity).get.values.head.startsWith("2026-01-07 09:00:00"),
      "ADVANCE (default) commits the captured upper even with rejects")
  }

  test("raw.delivery_mode=DEDUPLICATED_APPEND skips overlap rereads by source-version identity") {
    import org.apache.spark.sql.functions.col
    // BACKFILL: neither run commits the watermark, so both extract the
    // identical full window — the second run is a pure overlap reread.
    val dedupConf = ConfigFactory.parseString(
      """ingestion { pattern = "BACKFILL" }
        |raw {
        |  delivery_mode = "DEDUPLICATED_APPEND"
        |  idempotency_key = ["claim_id", "modified_ts"]
        |}""".stripMargin).withFallback(auditedConf("dedup"))

    new IngestPipeline(spark, dedupConf,
      Cli(entity = "claims_dedup", mode = "FULL", runId = Some("dd-1")), logger).run()
    val loaded = spark.table("j_raw.claims_dedup").count()
    assert(loaded > 0)

    // Re-extract the identical rows (--force-reprocess bypasses the window
    // guard): every row is an overlap reread, so NOTHING new is appended and
    // the adjusted raw_equals_accepted reconciliation still passes.
    new IngestPipeline(spark, dedupConf,
      Cli(entity = "claims_dedup", mode = "FULL", runId = Some("dd-2"), forceReprocess = true),
      logger).run()
    assert(spark.table("j_raw.claims_dedup").count() == loaded,
      "identical source versions must not duplicate in RAW under DEDUPLICATED_APPEND")
    assert(spark.table("j_raw.claims_dedup").filter(col("run_id") === "dd-2").count() == 0)

    val overlap = spark.table("j_audit.ingest_reconciliation")
      .filter(col("entity") === "claims_dedup" && col("run_id") === "dd-2" &&
        col("check_name") === "raw_overlap_reread")
      .select("actual").collect()
    assert(overlap.nonEmpty && overlap.head.getString(0).toLong == loaded,
      "the measured overlap reread is recorded in reconciliation")
  }

  test("CFG_013: DEDUPLICATED_APPEND without an identity, or run_id as identity, fails at startup") {
    val noKey = ConfigFactory.parseString("""raw { delivery_mode = "DEDUPLICATED_APPEND" }""")
      .withFallback(auditedConf("cfg13"))
    val e1 = intercept[IllegalStateException] {
      new IngestPipeline(spark, noKey,
        Cli(entity = "claims_cfg13", mode = "FULL", runId = Some("c13-1")), logger).run()
    }
    assert(e1.getMessage.contains("CFG_013"))

    val runIdKey = ConfigFactory.parseString(
      """raw { idempotency_key = ["run_id", "claim_id"] }""")
      .withFallback(auditedConf("cfg13"))
    val e2 = intercept[IllegalStateException] {
      new IngestPipeline(spark, runIdKey,
        Cli(entity = "claims_cfg13", mode = "FULL", runId = Some("c13-2")), logger).run()
    }
    assert(e2.getMessage.contains("CFG_013") && e2.getMessage.contains("run_id"))
  }

  test("concurrency.wait_ms retries a held lock instead of failing fast") {
    val lock = new com.hcsc.generic.ingest.lock.LockService(
      spark, "j_audit", "wait_locks", leaseMillis = 60000L, settleMillis = 1L,
      logger = logger, waitMillis = 8000L)
    lock.acquire("wait_entity", "holder-1")
    val releaser = new Thread(new Runnable {
      def run(): Unit = { Thread.sleep(1500); lock.release("wait_entity", "holder-1") }
    })
    releaser.start()
    lock.acquire("wait_entity", "run-2") // blocks until holder-1 releases, then wins
    releaser.join()
    lock.release("wait_entity", "run-2")
  }

  test("merge.require_ordering=true refuses the nondeterministic dedup fallback (CUR_004)") {
    val strict = ConfigFactory.parseString(
      """curated { merge { require_ordering = true } }""").withFallback(auditedConf("strict"))
    val e = intercept[IllegalStateException] {
      new IngestPipeline(spark, strict,
        Cli(entity = "claims_strict", mode = "FULL", runId = Some("st-1")), logger).run()
    }
    assert(e.getMessage.contains("CUR_004"))
  }

  test("merge.null_handling.policy=FAIL_RUN fails the run on null business keys (CUR_001)") {
    h2("INSERT INTO claims VALUES (NULL, 999, '2026-01-09 09:00:00')")
    val failRun = ConfigFactory.parseString(
      """curated { merge {
        |  null_handling { policy = "FAIL_RUN" }
        |  freshness { column = "modified_ts" }
        |} }""".stripMargin).withFallback(auditedConf("failrun"))
    val e = intercept[IllegalStateException] {
      new IngestPipeline(spark, failRun,
        Cli(entity = "claims_failrun", mode = "FULL", runId = Some("fr-1")), logger).run()
    }
    assert(e.getMessage.contains("CUR_001"))
    assert(InMemoryWatermarkStore.latest("claims_failrun").isEmpty,
      "the failed run must not seed the watermark")
  }

  test("watermark continuity mismatch between store and ledger fails reconciliation") {
    import org.apache.spark.sql.functions.col
    // A ghost ledger entry claims a newer window was already extracted for
    // the entity — the store (still at 2026-01-07) and ledger now disagree,
    // exactly what tampering/reseeding outside the pipeline looks like.
    spark.sql(
      "INSERT INTO j_audit.ingest_run_audit VALUES ('ghost-run', 'claims_hold', 'raw', 'SUCCESS', " +
        "0, 0, 0, 0, 0, 0, 0, CAST(NULL AS STRING), '', TIMESTAMP '2030-01-01 00:00:00', 'FULL', " +
        "'2027-01-01 00:00:00.0', '2027-02-01 00:00:00.0')")
    h2("INSERT INTO claims VALUES ('C008', 150, '2026-01-08 09:00:00')")

    val e = intercept[IllegalStateException] {
      new IngestPipeline(spark, auditedConf("hold"),
        Cli(entity = "claims_hold", mode = "FULL", runId = Some("hrun-3")), logger).run()
    }
    assert(e.getMessage.contains("watermark_continuity"),
      s"expected a continuity reconciliation failure, got: ${e.getMessage}")
    assert(InMemoryWatermarkStore.latest("claims_hold").get.values.head.startsWith("2026-01-07 09:00:00"),
      "the failed run must not advance the watermark")
  }

  test("FULL_SNAPSHOT_ABSENCE tombstones keys missing from a complete snapshot; reactivation revives them") {
    import org.apache.spark.sql.functions.col
    // FULL_TABLE every run = a complete snapshot each time (the incremental
    // block only seeds once; later runs read the whole table unbounded).
    val absenceConf = ConfigFactory.parseString(
      """source { mode = "FULL_TABLE" }
        |curated { merge { deletes {
        |  mode = "FULL_SNAPSHOT_ABSENCE"
        |  confirm_complete_extract = true
        |} } }""".stripMargin).withFallback(auditedConf("absence"))

    new IngestPipeline(spark, absenceConf,
      Cli(entity = "claims_absence", mode = "FULL", runId = Some("ab-1")), logger).run()
    val initial = spark.table("j_curated.claims_absence").count()
    assert(initial > 0)

    // Source row disappears -> next complete snapshot tombstones it
    h2("DELETE FROM claims WHERE claim_id = 'C002'")
    new IngestPipeline(spark, absenceConf,
      Cli(entity = "claims_absence", mode = "FULL", runId = Some("ab-2")), logger).run()
    val afterDelete = spark.table("j_curated.claims_absence")
    assert(afterDelete.count() == initial, "tombstoned rows are retained, not dropped")
    assert(afterDelete.filter(col("claim_id") === "C002")
      .select("last_modified_op").collect().head.getString(0) == "D")

    // Reactivation: the key returns to the source and publishes live again
    h2("INSERT INTO claims VALUES ('C002', 201, '2026-01-10 09:00:00')")
    new IngestPipeline(spark, absenceConf,
      Cli(entity = "claims_absence", mode = "FULL", runId = Some("ab-3")), logger).run()
    assert(spark.table("j_curated.claims_absence").filter(col("claim_id") === "C002")
      .select("last_modified_op").collect().head.getString(0) != "D",
      "a reappearing key publishes as a live row")
  }

  test("CFG_014: absence deletion rejects partial extracts and unimplemented capabilities") {
    val filtered = ConfigFactory.parseString(
      """source { mode = "FULL_TABLE", where = "amount > 0" }
        |curated { merge { deletes {
        |  mode = "FULL_SNAPSHOT_ABSENCE"
        |  confirm_complete_extract = true
        |} } }""".stripMargin).withFallback(auditedConf("cfg14"))
    val e1 = intercept[IllegalStateException] {
      new IngestPipeline(spark, filtered,
        Cli(entity = "claims_cfg14", mode = "FULL", runId = Some("c14-1")), logger).run()
    }
    assert(e1.getMessage.contains("CFG_014") && e1.getMessage.contains("PARTIAL"))

    val ct = ConfigFactory.parseString(
      """curated { merge { deletes { mode = "CHANGE_TRACKING" } } }""")
      .withFallback(auditedConf("cfg14"))
    val e2 = intercept[IllegalStateException] {
      new IngestPipeline(spark, ct,
        Cli(entity = "claims_cfg14", mode = "FULL", runId = Some("c14-2")), logger).run()
    }
    assert(e2.getMessage.contains("CFG_014"))
  }

}
