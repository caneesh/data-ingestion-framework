package com.hcsc.generic.ingest.app

import com.hcsc.generic.ingest.kafka.{KafkaOffsetLedger, KafkaOffsets, OffsetRange}
import com.hcsc.generic.ingest.runtime.RunContext
import com.hcsc.generic.ingest.stage.{CuratedMicroBatch, CuratedService}
import com.typesafe.config.ConfigFactory
import java.nio.file.{Files, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/** Input-mode independence: the ONE curated writer consumes standardized
  * frames from any source. Kafka-shaped batches (metadata columns,
  * out-of-order events, tombstones), the foreachBatch micro-batch path
  * (empty no-op, same-batch-id idempotency, FULL_OVERWRITE guard), the
  * offset codec and the offset ledger. File and JDBC pipelines already
  * drive this same writer end to end in their own integration specs. */
class InputModeCuratedSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var tempDir: Path = _
  private var spark: SparkSession = _
  private val logger = Logger.getLogger(getClass.getName)

  override def beforeAll(): Unit = {
    super.beforeAll()
    tempDir = Files.createTempDirectory("input-mode-")
    System.setProperty("derby.stream.error.file",
      tempDir.resolve("derby.log").toAbsolutePath.toString)
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("input-mode-test")
      .config("spark.sql.warehouse.dir", tempDir.resolve("warehouse").toAbsolutePath.toString)
      .config("javax.jdo.option.ConnectionURL",
        s"jdbc:derby:;databaseName=${tempDir.resolve("metastore_db").toAbsolutePath};create=true")
      .config("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver")
      .config("datanucleus.schema.autoCreateTables", "true")
      .config("hive.metastore.schema.verification", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.session.timeZone", "UTC")
      .enableHiveSupport()
      .getOrCreate()
    spark.sql("CREATE DATABASE IF NOT EXISTS im")
  }

  override def afterAll(): Unit = {
    try { if (spark != null) spark.stop() }
    finally {
      import scala.collection.JavaConverters._
      Files.walk(tempDir).iterator().asScala.toSeq.reverse.foreach(p => Files.deleteIfExists(p))
    }
    super.afterAll()
  }

  private def ctx(runId: String) = RunContext(runId, "im_feed", "INCR", "I")

  /** Kafka-shaped standardized frame: business payload + kafka_* metadata. */
  private def kafkaBatch(
    rows: Seq[(String, String, String, Long, Boolean)] // key, name, event_ts, offset, tombstone
  ): DataFrame = {
    val s = spark
    import s.implicits._
    rows.toDF("member_id", "name", "kafka_timestamp", "kafka_offset", "kafka_tombstone")
      .withColumn("kafka_timestamp", col("kafka_timestamp").cast("timestamp"))
      .withColumn("kafka_topic", org.apache.spark.sql.functions.lit("member-events"))
      .withColumn("kafka_partition", org.apache.spark.sql.functions.lit(0))
  }

  private val plainConf = ConfigFactory.parseString(
    """
      |database = im
      |table = members_plain
      |format = parquet
      |merge { keys = ["member_id"], freshness { column = "src_modified_ts" } }
      |""".stripMargin)

  private val kafkaConf = ConfigFactory.parseString(
    """
      |database = im
      |table = members_kafka
      |format = parquet
      |retain_source_metadata = true
      |merge {
      |  keys = ["member_id"]
      |  freshness { column = "kafka_timestamp", tie_breakers = ["kafka_offset"] }
      |}
      |""".stripMargin)

  // ---------------------------------------------------------------------------
  // Source metadata never becomes curated business columns by accident
  // ---------------------------------------------------------------------------

  test("kafka metadata columns are dropped from curated unless retained") {
    val s = spark
    import s.implicits._
    val df = Seq(("M1", "v1", "2026-01-01 10:00:00")).toDF("member_id", "name", "src_modified_ts")
      .withColumn("src_modified_ts", col("src_modified_ts").cast("timestamp"))
      .withColumn("kafka_topic", org.apache.spark.sql.functions.lit("t"))
      .withColumn("kafka_offset", org.apache.spark.sql.functions.lit(7L))
    new CuratedService(spark, plainConf).process(df, "FULL", ctx("im-1"), None, None)
    val cols = spark.table("im.members_plain").columns.map(_.toLowerCase).toSet
    assert(!cols.contains("kafka_topic") && !cols.contains("kafka_offset"),
      s"source metadata must not become curated business columns, got $cols")
    assert(cols.contains("member_id") && cols.contains("name"))
  }

  test("ordering by a dropped metadata column fails loudly (CUR_007)") {
    val noRetain = ConfigFactory.parseString(
      """
        |database = im
        |table = members_badorder
        |format = parquet
        |merge { keys = ["member_id"], freshness { column = "kafka_timestamp" } }
        |""".stripMargin)
    val e = intercept[IllegalArgumentException] {
      new CuratedService(spark, noRetain)
        .process(kafkaBatch(Seq(("M1", "v", "2026-01-01 10:00:00", 1L, false))),
          "FULL", ctx("im-2"), None, None)
    }
    assert(e.getMessage.contains("CUR_007"))
    assert(e.getMessage.contains("retain_source_metadata"))
  }

  // ---------------------------------------------------------------------------
  // Kafka batch semantics through the SAME writer
  // ---------------------------------------------------------------------------

  test("out-of-order kafka events: the older event never wins") {
    val svc = new CuratedService(spark, kafkaConf)
    svc.process(kafkaBatch(Seq(("K1", "current", "2026-01-01 12:00:00", 100L, false))),
      "FULL", ctx("im-k1"), None, None)
    // A LATE event with an older timestamp (and lower offset) arrives next batch
    val r = svc.process(kafkaBatch(Seq(("K1", "late_old", "2026-01-01 10:00:00", 50L, false))),
      "INCR", ctx("im-k2"), None, None).get
    assert(r.ignoredCount == 1 && r.updateCount == 0)
    assert(spark.table("im.members_kafka").filter(col("member_id") === "K1")
      .collect().head.getAs[String]("name") == "current")

    // In-batch disorder: same key twice, later offset wins deterministically
    val r2 = svc.process(kafkaBatch(Seq(
      ("K2", "first", "2026-01-01 13:00:00", 200L, false),
      ("K2", "second", "2026-01-01 13:00:00", 201L, false) // equal ts: offset breaks the tie
    )), "INCR", ctx("im-k3"), None, None).get
    assert(r2.insertCount == 1 && r2.dedupedCount == 1)
    assert(spark.table("im.members_kafka").filter(col("member_id") === "K2")
      .collect().head.getAs[String]("name") == "second")
  }

  test("kafka tombstones drive SOFT deletes through the shared delete machinery") {
    val tombConf = ConfigFactory.parseString(
      """
        |database = im
        |table = members_tomb
        |format = parquet
        |retain_source_metadata = true
        |merge {
        |  keys = ["member_id"]
        |  freshness { column = "kafka_timestamp", tie_breakers = ["kafka_offset"] }
        |  deletes { mode = "SOFT", indicator_column = "kafka_tombstone", indicator_values = ["true"] }
        |}
        |""".stripMargin)
    val svc = new CuratedService(spark, tombConf)
    svc.process(kafkaBatch(Seq(("T1", "alive", "2026-01-01 10:00:00", 1L, false))),
      "FULL", ctx("im-t1"), None, None)
    val r = svc.process(kafkaBatch(Seq(("T1", "alive", "2026-01-01 11:00:00", 2L, true))),
      "INCR", ctx("im-t2"), None, None).get
    assert(r.deleteCount == 1)
    assert(spark.table("im.members_tomb").filter(col("member_id") === "T1")
      .collect().head.getAs[String]("last_modified_op") == "D")
  }

  // ---------------------------------------------------------------------------
  // foreachBatch micro-batch path
  // ---------------------------------------------------------------------------

  test("an empty micro-batch is a successful no-op") {
    val before = spark.table("im.members_kafka").count()
    val r = CuratedMicroBatch.write(spark, "im_feed", kafkaConf, None, None,
      kafkaBatch(Seq.empty), "42", logger)
    assert(r.isEmpty)
    assert(spark.table("im.members_kafka").count() == before)
  }

  test("retrying the same micro-batch id is idempotent") {
    val batch = kafkaBatch(Seq(("MB1", "v1", "2026-01-02 10:00:00", 300L, false)))
    val r1 = CuratedMicroBatch.write(spark, "im_feed", kafkaConf, None, None, batch, "7", logger).get
    val snapshot = spark.table("im.members_kafka").collect()
      .map(_.getValuesMap[Any](Seq("member_id", "name"))).toSet
    val r2 = CuratedMicroBatch.write(spark, "im_feed", kafkaConf, None, None, batch, "7", logger).get
    val after = spark.table("im.members_kafka").collect()
      .map(_.getValuesMap[Any](Seq("member_id", "name"))).toSet
    assert(r1.insertCount == 1)
    assert(r2.ignoredCount == 1 && r2.insertCount == 0, "the replayed batch must be a no-op")
    assert(snapshot == after)
  }

  test("FULL_OVERWRITE is refused for micro-batches unless explicitly allowed") {
    val fullConf = ConfigFactory.parseString(
      """publish { mode = "FULL_OVERWRITE" }""").withFallback(kafkaConf)
    val e = intercept[IllegalArgumentException] {
      CuratedMicroBatch.write(spark, "im_feed", fullConf, None, None,
        kafkaBatch(Seq(("X", "v", "2026-01-01 10:00:00", 1L, false))), "9", logger)
    }
    assert(e.getMessage.contains("CUR_007"))
  }

  // ---------------------------------------------------------------------------
  // KafkaSource.decode — the actual source decode path (no broker required:
  // decode is a pure DataFrame transform over the kafka reader's shape)
  // ---------------------------------------------------------------------------

  test("KafkaSource.decode stamps metadata, captures tombstones pre-parse, honors retention") {
    val s = spark
    import s.implicits._
    val raw = Seq(
      ("k1", """{"member_id":"M1","name":"alice"}""", "member-events", 0, 100L),
      ("k2", null.asInstanceOf[String], "member-events", 1, 200L) // tombstone
    ).toDF("key", "value", "topic", "partition", "offset")
      .withColumn("timestamp", org.apache.spark.sql.functions.lit("2026-01-01 10:00:00").cast("timestamp"))

    val conf = com.typesafe.config.ConfigFactory.parseString(
      """value.format = "json"
        |value.schema = "{\"type\":\"struct\",\"fields\":[{\"name\":\"member_id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}"
        |""".stripMargin)
    val decoded = com.hcsc.generic.ingest.kafka.KafkaSource.decode(raw, conf)

    val cols = decoded.columns.toSet
    assert(Set("kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp",
      "kafka_tombstone").subsetOf(cols), s"spec-named metadata columns required, got $cols")
    val byKey = decoded.collect().map(r => r.getAs[String]("key") -> r).toMap
    assert(!byKey("k1").getAs[Boolean]("kafka_tombstone"))
    assert(byKey("k1").getAs[String]("member_id") == "M1" && byKey("k1").getAs[String]("name") == "alice")
    assert(byKey("k2").getAs[Boolean]("kafka_tombstone"),
      "a null VALUE must be flagged as tombstone BEFORE JSON parsing nulls everything")
    assert(byKey("k2").getAs[Long]("kafka_offset") == 200L)

    val dropped = com.hcsc.generic.ingest.kafka.KafkaSource.decode(raw,
      com.typesafe.config.ConfigFactory.parseString("retain_kafka_metadata = false")
        .withFallback(conf))
    assert(!dropped.columns.exists(_.startsWith("kafka_")),
      "retain_kafka_metadata=false must drop every kafka_* column")
  }

  // ---------------------------------------------------------------------------
  // Offset codec + ledger (no broker required)
  // ---------------------------------------------------------------------------

  test("offset codec: starting offsets, pinned ends and ranges read") {
    assert(KafkaOffsets.toJson("t", Map(0 -> 5L, 1 -> -2L)) == """{"t":{"0":5,"1":-2}}""")
    val ranges = KafkaOffsets.rangesRead(
      start = Map(0 -> 5L, 1 -> -2L, 2 -> 9L),
      end = Map(0 -> 8L, 1 -> 4L, 2 -> 9L))
    // partition 1 starts from "earliest" (-2): no committable numeric range;
    // partition 2 read nothing; partition 0 read (5, 8]
    assert(ranges == Seq(OffsetRange(0, 5L, 8L)))
  }

  test("offset ledger round-trips committed ranges and absorbs replays") {
    val ledger = new KafkaOffsetLedger(spark, "im", "kafka_offsets")
    assert(ledger.committedUntil("e1", "t1").isEmpty)
    ledger.commit("e1", "t1", "run-1", Seq(OffsetRange(0, 0L, 100L), OffsetRange(1, 0L, 40L)))
    assert(ledger.committedUntil("e1", "t1") == Map(0 -> 100L, 1 -> 40L))
    // replayed commit + a later range: MAX per partition wins
    ledger.commit("e1", "t1", "run-1", Seq(OffsetRange(0, 0L, 100L)))
    ledger.commit("e1", "t1", "run-2", Seq(OffsetRange(0, 100L, 250L)))
    assert(ledger.committedUntil("e1", "t1") == Map(0 -> 250L, 1 -> 40L))
    assert(ledger.committedUntil("e1", "OTHER").isEmpty, "topics are isolated")
  }
}
