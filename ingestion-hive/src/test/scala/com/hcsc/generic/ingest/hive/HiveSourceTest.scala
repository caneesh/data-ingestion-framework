package com.hcsc.generic.ingest.hive

import com.hcsc.generic.ingest.watermark.{InMemoryWatermarkStore, WatermarkCommitDetail, WatermarkConflictException, WatermarkValue}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.DataFrame
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

/**
  * HiveSource against an in-memory-catalog fixture mirroring the bstar
  * layout: inc_ful_flag / file_date / file_time, both I and F partitions.
  * Every test asserts the source table is untouched afterwards.
  */
class HiveSourceTest extends AnyFunSuite with SharedSparkSession with BeforeAndAfterEach {

  private val Db = "hs_src"
  private val T = s"$Db.t"
  private val Audit = "hs_audit"

  private def wv(date: String, time: String) = WatermarkValue(Seq(date, time))

  private def insert(flag: String, date: String, time: String, rows: Seq[(Int, String)]): Unit =
    spark.sql(s"INSERT INTO $T PARTITION (inc_ful_flag='$flag', file_date='$date', file_time='$time') VALUES " +
      rows.map { case (i, v) => s"($i, '$v')" }.mkString(", "))

  private def addEmptyPartition(flag: String, date: String, time: String): Unit =
    spark.sql(s"ALTER TABLE $T ADD PARTITION (inc_ful_flag='$flag', file_date='$date', file_time='$time')")

  override def beforeAll(): Unit = {
    super.beforeAll()
    purgeWarehouseDb(Db)
    purgeWarehouseDb(Audit)
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $Db")
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $Audit")
    spark.sql(s"DROP TABLE IF EXISTS $Db.t_int")
    spark.sql(s"CREATE TABLE $Db.t_int (v STRING) USING orc PARTITIONED BY (file_date STRING, seq INT)")
    spark.sql(s"INSERT INTO $Db.t_int PARTITION (file_date='2025-01-07', seq=1) VALUES ('x')")
  }

  override def beforeEach(): Unit = {
    InMemoryWatermarkStore.clear()
    HiveSource.clearReadWindows()
    HiveSource.listByFilterOverride = None
    spark.sql(s"DROP TABLE IF EXISTS $T")
    spark.sql(s"CREATE TABLE $T (id INT, val STRING) USING orc " +
      "PARTITIONED BY (inc_ful_flag STRING, file_date STRING, file_time STRING)")
    insert("I", "2025-01-07", "06.24.07", Seq(1 -> "a", 2 -> "b"))
    insert("I", "2025-01-08", "04.48.31", Seq(3 -> "c"))
    insert("F", "2025-01-10", "04.52.00", Seq(4 -> "d", 5 -> "e", 6 -> "f"))
    insert("I", "2025-01-11", "07.39.59", Seq(7 -> "g"))
  }

  // ---- config builder --------------------------------------------------------

  private def conf(
    entity: Option[String] = Some("e1"),
    runId: Option[String] = Some("r1"),
    runMode: Option[String] = None,
    columns: Seq[String] = Seq("file_date", "file_time"),
    initial: String = "1900-01-01|00.00.00",
    formats: Seq[String] = Nil,
    lookback: Option[String] = None,
    where: Option[String] = None,
    incremental: Boolean = true,
    table: String = "t",
    missingFiles: Option[String] = None,
    audit: Option[(String, String)] = None,
    schema: Option[String] = None,
    logCounts: Boolean = false
  ): Config = {
    val sb = new StringBuilder
    sb.append(s"database = $Db\ntable = $table\n")
    entity.foreach(e => sb.append(s"""entity = "$e"""" + "\n"))
    runId.foreach(r => sb.append(s"""run_id = "$r"""" + "\n"))
    runMode.foreach(m => sb.append(s"run_mode = $m\n"))
    where.foreach(w => sb.append("where = \"\"\"" + w + "\"\"\"\n"))
    missingFiles.foreach(m => sb.append(s"missing_files = $m\n"))
    if (logCounts) sb.append("log_partition_counts = true\n")
    audit.foreach { case (d, t) => sb.append(s"audit_database = $d\naudit_run_table = $t\n") }
    if (incremental) {
      sb.append("incremental {\n")
      sb.append(s"""  watermark_columns = [${columns.map(c => s""""$c"""").mkString(", ")}]""" + "\n")
      sb.append(s"""  initial_value = "$initial"""" + "\n")
      if (formats.nonEmpty) sb.append(s"""  watermark_formats = [${formats.map(f => s""""$f"""").mkString(", ")}]""" + "\n")
      lookback.foreach(lb => sb.append(s"  lookback { $lb }\n"))
      sb.append("  watermark_store { type = memory }\n}\n")
    }
    schema.foreach(sb.append)
    ConfigFactory.parseString(sb.toString)
  }

  // ---- helpers ---------------------------------------------------------------

  private def ids(df: DataFrame): Seq[Int] = df.select("id").collect().map(_.getInt(0)).sorted.toSeq

  private def sourceState(): (Seq[String], Long) =
    (spark.sql(s"SHOW PARTITIONS $T").collect().map(_.getString(0)).sorted.toSeq, spark.table(T).count())

  /** The invariant: HiveSource never writes, alters or drops the source. */
  private def untouched[A](body: => A): A = {
    val before = sourceState()
    val result = body
    assert(sourceState() == before, "HiveSource modified the source table")
    result
  }

  private def seed(entity: String, value: WatermarkValue, runId: String = "seed"): Unit =
    InMemoryWatermarkStore.record(entity, value, runId)

  private def latest(entity: String) = InMemoryWatermarkStore.latestVersioned(entity)

  private val emptyDf: DataFrame = spark.emptyDataFrame

  // ---- readAll: reconciliation's source side ----------------------------------

  test("readAll reads every partition regardless of the stored watermark and captures nothing") {
    seed("e1", wv("2025-01-10", "04.52.00"))
    untouched {
      val df = HiveSource.readAll(spark, conf())
      assert(ids(df) == (1 to 7), "the watermark must never scope a reconciliation read")
      assert(HiveSource.lastWindow("e1", Some("r1")).isEmpty, "readAll captures no read window")
      assert(latest("e1").get.version == 1L, "readAll never touches the watermark store")
    }
  }

  test("readAll honors where on a partition column — the extraction's own scoping") {
    untouched {
      assert(ids(HiveSource.readAll(spark, conf(where = Some("inc_ful_flag = 'I'")))) == Seq(1, 2, 3, 7))
    }
  }

  test("readAll works without an incremental block") {
    untouched {
      assert(ids(HiveSource.readAll(spark, conf(incremental = false))) == (1 to 7))
    }
  }

  test("HIVE_004: readAll rejects where on a data column") {
    val e = intercept[IllegalArgumentException](HiveSource.readAll(spark, conf(where = Some("id > 1"))))
    assert(e.getMessage.contains("HIVE_004"))
  }

  // ---- selection and watermark lifecycle --------------------------------------

  test("first light from initial_value reads every partition and commits the max tuple") {
    untouched {
      val df = HiveSource.read(spark, conf())
      assert(ids(df) == (1 to 7))
      assert(HiveSource.lastWindow("e1", Some("r1")).contains(("1900-01-01|00.00.00", Some("2025-01-11|07.39.59"))))
      HiveSource.advanceWatermark(spark, conf(), "e1", "r1", df)
      val v = latest("e1").get
      assert(v.value == wv("2025-01-11", "07.39.59") && v.version == 1L)
      assert(InMemoryWatermarkStore.latestDetail("e1").flatMap(_.queryHash).contains("pk:inc_ful_flag|file_date|file_time"))
      assert(InMemoryWatermarkStore.latestDetail("e1").flatMap(_.lower).contains("1900-01-01|00.00.00"))
      assert(HiveSource.lastWindow("e1", Some("r1")).isEmpty)
    }
  }

  test("steady state reads only the partitions beyond the stored watermark") {
    seed("e1", wv("2025-01-08", "04.48.31"))
    untouched {
      val df = HiveSource.read(spark, conf())
      assert(ids(df) == Seq(4, 5, 6, 7))
      assert(HiveSource.lastWindow("e1", Some("r1")).contains(("2025-01-08|04.48.31", Some("2025-01-11|07.39.59"))))
      HiveSource.advanceWatermark(spark, conf(), "e1", "r1", df)
      assert(latest("e1").get.version == 2L && latest("e1").get.value == wv("2025-01-11", "07.39.59"))
    }
  }

  test("empty window returns a schema-only frame and does not bump the version") {
    seed("e1", wv("2025-01-11", "07.39.59"))
    untouched {
      val df = HiveSource.read(spark, conf())
      assert(df.count() == 0L)
      assert(df.columns.toSeq == Seq("id", "val", "inc_ful_flag", "file_date", "file_time"))
      assert(HiveSource.lastWindow("e1", Some("r1")).contains(("2025-01-11|07.39.59", Some("2025-01-11|07.39.59"))))
      HiveSource.advanceWatermark(spark, conf(), "e1", "r1", df)
      assert(latest("e1").get.version == 1L)
      assert(HiveSource.lastWindow("e1", Some("r1")).isEmpty)
    }
  }

  test("lookback by days re-reads a late partition for an OLDER date inside the window, not outside it") {
    seed("e1", wv("2025-01-11", "07.39.59"))
    insert("I", "2025-01-09", "05.44.01", Seq(8 -> "h"))
    untouched {
      // 2025-01-11 − 3 days = 2025-01-08: every partition ON or after that date, so 01-08 (id 3) re-reads too
      val inside = HiveSource.read(spark, conf(lookback = Some("days = 3")))
      assert(ids(inside) == Seq(3, 4, 5, 6, 7, 8))
      // the recorded window lower is the STORED watermark, never the rewound bound
      assert(HiveSource.lastWindow("e1", Some("r1")).contains(("2025-01-11|07.39.59", Some("2025-01-11|07.39.59"))))
      HiveSource.advanceWatermark(spark, conf(lookback = Some("days = 3")), "e1", "r1", inside)
      assert(latest("e1").get.version == 1L, "a late partition below the watermark must not move it")

      val outside = HiveSource.read(spark, conf(lookback = Some("days = 1")))
      assert(ids(outside) == Seq(4, 5, 6, 7))
      HiveSource.discardWindow("e1", Some("r1"))
    }
  }

  test("lookback by partitions walks back N enumerated tuples, or to initial_value when fewer exist") {
    seed("e1", wv("2025-01-11", "07.39.59"))
    untouched {
      assert(ids(HiveSource.read(spark, conf(lookback = Some("partitions = 2")))) == Seq(4, 5, 6, 7))
      HiveSource.discardWindow("e1", Some("r1"))
      assert(ids(HiveSource.read(spark, conf(lookback = Some("partitions = 3")))) == Seq(3, 4, 5, 6, 7))
      HiveSource.discardWindow("e1", Some("r1"))
    }
    InMemoryWatermarkStore.clear()
    seed("e1", wv("2025-01-07", "06.24.07"))
    untouched {
      assert(ids(HiveSource.read(spark, conf(lookback = Some("partitions = 5")))) == (1 to 7))
      HiveSource.discardWindow("e1", Some("r1"))
    }
  }

  test("the committed upper comes from enumeration: a selected partition with zero rows still advances") {
    seed("e1", wv("2025-01-11", "07.39.59"))
    addEmptyPartition("I", "2025-01-12", "00.00.00")
    untouched {
      val df = HiveSource.read(spark, conf(logCounts = true))
      assert(df.count() == 0L)
      assert(HiveSource.lastWindow("e1", Some("r1")).contains(("2025-01-11|07.39.59", Some("2025-01-12|00.00.00"))))
      HiveSource.advanceWatermark(spark, conf(), "e1", "r1", df)
      assert(latest("e1").get.value == wv("2025-01-12", "00.00.00"))
    }
  }

  test("where on a partition column scopes both the enumeration and the committed upper") {
    insert("F", "2025-01-12", "01.00.00", Seq(9 -> "i"))
    untouched {
      val df = HiveSource.read(spark, conf(where = Some("inc_ful_flag = 'I'")))
      assert(ids(df) == Seq(1, 2, 3, 7))
      assert(HiveSource.lastWindow("e1", Some("r1")).map(_._2).contains(Some("2025-01-11|07.39.59")))
      HiveSource.advanceWatermark(spark, conf(where = Some("inc_ful_flag = 'I'")), "e1", "r1", df)
      assert(latest("e1").get.value == wv("2025-01-11", "07.39.59"))
    }
  }

  // ---- errors ----------------------------------------------------------------

  private def failsWith(code: String)(body: => Any): Unit = {
    val ex = intercept[IllegalArgumentException](body)
    assert(ex.getMessage.contains(code), s"expected $code, got: ${ex.getMessage}")
  }

  test("HIVE_004: where may reference partition columns only") {
    untouched { failsWith("HIVE_004")(HiveSource.read(spark, conf(where = Some("val = 'a'")))) }
  }

  test("HIVE_003: watermark columns must be STRING partition columns") {
    untouched {
      failsWith("HIVE_003")(HiveSource.read(spark, conf(columns = Seq("id", "val"), initial = "0|a")))
      failsWith("HIVE_003")(HiveSource.read(spark, conf(table = "t_int", columns = Seq("file_date", "seq"), initial = "1900-01-01|0")))
    }
  }

  test("HIVE_002: the source table must exist") {
    untouched { failsWith("HIVE_002")(HiveSource.read(spark, conf(table = "nope"))) }
  }

  test("HIVE_005: a stored watermark of the wrong arity fails closed") {
    seed("e1", WatermarkValue(Seq("2025-01-08")))
    untouched { failsWith("HIVE_005")(HiveSource.read(spark, conf())) }
  }

  test("HIVE_006: the equal-length guard and the strict format guard") {
    insert("I", "2025-01-12", "6.24.07", Seq(10 -> "j"))
    untouched { failsWith("HIVE_006")(HiveSource.read(spark, conf())) }
    spark.sql(s"ALTER TABLE $T DROP PARTITION (inc_ful_flag='I', file_date='2025-01-12', file_time='6.24.07')")
    insert("I", "2025-01-12", "99.99.99", Seq(11 -> "k"))
    untouched {
      failsWith("HIVE_006")(HiveSource.read(spark, conf(formats = Seq("yyyy-MM-dd", "HH.mm.ss"))))
      // the equal-length guard alone lets 99.99.99 through — exactly why the bstar feed declares formats
      assert(ids(HiveSource.read(spark, conf())).contains(11))
      HiveSource.discardWindow("e1", Some("r1"))
    }
  }

  test("HIVE_007: FULL is refused once a watermark exists, allowed at first light") {
    untouched {
      assert(ids(HiveSource.read(spark, conf(runMode = Some("FULL")))) == (1 to 7))
      HiveSource.discardWindow("e1", Some("r1"))
    }
    seed("e1", wv("2025-01-08", "04.48.31"))
    untouched { failsWith("HIVE_007")(HiveSource.read(spark, conf(runMode = Some("FULL")))) }
  }

  test("HIVE_001: run_id without entity") {
    untouched { failsWith("HIVE_001")(HiveSource.read(spark, conf(entity = None))) }
  }

  test("validate mode (no entity, no run_id) returns the schema and never touches the store") {
    untouched {
      val df = HiveSource.read(spark, conf(entity = None, runId = None, formats = Seq("yyyy-MM-dd", "HH.mm.ss")))
      assert(df.count() == 0L)
      assert(df.columns.toSeq == Seq("id", "val", "inc_ful_flag", "file_date", "file_time"))
      assert(latest("e1").isEmpty && HiveSource.lastWindow("e1", None).isEmpty)
    }
    // validate mode still surfaces HIVE_006
    insert("I", "2025-01-12", "99.99.99", Seq(11 -> "k"))
    untouched {
      failsWith("HIVE_006")(HiveSource.read(spark, conf(entity = None, runId = None, formats = Seq("yyyy-MM-dd", "HH.mm.ss"))))
    }
  }

  // ---- commit paths ----------------------------------------------------------

  test("advanceWatermark propagates a CAS conflict") {
    untouched {
      val df = HiveSource.read(spark, conf())
      InMemoryWatermarkStore.record("e1", wv("2025-01-01", "00.00.00"), "intruder")
      intercept[WatermarkConflictException](HiveSource.advanceWatermark(spark, conf(), "e1", "r1", df))
    }
  }

  test("advanceWatermark with no in-memory window recovers the run's window from the ledger") {
    spark.sql(s"DROP TABLE IF EXISTS $Audit.ingest_run_audit")
    spark.sql(s"CREATE TABLE $Audit.ingest_run_audit (run_id STRING, entity STRING, stage STRING, status STRING, " +
      "window_start STRING, window_end STRING, event_ts TIMESTAMP) USING orc")
    spark.sql(s"INSERT INTO $Audit.ingest_run_audit VALUES " +
      "('r9', 'e9', 'raw', 'SUCCESS', '1900-01-01|00.00.00', '2025-01-10|04.52.00', timestamp'2026-09-30 01:00:00'), " +
      "('r9', 'e9', 'raw', 'STARTED', null, null, timestamp'2026-09-30 00:59:00')")
    val c = conf(entity = Some("e9"), runId = Some("r9"), audit = Some((Audit, "ingest_run_audit")))
    untouched {
      HiveSource.advanceWatermark(spark, c, "e9", "r9", emptyDf)
      assert(latest("e9").get.value == wv("2025-01-10", "04.52.00") && latest("e9").get.version == 1L)
      HiveSource.advanceWatermark(spark, c, "e9", "r9", emptyDf)
      assert(latest("e9").get.version == 1L, "store already at the ledger upper: no second commit")
      HiveSource.advanceWatermark(spark, conf(entity = Some("e8"), runId = Some("r8")), "e8", "r8", emptyDf)
      assert(latest("e8").isEmpty, "no window and no audit config: nothing to commit, no error")
    }
  }

  test("lastWindow and discardWindow") {
    untouched {
      val df = HiveSource.read(spark, conf())
      assert(HiveSource.lastWindow("e1", Some("r1")).isDefined)
      HiveSource.discardWindow("e1", Some("r1"))
      assert(HiveSource.lastWindow("e1", Some("r1")).isEmpty)
      HiveSource.advanceWatermark(spark, conf(), "e1", "r1", df)
      assert(latest("e1").isEmpty, "a discarded window commits nothing")
    }
  }

  test("readWindow reads exactly the partitions in (lower, upper]; HIVE_008 for a vanished window") {
    untouched {
      val df = HiveSource.readWindow(spark, conf(), wv("2025-01-07", "06.24.07"), wv("2025-01-10", "04.52.00"))
      assert(ids(df) == Seq(3, 4, 5, 6))
      assert(HiveSource.readWindow(spark, conf(), wv("2025-01-11", "07.39.59"), wv("2025-01-11", "07.39.59")).count() == 0L)
      failsWith("HIVE_008")(HiveSource.readWindow(spark, conf(), wv("2025-01-20", "00.00.00"), wv("2025-01-21", "00.00.00")))
      assert(latest("e1").isEmpty && HiveSource.lastWindow("e1", Some("r1")).isEmpty)
    }
  }

  test("falls back to full enumeration with a driver-side filter when the catalog cannot push the predicate") {
    seed("e1", wv("2025-01-08", "04.48.31"))
    HiveSource.listByFilterOverride = Some((_, _) => throw new RuntimeException("pushdown unsupported"))
    try untouched {
      val df = HiveSource.read(spark, conf(lookback = Some("partitions = 1")))
      assert(ids(df) == Seq(3, 4, 5, 6, 7))
      assert(HiveSource.lastWindow("e1", Some("r1")).map(_._2).contains(Some("2025-01-11|07.39.59")))
      HiveSource.discardWindow("e1", Some("r1"))
    } finally HiveSource.listByFilterOverride = None
  }

  test("a changed partition key set since the last commit is warned about, not fatal") {
    InMemoryWatermarkStore.recordIfVersion("e1", wv("2025-01-08", "04.48.31"), "old", 0L,
      WatermarkCommitDetail(Some("1900-01-01|00.00.00"), Some("pk:file_date|file_time")))
    untouched {
      assert(ids(HiveSource.read(spark, conf())) == Seq(4, 5, 6, 7))
      HiveSource.discardWindow("e1", Some("r1"))
    }
  }

  test("a schema contract renames an aliased source column") {
    val schema =
      """schema {
        |  version = "1"
        |  columns = [
        |    { name = "id",           type = "int",    required = true },
        |    { name = "value_text",   type = "string", required = true, aliases = ["val"] },
        |    { name = "inc_ful_flag", type = "string", required = true },
        |    { name = "file_date",    type = "string", required = true },
        |    { name = "file_time",    type = "string", required = true }
        |  ]
        |}
        |""".stripMargin
    untouched {
      val df = HiveSource.read(spark, conf(schema = Some(schema)))
      assert(df.columns.contains("value_text") && !df.columns.contains("val"))
      assert(df.count() == 7L)
      HiveSource.discardWindow("e1", Some("r1"))
    }
  }

  test("without an incremental block the whole table is read and nothing is committed") {
    untouched {
      val df = HiveSource.read(spark, conf(incremental = false))
      assert(ids(df) == (1 to 7))
      assert(HiveSource.lastWindow("e1", Some("r1")).isEmpty)
      HiveSource.advanceWatermark(spark, conf(incremental = false), "e1", "r1", df)
      assert(latest("e1").isEmpty)
    }
  }

  test("missing_files pins spark.sql.files.ignoreMissingFiles for the session") {
    untouched {
      HiveSource.read(spark, conf(missingFiles = Some("IGNORE")))
      assert(spark.conf.get("spark.sql.files.ignoreMissingFiles") == "true")
      HiveSource.discardWindow("e1", Some("r1"))
      HiveSource.read(spark, conf())
      assert(spark.conf.get("spark.sql.files.ignoreMissingFiles") == "false")
      HiveSource.discardWindow("e1", Some("r1"))
    }
  }
}
