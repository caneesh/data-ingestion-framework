package com.hcsc.generic.ingest.hive

import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite

class HiveSourceConfigTest extends AnyFunSuite {

  private def parse(hocon: String) = HiveSourceConfig.parse(ConfigFactory.parseString(hocon))

  private def failsWith(code: String)(hocon: String): Unit = {
    val ex = intercept[IllegalArgumentException](parse(hocon))
    assert(ex.getMessage.contains(code), s"expected $code, got: ${ex.getMessage}")
  }

  private val base =
    """database = bstar_raw
      |table = priv_addr
      |incremental {
      |  watermark_columns = ["file_date", "file_time"]
      |  initial_value = "1900-01-01|00.00.00"
      |  watermark_store { type = hive, database = bluestar_raw }
      |}
      |""".stripMargin

  test("a complete config parses; injected keys are optional") {
    val c = parse(base)
    assert(c.fullTable == "bstar_raw.priv_addr")
    assert(c.missingFiles == "FAIL" && !c.logPartitionCounts)
    assert(c.entity.isEmpty && c.runId.isEmpty && c.runMode.isEmpty && c.auditDatabase.isEmpty)
    val wm = c.watermark.get
    assert(wm.columns == Seq("file_date", "file_time"))
    assert(wm.initialValue.values == Seq("1900-01-01", "00.00.00"))
    assert(wm.storeType == "hive" && wm.storeDatabase.contains("bluestar_raw") && wm.storeTable == "ingest_watermarks")
    assert(wm.lookback.isEmpty && wm.formats.isEmpty)
  }

  test("injected keys are read when present; run_mode is upper-cased") {
    val c = parse(base + """entity = e1
                           |run_id = r1
                           |run_mode = full
                           |audit_database = bluestar_raw
                           |audit_run_table = ingest_run_audit
                           |where = "inc_ful_flag = 'I'"
                           |missing_files = ignore
                           |log_partition_counts = true
                           |""".stripMargin)
    assert(c.entity.contains("e1") && c.runId.contains("r1") && c.runMode.contains("FULL"))
    assert(c.auditDatabase.contains("bluestar_raw") && c.auditRunTable.contains("ingest_run_audit"))
    assert(c.where.contains("inc_ful_flag = 'I'") && c.missingFiles == "IGNORE" && c.logPartitionCounts)
  }

  test("no incremental block means no watermark") {
    assert(parse("database = a\ntable = b\n").watermark.isEmpty)
  }

  test("database and table are required safe identifiers") {
    failsWith("CFG_023")("table = b\n")
    failsWith("CFG_023")("database = a\n")
    intercept[IllegalArgumentException](parse("database = \"a-b\"\ntable = t\n"))
  }

  test("watermark_columns must be explicit and non-empty") {
    failsWith("CFG_023")(base.replace("""watermark_columns = ["file_date", "file_time"]""", "watermark_columns = []"))
    failsWith("CFG_023")(base.replace("""watermark_columns = ["file_date", "file_time"]""", ""))
    failsWith("CFG_023")(base.replace("""["file_date", "file_time"]""", """["file_date", "FILE_DATE"]"""))
  }

  test("initial_value is required and its arity must match (HIVE_005)") {
    failsWith("CFG_023")(base.replace("""initial_value = "1900-01-01|00.00.00"""", ""))
    failsWith("HIVE_005")(base.replace("1900-01-01|00.00.00", "1900-01-01"))
    failsWith("HIVE_005")(base.replace("1900-01-01|00.00.00", "1900-01-01|00.00.00|extra"))
  }

  test("watermark_formats must have one pattern per column and be valid") {
    failsWith("CFG_023")(base.replace("initial_value", "watermark_formats = [\"yyyy-MM-dd\"]\n  initial_value"))
    failsWith("CFG_023")(base.replace("initial_value", "watermark_formats = [\"yyyy-MM-dd\", \"HH.mm.ss\", \"x\"]\n  initial_value"))
    failsWith("CFG_023")(base.replace("initial_value", "watermark_formats = [\"yyyy-MM-dd\", \"HH.mm.ss\"]\n  watermark_formats = [\"bogus{{\", \"HH.mm.ss\"]\n  initial_value"))
    assert(parse(base.replace("initial_value", "watermark_formats = [\"yyyy-MM-dd\", \"HH.mm.ss\"]\n  initial_value"))
      .watermark.get.formats == Seq("yyyy-MM-dd", "HH.mm.ss"))
  }

  test("lookback names exactly one positive bound (CFG_027)") {
    def withLookback(lb: String) = base.replace("initial_value", s"lookback { $lb }\n  initial_value")
    failsWith("CFG_027")(withLookback("days = 3, partitions = 2"))
    failsWith("CFG_027")(withLookback(""))
    failsWith("CFG_027")(withLookback("days = 0"))
    failsWith("CFG_027")(withLookback("partitions = -1"))
    assert(parse(withLookback("days = 3")).watermark.get.lookback.contains(Lookback(Some(3), None)))
    assert(parse(withLookback("partitions = 2")).watermark.get.lookback.contains(Lookback(None, Some(2))))
  }

  test("watermark store: hive needs a database; memory does not; unknown types are rejected") {
    failsWith("CFG_023")(base.replace("watermark_store { type = hive, database = bluestar_raw }", "watermark_store { type = hive }"))
    failsWith("CFG_023")(base.replace("watermark_store { type = hive, database = bluestar_raw }", ""))
    failsWith("CFG_023")(base.replace("type = hive", "type = redis"))
    val mem = parse(base.replace("watermark_store { type = hive, database = bluestar_raw }", "watermark_store { type = memory }"))
    assert(mem.watermark.get.storeType == "memory" && mem.watermark.get.storeDatabase.isEmpty)
    assert(parse(base.replace("database = bluestar_raw }", "database = bluestar_raw, table = wm_hist }"))
      .watermark.get.storeTable == "wm_hist")
  }

  test("missing_files must be FAIL or IGNORE") {
    failsWith("CFG_023")(base + "missing_files = maybe\n")
  }
}
