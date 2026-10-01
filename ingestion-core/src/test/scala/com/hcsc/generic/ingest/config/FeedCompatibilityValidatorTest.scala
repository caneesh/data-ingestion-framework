package com.hcsc.generic.ingest.config

import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite

class FeedCompatibilityValidatorTest extends AnyFunSuite {

  private def errorsOf(hocon: String): Seq[String] =
    FeedCompatibilityValidator.validate(ConfigFactory.parseString(hocon))

  test("a coherent jdbc incremental feed validates cleanly") {
    assert(errorsOf(
      """source { type = "jdbc", extraction { strategy = "TIMESTAMP",
        |  boundary { columns = [ { name = "ts" } ], initial = "1900-01-01 00:00:00" } } }
        |curated { strategy = "TYPE1_MERGE", merge { keys = ["id"] } }
        |audit { enabled = true, reconciliation { on_mismatch = "FAIL" } }
      """.stripMargin).isEmpty)
  }

  test("extraction on a non-jdbc source is rejected (CFG_001)") {
    val errors = errorsOf("""source { type = "file", extraction { strategy = "TIMESTAMP" } }""")
    assert(errors.exists(_.contains("CFG_001")))
  }

  test("incremental extraction without a boundary is rejected (CFG_002)") {
    assert(errorsOf("""source { type = "jdbc", extraction { strategy = "INCREASING_KEY" } }""")
      .exists(_.contains("CFG_002")))
  }

  test("overlap on FULL_SNAPSHOT is contradictory (CFG_003)") {
    assert(errorsOf(
      """source { type = "jdbc", extraction { strategy = "FULL_SNAPSHOT",
        |  boundary { overlap = 300 } } }""".stripMargin)
      .exists(_.contains("CFG_003")))
  }

  test("CDC raw events with a keyless state-deriving curated layer is rejected (CFG_004)") {
    // Keyless curated with no explicit strategy would derive state lossily
    assert(errorsOf(
      """source { type = "jdbc" }
        |raw { strategy = "CDC_EVENTS" }
        |curated { database = "d", table = "t" }""".stripMargin)
      .exists(_.contains("CFG_004")))
    // A keyed merge makes CDC coherent
    assert(errorsOf(
      """source { type = "jdbc" }
        |raw { strategy = "CDC_EVENTS" }
        |curated { strategy = "TYPE1_MERGE", merge { keys = ["id"] } }""".stripMargin)
      .isEmpty)
    // Explicit APPEND is a legitimate keyless event-history curated layer
    assert(errorsOf(
      """source { type = "jdbc" }
        |raw { strategy = "CDC_EVENTS" }
        |curated { strategy = "APPEND" }""".stripMargin)
      .isEmpty)
    // Raw-only CDC archive (no curated block at all) is legitimate
    assert(errorsOf(
      """source { type = "jdbc" }
        |raw { strategy = "CDC_EVENTS" }""".stripMargin)
      .isEmpty)
  }

  test("keyed merge without keys is rejected at startup, including the MERGE alias (CFG_005)") {
    assert(errorsOf("""curated { strategy = "TYPE1_MERGE", merge { keys = [] } }""")
      .exists(_.contains("CFG_005")))
    assert(errorsOf("""curated { strategy = "MERGE", merge { keys = [] } }""")
      .exists(_.contains("CFG_005")))
  }

  test("contract-derived rejects without a contract are rejected (CFG_006)") {
    assert(errorsOf("""rejects { use_contract_nullability = true }""")
      .exists(_.contains("CFG_006")))
  }

  test("failing reconciliation with audit disabled is rejected (CFG_007)") {
    assert(errorsOf(
      """audit { enabled = false, reconciliation { on_mismatch = "FAIL" } }""")
      .exists(_.contains("CFG_007")))
  }

  test("a negative volume floor is rejected (CFG_021)") {
    // A floor that can never trip reads as protection in the config while
    // detecting nothing — worse than declaring none.
    assert(errorsOf("""audit { reconciliation { min_accepted_rows = -1 } }""")
      .exists(_.contains("CFG_021")))
  }

  test("a volume floor with audit disabled is rejected (CFG_021)") {
    // The floor is evaluated as a reconciliation check and recorded in the
    // ledger; without the ledger it silently does nothing.
    assert(errorsOf(
      """audit { enabled = false, reconciliation { min_accepted_rows = 1 } }""")
      .exists(_.contains("CFG_021")))
  }

  test("a zero floor is legal — it declares 'empty is expected' explicitly") {
    assert(!errorsOf("""audit { enabled = true, reconciliation { min_accepted_rows = 0 } }""")
      .exists(_.contains("CFG_021")))
  }

  test("JDBC watermark blocks on file sources are rejected (CFG_008)") {
    assert(errorsOf(
      """source { type = "file", incremental { watermark_type = "TIMESTAMP" } }""")
      .exists(_.contains("CFG_008")))
  }

  test("incremental jdbc feeding a keyless state-deriving curated layer is rejected (CFG_009)") {
    assert(errorsOf(
      """source { type = "jdbc", mode = "INCREMENTAL" }
        |curated { database = "d", table = "t", merge { keys = [] } }""".stripMargin)
      .exists(_.contains("CFG_009")))
    // Incremental EXTRACTION strategies are incremental sources too
    assert(errorsOf(
      """source { type = "jdbc", extraction { strategy = "TIMESTAMP",
        |  boundary { columns = [ { name = "ts" } ], initial = "1900-01-01 00:00:00" } } }
        |curated { database = "d", table = "t", merge { keys = [] } }""".stripMargin)
      .exists(_.contains("CFG_009")))
    // Keys make it coherent; explicit APPEND keeps delta history legally;
    // FULL mode without keys stays legal (true snapshots)
    assert(errorsOf(
      """source { type = "jdbc", mode = "INCREMENTAL" }
        |curated { merge { keys = ["id"] } }""".stripMargin).isEmpty)
    assert(errorsOf(
      """source { type = "jdbc", mode = "INCREMENTAL" }
        |curated { strategy = "APPEND" }""".stripMargin).isEmpty)
    assert(errorsOf(
      """source { type = "jdbc", mode = "FULL_TABLE" }
        |curated { merge { keys = [] } }""".stripMargin).isEmpty)
  }

  test("logical comparison types validate at config time (CFG_017)") {
    assert(errorsOf(
      """curated { merge { freshness { column = "ts", compare_as = "not_a_type" } } }""")
      .exists(e => e.contains("CFG_017") && e.contains("not_a_type")))
    assert(errorsOf(
      """curated { merge { freshness { column = "ts", compare_as = "timestamp",
        |  compare_format = "M/d/yyyy" } } }""".stripMargin)
      .exists(e => e.contains("CFG_017") && e.contains("not both")))
    assert(errorsOf(
      """curated { merge { freshness { column = "ts",
        |  tie_breakers = ["seq as bigintt"] } } }""".stripMargin)
      .exists(e => e.contains("CFG_017") && e.contains("bigintt")))
    assert(errorsOf(
      """curated { dedup { order_by = ["seq wrongtoken"] }
        |  merge { freshness { column = "ts" } } }""".stripMargin)
      .exists(e => e.contains("CFG_017") && e.contains("wrongtoken")))
    // The valid shapes pass
    assert(!errorsOf(
      """curated { merge { freshness { column = "ts", compare_format = "M/d/yyyy",
        |  tie_breakers = ["seq desc as bigint"] } } }""".stripMargin)
      .exists(_.contains("CFG_017")))
  }

  test("multiple incompatibilities are all reported") {
    val errors = errorsOf(
      """source { type = "file", extraction { strategy = "TIMESTAMP" } }
        |curated { strategy = "TYPE1_MERGE", merge { keys = [] } }""".stripMargin)
    assert(errors.size >= 2)
  }

  // ---- hive sources (CFG_023, CFG_024, CFG_025, CFG_027) ---------------------

  private def hive(inc: String, mode: String = "mode = \"INCR\"", extra: String = ""): String =
    s"""$mode
       |source { type = "hive", database = "d", table = "t" $inc }
       |curated { merge { keys = ["id"] } }
       |$extra""".stripMargin

  private val goodInc =
    """, incremental { watermark_columns = ["file_date", "file_time"],
      |  initial_value = "1900-01-01|00.00.00", watermark_store { type = "memory" } }""".stripMargin

  test("a coherent hive incremental feed validates cleanly") {
    assert(errorsOf(hive(goodInc)).isEmpty, errorsOf(hive(goodInc)).mkString("; "))
  }

  test("hive with mode = INCR requires explicit watermark columns and an initial value (CFG_023)") {
    assert(errorsOf(hive("")).exists(_.startsWith("CFG_023")), "no incremental block")
    assert(errorsOf(hive(""", incremental { initial_value = "1900-01-01" }"""))
      .exists(_.startsWith("CFG_023")), "no watermark_columns")
    assert(errorsOf(hive(""", incremental { watermark_columns = [] , initial_value = "x" }"""))
      .exists(_.startsWith("CFG_023")), "empty watermark_columns")
    assert(errorsOf(hive(""", incremental { watermark_columns = ["file_date"] }"""))
      .exists(_.startsWith("CFG_023")), "no initial_value")
    // Not an INCR feed: the rule is silent
    assert(!errorsOf(hive("", mode = "")).exists(_.startsWith("CFG_023")))
    assert(!errorsOf(hive("", mode = "mode = \"FULL\"")).exists(_.startsWith("CFG_023")))
  }

  test("JDBC watermark settings on a hive source are rejected, one error each (CFG_024)") {
    Seq("overlap = \"300\"", "watermark_type = \"TIMESTAMP\"", "upper_bound = \"SOURCE_CLOCK\"",
      "clock_zone = \"UTC\"").foreach { k =>
      val errors = errorsOf(hive(
        s""", incremental { watermark_columns = ["file_date"], initial_value = "1900-01-01", $k }"""))
      assert(errors.count(_.startsWith("CFG_024")) == 1, s"$k -> $errors")
      assert(errors.exists(e => e.startsWith("CFG_024") && e.contains(k.takeWhile(_ != ' '))))
    }
  }

  test("an initial_value whose arity differs from watermark_columns is rejected (CFG_025)") {
    assert(errorsOf(hive(
      """, incremental { watermark_columns = ["file_date", "file_time"], initial_value = "1900-01-01" }"""))
      .exists(_.startsWith("CFG_025")))
    assert(!errorsOf(hive(goodInc)).exists(_.startsWith("CFG_025")))
  }

  test("lookback must name exactly one positive bound (CFG_027)") {
    def withLookback(lb: String) = hive(
      s""", incremental { watermark_columns = ["file_date"], initial_value = "1900-01-01",
         |  lookback { $lb } }""".stripMargin)
    assert(errorsOf(withLookback("")).exists(_.startsWith("CFG_027")), "neither")
    assert(errorsOf(withLookback("days = 3, partitions = 2")).exists(_.startsWith("CFG_027")), "both")
    assert(errorsOf(withLookback("days = 0")).exists(_.startsWith("CFG_027")), "zero")
    assert(errorsOf(withLookback("partitions = -1")).exists(_.startsWith("CFG_027")), "negative")
    assert(!errorsOf(withLookback("days = 3")).exists(_.startsWith("CFG_027")))
    assert(!errorsOf(withLookback("partitions = 2")).exists(_.startsWith("CFG_027")))
  }

  test("the file-only and jdbc-only rules stay silent for hive sources") {
    val errors = errorsOf(hive(goodInc))
    assert(!errors.exists(_.startsWith("CFG_008")), "CFG_008 is file-only")
    assert(!errorsOf(hive(goodInc, extra = "")).exists(_.startsWith("CFG_009")), "CFG_009 is jdbc-only")
  }
}
