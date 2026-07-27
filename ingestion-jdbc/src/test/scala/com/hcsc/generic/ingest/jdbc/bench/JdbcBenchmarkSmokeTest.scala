package com.hcsc.generic.ingest.jdbc.bench

import com.hcsc.generic.ingest.jdbc.SharedSparkSession
import com.hcsc.generic.ingest.jdbc.bench.JdbcBenchmarkRunner.{BenchmarkResult, Scenario}
import org.scalatest.funsuite.AnyFunSuite

/**
 * Fast smoke test for [[JdbcBenchmarkRunner]]. Its only job is to keep the
 * harness alive: it runs the runner end to end with a tiny row count and a
 * two-scenario matrix, and asserts the results are plausible. It intentionally
 * does NOT assert on relative performance (that is environment-dependent and
 * would be flaky) — only that the harness executes, returns one result per
 * scenario, reads the whole table, and produces non-negative durations.
 *
 * Kept well under ~30s: 2,000 rows against H2 in-memory with two scenarios.
 */
class JdbcBenchmarkSmokeTest extends AnyFunSuite with SharedSparkSession {

  private val Rows = 2000

  private val scenarios: Seq[Scenario] = Seq(
    Scenario("unpartitioned smoke", "fetchsize = 1000"),
    Scenario(
      "predicate-2part smoke",
      """
        |mode = "SELECT_QUERY"
        |partition_strategy = "PREDICATES"
        |partition_predicates = [ "id < 1000", "id >= 1000" ]
      """.stripMargin)
  )

  test("harness runs end to end and returns one plausible result per scenario") {
    val results: Seq[BenchmarkResult] = JdbcBenchmarkRunner.run(spark, Rows, scenarios)

    assert(results.size == scenarios.size, "one result per scenario")
    assert(results.map(_.label) == scenarios.map(_.label), "results preserve scenario order")

    results.foreach { r =>
      assert(r.rows == Rows, s"scenario '${r.label}' must read all $Rows rows, got ${r.rows}")
      assert(r.durationMs >= 0, s"scenario '${r.label}' duration must be non-negative, got ${r.durationMs}")
      // rowsPerSec is only meaningful for a positive duration; when >0 it must be finite.
      if (r.durationMs > 0) {
        assert(r.rowsPerSec > 0.0 && !r.rowsPerSec.isInfinite && !r.rowsPerSec.isNaN,
          s"scenario '${r.label}' rows/sec must be a positive finite number, got ${r.rowsPerSec}")
      }
    }
  }

  test("formatTable renders every scenario label and the H2 caveat") {
    val results = JdbcBenchmarkRunner.run(spark, Rows, scenarios)
    val table = JdbcBenchmarkRunner.formatTable(Rows, results)

    assert(table.contains("CAVEAT"), "results table must carry the H2-relative-only caveat")
    scenarios.foreach { s =>
      assert(table.contains(s.label), s"results table must list scenario '${s.label}'")
    }
  }

  test("seed is deterministic: identical row content across reseeds") {
    // Reseeding the same row count must produce identical data (same table,
    // dropped and recreated), so scenario comparisons are apples-to-apples.
    val table1 = JdbcBenchmarkRunner.seed(Rows)
    val df1 = com.hcsc.generic.ingest.jdbc.JdbcSource
      .read(spark, com.typesafe.config.ConfigFactory.parseString(
        com.hcsc.generic.ingest.jdbc.H2TestDatabase.sourceHocon(s"""table = "$table1"""")))
    val checksum1 = df1.selectExpr("sum(hash(id, region, amount, payload))").collect().head.get(0)

    val table2 = JdbcBenchmarkRunner.seed(Rows)
    val df2 = com.hcsc.generic.ingest.jdbc.JdbcSource
      .read(spark, com.typesafe.config.ConfigFactory.parseString(
        com.hcsc.generic.ingest.jdbc.H2TestDatabase.sourceHocon(s"""table = "$table2"""")))
    val checksum2 = df2.selectExpr("sum(hash(id, region, amount, payload))").collect().head.get(0)

    assert(table1 == table2, "same row count -> same table name")
    assert(checksum1 == checksum2, "reseed must produce byte-identical content")
  }
}
