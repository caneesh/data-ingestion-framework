package com.hcsc.generic.ingest.jdbc.bench

import com.hcsc.generic.ingest.jdbc.{H2TestDatabase, JdbcSource, SharedSparkSession}
import com.typesafe.config.ConfigFactory
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import java.sql.DriverManager

/**
 * JDBC extraction performance benchmark harness (test scope).
 *
 * Purpose: give operators and developers a repeatable way to compare the
 * relative effect of JDBC read *options* — `fetchsize` and the partitioning
 * strategy (unpartitioned vs. range read vs. predicate read) — on extraction
 * wall-clock. It seeds an H2 in-memory table with a configurable row count,
 * warms up the JVM/Spark once, then times a full extraction (`df.count()`) for
 * each scenario in a fixed config matrix and prints a formatted results table.
 *
 * ==IMPORTANT CAVEAT — read before interpreting any number==
 * H2 is an *in-memory, single-process* database with no network round-trip,
 * no real driver-side buffering pressure, and no server-side throttling. The
 * durations produced here are '''relative indicators for option tuning''',
 * not absolute throughput predictions for a production SQL Server / Oracle /
 * DB2 source. Use them to reason about the *shape* of the tradeoff (e.g. "does
 * bumping fetchsize help here", "does 4-way partitioning add coordination
 * overhead that isn't repaid at this row count") — never to size a real job.
 * Absolute throughput must be measured against the real target database over
 * the real network with a representative row width.
 *
 * ==Usage==
 * {{{
 *   // Ad-hoc run with defaults (50,000 rows, full matrix):
 *   mvn -pl ingestion-jdbc test-compile
 *   mvn -pl ingestion-jdbc exec:java \
 *     -Dexec.mainClass=com.hcsc.generic.ingest.jdbc.bench.JdbcBenchmarkRunner \
 *     -Dexec.classpathScope=test
 *
 *   // Or programmatically (see [[JdbcBenchmarkSmokeTest]]):
 *   val results = JdbcBenchmarkRunner.run(spark, rows = 50000, JdbcBenchmarkRunner.defaultScenarios(50000))
 * }}}
 *
 * A single row count may be supplied on the command line: the first argument,
 * if parseable as a positive Int, overrides the default.
 */
object JdbcBenchmarkRunner {

  private val logger = Logger.getLogger(getClass.getName)

  /** Default seed size. Kept modest so an ad-hoc CI-scoped run stays quick;
    * H2-in-memory throughput is high, so this is enough to separate scenarios. */
  val DefaultRows: Int = 50000

  /** Deterministic mixing salt — identical data every run, so scenario-to-scenario
    * comparisons are not polluted by different row content. */
  private val SeedSalt: Long = 20260726L

  /** One benchmarked scenario: a human label plus the extra HOCON options
    * appended to the base H2 source config. */
  final case class Scenario(label: String, extraHocon: String)

  /** One measured result. `durationMs` is wall-clock for the timed
    * `df.count()`; `rows` is the row count actually returned (verifies the
    * scenario read the whole table). */
  final case class BenchmarkResult(label: String, rows: Long, durationMs: Long) {
    def rowsPerSec: Double =
      if (durationMs <= 0) Double.NaN else rows.toDouble / (durationMs.toDouble / 1000.0)
  }

  // Partition bounds match the seeded id range [0, rows).
  private def rangeReadHocon(rows: Int): String =
    s"""
       |numPartitions = 4
       |partitionColumn = "id"
       |lowerBound = 0
       |upperBound = $rows
     """.stripMargin

  private def predicateReadHocon(rows: Int): String = {
    // Four disjoint, exhaustive predicates over the id range -> one partition each.
    val q = rows / 4
    val preds = Seq(
      s"id < $q",
      s"id >= $q AND id < ${2 * q}",
      s"id >= ${2 * q} AND id < ${3 * q}",
      s"id >= ${3 * q}"
    ).map(p => "\"" + p + "\"").mkString(", ")
    s"""
       |mode = "SELECT_QUERY"
       |partition_strategy = "PREDICATES"
       |partition_predicates = [ $preds ]
     """.stripMargin
  }

  /**
   * The config matrix: fetchsize (100 / 1000 / 5000) crossed with three read
   * shapes (unpartitioned, 4-partition range, 4-predicate). Bounds/predicates
   * are computed from `rows` so the matrix scales with the seed size.
   */
  def defaultScenarios(rows: Int): Seq[Scenario] = {
    val fetchSizes = Seq(100, 1000, 5000)
    val shapes: Seq[(String, String)] = Seq(
      "unpartitioned" -> "",
      "range-4part" -> rangeReadHocon(rows),
      "predicate-4part" -> predicateReadHocon(rows)
    )
    for {
      (shapeLabel, shapeHocon) <- shapes
      fs <- fetchSizes
    } yield Scenario(
      label = f"$shapeLabel%-16s fetchsize=$fs%-5d",
      extraHocon = s"""fetchsize = $fs
                      |$shapeHocon""".stripMargin
    )
  }

  /** Unique table name per row count so repeated runs in one JVM don't collide. */
  private def tableName(rows: Int): String = s"bench_orders_$rows"

  /**
   * Deterministically (re)seed the benchmark table with `rows` rows.
   *
   * Schema: `id INT` (dense 0..rows-1, the partition/predicate key),
   * `region VARCHAR`, `amount INT`, `payload VARCHAR` (a fixed-width filler so
   * each row carries some bytes rather than being trivially small).
   *
   * The table is dropped and recreated on every call, so seeding is idempotent.
   *
   * @return the seeded table name.
   */
  def seed(rows: Int): String = {
    require(rows > 0, s"row count must be positive, got $rows")
    val table = tableName(rows)
    Class.forName(H2TestDatabase.Driver)
    val conn = DriverManager.getConnection(H2TestDatabase.Url, "sa", "")
    try {
      val stmt = conn.createStatement()
      try {
        stmt.execute(s"DROP TABLE IF EXISTS $table")
        stmt.execute(
          s"""CREATE TABLE $table (
             |  id INT,
             |  region VARCHAR(8),
             |  amount INT,
             |  payload VARCHAR(64)
             |)""".stripMargin)
      } finally stmt.close()

      // Batched, deterministic insert. No RNG state that varies between runs:
      // every derived value is a pure function of the row index + a fixed salt.
      conn.setAutoCommit(false)
      val ps = conn.prepareStatement(
        s"INSERT INTO $table (id, region, amount, payload) VALUES (?, ?, ?, ?)")
      try {
        val regions = Array("EAST", "WEST", "NORTH", "SOUTH")
        var i = 0
        while (i < rows) {
          val mixed = i.toLong ^ SeedSalt
          ps.setInt(1, i)
          ps.setString(2, regions((mixed & 3L).toInt))
          ps.setInt(3, ((mixed >>> 3) % 100000L).toInt)
          ps.setString(4, f"payload-$i%08d-${(mixed >>> 7) & 0xffffL}%04x")
          ps.addBatch()
          if ((i % 1000) == 999) ps.executeBatch()
          i += 1
        }
        ps.executeBatch()
        conn.commit()
      } finally {
        ps.close()
        conn.setAutoCommit(true)
      }
    } finally conn.close()
    table
  }

  private def baseHocon(table: String): String =
    H2TestDatabase.sourceHocon(s"""table = "$table"""")

  /** Time a full extraction of one scenario. Returns (rowCount, wallClockMs). */
  private def timeScenario(spark: SparkSession, table: String, scenario: Scenario): BenchmarkResult = {
    val hocon = baseHocon(table) + "\n" + scenario.extraHocon
    val config = ConfigFactory.parseString(hocon)
    val start = System.nanoTime()
    val count = JdbcSource.read(spark, config).count()
    val elapsedMs = (System.nanoTime() - start) / 1000000L
    BenchmarkResult(scenario.label, count, elapsedMs)
  }

  /**
   * Programmatic entry point.
   *
   * Steps: (1) deterministically seed `rows`; (2) run ONE warm-up extraction
   * (untimed) so JIT compilation, classloading and Spark planning are paid for
   * before measurement; (3) time each scenario's full `df.count()`.
   *
   * @return one [[BenchmarkResult]] per scenario, in input order.
   */
  def run(spark: SparkSession, rows: Int, scenarios: Seq[Scenario]): Seq[BenchmarkResult] = {
    require(scenarios.nonEmpty, "at least one scenario is required")
    val table = seed(rows)

    // Warm-up: a plain full read, untimed, to amortize one-time startup costs.
    logger.info(s"Benchmark warm-up read of $rows rows from $table ...")
    val warmCount = JdbcSource.read(spark, ConfigFactory.parseString(baseHocon(table))).count()
    require(warmCount == rows, s"warm-up expected $rows rows, got $warmCount")

    scenarios.map { s =>
      val result = timeScenario(spark, table, s)
      logger.info(f"scenario='${s.label}' rows=${result.rows} durationMs=${result.durationMs}")
      result
    }
  }

  /** Render results as a fixed-width table, returned as a String so callers can
    * log it and/or print it. */
  def formatTable(rows: Int, results: Seq[BenchmarkResult]): String = {
    val sb = new StringBuilder
    sb.append(f"%nJDBC extraction benchmark — $rows%,d rows (H2 in-memory)%n")
    sb.append("CAVEAT: relative option-tuning indicators only, NOT absolute throughput.%n".format())
    sb.append(("-" * 78) + "%n".format())
    sb.append(f"${"scenario"}%-40s ${"rows"}%10s ${"duration_ms"}%13s ${"rows/sec"}%12s%n")
    sb.append(("-" * 78) + "%n".format())
    results.foreach { r =>
      sb.append(f"${r.label}%-40s ${r.rows}%10d ${r.durationMs}%13d ${r.rowsPerSec}%12.0f%n")
    }
    sb.append(("-" * 78) + "%n".format())
    sb.toString
  }

  def main(args: Array[String]): Unit = {
    val rows = args.headOption.flatMap(a => scala.util.Try(a.trim.toInt).toOption)
      .filter(_ > 0).getOrElse(DefaultRows)
    val spark = SharedSparkSession.session
    val results = run(spark, rows, defaultScenarios(rows))
    val table = formatTable(rows, results)
    logger.info(table)
    // Also to stdout so an ad-hoc `exec:java` run shows the table without a
    // configured log appender.
    println(table)
  }
}
