package com.hcsc.generic.ingest.jdbc.extraction

import com.hcsc.generic.ingest.jdbc.dialect.SqlServerDialect
import com.hcsc.generic.ingest.jdbc.extraction.ExtractionPartition.RangeSlice
import com.hcsc.generic.ingest.jdbc.extraction.render.SqlPlanRenderer
import com.hcsc.generic.ingest.jdbc.extraction.strategies.StrategyKinds
import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite

class ParserAndRendererTest extends AnyFunSuite {
  import ExtractionTestFixtures._

  // ---------------------------------------------------------------------------
  // Configuration parsing
  // ---------------------------------------------------------------------------

  test("parser returns None without an extraction block") {
    assert(ExtractionSpecParser.parse(ConfigFactory.parseString("""type = "jdbc"""")).isEmpty)
  }

  test("parser maps a full block to a typed spec") {
    val parsed = ExtractionSpecParser.parse(ConfigFactory.parseString(
      """extraction {
        |  strategy = "timestamp_key"
        |  table = "dbo.claims"
        |  columns = ["claim_id", "amount", "modified_ts"]
        |  filters = ["state = 'IL'"]
        |  boundary {
        |    columns = [
        |      { name = "modified_ts", type = "TIMESTAMP" },
        |      { name = "claim_id", type = "NUMERIC" }
        |    ]
        |    initial = "1900-01-01 00:00:00|0"
        |    overlap = 300
        |  }
        |  partition { column = "claim_id", num_partitions = 8 }
        |  fetchsize = 5000
        |}""".stripMargin)).get

    assert(parsed.strategyKind == StrategyKinds.TimestampKey)
    assert(parsed.table == "dbo.claims")
    assert(parsed.boundaryColumns ==
      Seq(BoundaryColumn("modified_ts", BoundaryType.Timestamp), BoundaryColumn("claim_id", BoundaryType.Numeric)))
    assert(parsed.initialBoundary.contains(BoundaryValue(Seq("1900-01-01 00:00:00", "0"))))
    assert(parsed.overlap.contains(BigDecimal(300)))
    assert(parsed.partition.column.contains("claim_id") && parsed.partition.numPartitions.contains(8))
    assert(parsed.fetchSize == 5000)
  }

  test("parser defaults: TIMESTAMP column type, fetchsize 1000, empty partition spec") {
    val parsed = ExtractionSpecParser.parse(ConfigFactory.parseString(
      """extraction {
        |  strategy = "TIMESTAMP"
        |  table = "t"
        |  boundary { columns = [ { name = "ts" } ] }
        |}""".stripMargin)).get
    assert(parsed.boundaryColumns == Seq(BoundaryColumn("ts", BoundaryType.Timestamp)))
    assert(parsed.fetchSize == 1000)
    assert(parsed.partition.isEmpty)
  }

  test("parser rejects missing strategy/table, bad types, arity mismatch, half bounds, bad counts") {
    def failMessage(hocon: String): String =
      intercept[IllegalArgumentException](ExtractionSpecParser.parse(ConfigFactory.parseString(hocon))).getMessage

    assert(failMessage("""extraction { table = "t" }""").contains("strategy is required"))
    assert(failMessage("""extraction { strategy = "TIMESTAMP" }""").contains("table is required"))
    assert(failMessage(
      """extraction { strategy = "T", table = "t",
        |  boundary { columns = [ { name = "c", type = "GUID" } ] } }""".stripMargin)
      .contains("must be one of"))
    assert(failMessage(
      """extraction { strategy = "T", table = "t",
        |  boundary { columns = [ { name = "c" } ], initial = "a|b" } }""".stripMargin)
      .contains("part(s)"))
    assert(failMessage(
      """extraction { strategy = "T", table = "t", partition { lower_bound = 1 } }""")
      .contains("together"))
    assert(failMessage(
      """extraction { strategy = "T", table = "t", partition { num_partitions = 0 } }""")
      .contains("positive"))
    assert(failMessage(
      """extraction { strategy = "T", table = "t", boundary { overlap = -1 } }""")
      .contains("negative"))
  }

  // ---------------------------------------------------------------------------
  // SQL rendering
  // ---------------------------------------------------------------------------

  private val renderer = new SqlPlanRenderer(SqlServerDialect)

  private def plainPlan(
    boundary: ExtractionBoundary,
    columns: Seq[BoundaryColumn] = Seq.empty,
    projection: Seq[String] = Seq.empty,
    filters: Seq[String] = Seq.empty,
    partitions: Seq[ExtractionPartition] = Seq.empty
  ) = ExtractionPlan("e", "K", Relation.Table("dbo.claims"), projection, filters,
    columns, boundary, partitions, CheckpointProposal.NoCheckpoint, 1000)

  test("unbounded plan without projection or filters renders the bare table") {
    assert(renderer.render(plainPlan(ExtractionBoundary.Unbounded)).dbtable == "dbo.claims")
  }

  test("bounded single-column window renders quoted, typed, half-open SQL") {
    val plan = plainPlan(
      ExtractionBoundary.HalfOpen(
        BoundaryValue(Seq("2026-07-28 08:55:00")), Some(BoundaryValue(Seq("2026-07-28 10:00:00")))),
      columns = Seq(tsColumn))
    assert(renderer.render(plan).dbtable ==
      "(SELECT * FROM dbo.claims WHERE (([modified_ts] > '2026-07-28 08:55:00.0') " +
        "AND NOT ([modified_ts] > '2026-07-28 10:00:00.0'))) src")
  }

  test("composite window renders the lexicographic OR-expansion") {
    val plan = plainPlan(
      ExtractionBoundary.HalfOpen(BoundaryValue(Seq("2026-07-28 09:00:00", "7")), None),
      columns = Seq(tsColumn, keyColumn))
    assert(renderer.render(plan).dbtable ==
      "(SELECT * FROM dbo.claims WHERE ((([modified_ts] > '2026-07-28 09:00:00.0') OR " +
        "([modified_ts] = '2026-07-28 09:00:00.0' AND [claim_id] > 7)))) src")
  }

  test("boundary literals reject SQL-shaped garbage instead of rendering it") {
    val plan = plainPlan(
      ExtractionBoundary.HalfOpen(BoundaryValue(Seq("1' OR '1'='1")), None),
      columns = Seq(tsColumn))
    val ex = intercept[IllegalArgumentException](renderer.render(plan))
    assert(ex.getMessage.contains("JDBC_003"))
  }

  test("projection and filters render quoted and parenthesized") {
    val rendered = renderer.render(plainPlan(ExtractionBoundary.Unbounded,
      projection = Seq("claim_id", "amount"), filters = Seq("state = 'IL'")))
    assert(rendered.dbtable == "(SELECT [claim_id], [amount] FROM dbo.claims WHERE (state = 'IL')) src")
  }

  test("range slices render disjoint predicates with NULLs in the first slice only") {
    val partitions = Seq(
      RangeSlice("claim_id", 1L, 5L, upperInclusive = false, includeNulls = true, index = 0),
      RangeSlice("claim_id", 5L, 10L, upperInclusive = true, includeNulls = false, index = 1))
    val rendered = renderer.render(plainPlan(ExtractionBoundary.Unbounded, partitions = partitions))
    assert(rendered.partitionPredicates == Seq(
      "([claim_id] >= 1 AND [claim_id] < 5) OR [claim_id] IS NULL",
      "[claim_id] >= 5 AND [claim_id] <= 10"))
  }
}
