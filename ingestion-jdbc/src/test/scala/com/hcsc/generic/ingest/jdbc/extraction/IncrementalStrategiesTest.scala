package com.hcsc.generic.ingest.jdbc.extraction

import com.hcsc.generic.ingest.jdbc.extraction.strategies._
import org.scalatest.funsuite.AnyFunSuite

class IncrementalStrategiesTest extends AnyFunSuite {
  import ExtractionTestFixtures._

  private def timestampStrategy(probe: StubProbe) =
    new TimestampStrategy(probe, new PartitionSplitter(probe))
  private def timestampKeyStrategy(probe: StubProbe) =
    new TimestampKeyStrategy(probe, new PartitionSplitter(probe))
  private def increasingKeyStrategy(probe: StubProbe) =
    new IncreasingKeyStrategy(probe, new PartitionSplitter(probe))

  test("TIMESTAMP golden plan: overlap-widened lower, captured upper, captured-upper proposal") {
    val probe = new StubProbe(maxAnswer = Some(BoundaryValue(Seq("2026-07-28 10:00:00.0"))))
    val s = spec(StrategyKinds.Timestamp, Seq(tsColumn), overlap = Some(BigDecimal(300)))
    val ctx = context(s, previous = Some(checkpoint("2026-07-28 09:00:00")))
    val strategy = timestampStrategy(probe)

    assert(strategy.validate(ctx).isEmpty)
    val plan = strategy.plan(ctx)

    assert(plan.boundary == ExtractionBoundary.HalfOpen(
      BoundaryValue(Seq("2026-07-28 08:55:00.0")),           // 09:00 minus 300s overlap
      Some(BoundaryValue(Seq("2026-07-28 10:00:00.0")))))
    assert(plan.checkpointProposal ==
      CheckpointProposal.CommitCapturedUpper(Seq(tsColumn), BoundaryValue(Seq("2026-07-28 10:00:00.0"))),
      "the CAPTURED upper is proposed, never the extracted max")
    assert(plan.strategyKind == StrategyKinds.Timestamp)
    assert(plan.partitions.isEmpty)
  }

  test("TIMESTAMP first run uses the configured initial boundary") {
    val probe = new StubProbe(maxAnswer = Some(BoundaryValue(Seq("2026-07-28 10:00:00.0"))))
    val s = spec(StrategyKinds.Timestamp, Seq(tsColumn), initial = Some("1900-01-01 00:00:00"))
    val plan = timestampStrategy(probe).plan(context(s))
    plan.boundary match {
      case ExtractionBoundary.HalfOpen(lower, _) => assert(lower.values.head.startsWith("1900-01-01"))
      case other => fail(s"unexpected boundary $other")
    }
  }

  test("TIMESTAMP empty source: upper-less window, no checkpoint proposal") {
    val probe = new StubProbe(maxAnswer = None)
    val s = spec(StrategyKinds.Timestamp, Seq(tsColumn), initial = Some("1900-01-01 00:00:00"))
    val plan = timestampStrategy(probe).plan(context(s))
    assert(plan.boundary == ExtractionBoundary.HalfOpen(BoundaryValue(Seq("1900-01-01 00:00:00")), None))
    assert(plan.checkpointProposal == CheckpointProposal.NoCheckpoint,
      "an empty read must not move the position")
  }

  test("TIMESTAMP determinism: equal contexts and probe answers produce equal plans") {
    def freshPlan() = {
      val probe = new StubProbe(maxAnswer = Some(BoundaryValue(Seq("2026-07-28 10:00:00.0"))))
      val s = spec(StrategyKinds.Timestamp, Seq(tsColumn), initial = Some("1900-01-01 00:00:00"))
      timestampStrategy(probe).plan(context(s))
    }
    assert(freshPlan() == freshPlan())
  }

  test("TIMESTAMP validation rejects missing boundary column, missing initial, wrong type, composites") {
    val probe = new StubProbe()
    val strategy = timestampStrategy(probe)
    assert(strategy.validate(context(spec(StrategyKinds.Timestamp)))
      .exists(_.contains("boundary.columns")))
    assert(strategy.validate(context(spec(StrategyKinds.Timestamp, Seq(tsColumn))))
      .exists(_.contains("initial")))
    assert(strategy.validate(context(spec(StrategyKinds.Timestamp, Seq(keyColumn), initial = Some("1"))))
      .exists(_.contains("TIMESTAMP")))
    assert(strategy.validate(context(
      spec(StrategyKinds.Timestamp, Seq(tsColumn, keyColumn), initial = Some("x|1"))))
      .exists(_.contains("exactly one")))
  }

  test("TIMESTAMP_KEY golden plan: composite lexicographic window") {
    val probe = new StubProbe(maxAnswer = Some(BoundaryValue(Seq("2026-07-28 10:00:00.0", "42"))))
    val s = spec(StrategyKinds.TimestampKey, Seq(tsColumn, keyColumn),
      initial = Some("1900-01-01 00:00:00|0"), overlap = Some(BigDecimal(60)))
    val ctx = context(s, previous = Some(checkpoint("2026-07-28 09:00:00|7", kind = "TIMESTAMP_KEY")))
    val strategy = timestampKeyStrategy(probe)

    assert(strategy.validate(ctx).isEmpty)
    val plan = strategy.plan(ctx)
    // Overlap applies to the driving timestamp only; the tie-break key part is untouched
    assert(plan.boundary == ExtractionBoundary.HalfOpen(
      BoundaryValue(Seq("2026-07-28 08:59:00.0", "7")),
      Some(BoundaryValue(Seq("2026-07-28 10:00:00.0", "42")))))
  }

  test("TIMESTAMP_KEY validation: needs 2+ columns, TIMESTAMP first, NUMERIC/STRING tie-breaks") {
    val strategy = timestampKeyStrategy(new StubProbe())
    assert(strategy.validate(context(spec(StrategyKinds.TimestampKey, Seq(tsColumn), initial = Some("x"))))
      .exists(_.contains("at least one key column")))
    assert(strategy.validate(context(
      spec(StrategyKinds.TimestampKey, Seq(keyColumn, tsColumn), initial = Some("1|x"))))
      .exists(_.contains("first boundary column to be TIMESTAMP")))
  }

  test("INCREASING_KEY golden plan: numeric window without overlap") {
    val probe = new StubProbe(maxAnswer = Some(BoundaryValue(Seq("500"))))
    val s = spec(StrategyKinds.IncreasingKey, Seq(keyColumn), initial = Some("0"))
    val ctx = context(s, previous = Some(checkpoint("120", kind = "INCREASING_KEY")))
    val plan = increasingKeyStrategy(probe).plan(ctx)
    assert(plan.boundary == ExtractionBoundary.HalfOpen(
      BoundaryValue(Seq("120")), Some(BoundaryValue(Seq("500")))))
    assert(plan.checkpointProposal ==
      CheckpointProposal.CommitCapturedUpper(Seq(keyColumn), BoundaryValue(Seq("500"))))
  }

  test("INCREASING_KEY validation requires one NUMERIC column") {
    val strategy = increasingKeyStrategy(new StubProbe())
    assert(strategy.validate(context(spec(StrategyKinds.IncreasingKey, Seq(tsColumn), initial = Some("x"))))
      .exists(_.contains("NUMERIC")))
  }

  test("probes are scoped by the extraction's own filters") {
    val probe = new StubProbe(maxAnswer = Some(BoundaryValue(Seq("500"))))
    val s = spec(StrategyKinds.IncreasingKey, Seq(keyColumn), initial = Some("0"),
      filters = Seq("state = 'IL'"))
    increasingKeyStrategy(probe).plan(context(s))
    assert(probe.maxCalls.head._2 == Seq("state = 'IL'"))
  }
}
