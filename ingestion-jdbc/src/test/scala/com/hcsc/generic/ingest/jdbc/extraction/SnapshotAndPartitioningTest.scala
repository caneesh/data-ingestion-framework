package com.hcsc.generic.ingest.jdbc.extraction

import com.hcsc.generic.ingest.jdbc.extraction.ExtractionPartition.RangeSlice
import com.hcsc.generic.ingest.jdbc.extraction.strategies._
import org.scalatest.funsuite.AnyFunSuite

class SnapshotAndPartitioningTest extends AnyFunSuite {
  import ExtractionTestFixtures._

  test("FULL_SNAPSHOT: unbounded, no checkpoint, no boundary columns") {
    val probe = new StubProbe()
    val plan = new FullSnapshotStrategy(new PartitionSplitter(probe))
      .plan(context(spec(StrategyKinds.FullSnapshot)))
    assert(plan.boundary == ExtractionBoundary.Unbounded)
    assert(plan.checkpointProposal == CheckpointProposal.NoCheckpoint)
    assert(plan.partitions.isEmpty)
  }

  test("PARTITION_RANGE requires partitioning configuration") {
    val probe = new StubProbe()
    val strategy = new PartitionRangeStrategy(new PartitionSplitter(probe))
    assert(strategy.validate(context(spec(StrategyKinds.PartitionRange)))
      .exists(_.contains("requires extraction.partition")))
    assert(strategy.validate(context(spec(StrategyKinds.PartitionRange,
      partition = PartitionSpec(Some("claim_id"), Some(1L), Some(100L), Some(4), Seq.empty)))).isEmpty)
  }

  test("splitter law: slices are disjoint, contiguous, unbounded at the ends, NULLs exactly once") {
    val splitter = new PartitionSplitter(new StubProbe())
    val slices = splitter.split(
      PartitionSpec(Some("claim_id"), Some(1L), Some(10L), Some(3), Seq.empty),
      Relation.Table("t"), Seq.empty).collect { case r: RangeSlice => r }

    assert(slices.size == 3)
    // Contiguous: each slice starts exactly where the previous ended
    slices.sliding(2).foreach { case Seq(a, b) => assert(a.upperExclusive == b.lowerInclusive) }
    // Ends are UNBOUNDED — bounds size the strides, they never filter, so
    // rows outside [lo, hi] (stale bounds, post-probe arrivals) are still read
    assert(slices.head.lowerInclusive.isEmpty)
    assert(slices.last.upperExclusive.isEmpty)
    assert(slices.init.forall(_.upperExclusive.isDefined))
    // NULLs live in exactly one slice
    assert(slices.count(_.includeNulls) == 1)
    assert(slices.head.includeNulls)
  }

  test("splitter probes MIN/MAX when bounds are absent") {
    val probe = new StubProbe(minMaxAnswer = Some((100L, 200L)))
    val splitter = new PartitionSplitter(probe)
    val slices = splitter.split(
      PartitionSpec(Some("claim_id"), None, None, Some(2), Seq.empty),
      Relation.Table("t"), Seq.empty).collect { case r: RangeSlice => r }
    assert(probe.minMaxCalls == List("claim_id"))
    // Interior cut derived from the probed range; ends stay unbounded
    assert(slices.size == 2)
    assert(slices.head.lowerInclusive.isEmpty && slices.last.upperExclusive.isEmpty)
    assert(slices.head.upperExclusive.contains(151L)) // 100 + ceil(101/2)
  }

  test("splitter stride math is safe at Long extremes") {
    val splitter = new PartitionSplitter(new StubProbe())
    val slices = splitter.split(
      PartitionSpec(Some("claim_id"), Some(Long.MaxValue - 9), Some(Long.MaxValue), Some(2), Seq.empty),
      Relation.Table("t"), Seq.empty).collect { case r: RangeSlice => r }
    assert(slices.size == 2)
    val cut = slices.head.upperExclusive.get
    assert(cut > Long.MaxValue - 9 && cut <= Long.MaxValue, "no overflowed negative cut point")
  }

  test("splitter: empty source (no MIN/MAX) degrades to a single full read") {
    val splitter = new PartitionSplitter(new StubProbe(minMaxAnswer = None))
    assert(splitter.split(
      PartitionSpec(Some("claim_id"), None, None, Some(4), Seq.empty),
      Relation.Table("t"), Seq.empty).isEmpty)
  }

  test("splitter: more partitions than values collapses to the value count") {
    val splitter = new PartitionSplitter(new StubProbe())
    val slices = splitter.split(
      PartitionSpec(Some("claim_id"), Some(1L), Some(3L), Some(10), Seq.empty),
      Relation.Table("t"), Seq.empty)
    assert(slices.size == 3)
  }

  test("splitter: half-configured bounds are rejected") {
    val splitter = new PartitionSplitter(new StubProbe())
    val ex = intercept[IllegalArgumentException] {
      splitter.split(PartitionSpec(Some("claim_id"), Some(1L), None, Some(2), Seq.empty),
        Relation.Table("t"), Seq.empty)
    }
    assert(ex.getMessage.contains("EXT_001"))
  }

  test("explicit predicates pass through as predicate slices") {
    val splitter = new PartitionSplitter(new StubProbe())
    val slices = splitter.split(
      PartitionSpec(None, None, None, None, Seq("region = 'E'", "region = 'W'")),
      Relation.Table("t"), Seq.empty)
    assert(slices.map(_.index) == Seq(0, 1))
  }
}
