package com.hcsc.generic.ingest.jdbc.extraction

import com.hcsc.generic.ingest.jdbc.extraction.strategies._

/**
  * Composition root (factory). All dependency injection happens here, by
  * constructor: callers supply the two ports (probe + checkpoint store) and
  * receive a fully wired planner. Nothing global, nothing reflective.
  */
object ExtractionFramework {

  /** The five built-in strategies wired against the given probe. */
  def builtInStrategies(probe: SourceBoundaryProbe): Seq[ExtractionStrategy] = {
    val splitter = new PartitionSplitter(probe)
    Seq(
      new FullSnapshotStrategy(splitter),
      new TimestampStrategy(probe, splitter),
      new TimestampKeyStrategy(probe, splitter),
      new IncreasingKeyStrategy(probe, splitter),
      new PartitionRangeStrategy(splitter)
    )
  }

  /** A planner over the built-in strategies. Pass `extra` to register
    * additional strategies (e.g. a future CDC strategy) — open for
    * extension, closed for modification. */
  def planner(
    probe: SourceBoundaryProbe,
    checkpoints: CheckpointStore,
    extra: Seq[ExtractionStrategy] = Seq.empty
  ): ExtractionPlanner =
    new ExtractionPlanner(ExtractionStrategyRegistry(builtInStrategies(probe) ++ extra), checkpoints)

  /** The commit half of the lifecycle, invoked only after the ingestion run
    * fully succeeded. */
  def committer(checkpoints: CheckpointStore): CheckpointCommitter =
    new CheckpointCommitter(checkpoints)
}
