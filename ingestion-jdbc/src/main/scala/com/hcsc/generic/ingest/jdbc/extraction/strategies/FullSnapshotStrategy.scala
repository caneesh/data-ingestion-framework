package com.hcsc.generic.ingest.jdbc.extraction.strategies

import com.hcsc.generic.ingest.jdbc.extraction._

/** Reads the whole (filtered) relation every run. Tracks no position;
  * optional parallel slicing via the shared splitter. */
final class FullSnapshotStrategy(splitter: PartitionSplitter) extends ExtractionStrategy {

  val kind: String = StrategyKinds.FullSnapshot

  def validate(context: ExtractionContext): Seq[String] = Seq.empty

  def plan(context: ExtractionContext): ExtractionPlan = {
    val spec = context.spec
    ExtractionPlan(
      entity = context.entity,
      strategyKind = kind,
      relation = context.relation,
      projection = spec.projection,
      staticFilters = spec.filters,
      boundaryColumns = Seq.empty,
      boundary = ExtractionBoundary.Unbounded,
      partitions = splitter.split(spec.partition, context.relation, spec.filters),
      checkpointProposal = CheckpointProposal.NoCheckpoint,
      fetchSize = spec.fetchSize
    )
  }
}
