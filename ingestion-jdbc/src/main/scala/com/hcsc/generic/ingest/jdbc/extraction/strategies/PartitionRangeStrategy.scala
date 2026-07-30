package com.hcsc.generic.ingest.jdbc.extraction.strategies

import com.hcsc.generic.ingest.jdbc.extraction._

/** Partition Range Extraction: the partition column IS the extraction
  * driver (backfills, one-off parallel exports). Unbounded window, required
  * partitioning, no checkpoint. */
final class PartitionRangeStrategy(splitter: PartitionSplitter) extends ExtractionStrategy {

  val kind: String = StrategyKinds.PartitionRange

  def validate(context: ExtractionContext): Seq[String] = {
    val p = context.spec.partition
    val hasRange = p.column.isDefined && p.numPartitions.isDefined
    if (hasRange || p.predicates.nonEmpty) Seq.empty
    else Seq(s"$kind requires extraction.partition (column + num_partitions, or explicit predicates)")
  }

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
