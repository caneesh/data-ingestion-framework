package com.hcsc.generic.ingest.jdbc.extraction.strategies

import com.hcsc.generic.ingest.jdbc.extraction._

/** Registry keys for the built-in strategies. */
object StrategyKinds {
  val FullSnapshot = "FULL_SNAPSHOT"
  val Timestamp = "TIMESTAMP"
  val TimestampKey = "TIMESTAMP_KEY"
  val IncreasingKey = "INCREASING_KEY"
  val PartitionRange = "PARTITION_RANGE"
}

/**
  * The bounded-window planning shared by every incremental strategy —
  * composed in, not inherited:
  *
  *   lower = (previous checkpoint | configured initial) with overlap applied
  *   upper = the source's current max, captured on the driver NOW
  *   plan  = HalfOpen(lower, upper), proposing the CAPTURED upper as the
  *           next checkpoint (never the extracted max), so the committed
  *           position equals the reproducible window that was read.
  *
  * An empty source (no upper) plans an upper-less window and proposes no
  * checkpoint: nothing was read, so the position must not move.
  */
private[strategies] final class BoundedWindowPlanning(
  probe: SourceBoundaryProbe,
  splitter: PartitionSplitter
) {

  def validateBoundary(context: ExtractionContext, expected: String): Seq[String] = {
    val spec = context.spec
    val errors = Seq.newBuilder[String]
    if (spec.boundaryColumns.isEmpty)
      errors += s"extraction.boundary.columns is required for $expected"
    if (context.previousCheckpoint.isEmpty && spec.initialBoundary.isEmpty)
      errors += s"extraction.boundary.initial is required for the first run of $expected"
    spec.initialBoundary.foreach { v =>
      if (spec.boundaryColumns.nonEmpty && v.values.size != spec.boundaryColumns.size)
        errors += s"extraction.boundary.initial arity ${v.values.size} does not match ${spec.boundaryColumns.size} boundary column(s)"
    }
    errors.result()
  }

  def plan(context: ExtractionContext, kind: String): ExtractionPlan = {
    val spec = context.spec
    val columns = spec.boundaryColumns
    val lowerBase = context.previousCheckpoint.map(_.value)
      .orElse(spec.initialBoundary)
      .getOrElse(throw new ExtractionPlanningException(
        s"EXT_002 no previous checkpoint and no initial boundary for entity '${context.entity}'"))
    val lower = OverlapPolicy.applyTo(lowerBase, columns, spec.overlap)
    val upper = probe.max(columns, context.relation, spec.filters)

    ExtractionPlan(
      entity = context.entity,
      strategyKind = kind,
      relation = context.relation,
      projection = spec.projection,
      staticFilters = spec.filters,
      boundaryColumns = columns,
      boundary = ExtractionBoundary.HalfOpen(lower, upper),
      partitions = splitter.split(spec.partition, context.relation, spec.filters),
      checkpointProposal = upper
        .map(u => CheckpointProposal.CommitCapturedUpper(columns, u): CheckpointProposal)
        .getOrElse(CheckpointProposal.NoCheckpoint),
      fetchSize = spec.fetchSize
    )
  }
}
