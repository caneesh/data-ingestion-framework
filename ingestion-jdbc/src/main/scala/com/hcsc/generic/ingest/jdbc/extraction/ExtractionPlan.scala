package com.hcsc.generic.ingest.jdbc.extraction

/** What the plan reads from: a validated table name. (Custom SQL relations
  * join this ADT when the CustomSql strategy lands; nothing else changes.) */
sealed trait Relation
object Relation {
  final case class Table(name: String) extends Relation {
    require(name.matches("[A-Za-z0-9_.$#]+"),
      s"EXT_001 table name '$name' contains characters outside the safe identifier pattern")
  }
}

/** One parallel read unit. A plan with no partitions is a single full read.
  * Law (enforced by PartitionSplitter, asserted in tests): the slices of one
  * plan are pairwise disjoint and together cover the whole boundary,
  * including NULL partition-column rows. */
sealed trait ExtractionPartition { def index: Int }
object ExtractionPartition {
  /** [lowerInclusive, upperBound) — or inclusive upper on the final slice.
    * `includeNulls` puts NULL partition-column rows into this slice so they
    * are never silently dropped. */
  final case class RangeSlice(
    column: String,
    lowerInclusive: Long,
    upperBound: Long,
    upperInclusive: Boolean,
    includeNulls: Boolean,
    index: Int
  ) extends ExtractionPartition

  /** Operator-authored predicate (same trust level as reject rules). */
  final case class PredicateSlice(predicate: String, index: Int) extends ExtractionPartition
}

/**
  * The complete, immutable output of planning: everything the executor needs
  * and everything --validate-only prints. Strategies produce this and stop —
  * no strategy reads data or commits state.
  */
final case class ExtractionPlan(
  entity: String,
  strategyKind: String,
  relation: Relation,
  projection: Seq[String],
  staticFilters: Seq[String],
  boundaryColumns: Seq[BoundaryColumn],
  boundary: ExtractionBoundary,
  partitions: Seq[ExtractionPartition],
  checkpointProposal: CheckpointProposal,
  fetchSize: Int
)
