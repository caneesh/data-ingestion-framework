package com.hcsc.generic.ingest.jdbc.extraction

/**
  * The planning port. Contract laws every implementation must honor (the
  * substitutability definition; asserted by the shared strategy tests):
  *
  *   1. plan() is deterministic given the context and the probe's answers.
  *   2. plan() reads no data and writes no state; the only I/O allowed is
  *      through the injected SourceBoundaryProbe.
  *   3. The emitted partitions are pairwise disjoint and cover the emitted
  *      boundary completely (including NULL partition-column rows).
  *   4. The checkpoint proposal is derivable without re-reading the source.
  */
trait ExtractionStrategy {
  /** Registry key, e.g. TIMESTAMP or FULL_SNAPSHOT. */
  def kind: String

  /** Fail-fast configuration checks; empty means plannable. Pure. */
  def validate(context: ExtractionContext): Seq[String]

  /** Produces the complete extraction plan. Called only after validate
    * returned no errors. */
  def plan(context: ExtractionContext): ExtractionPlan
}

/** Driver-side lookups against the source system, scoped by the SAME
  * relation and filters as the extraction itself so captured bounds always
  * describe rows the read can actually see. */
trait SourceBoundaryProbe {
  /** Highest boundary value present (lexicographic for composites), or None
    * when the filtered relation is empty. */
  def max(columns: Seq[BoundaryColumn], relation: Relation, filters: Seq[String]): Option[BoundaryValue]

  /** MIN/MAX of a numeric partition column, or None when empty. */
  def minMax(column: String, relation: Relation, filters: Seq[String]): Option[(Long, Long)]
}

/** Raised when a plan cannot be produced from a validated context. */
final class ExtractionPlanningException(message: String) extends RuntimeException(message)
