package com.hcsc.generic.ingest.reconcile

import org.apache.log4j.Logger

/**
  * Declarative reconciliation: named measures, balance equations over sums
  * of measures, per-equation tolerance, and a failure policy. Entirely pure —
  * counts in, findings out — so every equation is unit-testable without
  * Spark. Persistence stays with the existing AuditService reconciliation
  * table; this engine produces what gets recorded.
  */
object Measure {
  val SourceCount = "source_count"
  val RawCount = "raw_count"
  val RejectCount = "reject_count"
  val CuratedCount = "curated_count"
  val InsertCount = "insert_count"
  val UpdateCount = "update_count"
  val DeleteCount = "delete_count"
  val DuplicateCount = "duplicate_count"
  val AcceptedCount = "accepted_count"
  val all: Seq[String] = Seq(SourceCount, RawCount, RejectCount, CuratedCount,
    InsertCount, UpdateCount, DeleteCount, DuplicateCount, AcceptedCount)
}

/** Observed values; a measure that was not observed is simply absent, and
  * equations referencing it are skipped (reported as NOT_MEASURED), never
  * failed — an unmeasured count is not a mismatch. */
final case class Measurements(values: Map[String, Long]) {
  def get(measure: String): Option[Long] = values.get(measure)
}

sealed trait Tolerance {
  def withinTolerance(expected: Long, actual: Long): Boolean
}
object Tolerance {
  // All arithmetic through BigInt/BigDecimal: Long subtraction overflow
  // would otherwise make a maximal mismatch pass as within-tolerance — the
  // worst possible failure direction for a reconciliation engine.
  case object Exact extends Tolerance {
    def withinTolerance(expected: Long, actual: Long): Boolean = expected == actual
  }
  final case class Absolute(maxDelta: Long) extends Tolerance {
    require(maxDelta >= 0, "REC_001 tolerance delta must not be negative")
    def withinTolerance(expected: Long, actual: Long): Boolean =
      (BigInt(expected) - BigInt(actual)).abs <= BigInt(maxDelta)
  }
  /** Note: an expected value of exactly 0 tolerates only an actual of 0 —
    * a percentage of nothing is undefined, so any deviation fails. */
  final case class Percent(maxPercent: Double) extends Tolerance {
    require(maxPercent >= 0, "REC_001 tolerance percent must not be negative")
    def withinTolerance(expected: Long, actual: Long): Boolean = {
      if (expected == 0L) actual == 0L
      else (BigDecimal(BigInt(expected) - BigInt(actual)).abs * 100 /
        BigDecimal(expected).abs) <= BigDecimal(maxPercent)
    }
  }
}

/** sum(left) must equal sum(right) within tolerance. */
final case class BalanceEquation(
  name: String,
  left: Seq[String],
  right: Seq[String],
  tolerance: Tolerance = Tolerance.Exact
) {
  require(left.nonEmpty && right.nonEmpty, s"REC_001 equation '$name' needs measures on both sides")
}

object BalanceEquation {
  /** The framework's standard invariants; feeds add their own on top.
    * Deliberately absent: a curated-movement equation — for merge
    * strategies the published count is the WHOLE merged table while
    * insert+update counts only the incoming keys, so no equality holds;
    * that coverage is an inequality the pipeline checks separately. */
  val standard: Seq[BalanceEquation] = Seq(
    BalanceEquation("source_equals_accepted_plus_rejected",
      left = Seq(Measure.SourceCount), right = Seq(Measure.AcceptedCount, Measure.RejectCount)),
    BalanceEquation("raw_equals_accepted",
      left = Seq(Measure.RawCount), right = Seq(Measure.AcceptedCount))
  )
}

final case class ReconciliationFinding(
  equation: String,
  expected: Long,
  actual: Long,
  delta: Long,
  status: String // PASSED | FAILED | NOT_MEASURED
)

object ReconciliationStatus {
  val Passed = "PASSED"
  val Failed = "FAILED"
  val NotMeasured = "NOT_MEASURED"
}

sealed trait FailurePolicy
object FailurePolicy {
  case object Warn extends FailurePolicy
  case object Fail extends FailurePolicy
  /** Fail only when one of the named equations mismatches; others warn. */
  final case class FailOn(equations: Set[String]) extends FailurePolicy
}

final class ReconciliationException(message: String) extends RuntimeException(message)

object ReconciliationEngine {

  /** Evaluates every equation against the measurements. Pure. */
  def evaluate(equations: Seq[BalanceEquation], measurements: Measurements): Seq[ReconciliationFinding] =
    equations.map { eq =>
      val leftValues = eq.left.map(measurements.get)
      val rightValues = eq.right.map(measurements.get)
      if (leftValues.exists(_.isEmpty) || rightValues.exists(_.isEmpty))
        ReconciliationFinding(eq.name, -1L, -1L, 0L, ReconciliationStatus.NotMeasured)
      else {
        val expected = leftValues.flatten.sum
        val actual = rightValues.flatten.sum
        val ok = eq.tolerance.withinTolerance(expected, actual)
        ReconciliationFinding(eq.name, expected, actual, actual - expected,
          if (ok) ReconciliationStatus.Passed else ReconciliationStatus.Failed)
      }
    }

  /** Applies the policy: failures log; the policy decides which of them
    * abort the run. NOT_MEASURED never fails. */
  def enforce(findings: Seq[ReconciliationFinding], policy: FailurePolicy, logger: Logger): Unit = {
    val failed = findings.filter(_.status == ReconciliationStatus.Failed)
    if (failed.isEmpty) return
    failed.foreach(f => logger.warn(
      s"[Reconciliation] ${f.equation}: expected=${f.expected} actual=${f.actual} delta=${f.delta}"))

    val fatal = policy match {
      case FailurePolicy.Warn => Seq.empty
      case FailurePolicy.Fail => failed
      case FailurePolicy.FailOn(names) =>
        // A typo'd equation name would silently disable enforcement forever;
        // surface names that match nothing evaluated this run.
        val evaluated = findings.map(_.equation).toSet
        val unmatched = names.diff(evaluated)
        if (unmatched.nonEmpty)
          logger.warn(s"[Reconciliation] FailOn names match no evaluated equation " +
            s"(dead policy entries): ${unmatched.mkString(", ")}")
        failed.filter(f => names.contains(f.equation))
    }
    if (fatal.nonEmpty)
      throw new ReconciliationException(
        "REC_002 reconciliation failed: " +
          fatal.map(f => s"${f.equation} (expected=${f.expected}, actual=${f.actual})").mkString("; "))
  }
}
