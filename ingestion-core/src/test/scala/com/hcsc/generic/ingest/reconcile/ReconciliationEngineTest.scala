package com.hcsc.generic.ingest.reconcile

import org.apache.log4j.Logger
import org.scalatest.funsuite.AnyFunSuite

class ReconciliationEngineTest extends AnyFunSuite {

  private val logger = Logger.getLogger(getClass.getName)

  private def measurements(pairs: (String, Long)*) = Measurements(pairs.toMap)

  test("standard equations pass on balanced counts") {
    val findings = ReconciliationEngine.evaluate(BalanceEquation.standard, measurements(
      Measure.SourceCount -> 100L, Measure.AcceptedCount -> 97L, Measure.RejectCount -> 3L,
      Measure.RawCount -> 97L, Measure.CuratedCount -> 97L,
      Measure.InsertCount -> 90L, Measure.UpdateCount -> 7L))
    assert(findings.forall(_.status == ReconciliationStatus.Passed))
  }

  test("a mismatch fails with the exact delta") {
    val findings = ReconciliationEngine.evaluate(
      Seq(BalanceEquation("source_balance", Seq(Measure.SourceCount), Seq(Measure.AcceptedCount, Measure.RejectCount))),
      measurements(Measure.SourceCount -> 100L, Measure.AcceptedCount -> 90L, Measure.RejectCount -> 3L))
    assert(findings.head.status == ReconciliationStatus.Failed)
    assert(findings.head.delta == -7L)
  }

  test("unmeasured inputs are reported NOT_MEASURED, never failed") {
    val findings = ReconciliationEngine.evaluate(BalanceEquation.standard,
      measurements(Measure.SourceCount -> 100L)) // everything else absent
    assert(findings.forall(_.status == ReconciliationStatus.NotMeasured))
  }

  test("absolute and percent tolerances bound the acceptable delta") {
    val abs = BalanceEquation("abs", Seq(Measure.SourceCount), Seq(Measure.RawCount), Tolerance.Absolute(5))
    val pct = BalanceEquation("pct", Seq(Measure.SourceCount), Seq(Measure.RawCount), Tolerance.Percent(1.0))

    def statusOf(eq: BalanceEquation, source: Long, raw: Long): String =
      ReconciliationEngine.evaluate(Seq(eq),
        measurements(Measure.SourceCount -> source, Measure.RawCount -> raw)).head.status

    assert(statusOf(abs, 100, 95) == ReconciliationStatus.Passed)
    assert(statusOf(abs, 100, 94) == ReconciliationStatus.Failed)
    assert(statusOf(pct, 1000, 990) == ReconciliationStatus.Passed)
    assert(statusOf(pct, 1000, 989) == ReconciliationStatus.Failed)
    // Percent tolerance on an expected of zero passes only an actual of zero
    assert(statusOf(pct, 0, 0) == ReconciliationStatus.Passed)
    assert(statusOf(pct, 0, 1) == ReconciliationStatus.Failed)
  }

  test("multi-measure sides sum before comparison") {
    val eq = BalanceEquation("movement",
      Seq(Measure.InsertCount, Measure.UpdateCount, Measure.DeleteCount), Seq(Measure.CuratedCount))
    val findings = ReconciliationEngine.evaluate(Seq(eq), measurements(
      Measure.InsertCount -> 10L, Measure.UpdateCount -> 5L, Measure.DeleteCount -> 2L,
      Measure.CuratedCount -> 17L))
    assert(findings.head.status == ReconciliationStatus.Passed)
  }

  test("failure policies: Warn never throws, Fail throws, FailOn is selective") {
    val failed = Seq(
      ReconciliationFinding("critical", 10, 8, -2, ReconciliationStatus.Failed),
      ReconciliationFinding("advisory", 10, 9, -1, ReconciliationStatus.Failed))

    ReconciliationEngine.enforce(failed, FailurePolicy.Warn, logger) // no throw

    val ex = intercept[ReconciliationException] {
      ReconciliationEngine.enforce(failed, FailurePolicy.Fail, logger)
    }
    assert(ex.getMessage.contains("REC_002") && ex.getMessage.contains("critical"))

    val selective = intercept[ReconciliationException] {
      ReconciliationEngine.enforce(failed, FailurePolicy.FailOn(Set("critical")), logger)
    }
    assert(selective.getMessage.contains("critical") && !selective.getMessage.contains("advisory"))

    ReconciliationEngine.enforce(failed, FailurePolicy.FailOn(Set("other")), logger) // no throw
  }

  test("negative tolerances are rejected at construction") {
    intercept[IllegalArgumentException](Tolerance.Absolute(-1))
    intercept[IllegalArgumentException](Tolerance.Percent(-0.1))
    intercept[IllegalArgumentException](BalanceEquation("empty", Seq.empty, Seq(Measure.RawCount)))
  }
}
