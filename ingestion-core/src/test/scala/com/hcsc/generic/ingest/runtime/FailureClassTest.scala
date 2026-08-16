package com.hcsc.generic.ingest.runtime

import com.hcsc.generic.ingest.lock.PipelineLockException
import com.hcsc.generic.ingest.schema.{SchemaContractViolationException, SchemaViolation, ViolationKind}
import org.scalatest.funsuite.AnyFunSuite

/**
  * Failure classification, which exists so a scheduler can decide whether
  * retrying is useful.
  *
  * The asymmetry matters more than the accuracy: a missed TRANSIENT costs
  * one manual rerun, while a WRONG transient makes a scheduler retry a
  * data-integrity failure forever and call it self-healing. So the tests
  * weight heavily toward "these must never be called retryable".
  */
class FailureClassTest extends AnyFunSuite {

  private def classOf(message: String) =
    FailureClass.classify(new IllegalStateException(message))

  // ---- must never be retryable ---------------------------------------------

  test("reconciliation failure is DATA_INTEGRITY, never retryable") {
    val f = classOf("Reconciliation failed: raw_equals_accepted: expected=7 actual=1")
    assert(f == FailureClass.DataIntegrity)
    assert(!f.retryable, "retrying reproduces the same counts; a human must look")
  }

  test("a contract violation is DATA_INTEGRITY by TYPE, whatever its text") {
    val e = new SchemaContractViolationException(
      Seq(SchemaViolation(ViolationKind.MissingColumn, "connection reset")))
    // The message deliberately contains a transient-looking phrase: type
    // must win, or a crafted message could downgrade a data failure.
    assert(FailureClass.classify(e) == FailureClass.DataIntegrity)
  }

  test("curated and header codes are DATA_INTEGRITY") {
    assert(classOf("CUR_008 freshness column missing") == FailureClass.DataIntegrity)
    assert(classOf("HDR_001 required column absent") == FailureClass.DataIntegrity)
  }

  test("watermark continuity is DATA_INTEGRITY, not transient") {
    assert(!classOf("Reconciliation failed: watermark_continuity: expected=X actual=Y").retryable)
  }

  test("configuration faults are not retryable either") {
    assert(classOf("CFG_018 config file not found") == FailureClass.Configuration)
    assert(classOf("JDBC_002 Environment variable is not set") == FailureClass.Configuration)
    assert(classOf("RAW_001 target missing metadata columns") == FailureClass.Configuration)
    assert(!FailureClass.Configuration.retryable, "editing is required; retrying cannot help")
  }

  test("OutOfMemoryError is NOT transient — the same heap reproduces it") {
    assert(FailureClass.classify(new OutOfMemoryError("Java heap space")) != FailureClass.Transient)
  }

  // ---- genuinely retryable --------------------------------------------------

  test("lock contention is TRANSIENT by type") {
    val f = FailureClass.classify(new PipelineLockException("PIPE_001 entity locked by run x"))
    assert(f == FailureClass.Transient && f.retryable)
  }

  test("a connection failure is TRANSIENT") {
    assert(classOf("JDBC_001 Connection to jdbc:sqlserver://h failed") == FailureClass.Transient)
    assert(classOf("The TCP/IP connection to the host x has failed") == FailureClass.Transient)
  }

  test("a watermark optimistic-concurrency conflict is TRANSIENT") {
    assert(classOf("JDBC_005 watermark version conflict") == FailureClass.Transient)
  }

  test("YARN preemption is TRANSIENT") {
    assert(classOf("Container killed by YARN for exceeding limits") == FailureClass.Transient)
  }

  // ---- conservative defaults ------------------------------------------------

  test("anything unrecognised stays UNCLASSIFIED with the historical exit 1") {
    val f = classOf("something nobody anticipated")
    assert(f == FailureClass.Unclassified)
    assert(f.exitCode == 1, "unclassified must behave exactly as before this existed")
    assert(!f.retryable)
  }

  test("a null message does not break classification") {
    assert(FailureClass.classify(new RuntimeException()) == FailureClass.Unclassified)
  }

  test("the informative code is found through a wrapped cause") {
    val wrapped = new RuntimeException("stage failed", new IllegalStateException("CUR_008 bad"))
    assert(FailureClass.classify(wrapped) == FailureClass.DataIntegrity)
  }

  test("a cyclic cause chain terminates rather than spinning") {
    val a = new RuntimeException("outer")
    val b = new RuntimeException("inner", a)
    a.initCause(b)
    assert(FailureClass.classify(a) != null)
  }

  test("exit codes are distinct so a scheduler can branch on them") {
    val all = Seq(FailureClass.Transient, FailureClass.DataIntegrity,
      FailureClass.Configuration, FailureClass.Unclassified)
    assert(all.map(_.exitCode).distinct.length == all.length)
    assert(!all.map(_.exitCode).contains(0), "0 must stay reserved for success")
  }
}
