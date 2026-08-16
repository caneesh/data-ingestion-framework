package com.hcsc.generic.ingest.runtime

/**
  * What KIND of failure a run hit, so a scheduler can decide whether
  * retrying is useful.
  *
  * Every failure currently exits 1, so Control-M cannot distinguish "SQL
  * Server was briefly unreachable" (retry in ten minutes, silently) from
  * "reconciliation failed" (never retry; wake someone). That single
  * distinction is what turns manual recovery into self-healing, and it
  * belongs in the scheduler — which owns backoff and escalation — rather
  * than inside the pipeline.
  *
  * DELIBERATELY CONSERVATIVE. Only failures whose classification is
  * unambiguous are classified; everything else stays [[Unclassified]] with
  * the historical exit code 1. A wrong TRANSIENT verdict is the dangerous
  * one: it would make a scheduler retry a data-integrity failure forever,
  * so the default on doubt is "do not claim it is retryable".
  */
sealed abstract class FailureClass(val name: String, val exitCode: Int, val retryable: Boolean)

object FailureClass {

  /** The environment misbehaved, not the data or the config. Re-running the
    * same command unchanged is a reasonable next step. */
  case object Transient extends FailureClass("TRANSIENT", 10, retryable = true)

  /** The data is not what was expected — reconciliation, contract, curated
    * integrity. Retrying reproduces it exactly; a human must look. */
  case object DataIntegrity extends FailureClass("DATA_INTEGRITY", 20, retryable = false)

  /** The feed, credentials or environment are wrong. Retrying cannot help
    * until something is edited. */
  case object Configuration extends FailureClass("CONFIGURATION", 30, retryable = false)

  /** Not confidently classified: exit 1, exactly as before this existed. */
  case object Unclassified extends FailureClass("UNCLASSIFIED", 1, retryable = false)

  /** Codes whose family alone settles the class. */
  private val transientCodes = Set(
    "PIPE_001", // entity lock held by another run — the classic "try later"
    "JDBC_001", // connection failed: network, firewall, server restart
    "JDBC_005"  // watermark optimistic-concurrency conflict — a racing run won
  )

  /** Exception types that carry their own verdict, checked before any text. */
  private def byType(error: Throwable): Option[FailureClass] = error match {
    case _: com.hcsc.generic.ingest.lock.PipelineLockException      => Some(Transient)
    case _: com.hcsc.generic.ingest.schema.SchemaContractViolationException => Some(DataIntegrity)
    case _: com.hcsc.generic.ingest.publish.PublishValidationException     => Some(DataIntegrity)
    case _: com.hcsc.generic.ingest.reject.RejectThresholdExceededException => Some(DataIntegrity)
    case _ => None
  }

  /** Substrings that identify a transient ENVIRONMENT fault regardless of
    * which layer raised them. Deliberately narrow — each is a fault where a
    * later attempt genuinely may succeed with no change. */
  private val transientText = Seq(
    "connection reset", "connection refused", "connection timed out",
    "broken pipe", "the tcp/ip connection to the host",
    "socket closed", "no route to host", "temporarily unavailable",
    "container killed by yarn", "container preempted", "executor lost")

  /**
    * Classifies a failure by exception type, then error code, then a narrow
    * set of environment-fault signatures. Note what is NOT here:
    * OutOfMemoryError is not transient — retrying with the same heap
    * reproduces it — and an authentication failure is configuration, since
    * a wrong credential does not fix itself.
    */
  def classify(error: Throwable): FailureClass = {
    byType(error).getOrElse {
      val text = messageChain(error)
      val upper = text.toUpperCase

      if (transientCodes.exists(upper.contains)) Transient
      else if (upper.contains("RECONCILIATION FAILED")) DataIntegrity
      else if (Seq("CUR_", "HDR_").exists(upper.contains)) DataIntegrity
      else if (Seq("CFG_", "RAW_001", "JDBC_002", "JDBC_003").exists(upper.contains)) Configuration
      else if (transientText.exists(text.toLowerCase.contains)) Transient
      else Unclassified
    }
  }

  /** Causes matter: the informative code is often wrapped. Bounded so a
    * cyclic cause chain cannot spin. */
  private def messageChain(error: Throwable): String = {
    val builder = new StringBuilder
    var current: Throwable = error
    var depth = 0
    while (current != null && depth < 10) {
      builder.append(String.valueOf(current.getMessage)).append(' ')
      builder.append(current.getClass.getName).append(' ')
      current = if (current.getCause eq current) null else current.getCause
      depth += 1
    }
    builder.toString
  }
}
