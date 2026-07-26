package com.hcsc.generic.ingest.jdbc.read

import com.hcsc.generic.ingest.jdbc.RetryConfig
import org.apache.log4j.Logger

import scala.util.control.NonFatal

/**
  * Bounded exponential-backoff retry WITH JITTER for DRIVER-SIDE JDBC
  * operations only: health checks, metadata queries, watermark upper-bound
  * capture, and watermark-store operations.
  *
  * Retry layering (important — see README "JDBC retry model"):
  *   1. This policy: driver-side operations, transient categories only
  *      (classified by SqlFailureClassifier; permanent errors fail fast).
  *   2. Executor-side partition reads: governed by Spark task/stage retry
  *      (spark.task.maxFailures) — NOT by this policy. Wrapping
  *      DataFrameReader.load() does not retry executor reads.
  *   3. Whole-run restart: the pipeline is restart-safe; a failed run never
  *      advances the watermark, so replay re-extracts the same window.
  */
object RetryPolicy {

  def withRetries[T](operation: String, retry: RetryConfig, logger: Logger)(body: => T): T = {
    var attempt = 1
    var lastError: Throwable = null
    val maxAttempts = math.max(1, retry.maxAttempts)

    while (attempt <= maxAttempts) {
      try {
        return body
      } catch {
        case NonFatal(e) =>
          val category = SqlFailureClassifier.classify(e)
          if (!category.retryable) {
            com.hcsc.generic.ingest.jdbc.JdbcMetrics.increment("jdbc_permanent_failure_total")
            logger.error(s"[RetryPolicy] $operation failed with permanent error " +
              s"(category=$category); not retrying: ${e.getMessage}")
            throw e
          }
          com.hcsc.generic.ingest.jdbc.JdbcMetrics.increment("jdbc_transient_failure_total")
          lastError = e
          if (attempt < maxAttempts) {
            com.hcsc.generic.ingest.jdbc.JdbcMetrics.increment("jdbc_retry_total")
            val sleepMs = backoffWithJitter(retry.backoffMs, attempt)
            logger.warn(s"[RetryPolicy] $operation attempt $attempt/$maxAttempts failed " +
              s"(category=$category, ${e.getClass.getSimpleName}: ${e.getMessage}); " +
              s"retrying in ${sleepMs}ms")
            Thread.sleep(sleepMs)
          }
          attempt += 1
      }
    }
    throw new RuntimeException(
      s"JDBC_001 $operation failed after $maxAttempts attempt(s): ${lastError.getMessage}", lastError)
  }

  /** Exponential backoff (base * 2^(attempt-1)) with 50-100% jitter, capped
    * at 60s per sleep so total wait stays bounded. */
  private def backoffWithJitter(baseMs: Long, attempt: Int): Long = {
    val exponential = baseMs * math.pow(2, attempt - 1).toLong
    val capped = math.min(exponential, 60000L)
    (capped * (0.5 + scala.util.Random.nextDouble() * 0.5)).toLong.max(1L)
  }
}
