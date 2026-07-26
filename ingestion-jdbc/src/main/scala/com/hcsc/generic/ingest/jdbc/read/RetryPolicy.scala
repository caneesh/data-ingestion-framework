package com.hcsc.generic.ingest.jdbc.read

import com.hcsc.generic.ingest.jdbc.RetryConfig
import org.apache.log4j.Logger

import scala.util.control.NonFatal

/** Linear-backoff retry for transient JDBC failures (connection resets,
  * Azure SQL transient errors, failovers). Configuration errors
  * (IllegalArgumentException, e.g. JDBC_003) are never retried. */
object RetryPolicy {

  def withRetries[T](operation: String, retry: RetryConfig, logger: Logger)(body: => T): T = {
    var attempt = 1
    var lastError: Throwable = null
    while (attempt <= math.max(1, retry.maxAttempts)) {
      try {
        return body
      } catch {
        case e: IllegalArgumentException => throw e // config errors are permanent
        case NonFatal(e) =>
          lastError = e
          if (attempt < retry.maxAttempts) {
            val sleepMs = retry.backoffMs * attempt
            logger.warn(s"[RetryPolicy] $operation attempt $attempt/${retry.maxAttempts} failed " +
              s"(${e.getClass.getSimpleName}: ${e.getMessage}); retrying in ${sleepMs}ms")
            Thread.sleep(sleepMs)
          }
          attempt += 1
      }
    }
    throw new RuntimeException(
      s"JDBC_001 $operation failed after ${retry.maxAttempts} attempt(s): ${lastError.getMessage}", lastError)
  }
}
