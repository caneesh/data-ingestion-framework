package com.hcsc.generic.ingest.jdbc.read

import com.hcsc.generic.ingest.jdbc.RetryConfig
import org.apache.log4j.Logger
import org.scalatest.funsuite.AnyFunSuite

class RetryPolicyTest extends AnyFunSuite {

  private val logger = Logger.getLogger(getClass.getName)
  private val fastRetry = RetryConfig(maxAttempts = 3, backoffMs = 1)

  test("succeeds immediately without retries") {
    var calls = 0
    val result = RetryPolicy.withRetries("op", fastRetry, logger) { calls += 1; "ok" }
    assert(result == "ok" && calls == 1)
  }

  test("retries transient failures until success") {
    var calls = 0
    val result = RetryPolicy.withRetries("op", fastRetry, logger) {
      calls += 1
      if (calls < 3) throw new java.sql.SQLTransientConnectionException("blip")
      "recovered"
    }
    assert(result == "recovered" && calls == 3)
  }

  test("exhausts attempts on persistent transient errors and wraps with JDBC_001") {
    var calls = 0
    val ex = intercept[RuntimeException] {
      RetryPolicy.withRetries("op", fastRetry, logger) {
        calls += 1
        throw new java.sql.SQLTransientConnectionException("down")
      }
    }
    assert(calls == 3)
    assert(ex.getMessage.contains("JDBC_001"))
    assert(ex.getCause.getMessage == "down")
  }

  test("configuration errors are never retried") {
    var calls = 0
    intercept[IllegalArgumentException] {
      RetryPolicy.withRetries("op", fastRetry, logger) {
        calls += 1
        throw new IllegalArgumentException("JDBC_003 bad config")
      }
    }
    assert(calls == 1)
  }

  test("authentication failures are never retried") {
    var calls = 0
    intercept[java.sql.SQLException] {
      RetryPolicy.withRetries("op", fastRetry, logger) {
        calls += 1
        throw new java.sql.SQLException("login failed", "28000")
      }
    }
    assert(calls == 1)
  }

  test("unknown SQL errors are conservatively not retried") {
    var calls = 0
    intercept[java.sql.SQLException] {
      RetryPolicy.withRetries("op", fastRetry, logger) {
        calls += 1
        throw new java.sql.SQLException("mystery")
      }
    }
    assert(calls == 1)
  }

  test("deadlocks and throttling are retried") {
    var calls = 0
    val result = RetryPolicy.withRetries("op", fastRetry, logger) {
      calls += 1
      if (calls == 1) throw new java.sql.SQLException("deadlock victim", "40001", 1205)
      else if (calls == 2) throw new java.sql.SQLException("throttled", "S0001", 40501)
      else "ok"
    }
    assert(result == "ok" && calls == 3)
  }
}
