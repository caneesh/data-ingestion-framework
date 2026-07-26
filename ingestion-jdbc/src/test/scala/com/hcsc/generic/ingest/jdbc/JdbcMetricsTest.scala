package com.hcsc.generic.ingest.jdbc

import com.hcsc.generic.ingest.jdbc.read.RetryPolicy
import org.apache.log4j.Logger
import org.scalatest.funsuite.AnyFunSuite

class JdbcMetricsTest extends AnyFunSuite {

  private val logger = Logger.getLogger(getClass.getName)

  test("counters accumulate and snapshot") {
    JdbcMetrics.reset()
    JdbcMetrics.increment("jdbc_test_counter")
    JdbcMetrics.increment("jdbc_test_counter", 4)
    assert(JdbcMetrics.value("jdbc_test_counter") == 5L)
    assert(JdbcMetrics.value("jdbc_absent") == 0L)
    assert(JdbcMetrics.snapshot.get("jdbc_test_counter").contains(5L))
  }

  test("retry policy feeds transient/permanent/retry counters") {
    JdbcMetrics.reset()
    var calls = 0
    RetryPolicy.withRetries("op", RetryConfig(3, 1), logger) {
      calls += 1
      if (calls < 2) throw new java.sql.SQLTransientConnectionException("blip")
      "ok"
    }
    assert(JdbcMetrics.value("jdbc_transient_failure_total") == 1L)
    assert(JdbcMetrics.value("jdbc_retry_total") == 1L)

    intercept[java.sql.SQLException] {
      RetryPolicy.withRetries("op", RetryConfig(3, 1), logger) {
        throw new java.sql.SQLException("login failed", "28000")
      }
    }
    assert(JdbcMetrics.value("jdbc_permanent_failure_total") == 1L)
  }
}
