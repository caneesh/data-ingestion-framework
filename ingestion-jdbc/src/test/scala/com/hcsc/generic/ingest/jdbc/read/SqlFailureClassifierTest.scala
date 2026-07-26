package com.hcsc.generic.ingest.jdbc.read

import com.hcsc.generic.ingest.jdbc.read.SqlFailureClassifier._
import org.scalatest.funsuite.AnyFunSuite

import java.sql.SQLException

class SqlFailureClassifierTest extends AnyFunSuite {

  private def sql(message: String, state: String = null, vendor: Int = 0) =
    new SQLException(message, state, vendor)

  test("SQLState class 08 is transient network") {
    assert(classify(sql("conn reset", "08S01")) == TransientNetwork)
    assert(classify(sql("conn failure", "08006")) == TransientNetwork)
    assert(TransientNetwork.retryable)
  }

  test("SQLState class 28 is authentication and never retryable") {
    val c = classify(sql("login failed", "28000"))
    assert(c == Authentication && !c.retryable)
  }

  test("SQLState 40001 and vendor 1205 are deadlocks") {
    assert(classify(sql("serialization", "40001")) == Deadlock)
    assert(classify(sql("victim", "S0001", 1205)) == Deadlock)
    assert(Deadlock.retryable)
  }

  test("Azure SQL throttling vendor codes are retryable throttling") {
    Seq(40501, 10928, 49918).foreach { code =>
      val c = classify(sql("busy", "S0001", code))
      assert(c == Throttling && c.retryable)
    }
  }

  test("SQL Server auth and object-missing vendor codes") {
    assert(classify(sql("login", null, 18456)) == Authentication)
    assert(classify(sql("no table", null, 208)) == ObjectNotFound)
    assert(classify(sql("no column", null, 207)) == SchemaMismatch)
  }

  test("42-class states split into syntax, missing objects and authorization") {
    assert(classify(sql("bad sql", "42000")) == SqlSyntax)
    assert(classify(sql("no table", "42S02")) == ObjectNotFound)
    assert(classify(sql("no col", "42S22")) == SchemaMismatch)
    assert(classify(sql("denied", "42501")) == Authorization)
  }

  test("timeouts via state, subtype and vendor code") {
    assert(classify(sql("t/o", "HYT00")) == Timeout)
    assert(classify(new java.sql.SQLTimeoutException("slow")) == Timeout)
    assert(classify(sql("t/o", null, -2)) == Timeout)
    assert(Timeout.retryable)
  }

  test("classification walks nested cause chains") {
    val wrapped = new RuntimeException("outer",
      new RuntimeException("middle", sql("deadlock", "40001")))
    assert(classify(wrapped) == Deadlock)
  }

  test("network exceptions without SQLException classify as transient") {
    assert(classify(new java.net.SocketException("reset")) == TransientNetwork)
    assert(classify(new java.net.SocketTimeoutException("slow")) == Timeout)
  }

  test("JDBC_-prefixed config errors and unknowns are not retryable") {
    assert(classify(new IllegalArgumentException("JDBC_003 bad")) == Configuration)
    val unknown = classify(sql("mystery"))
    assert(unknown == Unknown && !unknown.retryable)
  }
}
