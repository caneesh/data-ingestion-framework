package com.hcsc.generic.ingest.audit

import org.scalatest.funsuite.AnyFunSuite

/** Raw exception messages land in ingest_run_audit.message — credentials
  * embedded in JDBC URLs must never reach the ledger, and unbounded driver
  * messages must not bloat it. */
class AuditSanitizeTest extends AnyFunSuite {

  test("credential-shaped pairs are redacted") {
    val msg = "Login failed for jdbc:sqlserver://h:1433;user=svc;password=Hunter2!;encrypt=true"
    val out = AuditService.sanitizeMessage(msg)
    assert(!out.contains("Hunter2!"))
    assert(out.contains("password=***"))
    assert(out.contains("user=svc"), "non-secret pairs stay readable")
  }

  test("token and accessKey variants are redacted case-insensitively") {
    val out = AuditService.sanitizeMessage("auth TOKEN=abc123 accessKey = xyz, pwd=p")
    assert(!out.contains("abc123") && !out.contains("xyz") && !out.contains("=p"))
  }

  test("null message becomes empty, long messages are truncated") {
    assert(AuditService.sanitizeMessage(null) == "")
    val long = AuditService.sanitizeMessage("x" * 5000)
    assert(long.length < 4100)
    assert(long.endsWith("...(truncated)"))
  }

  test("ordinary messages pass through unchanged") {
    val msg = "CUR_002 Cast would silently turn non-null values into NULL"
    assert(AuditService.sanitizeMessage(msg) == msg)
  }
}
