package com.hcsc.generic.ingest

import org.scalatest.funsuite.AnyFunSuite

/**
  * Build identity must never be load-bearing.
  *
  * It exists so a driver log can answer "is this the jar I just built?" on a
  * server that is not a checkout. That is diagnostic value only — so the
  * property worth pinning is that reading it cannot fail, whatever the
  * classloader or code source, including from plain classes as here where
  * there is no manifest at all.
  */
class BuildInfoTest extends AnyFunSuite {

  test("identity resolves without throwing when running from classes") {
    // Under test there is no jar manifest; "unknown" is the honest answer
    // rather than a fabricated version.
    assert(BuildInfo.version != null && BuildInfo.version.nonEmpty)
    assert(BuildInfo.buildTime != null && BuildInfo.buildTime.nonEmpty)
  }

  test("summary is a single line safe to log and to store in the ledger") {
    val s = BuildInfo.summary
    assert(s.nonEmpty)
    assert(!s.contains("\n") && !s.contains("\r"),
      s"a newline would corrupt both the log line and the audit row: '$s'")
    assert(s.contains(BuildInfo.version) && s.contains(BuildInfo.buildTime),
      s"summary must expose both parts, since version alone cannot separate " +
        s"two builds of the same SNAPSHOT: '$s'")
  }

  test("repeated reads are stable") {
    assert(BuildInfo.summary == BuildInfo.summary)
  }
}
