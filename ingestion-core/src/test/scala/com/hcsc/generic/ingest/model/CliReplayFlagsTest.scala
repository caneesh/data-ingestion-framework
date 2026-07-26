package com.hcsc.generic.ingest.model

import org.scalatest.funsuite.AnyFunSuite

class CliReplayFlagsTest extends AnyFunSuite {

  private def parse(extra: String*): Cli =
    CliParser.parse(Array("--entity", "member", "--mode", "FULL") ++ extra)

  test("replay flags default to off") {
    val cli = parse()
    assert(cli.runId.isEmpty)
    assert(!cli.resume)
    assert(cli.fileId.isEmpty)
    assert(!cli.dryRun)
    assert(!cli.forceReprocess)
  }

  test("--run-id, --file-id are parsed with values") {
    val cli = parse("--run-id", "abc-123", "--file-id", "deadbeef")
    assert(cli.runId.contains("abc-123"))
    assert(cli.fileId.contains("deadbeef"))
  }

  test("--resume, --dry-run, --force-reprocess are boolean flags") {
    val cli = parse("--run-id", "abc", "--resume", "--dry-run", "--force-reprocess")
    assert(cli.resume)
    assert(cli.dryRun)
    assert(cli.forceReprocess)
  }

  test("--resume without --run-id is rejected") {
    intercept[IllegalArgumentException] { parse("--resume") }
  }

  test("--run-id without value is rejected") {
    intercept[IllegalArgumentException] { parse("--run-id") }
  }
}
