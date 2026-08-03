package com.hcsc.generic.ingest.model

import org.scalatest.funsuite.AnyFunSuite

/** Decoupled batch selectors: parse, stage restriction, exclusivity. */
class CliPendingFlagsTest extends AnyFunSuite {

  test("selectors parse with --stage curated") {
    val cli = CliParser.parse(Array("--entity", "e", "--stage", "curated", "--pending"))
    assert(cli.pending && cli.batchSelector)
    val last = CliParser.parse(Array("--entity", "e", "--stage", "curated", "--replay-last", "5"))
    assert(last.replayLast.contains(5))
    val range = CliParser.parse(Array("--entity", "e", "--stage", "curated",
      "--replay-from", "2026-01-01", "--replay-to", "2026-01-31"))
    assert(range.replayFrom.contains("2026-01-01") && range.replayTo.contains("2026-01-31"))
    val failed = CliParser.parse(Array("--entity", "e", "--stage", "c", "--replay-failed"))
    assert(failed.replayFailed)
    val sys = CliParser.parse(Array("--entity", "e", "--stage", "curated",
      "--pending", "--replay-source-system", "SAP"))
    assert(sys.pending && sys.replaySourceSystem.contains("SAP"))
  }

  test("selectors require --stage curated") {
    intercept[IllegalArgumentException] {
      CliParser.parse(Array("--entity", "e", "--pending"))
    }
    intercept[IllegalArgumentException] {
      CliParser.parse(Array("--entity", "e", "--stage", "raw", "--replay-failed"))
    }
  }

  test("selectors are mutually exclusive with each other and with single-shot selectors") {
    intercept[IllegalArgumentException] {
      CliParser.parse(Array("--entity", "e", "--stage", "curated", "--pending", "--replay-failed"))
    }
    intercept[IllegalArgumentException] {
      CliParser.parse(Array("--entity", "e", "--stage", "curated", "--pending", "--run-id", "r1"))
    }
    intercept[IllegalArgumentException] {
      CliParser.parse(Array("--entity", "e", "--stage", "curated", "--replay-last", "3",
        "--resume-ingest-dt", "2026-01-01"))
    }
  }

  test("date and count validation") {
    intercept[IllegalArgumentException] {
      CliParser.parse(Array("--entity", "e", "--stage", "curated", "--replay-from", "01/01/2026"))
    }
    intercept[IllegalArgumentException] {
      CliParser.parse(Array("--entity", "e", "--stage", "curated",
        "--replay-from", "2026-02-01", "--replay-to", "2026-01-01"))
    }
    intercept[IllegalArgumentException] {
      CliParser.parse(Array("--entity", "e", "--stage", "curated", "--replay-last", "0"))
    }
  }

  test("selectors reject --dry-run: a dry-run replay would checkpoint unpublished batches") {
    val e = intercept[IllegalArgumentException] {
      CliParser.parse(Array("--entity", "e", "--stage", "curated", "--pending", "--dry-run"))
    }
    assert(e.getMessage.contains("dry-run"))
  }

  test("single-shot --run-id and --resume-ingest-dt remain valid without selectors") {
    val cli = CliParser.parse(Array("--entity", "e", "--stage", "curated", "--run-id", "r1"))
    assert(!cli.batchSelector && cli.runId.contains("r1"))
  }
}
