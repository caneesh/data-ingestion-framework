package com.hcsc.generic.ingest.model

import org.scalatest.funsuite.AnyFunSuite

class CliParserTest extends AnyFunSuite {

  // ---------------------------------------------------------------------------
  // Happy path
  // ---------------------------------------------------------------------------

  test("parse minimal args with only --entity") {
    val cli = CliParser.parse(Array("--entity", "customer"))
    assert(cli.entity == "customer")
    assert(cli.mode == "FULL")
    assert(cli.stage == "all")
    assert(cli.confPath.isEmpty)
    assert(cli.rawFlag.isEmpty)
    assert(cli.resumeIngestDt.isEmpty)
  }

  test("parse all supported flags") {
    val cli = CliParser.parse(Array(
      "--entity", "order",
      "--mode", "INCR",
      "--conf-path", "/etc/app.conf",
      "--raw-flag", "CSV",
      "--stage", "raw",
      "--resume-ingest-dt", "2024-01-01"
    ))
    assert(cli.entity == "order")
    assert(cli.mode == "INCR")
    assert(cli.confPath == Some("/etc/app.conf"))
    assert(cli.rawFlag == Some("CSV"))
    assert(cli.stage == "raw")
    assert(cli.resumeIngestDt == Some("2024-01-01"))
  }

  // ---------------------------------------------------------------------------
  // --mode validation
  // ---------------------------------------------------------------------------

  test("--mode FULL (uppercase) is accepted and stored as FULL") {
    val cli = CliParser.parse(Array("--entity", "e", "--mode", "FULL"))
    assert(cli.mode == "FULL")
  }

  test("--mode full (lowercase) is accepted and normalised to FULL") {
    val cli = CliParser.parse(Array("--entity", "e", "--mode", "full"))
    assert(cli.mode == "FULL")
  }

  test("--mode incr (mixed case) is accepted and normalised to INCR") {
    val cli = CliParser.parse(Array("--entity", "e", "--mode", "Incr"))
    assert(cli.mode == "INCR")
  }

  test("invalid --mode value throws IllegalArgumentException") {
    intercept[IllegalArgumentException] {
      CliParser.parse(Array("--entity", "e", "--mode", "BATCH"))
    }
  }

  // ---------------------------------------------------------------------------
  // --entity validation
  // ---------------------------------------------------------------------------

  test("missing --entity throws IllegalArgumentException") {
    intercept[IllegalArgumentException] {
      CliParser.parse(Array("--mode", "FULL"))
    }
  }

  // ---------------------------------------------------------------------------
  // --stage validation
  // ---------------------------------------------------------------------------

  test("--stage all is accepted") {
    val cli = CliParser.parse(Array("--entity", "e", "--stage", "all"))
    assert(cli.stage == "all")
  }

  test("--stage raw is accepted") {
    val cli = CliParser.parse(Array("--entity", "e", "--stage", "raw"))
    assert(cli.stage == "raw")
  }

  test("--stage curated is accepted") {
    val cli = CliParser.parse(Array("--entity", "e", "--stage", "curated"))
    assert(cli.stage == "curated")
  }

  test("--stage curated-only is accepted") {
    val cli = CliParser.parse(Array("--entity", "e", "--stage", "curated-only"))
    assert(cli.stage == "curated-only")
  }

  test("--stage c (shorthand) is accepted") {
    val cli = CliParser.parse(Array("--entity", "e", "--stage", "c"))
    assert(cli.stage == "c")
  }

  test("invalid --stage value throws IllegalArgumentException") {
    intercept[IllegalArgumentException] {
      CliParser.parse(Array("--entity", "e", "--stage", "invalid"))
    }
  }

  // ---------------------------------------------------------------------------
  // Error handling
  // ---------------------------------------------------------------------------

  test("unknown flag throws IllegalArgumentException") {
    intercept[IllegalArgumentException] {
      CliParser.parse(Array("--entity", "e", "--unknown-flag", "value"))
    }
  }

  test("flag without value throws IllegalArgumentException") {
    intercept[IllegalArgumentException] {
      CliParser.parse(Array("--entity", "e", "--mode"))
    }
  }

  test("--entity flag without value throws IllegalArgumentException") {
    intercept[IllegalArgumentException] {
      CliParser.parse(Array("--entity"))
    }
  }
}
