package com.hcsc.generic.ingest.config

import com.hcsc.generic.ingest.model.CliParser
import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite

/** ingestion.execution = COUPLED | DECOUPLED — the scheduler-facing choice
  * of one process vs two, with the consistency rules that make a
  * half-configured decoupled feed impossible. */
class ExecutionModeTest extends AnyFunSuite {

  private def conf(s: String) = ConfigFactory.parseString(s)

  test("default is COUPLED; values validate") {
    assert(ExecutionMode.declared(conf("{}")) == ExecutionMode.Coupled)
    assert(ExecutionMode.declared(conf("""ingestion.execution = decoupled""")) ==
      ExecutionMode.Decoupled)
    val e = intercept[IllegalArgumentException] {
      ExecutionMode.declared(conf("""ingestion.execution = "BOTH""""))
    }
    assert(e.getMessage.contains("CFG_016"))
  }

  test("DECOUPLED with an incremental source requires advance_after = RAW") {
    val bad = conf(
      """ingestion.execution = DECOUPLED
        |source { incremental { watermark_columns = ["ts"] } }
        |""".stripMargin)
    assert(ExecutionMode.configProblems(bad).exists(_.contains("advance_after")))

    val good = conf(
      """ingestion.execution = DECOUPLED
        |source { incremental { watermark_columns = ["ts"] } }
        |watermark { advance_after = RAW }
        |""".stripMargin)
    assert(ExecutionMode.configProblems(good).isEmpty)

    // Kafka offset tracking counts as an incremental position too
    val kafka = conf(
      """ingestion.execution = DECOUPLED
        |source { offsets { track = true } }
        |""".stripMargin)
    assert(ExecutionMode.configProblems(kafka).exists(_.contains("advance_after")))
  }

  test("DECOUPLED requires the run ledger") {
    val noLedger = conf(
      """ingestion.execution = DECOUPLED
        |audit { enabled = false }
        |""".stripMargin)
    assert(ExecutionMode.configProblems(noLedger).exists(_.contains("ledger")))
  }

  test("COUPLED feeds have no execution constraints") {
    assert(ExecutionMode.configProblems(conf(
      """source { incremental { watermark_columns = ["ts"] } }""")).isEmpty)
  }

  test("a DECOUPLED feed rejects the coupled --stage all invocation") {
    val feed = conf("""ingestion.execution = DECOUPLED""")
    val all = CliParser.parse(Array("--entity", "e"))
    val e = intercept[IllegalArgumentException] {
      ExecutionMode.validateInvocation(all, feed)
    }
    assert(e.getMessage.contains("CFG_016") && e.getMessage.contains("double-process"))

    // The two decoupled invocations pass, as does validate-only
    ExecutionMode.validateInvocation(
      CliParser.parse(Array("--entity", "e", "--stage", "raw")), feed)
    ExecutionMode.validateInvocation(
      CliParser.parse(Array("--entity", "e", "--stage", "curated", "--pending")), feed)
    ExecutionMode.validateInvocation(
      CliParser.parse(Array("--entity", "e", "--validate-only")), feed)
    // And a COUPLED feed accepts --stage all
    ExecutionMode.validateInvocation(all, conf("{}"))
  }
}
