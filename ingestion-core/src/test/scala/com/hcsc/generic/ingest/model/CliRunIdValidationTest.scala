package com.hcsc.generic.ingest.model

import org.scalatest.funsuite.AnyFunSuite

/** --run-id reaches staging-table identifiers and audit records: it is
  * constrained at the CLI boundary. */
class CliRunIdValidationTest extends AnyFunSuite {

  private def parse(runId: String) =
    CliParser.parse(Array("--entity", "e", "--run-id", runId))

  test("letters, digits, underscore and hyphen are accepted") {
    assert(parse("run-2026_07_27-001").runId.contains("run-2026_07_27-001"))
    assert(parse("6f9619ff-8b86-d011-b42d-00c04fc964ff").runId.isDefined) // UUID
  }

  test("identifier-hostile run ids are rejected at the boundary") {
    Seq("run 1", "run;drop", "run.id", "run'quote", "run`tick", "run$sub", "").foreach { bad =>
      val ex = intercept[IllegalArgumentException] { parse(bad) }
      assert(ex.getMessage.contains("--run-id"), s"'$bad' should be rejected")
    }
  }

  test("overlong run ids are rejected") {
    intercept[IllegalArgumentException] { parse("a" * 129) }
    assert(parse("a" * 128).runId.isDefined)
  }
}
