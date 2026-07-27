package com.hcsc.generic.ingest.confgen.input

import com.hcsc.generic.ingest.confgen.model.AnswerValue
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.Files

/** Large-collection input: inline CSV, inline JSON, @file with preview. */
class CollectionInputTest extends AnyFunSuite {

  test("inline CSV splits, trims and drops empties") {
    val Right(parsed) = CollectionInput.parse(" a , b ,, c ")
    assert(parsed.value == AnswerValue.Items(Seq("a", "b", "c")))
    assert(parsed.sourceDescription.contains("3 items"))
  }

  test("inline JSON string array becomes items") {
    val Right(parsed) = CollectionInput.parse("""["x", "y"]""")
    assert(parsed.value == AnswerValue.Items(Seq("x", "y")))
  }

  test("inline JSON object array becomes structured blocks with name preview") {
    val Right(parsed) = CollectionInput.parse(
      """[{"name": "subscriber_id", "type": "string"}, {"name": "hios_id", "type": "string"}]""")
    parsed.value match {
      case AnswerValue.Blocks(blocks) =>
        assert(blocks.map(_.getString("name")) == Seq("subscriber_id", "hios_id"))
      case other => fail(s"expected Blocks, got $other")
    }
    assert(parsed.preview == Seq("subscriber_id", "hios_id"))
  }

  test("invalid JSON reports a parse error instead of silently degrading") {
    val Left(problem) = CollectionInput.parse("""[{"name": }]""")
    assert(problem.contains("Invalid JSON"))
  }

  test("@file text input: one item per line, comments and blanks skipped, commas split") {
    val f = Files.createTempFile("cols", ".txt")
    Files.write(f, "# header comment\ncol_a\ncol_b, col_c\n\n".getBytes("UTF-8"))
    val Right(parsed) = CollectionInput.parse(s"@$f")
    assert(parsed.value == AnswerValue.Items(Seq("col_a", "col_b", "col_c")))
    assert(parsed.sourceDescription.contains(f.toString))
  }

  test("@file JSON input parses objects") {
    val f = Files.createTempFile("cols", ".json")
    Files.write(f, """[{"name": "a", "type": "int"}]""".getBytes("UTF-8"))
    val Right(parsed) = CollectionInput.parse(s"@$f")
    parsed.value match {
      case AnswerValue.Blocks(blocks) => assert(blocks.head.getString("type") == "int")
      case other => fail(s"expected Blocks, got $other")
    }
  }

  test("@file that does not exist is a validation error") {
    val Left(problem) = CollectionInput.parse("@/no/such/file.csv")
    assert(problem.contains("File not found"))
  }

  test("preview truncates long collections") {
    val Right(parsed) = CollectionInput.parse((1 to 12).map(i => s"c$i").mkString(","))
    assert(parsed.preview.size == 6)
    assert(parsed.preview.last.contains("7 more"))
  }
}
