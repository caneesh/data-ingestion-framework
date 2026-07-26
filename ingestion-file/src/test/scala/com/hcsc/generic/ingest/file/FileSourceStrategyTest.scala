package com.hcsc.generic.ingest.file

import com.hcsc.generic.ingest.schema.SchemaContractViolationException
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Path}

class FileSourceStrategyTest extends AnyFunSuite with SharedSparkSession {

  private def writeCsv(lines: String*): Path = {
    val dir = Files.createTempDirectory("filesource-strategy-test")
    val file = dir.resolve("data.csv")
    Files.write(file, lines.mkString("\n").getBytes("UTF-8"))
    file
  }

  private def conf(path: Path, schemaBlock: String): Config =
    ConfigFactory.parseString(
      s"""
         |type = "file"
         |path = "${path.toString}"
         |file_type = "csv"
         |header = true
         |$schemaBlock
      """.stripMargin)

  private val baseColumns =
    """columns = [
      |  { name = "subscriber_id", position = 0, aliases = ["subscriber id"] },
      |  { name = "hios_id", position = 1, aliases = ["plan hios id", "plan_hios_id"] }
      |]""".stripMargin

  test("STRICT_NAME accepts canonical names but rejects known aliases") {
    val schema =
      s"""schema { version = "1.0", $baseColumns
         |  header_validation { strategy = "STRICT_NAME" }
         |}""".stripMargin

    val exact = writeCsv("subscriber_id,hios_id", "S1,H1")
    assert(FileSource.read(spark, conf(exact, schema)).count() == 1)

    val aliased = writeCsv("subscriber id,plan_hios_id", "S1,H1")
    intercept[SchemaContractViolationException] {
      FileSource.read(spark, conf(aliased, schema))
    }
  }

  test("NAME_WITH_ALIASES maps a renamed vendor header (Plan HIOS ID -> hios_id)") {
    val schema = s"""schema { version = "1.0", $baseColumns }"""
    val csv = writeCsv("Subscriber ID,Plan HIOS ID", "S1,12345A6789")
    val df = FileSource.read(spark, conf(csv, schema))
    assert(df.columns.toSet == Set("subscriber_id", "hios_id", "source_file"))
    assert(df.select("hios_id").collect().head.getString(0) == "12345A6789")
  }

  test("positional fallback engages only after name/alias matching fails") {
    val schema =
      s"""schema { version = "1.0", $baseColumns
         |  header_validation {
         |    strategy = "NAME_ALIAS_POSITION_FALLBACK"
         |    positional_fallback { enabled = true, require_exact_column_count = true }
         |  }
         |}""".stripMargin

    // Completely unknown headers, but correct column count -> fallback maps by position
    val csv = writeCsv("mystery_a,mystery_b", "S1,H1", "S2,H2")
    val df = FileSource.read(spark, conf(csv, schema))
    assert(df.columns.toSet == Set("subscriber_id", "hios_id", "source_file"))
    assert(df.count() == 2)
  }

  test("positional fallback fails on column count mismatch (HDR_005)") {
    val schema =
      s"""schema { version = "1.0", $baseColumns
         |  header_validation {
         |    strategy = "NAME_ALIAS_POSITION_FALLBACK"
         |    positional_fallback { enabled = true, require_exact_column_count = true }
         |  }
         |}""".stripMargin

    val csv = writeCsv("mystery_a,mystery_b,mystery_c", "S1,H1,extra")
    val ex = intercept[SchemaContractViolationException] {
      FileSource.read(spark, conf(csv, schema))
    }
    assert(ex.getMessage.contains("HDR_005"))
  }

  test("fallback strategy does not engage when aliases resolve the headers") {
    val schema =
      s"""schema { version = "1.0", $baseColumns
         |  header_validation {
         |    strategy = "NAME_ALIAS_POSITION_FALLBACK"
         |    positional_fallback { enabled = true }
         |  }
         |}""".stripMargin

    // Headers arrive in REVERSE order but resolve by alias — name matching
    // must win over position, so values stay with their proper columns.
    val csv = writeCsv("plan_hios_id,subscriber id", "H1,S1")
    val df = FileSource.read(spark, conf(csv, schema))
    assert(df.select("subscriber_id").collect().head.getString(0) == "S1")
    assert(df.select("hios_id").collect().head.getString(0) == "H1")
  }

  test("missing optional column gets null default; non-null default is applied") {
    val schema =
      """schema { version = "1.0", columns = [
        |  { name = "subscriber_id", position = 0 },
        |  { name = "middle_name", required = false, default = null, position = 1 },
        |  { name = "status", required = false, default = "UNKNOWN", position = 2 }
        |]}""".stripMargin

    val csv = writeCsv("subscriber_id", "S1")
    val df = FileSource.read(spark, conf(csv, schema))
    assert(df.columns.toSet == Set("subscriber_id", "middle_name", "status", "source_file"))
    val row = df.select("middle_name", "status").collect().head
    assert(row.isNullAt(0))
    assert(row.getString(1) == "UNKNOWN")
  }

  test("required column is never auto-added: missing required still fails") {
    val schema =
      """schema { version = "1.0", columns = [
        |  { name = "subscriber_id" },
        |  { name = "hios_id" }
        |]}""".stripMargin
    val csv = writeCsv("subscriber_id", "S1")
    val ex = intercept[SchemaContractViolationException] {
      FileSource.read(spark, conf(csv, schema))
    }
    assert(ex.getMessage.contains("HDR_001") || ex.getMessage.contains("hios_id"))
  }

  test("require_content_validation without content_validation enabled is a config error") {
    val schema =
      s"""schema { version = "1.0", $baseColumns
         |  header_validation {
         |    strategy = "NAME_ALIAS_POSITION_FALLBACK"
         |    positional_fallback { enabled = true, require_content_validation = true }
         |  }
         |}""".stripMargin
    val csv = writeCsv("mystery_a,mystery_b", "S1,H1")
    val ex = intercept[IllegalArgumentException] {
      FileSource.read(spark, conf(csv, schema))
    }
    assert(ex.getMessage.contains("HDR_006"))
  }
}
