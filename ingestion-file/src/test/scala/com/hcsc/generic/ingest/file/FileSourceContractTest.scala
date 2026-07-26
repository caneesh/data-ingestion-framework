package com.hcsc.generic.ingest.file

import com.hcsc.generic.ingest.schema.SchemaContractViolationException
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Path}

class FileSourceContractTest extends AnyFunSuite with SharedSparkSession {

  private def writeCsv(lines: String*): Path = {
    val dir = Files.createTempDirectory("filesource-contract-test")
    val file = dir.resolve("data.csv")
    Files.write(file, lines.mkString("\n").getBytes("UTF-8"))
    file
  }

  private def sourceConf(path: Path, extra: String = ""): Config =
    ConfigFactory.parseString(
      s"""
         |type = "file"
         |path = "${path.toString}"
         |file_type = "csv"
         |header = true
         |$extra
         |schema {
         |  version = "1.0"
         |  columns = [
         |    { name = "subscriber_id", type = "string", nullable = false,
         |      aliases = ["subscriber id", "subscriberid"], position = 0 },
         |    { name = "hios_id", type = "string", nullable = false,
         |      aliases = ["plan_hios_id", "hios id"], position = 1 }
         |  ]
         |}
      """.stripMargin
    )

  test("contract resolves exact headers and reads data") {
    val csv = writeCsv("subscriber_id,hios_id", "S001,H123", "S002,H456")
    val df = FileSource.read(spark, sourceConf(csv))
    assert(df.columns.toSet == Set("subscriber_id", "hios_id", "source_file"))
    assert(df.count() == 2)
  }

  test("contract renames aliased vendor headers to canonical names") {
    val csv = writeCsv("subscriber id,plan_hios_id", "S001,H123")
    val df = FileSource.read(spark, sourceConf(csv))
    assert(df.columns.toSet == Set("subscriber_id", "hios_id", "source_file"))
    assert(df.select("subscriber_id").collect().map(_.getString(0)).toSeq == Seq("S001"))
  }

  test("missing required column fails under default FAIL policy") {
    val csv = writeCsv("subscriber_id", "S001")
    val ex = intercept[SchemaContractViolationException] {
      FileSource.read(spark, sourceConf(csv))
    }
    assert(ex.getMessage.contains("hios_id"))
  }

  test("unknown extra column only warns under default WARN policy") {
    val csv = writeCsv("subscriber_id,hios_id,mystery", "S001,H123,x")
    val df = FileSource.read(spark, sourceConf(csv))
    assert(df.columns.contains("mystery"))
    assert(df.count() == 1)
  }

  test("extra column fails when policy is escalated to FAIL") {
    val csv = writeCsv("subscriber_id,hios_id,mystery", "S001,H123,x")
    val strict = ConfigFactory.parseString("""schema { on_extra_column = "FAIL" }""")
      .withFallback(sourceConf(csv))
    val ex = intercept[SchemaContractViolationException] {
      FileSource.read(spark, strict)
    }
    assert(ex.getMessage.contains("mystery"))
  }

  test("duplicate headers mapping to one contract column fail") {
    val csv = writeCsv("subscriber_id,subscriber id,hios_id", "S001,S001,H123")
    intercept[SchemaContractViolationException] {
      FileSource.read(spark, sourceConf(csv))
    }
  }

  test("positional recovery maps headerless files by contract position") {
    val csv = writeCsv("S001,H123", "S002,H456")
    val conf = ConfigFactory.parseString("header = false")
      .withFallback(sourceConf(csv))
    val df = FileSource.read(spark, conf)
    assert(df.columns.toSet == Set("subscriber_id", "hios_id", "source_file"))
    assert(df.count() == 2)
  }

  test("positional recovery fails on column count mismatch") {
    val csv = writeCsv("S001,H123,extra")
    val conf = ConfigFactory.parseString("header = false")
      .withFallback(sourceConf(csv))
    intercept[IllegalArgumentException] {
      FileSource.read(spark, conf)
    }
  }
}
