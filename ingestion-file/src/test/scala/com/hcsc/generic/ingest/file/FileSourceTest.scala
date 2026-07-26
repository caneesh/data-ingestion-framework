package com.hcsc.generic.ingest.file

import com.typesafe.config.ConfigFactory
import java.nio.file.{Files, Path}
import java.nio.charset.StandardCharsets
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll

class FileSourceTest extends AnyFunSuite with SharedSparkSession with BeforeAndAfterAll {

  private var tempDir: Path = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    tempDir = Files.createTempDirectory("filesource-test")
  }

  override def afterAll(): Unit = {
    // Clean up temp files
    if (tempDir != null) {
      import scala.collection.JavaConverters._
      Files.walk(tempDir).iterator().asScala.toSeq.reverse.foreach(Files.deleteIfExists)
    }
    super.afterAll()
  }

  private def writeCsv(filename: String, content: String): Path = {
    val file = tempDir.resolve(filename)
    Files.write(file, content.getBytes(StandardCharsets.UTF_8))
    file
  }

  // ---------------------------------------------------------------------------
  // header=true: reads CSV and normalizes headers
  // ---------------------------------------------------------------------------

  test("read with header=true reads data and normalizes column names") {
    val file = writeCsv("basic.csv",
      "First Name,Last Name,Age\nAlice,Smith,30\nBob,Jones,25")
    val conf = ConfigFactory.parseString(
      s"""path = "${file.toUri.toString}", header = true""")
    val df = FileSource.read(spark, conf)
    val cols = df.columns.toSet
    assert(cols.contains("first_name"))
    assert(cols.contains("last_name"))
    assert(cols.contains("age"))
    assert(df.count() == 2)
  }

  test("read adds source_file column") {
    val file = writeCsv("sf.csv", "id\n1\n2")
    val conf = ConfigFactory.parseString(
      s"""path = "${file.toUri.toString}", header = true""")
    val df = FileSource.read(spark, conf)
    assert(df.columns.contains("source_file"))
  }

  // ---------------------------------------------------------------------------
  // header_aliases
  // ---------------------------------------------------------------------------

  test("header_aliases renames vendor headers to target names") {
    val file = writeCsv("alias.csv", "MemberID,PlanCode\nS001,H123\nS002,H456")
    val conf = ConfigFactory.parseString(
      s"""
         |path = "${file.toUri.toString}"
         |header = true
         |header_aliases { MemberID = member_id, PlanCode = plan_code }
         |""".stripMargin)
    val df = FileSource.read(spark, conf)
    val cols = df.columns.toSet
    assert(cols.contains("member_id"))
    assert(cols.contains("plan_code"))
    assert(!cols.contains("MemberID"))
    assert(!cols.contains("PlanCode"))
  }

  // ---------------------------------------------------------------------------
  // force_columns_by_position
  // ---------------------------------------------------------------------------

  test("force_columns_by_position=true renames columns positionally") {
    // No header row in the file; columns assigned by position
    val file = writeCsv("pos.csv", "S001,H123\nS002,H456")
    val conf = ConfigFactory.parseString(
      s"""
         |path = "${file.toUri.toString}"
         |header = false
         |columns = ["subscriber_id", "hios_id"]
         |force_columns_by_position = true
         |""".stripMargin)
    val df = FileSource.read(spark, conf)
    val cols = df.columns.toSet
    assert(cols.contains("subscriber_id"))
    assert(cols.contains("hios_id"))
    assert(df.count() == 2)
  }

  test("force_columns_by_position=true throws when column counts mismatch") {
    val file = writeCsv("mismatch.csv", "A,B,C\n1,2,3")
    val conf = ConfigFactory.parseString(
      s"""
         |path = "${file.toUri.toString}"
         |header = true
         |columns = ["x", "y"]
         |force_columns_by_position = true
         |""".stripMargin)
    intercept[IllegalArgumentException] {
      FileSource.read(spark, conf)
    }
  }

  // ---------------------------------------------------------------------------
  // columns with name-based matching (force_columns_by_position=false)
  // ---------------------------------------------------------------------------

  test("configured columns with name-based matching succeeds when all columns present") {
    val file = writeCsv("names.csv", "name,age\nAlice,30")
    val conf = ConfigFactory.parseString(
      s"""
         |path = "${file.toUri.toString}"
         |header = true
         |columns = ["name", "age"]
         |force_columns_by_position = false
         |""".stripMargin)
    val df = FileSource.read(spark, conf)
    assert(df.columns.contains("name"))
    assert(df.columns.contains("age"))
  }

  test("configured columns with name-based matching throws when required column missing") {
    val file = writeCsv("missing_col.csv", "name\nAlice")
    val conf = ConfigFactory.parseString(
      s"""
         |path = "${file.toUri.toString}"
         |header = true
         |columns = ["name", "missing_column"]
         |force_columns_by_position = false
         |""".stripMargin)
    intercept[IllegalArgumentException] {
      FileSource.read(spark, conf)
    }
  }

  // ---------------------------------------------------------------------------
  // skip_first_n
  // ---------------------------------------------------------------------------

  test("skip_first_n drops N rows per file") {
    // 3 data rows; skip 1 => 2 remain
    val file = writeCsv("skip.csv", "id\n10\n20\n30")
    val conf = ConfigFactory.parseString(
      s"""
         |path = "${file.toUri.toString}"
         |header = true
         |skip_first_n = 1
         |""".stripMargin)
    val df = FileSource.read(spark, conf)
    assert(df.count() == 2)
  }

  // ---------------------------------------------------------------------------
  // drop_trailer with trailer.marker
  // ---------------------------------------------------------------------------

  test("drop_trailer with trailer.marker filters rows starting with marker") {
    val file = writeCsv("trailer.csv",
      "id,name\n1,Alice\n2,Bob\nTRAILER,99999")
    val conf = ConfigFactory.parseString(
      s"""
         |path = "${file.toUri.toString}"
         |header = true
         |drop_trailer = true
         |trailer.marker = "TRAILER"
         |""".stripMargin)
    val df = FileSource.read(spark, conf)
    // The TRAILER row should be excluded
    import spark.implicits._
    val ids = df.select("id").as[String].collect().toSet
    assert(!ids.contains("TRAILER"))
    assert(ids.contains("1"))
    assert(ids.contains("2"))
  }

  // ---------------------------------------------------------------------------
  // normalizeHeaders collision
  // ---------------------------------------------------------------------------

  test("normalizeHeaders throws IllegalArgumentException on header collision") {
    // "First Name" and "first_name" both normalize to "first_name"
    val file = writeCsv("collision.csv", "First Name,first_name\nA,B")
    val conf = ConfigFactory.parseString(
      s"""path = "${file.toUri.toString}", header = true""")
    intercept[IllegalArgumentException] {
      FileSource.read(spark, conf)
    }
  }
}
