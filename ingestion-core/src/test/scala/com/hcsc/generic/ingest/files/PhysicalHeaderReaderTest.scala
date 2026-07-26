package com.hcsc.generic.ingest.files

import com.hcsc.generic.ingest.schema.ViolationKind
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Path => JPath}

class PhysicalHeaderReaderTest extends AnyFunSuite with BeforeAndAfterAll {

  private var tempDir: JPath = _
  private var fs: FileSystem = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    tempDir = Files.createTempDirectory("physical-header-test")
    fs = FileSystem.getLocal(new Configuration())
  }

  override def afterAll(): Unit = {
    import scala.collection.JavaConverters._
    Files.walk(tempDir).iterator().asScala.toSeq.reverse.foreach(p => Files.deleteIfExists(p))
    super.afterAll()
  }

  private var counter = 0
  private def write(content: String): Path = {
    counter += 1
    val f = tempDir.resolve(s"file_$counter.csv")
    Files.write(f, content.getBytes("UTF-8"))
    new Path(f.toUri)
  }

  private def read(content: String, delimiter: Char = ','): PhysicalHeader =
    PhysicalHeaderReader.read(fs, write(content), delimiter, '"', '\\', "UTF-8")

  test("normal CSV header parses with data-row detection") {
    val h = read("subscriber_id,hios_id\nS1,H1\n")
    assert(h.fields == Seq("subscriber_id", "hios_id"))
    assert(h.hasDataRows)
    assert(h.issues.isEmpty)
  }

  test("quoted headers with delimiters inside quotes parse correctly") {
    val h = read("\"Subscriber ID\",\"Plan, HIOS ID\"\nS1,H1\n")
    assert(h.fields == Seq("Subscriber ID", "Plan, HIOS ID"))
    assert(h.issues.isEmpty)
  }

  test("doubled quotes inside quoted headers are unescaped") {
    val h = read("\"He said \"\"hi\"\"\",b\nx,y\n")
    assert(h.fields == Seq("He said \"hi\"", "b"))
  }

  test("trailing delimiter produces a blank header field (HDR_010)") {
    val h = read("a,b,\nx,y,z\n")
    assert(h.issues.exists(_.kind == ViolationKind.BlankHeader))
  }

  test("consecutive delimiters produce a blank header field (HDR_010)") {
    val h = read("a,,b\nx,y,z\n")
    assert(h.issues.exists(_.kind == ViolationKind.BlankHeader))
  }

  test("duplicate physical headers are detected before Spark renames them (HDR_002)") {
    val h = read("hios_id,hios_id\nx,y\n")
    assert(h.issues.exists(v => v.kind == ViolationKind.DuplicatePhysicalHeader && v.toString.contains("HDR_002")))
  }

  test("malformed (unterminated) quote is detected (HDR_011)") {
    val h = read("\"unclosed,b\nx,y\n")
    assert(h.issues.exists(_.kind == ViolationKind.MalformedQuote))
  }

  test("suspected delimiter mismatch is detected (HDR_009)") {
    val h = read("subscriber_id|hios_id|exchange\nS1|H1|X\n", delimiter = ',')
    assert(h.issues.exists(_.kind == ViolationKind.DelimiterMismatch))
  }

  test("quoted single-column header containing another delimiter is NOT a mismatch") {
    val h = read("\"metric|name\"\nvalue1\n", delimiter = ',')
    assert(h.fields == Seq("metric|name"))
    assert(!h.issues.exists(_.kind == ViolationKind.DelimiterMismatch))
  }

  test("empty file and blank header line are detected (HDR_010)") {
    assert(read("").issues.exists(_.kind == ViolationKind.BlankHeader))
    assert(read("   \nx,y\n").issues.exists(_.kind == ViolationKind.BlankHeader))
  }

  test("header-only file reports no data rows") {
    val h = read("subscriber_id,hios_id\n")
    assert(h.fields.nonEmpty)
    assert(!h.hasDataRows)
  }

  test("UTF-8 BOM is stripped from the first header field") {
    val h = read("\uFEFFsubscriber_id,hios_id\nS1,H1\n")
    assert(h.fields.head == "subscriber_id")
  }

  test("unknown charset name is reported as HDR_012, not thrown") {
    val h = PhysicalHeaderReader.read(fs, write("a,b\nx,y\n"), ',', '"', '\\', "NOT-A-CHARSET")
    assert(h.issues.exists(v => v.kind == ViolationKind.UnsupportedEncoding && v.toString.contains("HDR_012")))
  }

  test("content invalid in the configured charset is reported as HDR_012, not silently replaced") {
    counter += 1
    val f = tempDir.resolve(s"file_$counter.csv")
    // 0xFF 0xFE is not valid UTF-8
    Files.write(f, Array[Byte]('a'.toByte, ','.toByte, 0xFF.toByte, 0xFE.toByte, '\n'.toByte))
    val h = PhysicalHeaderReader.read(fs, new Path(f.toUri), ',', '"', '\\', "UTF-8")
    assert(h.issues.exists(_.kind == ViolationKind.UnsupportedEncoding))
    assert(h.fields.isEmpty)
  }
}
