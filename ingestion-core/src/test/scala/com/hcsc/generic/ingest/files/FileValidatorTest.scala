package com.hcsc.generic.ingest.files

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path => JPath}

class FileValidatorTest extends AnyFunSuite with BeforeAndAfterAll {

  private var tempDir: JPath = _
  private var fs: FileSystem = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    tempDir = Files.createTempDirectory("file-validator-test")
    fs = FileSystem.getLocal(new Configuration())
  }

  override def afterAll(): Unit = {
    import scala.collection.JavaConverters._
    Files.walk(tempDir).iterator().asScala.toSeq.reverse.foreach(p => Files.deleteIfExists(p))
    super.afterAll()
  }

  private def write(name: String, bytes: Array[Byte]): Path = {
    val f = tempDir.resolve(name)
    Files.write(f, bytes)
    new Path(f.toUri)
  }

  private def write(name: String, content: String): Path =
    write(name, content.getBytes(StandardCharsets.UTF_8))

  private def validator(hocon: String): FileValidator =
    new FileValidator(Some(ConfigFactory.parseString(hocon)))

  private def failuresOf(v: FileValidator, file: Path): Seq[String] =
    v.validate(fs, fs.getFileStatus(file))

  test("no validation config accepts everything") {
    val file = write("anything.xyz", "data")
    assert(new FileValidator(None).validate(fs, fs.getFileStatus(file)).isEmpty)
  }

  test("filename pattern mismatch is reported") {
    val file = write("wrong_name.csv", "a,b\n1,2\n")
    val fails = failuresOf(validator("""filename_pattern = "member_.*[.]csv" """), file)
    assert(fails.exists(_.contains("does not match pattern")))
  }

  test("extension whitelist is enforced") {
    val file = write("member_1.dat", "a,b\n1,2\n")
    val fails = failuresOf(validator("""allowed_extensions = ["csv", "txt"]"""), file)
    assert(fails.exists(_.contains("not in allowed extensions")))
  }

  test("minimum size is enforced") {
    val file = write("member_2.csv", "")
    val fails = failuresOf(validator("min_size_bytes = 1"), file)
    assert(fails.exists(_.contains("below minimum")))
  }

  test("maximum size is enforced") {
    val file = write("member_3.csv", "a,b\n" + ("x,y\n" * 100))
    val fails = failuresOf(validator("max_size_bytes = 10"), file)
    assert(fails.exists(_.contains("above maximum")))
  }

  test("invalid encoding is reported") {
    // 0xC3 0x28 is an invalid UTF-8 sequence
    val file = write("member_4.csv", Array[Byte]('a', ',', 'b', '\n', 0xC3.toByte, 0x28, '\n'))
    val fails = failuresOf(validator("""encoding = "UTF-8" """), file)
    assert(fails.exists(_.contains("not valid UTF-8")))
  }

  test("checksum sidecar mismatch is reported and match passes") {
    val file = write("member_5.csv", "a,b\n1,2\n")
    val actual = FsUtils.checksum(fs, file)

    write("member_5.csv.sha256", "deadbeef")
    val v = validator("""checksum { sidecar_suffix = ".sha256" }""")
    assert(failuresOf(v, file).exists(_.contains("Checksum mismatch")))

    write("member_5.csv.sha256", actual)
    assert(failuresOf(v, file).isEmpty)
  }

  test("required checksum sidecar missing is reported") {
    val file = write("member_6.csv", "a,b\n1,2\n")
    val fails = failuresOf(validator("""checksum { required = true }"""), file)
    assert(fails.exists(_.contains("sidecar")))
  }

  test("header column count and names are validated") {
    val file = write("member_7.csv", "subscriber_id,hios_id\nS1,H1\n")

    assert(failuresOf(validator("header { expected_columns = 2 }"), file).isEmpty)
    assert(failuresOf(validator("header { expected_columns = 3 }"), file)
      .exists(_.contains("column count")))
    assert(failuresOf(validator("""header { expected_names = ["subscriber_id", "hios_id"] }"""), file).isEmpty)
    assert(failuresOf(validator("""header { expected_names = ["a", "b"] }"""), file)
      .exists(_.contains("do not match expected")))
  }

  test("required trailer marker is validated") {
    val noTrailer = write("member_8.csv", "a,b\n1,2\n")
    val withTrailer = write("member_9.csv", "a,b\n1,2\nTRL|2\n")
    val v = validator("""trailer { required = true, marker = "TRL" }""")

    assert(failuresOf(v, noTrailer).exists(_.contains("trailer marker")))
    assert(failuresOf(v, withTrailer).isEmpty)
  }

  test("multiple failures are all reported") {
    val file = write("bad.dat", "")
    val v = validator(
      """
        |filename_pattern = "member_.*[.]csv"
        |allowed_extensions = ["csv"]
        |min_size_bytes = 1
      """.stripMargin)
    assert(failuresOf(v, file).size == 3)
  }
}
