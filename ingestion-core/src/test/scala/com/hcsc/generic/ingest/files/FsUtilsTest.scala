package com.hcsc.generic.ingest.files

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path => JPath}

class FsUtilsTest extends AnyFunSuite with BeforeAndAfterAll {

  private var tempDir: JPath = _
  private var fs: FileSystem = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    tempDir = Files.createTempDirectory("fsutils-test")
    fs = FileSystem.getLocal(new Configuration())
  }

  override def afterAll(): Unit = {
    import scala.collection.JavaConverters._
    Files.walk(tempDir).iterator().asScala.toSeq.reverse.foreach(p => Files.deleteIfExists(p))
    super.afterAll()
  }

  private def write(dir: String, name: String, content: String): Path = {
    val d = tempDir.resolve(dir)
    Files.createDirectories(d)
    val f = d.resolve(name)
    Files.write(f, content.getBytes(StandardCharsets.UTF_8))
    new Path(f.toUri)
  }

  test("moveToDir moves a file into the destination directory") {
    val src = write("landing1", "member.csv", "a,b\n1,2\n")
    val dest = FsUtils.moveToDir(fs, src, new Path(tempDir.resolve("processed1").toUri))
    assert(dest.getName == "member.csv")
    assert(fs.exists(dest))
    assert(!fs.exists(src))
  }

  test("moveToDir never overwrites an existing same-named file") {
    val existing = write("processed2", "member.csv", "ORIGINAL\n")
    val src = write("landing2", "member.csv", "REDELIVERED\n")

    val dest = FsUtils.moveToDir(fs, src, new Path(tempDir.resolve("processed2").toUri))

    // The re-delivered file lands under a suffixed name; the original survives
    assert(dest.getName == "member.csv.1")
    assert(fs.exists(dest))
    assert(FsUtils.readFirstLine(fs, existing).contains("ORIGINAL"))
    assert(FsUtils.readFirstLine(fs, dest).contains("REDELIVERED"))

    // A third delivery picks the next free suffix
    val src2 = write("landing2", "member.csv", "THIRD\n")
    val dest2 = FsUtils.moveToDir(fs, src2, new Path(tempDir.resolve("processed2").toUri))
    assert(dest2.getName == "member.csv.2")
    assert(FsUtils.readFirstLine(fs, dest2).contains("THIRD"))
  }

  test("moveToDir returns the file unchanged when it is already in place") {
    val src = write("inprogress3", "member.csv", "a,b\n")
    val dest = FsUtils.moveToDir(fs, src, new Path(tempDir.resolve("inprogress3").toUri))
    assert(fs.makeQualified(dest) == fs.makeQualified(src))
    assert(fs.exists(src))
  }
}
