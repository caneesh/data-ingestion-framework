package com.hcsc.generic.ingest.files

import com.hcsc.generic.ingest.audit.AuditService
import com.hcsc.generic.ingest.runtime.RunContext
import com.hcsc.generic.ingest.transform.SharedSparkSession
import com.typesafe.config.ConfigFactory
import org.apache.log4j.Logger
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path => JPath}

class FileIntakeServiceTest extends AnyFunSuite with SharedSparkSession with BeforeAndAfterEach {

  private val logger = Logger.getLogger(getClass.getName)
  private var root: JPath = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    root = Files.createTempDirectory("file-intake-test")
    Seq("landing", "inprogress", "processed", "archive", "quarantine")
      .foreach(d => Files.createDirectories(root.resolve(d)))
  }

  private def writeLanding(name: String, content: String): JPath = {
    val f = root.resolve("landing").resolve(name)
    Files.write(f, content.getBytes(StandardCharsets.UTF_8))
    f
  }

  private def listNames(dir: String): Set[String] = {
    import scala.collection.JavaConverters._
    Files.list(root.resolve(dir)).iterator().asScala
      .map(_.getFileName.toString)
      .filterNot(n => n.startsWith(".") || n.startsWith("_"))
      .toSet
  }

  private def sourceConf(withArchive: Boolean = false) = ConfigFactory.parseString(
    s"""
       |folders {
       |  landing = "${root.resolve("landing")}"
       |  inprogress = "${root.resolve("inprogress")}"
       |  processed = "${root.resolve("processed")}"
       |  quarantine = "${root.resolve("quarantine")}"
       |  ${if (withArchive) s"""archive = "${root.resolve("archive")}"""" else ""}
       |}
       |validation {
       |  allowed_extensions = ["csv"]
       |  min_size_bytes = 1
       |}
    """.stripMargin)

  private def service(withArchive: Boolean = false) =
    new FileIntakeService(spark, sourceConf(withArchive), None, new AuditService(spark, None), logger)

  private val ctx = RunContext("run-1", "member", "FULL", "F")

  test("feeds without folders are unmanaged and return None") {
    val svc = new FileIntakeService(
      spark, ConfigFactory.parseString("""path = "/tmp/x" """), None, new AuditService(spark, None), logger)
    assert(!svc.managed)
    assert(svc.stage(ctx).isEmpty)
  }

  test("valid files stage to inprogress; invalid files quarantine with reason") {
    writeLanding("good.csv", "a,b\n1,2\n")
    writeLanding("bad.dat", "nope")
    writeLanding("empty.csv", "")

    val staged = service().stage(ctx).get

    assert(staged.map(_.name) == Seq("good.csv"))
    assert(staged.head.checksum.nonEmpty)
    assert(listNames("inprogress") == Set("good.csv"))
    assert(listNames("quarantine") == Set("bad.dat", "empty.csv"))
    assert(listNames("landing").isEmpty)
  }

  test("complete moves staged files to processed and copies to archive") {
    writeLanding("good.csv", "a,b\n1,2\n")
    val svc = service(withArchive = true)
    val staged = svc.stage(ctx).get

    svc.complete(ctx, staged)

    assert(listNames("processed") == Set("good.csv"))
    assert(listNames("archive") == Set("good.csv"))
    assert(listNames("inprogress").isEmpty)
  }

  test("restart-safe: leftover inprogress files from a crashed run are re-staged") {
    Files.write(root.resolve("inprogress").resolve("leftover.csv"),
      "a,b\n1,2\n".getBytes(StandardCharsets.UTF_8))

    val staged = service().stage(ctx).get
    assert(staged.map(_.name) == Seq("leftover.csv"))
  }

  test("dry-run validates and reports but moves nothing") {
    writeLanding("good.csv", "a,b\n1,2\n")
    writeLanding("bad.dat", "nope")

    val staged = service().stage(ctx.copy(dryRun = true)).get

    assert(staged.map(_.name) == Seq("good.csv"))
    assert(listNames("landing") == Set("good.csv", "bad.dat"))
    assert(listNames("inprogress").isEmpty)
    assert(listNames("quarantine").isEmpty)
  }

  test("checksum sidecar files are not treated as data files") {
    writeLanding("good.csv", "a,b\n1,2\n")
    writeLanding("good.csv.sha256", "abc123")

    val staged = service().stage(ctx).get
    assert(staged.map(_.name) == Seq("good.csv"))
  }
}
