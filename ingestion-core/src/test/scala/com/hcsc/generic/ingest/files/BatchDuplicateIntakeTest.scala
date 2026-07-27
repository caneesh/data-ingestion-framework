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

/** Two files with identical content arriving in the SAME batch must not both
  * stage — the registry only knows about completed runs, so the batch itself
  * has to deduplicate. */
class BatchDuplicateIntakeTest extends AnyFunSuite with SharedSparkSession with BeforeAndAfterEach {

  private val logger = Logger.getLogger(getClass.getName)
  private var root: JPath = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    root = Files.createTempDirectory("batch-dup-test")
    Seq("landing", "inprogress", "processed", "quarantine")
      .foreach(d => Files.createDirectories(root.resolve(d)))
  }

  private def write(name: String, content: String): Unit =
    Files.write(root.resolve("landing").resolve(name), content.getBytes(StandardCharsets.UTF_8))

  private def listNames(dir: String): Set[String] = {
    import scala.collection.JavaConverters._
    Files.list(root.resolve(dir)).iterator().asScala
      .map(_.getFileName.toString)
      .filterNot(n => n.startsWith(".") || n.startsWith("_"))
      .toSet
  }

  private def sourceConf = ConfigFactory.parseString(
    s"""
       |folders {
       |  landing = "${root.resolve("landing")}"
       |  inprogress = "${root.resolve("inprogress")}"
       |  processed = "${root.resolve("processed")}"
       |  quarantine = "${root.resolve("quarantine")}"
       |}
    """.stripMargin)

  private val idempotency = Some(ConfigFactory.parseString("""database = "batch_dup_db""""))

  private def service =
    new FileIntakeService(spark, sourceConf, idempotency, new AuditService(spark, None), logger)

  test("identical content under two names in one batch stages once; duplicate follows the policy") {
    write("member_a.csv", "id,name\n1,alpha\n")
    write("member_b.csv", "id,name\n1,alpha\n") // same bytes, different name
    write("member_c.csv", "id,name\n2,beta\n")

    val staged = service.stage(RunContext("run-bd-1", "batch_dup_feed", "FULL", "F")).get
    assert(staged.size == 2, s"one of the identical pair must be deduplicated, got: ${staged.map(_.name)}")
    assert(staged.map(_.checksum).distinct.size == 2, "staged checksums are unique")

    // Default SKIP policy moved the duplicate straight to processed.
    val stagedNames = staged.map(_.name).toSet
    val duplicateName = Set("member_a.csv", "member_b.csv") -- stagedNames
    assert(duplicateName.size == 1)
    assert(listNames("processed") == duplicateName)
    assert(listNames("inprogress") == stagedNames)
  }
}
