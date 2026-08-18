package com.hcsc.generic.ingest.files

import com.hcsc.generic.ingest.audit.AuditService
import com.hcsc.generic.ingest.runtime.RunContext
import com.hcsc.generic.ingest.transform.SharedSparkSession
import com.typesafe.config.ConfigFactory
import org.apache.log4j.spi.LoggingEvent
import org.apache.log4j.{AppenderSkeleton, Level, Logger}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path => JPath}

/**
  * Duplicate protection must never be OFF by accident.
  *
  * `idempotency` is read from its own config block. Declaring
  * `registry_table` anywhere else — under `rejects { }`, which DEPLOYMENT.md
  * itself once instructed — leaves the service with no idempotency config,
  * and the code path for "no config" is indistinguishable from "deliberately
  * disabled": checksums are never computed, the duplicate policy is never
  * consulted, and every re-delivered file is re-ingested. Nothing failed, so
  * nothing surfaces until duplicate rows appear in curated days later.
  *
  * These tests pin BOTH halves: the silent re-ingestion is real (so the
  * warning is warranted), and a managed feed that hits it says so loudly
  * while a feed that opted out on purpose stays quiet.
  */
class FileIntakeIdempotencyPostureTest extends AnyFunSuite with SharedSparkSession with BeforeAndAfterEach {

  private var root: JPath = _
  private var captured: CapturingAppender = _
  private var logger: Logger = _
  /** A fresh database per test. `mvn clean` removes target/ — including the
    * derby metastore — but NOT spark-warehouse/, so a fixed name eventually
    * meets a leftover data directory with no matching catalog entry and the
    * create fails with LOCATION_ALREADY_EXISTS. Uniqueness makes the suite
    * independent of whatever earlier runs left on disk. */
  private var registryDb: String = _

  /** Collects events off the exact Logger handed to the service. */
  private final class CapturingAppender extends AppenderSkeleton {
    private val events = scala.collection.mutable.ArrayBuffer.empty[LoggingEvent]
    override def append(event: LoggingEvent): Unit = synchronized { events += event }
    override def close(): Unit = ()
    override def requiresLayout(): Boolean = false
    def messagesAt(level: Level): Seq[String] = synchronized {
      events.filter(_.getLevel == level).map(e => String.valueOf(e.getMessage)).toList
    }
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    root = Files.createTempDirectory("intake-posture")
    Seq("landing", "inprogress", "processed", "quarantine")
      .foreach(d => Files.createDirectories(root.resolve(d)))
    // A uniquely named logger per test: log4j Loggers are global singletons,
    // so a shared name would leak events between tests.
    registryDb = s"posture_dup_db_${System.nanoTime()}"
    logger = Logger.getLogger(s"intake-posture-${System.nanoTime()}")
    captured = new CapturingAppender
    logger.addAppender(captured)
  }

  override def afterEach(): Unit = {
    if (logger != null && captured != null) logger.removeAppender(captured)
    super.afterEach()
  }

  private def writeLanding(name: String, content: String): Unit =
    Files.write(root.resolve("landing").resolve(name), content.getBytes(StandardCharsets.UTF_8))

  private def sourceConf = ConfigFactory.parseString(
    s"""
       |folders {
       |  landing = "${root.resolve("landing")}"
       |  inprogress = "${root.resolve("inprogress")}"
       |  processed = "${root.resolve("processed")}"
       |  quarantine = "${root.resolve("quarantine")}"
       |}
       |validation { allowed_extensions = ["csv"], min_size_bytes = 1 }
    """.stripMargin)

  private def service(idempotency: Option[String]) =
    new FileIntakeService(
      spark, sourceConf, idempotency.map(ConfigFactory.parseString),
      new AuditService(spark, None), logger)

  private def ctx(runId: String) = RunContext(runId, "member", "FULL", "F")

  private def warnings = captured.messagesAt(Level.WARN).filter(_.contains("Duplicate protection"))
  private def infos = captured.messagesAt(Level.INFO).filter(_.contains("Duplicate protection"))

  // ---- the behaviour the warning is about -----------------------------------

  /** One full delivery: stage, then complete as a successful run would. */
  private def deliverAndComplete(svc: FileIntakeService, runId: String, content: String): Int = {
    writeLanding("member.csv", content)
    val staged = svc.stage(ctx(runId)).getOrElse(Seq.empty)
    svc.complete(ctx(runId), staged)
    staged.size
  }

  test("without an idempotency block a re-delivered file is silently re-ingested") {
    // The damage, on the realistic sequence: a run completes, the identical
    // file is delivered again, and it loads a second time. Nothing fails,
    // nothing is quarantined, nothing is logged as a duplicate — which is
    // precisely why intake has to announce the posture up front.
    val svc = service(None)
    assert(deliverAndComplete(svc, "r1", "a,b\n1,2\n") == 1)
    assert(deliverAndComplete(svc, "r2", "a,b\n1,2\n") == 1,
      "with no idempotency config the identical re-delivery stages again — " +
        "no checksum is consulted")
  }

  test("the same sequence with idempotency configured skips the re-delivery") {
    // The contrast that makes the warning meaningful: identical inputs, one
    // config block apart, opposite outcomes.
    val svc = service(Some(s"""database = "$registryDb" """))
    assert(deliverAndComplete(svc, "r1", "a,b\n1,2\n") == 1)
    assert(deliverAndComplete(svc, "r2", "a,b\n1,2\n") == 0,
      "the registered checksum must suppress the re-delivery")
  }

  // ---- and how loudly it is announced ---------------------------------------

  test("a managed feed with no idempotency block WARNS that protection is off") {
    writeLanding("member.csv", "a,b\n1,2\n")
    service(None).stage(ctx("r1"))

    assert(warnings.size == 1, s"expected exactly one posture warning, got: $warnings")
    val w = warnings.head
    assert(w.contains("RE-INGESTED"), "the consequence must be stated, not just the state")
    assert(w.contains("rejects"),
      "the message must name the wrong-block mistake it exists to catch")
    assert(w.contains("idempotency { enabled = false }"),
      "an operator who meant it needs the documented way to silence this")
  }

  test("an explicit idempotency.enabled = false is a decision, not a warning") {
    writeLanding("member.csv", "a,b\n1,2\n")
    service(Some("""enabled = false, database = "d" """)).stage(ctx("r1"))

    assert(warnings.isEmpty,
      "a recorded decision must not nag; only the accidental case warrants WARN")
    assert(infos.exists(_.contains("OFF by configuration")),
      "it still has to be visible in the log — just not as a fault")
  }

  test("protection ON reports the registry and policy it will actually use") {
    writeLanding("member.csv", "a,b\n1,2\n")
    service(Some(s"""database = "$registryDb", registry_table = "reg", duplicate_policy = "REJECT" """))
      .stage(ctx("r1"))

    assert(warnings.isEmpty)
    val i = infos.mkString(" ")
    assert(i.contains(s"$registryDb.reg"), s"the resolved table must be logged, got: $i")
    assert(i.contains("REJECT"),
      "the policy decides whether a duplicate is skipped or quarantined; log which one")
  }

  test("an unmanaged feed says nothing — there is no folder lifecycle to protect") {
    val svc = new FileIntakeService(
      spark, ConfigFactory.parseString("""path = "/tmp/x" """), None,
      new AuditService(spark, None), logger)
    assert(svc.stage(ctx("r1")).isEmpty)
    assert(warnings.isEmpty && infos.isEmpty,
      "a static-path feed has no registry and no moves; warning there would be noise")
  }
}
