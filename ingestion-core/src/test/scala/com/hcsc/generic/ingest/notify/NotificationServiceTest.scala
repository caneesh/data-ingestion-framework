package com.hcsc.generic.ingest.notify

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import com.typesafe.config.ConfigFactory
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}
import org.apache.log4j.Logger
import org.scalatest.funsuite.AnyFunSuite

/**
  * Failure notification.
  *
  * The governing property is not "an alert is sent" but "a run's outcome is
  * never changed by alerting". A pipeline that fails because its webhook is
  * down, or that hides a data failure behind a delivery failure, is worse
  * than one that stays silent — so most of these tests are about what does
  * NOT happen.
  */
class NotificationServiceTest extends AnyFunSuite {

  private val logger = Logger.getLogger(getClass.getName)

  /** Minimal in-process HTTP sink; the JDK ships one, so no dependency. */
  private def withServer(status: Int = 200)(body: (String => Unit) => Unit): Seq[String] = {
    val received = new ConcurrentLinkedQueue[String]()
    val latch = new CountDownLatch(1)
    val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext("/hook", new HttpHandler {
      override def handle(exchange: HttpExchange): Unit = {
        val payload = scala.io.Source.fromInputStream(
          exchange.getRequestBody, StandardCharsets.UTF_8.name).mkString
        received.add(payload)
        exchange.sendResponseHeaders(status, -1)
        exchange.close()
        latch.countDown()
      }
    })
    server.start()
    try {
      body(_ => ())
      latch.await(5, TimeUnit.SECONDS)
    } finally server.stop(0)
    import scala.collection.JavaConverters._
    received.asScala.toSeq
  }

  private def url(port: Int) = s"http://127.0.0.1:$port/hook"

  private def serviceFor(hocon: String) =
    NotificationService(ConfigFactory.parseString(hocon), logger)

  test("absent notifications block leaves every existing feed silent") {
    val svc = NotificationService(ConfigFactory.parseString("audit { database = d }"), logger)
    assert(!svc.enabled, "no notifications block must mean disabled, not default-on")
    // Must not throw despite no sink configured.
    svc.notifyFailure("e", "r", "raw", "boom")
  }

  test("a failure reaches the webhook as JSON with the run's identity") {
    val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    val received = new ConcurrentLinkedQueue[String]()
    val latch = new CountDownLatch(1)
    server.createContext("/hook", new HttpHandler {
      override def handle(e: HttpExchange): Unit = {
        received.add(scala.io.Source.fromInputStream(
          e.getRequestBody, StandardCharsets.UTF_8.name).mkString)
        e.sendResponseHeaders(200, -1); e.close(); latch.countDown()
      }
    })
    server.start()
    try {
      serviceFor(s"""notifications { webhook { url = "${url(server.getAddress.getPort)}" } }""")
        .notifyFailure("claims", "run-9", "curated", "CUR_008 freshness unusable")
      assert(latch.await(5, TimeUnit.SECONDS), "webhook was not called")
      val body = received.peek()
      assert(body.contains("\"entity\":\"claims\""), body)
      assert(body.contains("\"run_id\":\"run-9\""), body)
      assert(body.contains("\"outcome\":\"FAILURE\""), body)
      assert(body.contains("\"stage\":\"curated\""), body)
      assert(body.contains("CUR_008"), s"the cause must survive into the alert: $body")
    } finally server.stop(0)
  }

  test("SUCCESS is not delivered unless asked for") {
    val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    val calls = new ConcurrentLinkedQueue[String]()
    server.createContext("/hook", new HttpHandler {
      override def handle(e: HttpExchange): Unit = {
        calls.add("x"); e.sendResponseHeaders(200, -1); e.close()
      }
    })
    server.start()
    try {
      val svc = serviceFor(s"""notifications { webhook { url = "${url(server.getAddress.getPort)}" } }""")
      svc.notifySuccess("claims", "run-9", "raw", "fine")
      Thread.sleep(300)
      assert(calls.isEmpty,
        "success on every run of every feed is how alerting gets muted; it must be opt-in")

      val optedIn = serviceFor(
        s"""notifications { on = ["SUCCESS"], webhook { url = "${url(server.getAddress.getPort)}" } }""")
      optedIn.notifySuccess("claims", "run-9", "raw", "fine")
      Thread.sleep(500)
      assert(!calls.isEmpty, "opting in must actually deliver")
    } finally server.stop(0)
  }

  test("an unreachable webhook does not fail the run") {
    // Port 1 is not listenable by a normal user: a guaranteed refusal.
    val svc = serviceFor("""notifications { webhook { url = "http://127.0.0.1:1/hook", timeout_ms = 500 } }""")
    svc.notifyFailure("claims", "run-9", "raw", "original data failure")
  }

  test("an HTTP error from the sink does not fail the run") {
    val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext("/hook", new HttpHandler {
      override def handle(e: HttpExchange): Unit = { e.sendResponseHeaders(500, -1); e.close() }
    })
    server.start()
    try {
      serviceFor(s"""notifications { webhook { url = "${url(server.getAddress.getPort)}" } }""")
        .notifyFailure("claims", "run-9", "raw", "boom")
    } finally server.stop(0)
  }

  test("a malformed url does not fail the run") {
    serviceFor("""notifications { webhook { url = "not-a-url" } }""")
      .notifyFailure("claims", "run-9", "raw", "boom")
  }

  test("credentials in an exception message are redacted before leaving the host") {
    val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    val received = new ConcurrentLinkedQueue[String]()
    val latch = new CountDownLatch(1)
    server.createContext("/hook", new HttpHandler {
      override def handle(e: HttpExchange): Unit = {
        received.add(scala.io.Source.fromInputStream(
          e.getRequestBody, StandardCharsets.UTF_8.name).mkString)
        e.sendResponseHeaders(200, -1); e.close(); latch.countDown()
      }
    })
    server.start()
    try {
      serviceFor(s"""notifications { webhook { url = "${url(server.getAddress.getPort)}" } }""")
        .notifyFailure("claims", "run-9", "raw",
          "JDBC_001 Connection to jdbc:sqlserver://h;password=SuperSecret1 failed")
      assert(latch.await(5, TimeUnit.SECONDS))
      val body = received.peek()
      assert(!body.contains("SuperSecret1"),
        s"a webhook is a wider audience than a log file; the credential must not reach it: $body")
      assert(body.contains("***"), s"expected the redaction marker: $body")
    } finally server.stop(0)
  }

  test("JSON payload escapes quotes and newlines from the exception text") {
    val event = NotificationEvent("e", "r", "FAILURE", "raw",
      "he said \"boom\"\nand a tab\there", "h")
    val json = event.toJson
    assert(!json.contains("\n"), s"a raw newline would break the payload: $json")
    assert(json.contains("\\\"boom\\\""), json)
    assert(json.contains("\\n") && json.contains("\\t"), json)
  }

  test("enabled = false silences a configured sink") {
    val svc = serviceFor("""notifications { enabled = false, webhook { url = "http://127.0.0.1:1/x" } }""")
    assert(!svc.enabled)
    svc.notifyFailure("e", "r", "raw", "boom")
  }
}
