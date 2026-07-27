package com.hcsc.generic.ingest.jdbc.auth

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicInteger

/**
  * ConjurClient against an in-process HTTP stub: URL encoding, header
  * formats, error mapping, transient-only retry, token caching with single
  * re-authentication, blank-secret rejection and credential redaction.
  */
class ConjurClientTest extends AnyFunSuite with BeforeAndAfterAll {

  private var server: HttpServer = _
  private var port: Int = 0

  // Stub behavior knobs (reset per test via fresh()).
  private val authCalls = new AtomicInteger(0)
  private val secretCalls = new AtomicInteger(0)
  @volatile private var authStatus = 200
  @volatile private var tokenBody: () => String = () => "tok-1"
  @volatile private var secretStatus = 200
  @volatile private var secretValue = "s3cret-value"
  @volatile private var secretFailuresRemaining = 0
  @volatile private var acceptOnlyToken: Option[String] = None
  @volatile private var lastAuthRawPath = ""
  @volatile private var lastSecretRawPath = ""
  @volatile private var lastAuthBody = ""
  @volatile private var lastAuthHeader = ""

  override def beforeAll(): Unit = {
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0)
    server.createContext("/", new HttpHandler {
      override def handle(exchange: HttpExchange): Unit = {
        val rawPath = exchange.getRequestURI.getRawPath
        if (rawPath.startsWith("/authn/")) {
          authCalls.incrementAndGet()
          lastAuthRawPath = rawPath
          lastAuthBody = new String(readAll(exchange), StandardCharsets.UTF_8)
          if (authStatus != 200) respond(exchange, authStatus, "authentication refused")
          else respond(exchange, 200, tokenBody())
        } else if (rawPath.startsWith("/secrets/")) {
          secretCalls.incrementAndGet()
          lastSecretRawPath = rawPath
          lastAuthHeader = Option(exchange.getRequestHeaders.getFirst("Authorization")).getOrElse("")
          if (secretFailuresRemaining > 0) {
            secretFailuresRemaining -= 1
            respond(exchange, 503, "temporarily unavailable")
          } else if (acceptOnlyToken.exists(t => lastAuthHeader != s"""Token token="$t""""))
            respond(exchange, 401, "token expired")
          else if (secretStatus != 200) respond(exchange, secretStatus, "secret error")
          else respond(exchange, 200, secretValue)
        } else respond(exchange, 404, "unknown path")
      }
    })
    server.start()
    port = server.getAddress.getPort
  }

  override def afterAll(): Unit = if (server != null) server.stop(0)

  private def readAll(exchange: HttpExchange): Array[Byte] = {
    val in = exchange.getRequestBody
    try {
      val out = new java.io.ByteArrayOutputStream()
      val buf = new Array[Byte](4096)
      var n = in.read(buf)
      while (n != -1) { out.write(buf, 0, n); n = in.read(buf) }
      out.toByteArray
    } finally in.close()
  }

  private def respond(exchange: HttpExchange, status: Int, body: String): Unit = {
    val bytes = body.getBytes(StandardCharsets.UTF_8)
    exchange.sendResponseHeaders(status, if (bytes.isEmpty) -1 else bytes.length)
    if (bytes.nonEmpty) {
      val out = exchange.getResponseBody
      try out.write(bytes) finally out.close()
    }
    exchange.close()
  }

  private def fresh(): Unit = {
    ConjurClient.clearTokenCache()
    authCalls.set(0); secretCalls.set(0)
    authStatus = 200
    tokenBody = () => "tok-1"
    secretStatus = 200
    secretValue = "s3cret-value"
    secretFailuresRemaining = 0
    acceptOnlyToken = None
  }

  private def client(hostId: String = "data/ingest", maxAttempts: Int = 3) =
    new ConjurClient(ConjurConfig(
      baseUrl = s"http://localhost:$port",
      account = "myorg",
      hostId = hostId,
      apiKey = "api-key-123",
      maxAttempts = maxAttempts,
      initialBackoffMs = 1L,
      maxBackoffMs = 5L))

  // ---------------------------------------------------------------------------

  test("happy path: encoded login and variable paths, api key body, token header") {
    fresh()
    val result = client().getSecret("db/password")
    assert(result == Right("s3cret-value"))

    assert(lastAuthRawPath == "/authn/myorg/host%2Fdata%2Fingest/authenticate")
    assert(lastSecretRawPath == "/secrets/myorg/variable/db%2Fpassword")
    assert(lastAuthBody == "api-key-123")
    assert(lastAuthHeader == """Token token="tok-1"""")
  }

  test("access token is cached: a second fetch does not re-authenticate") {
    fresh()
    assert(client().getSecret("v1").isRight)
    assert(client().getSecret("v2").isRight)
    assert(authCalls.get() == 1)
    assert(secretCalls.get() == 2)
  }

  test("getUsernamePassword resolves both variables with one authentication") {
    fresh()
    val result = client(hostId = "up/host").getUsernamePassword("db/user", "db/pwd")
    assert(result == Right(UsernamePassword("s3cret-value", "s3cret-value")))
    assert(authCalls.get() == 1)
  }

  test("authentication failures map to ConjurAuthenticationError") {
    fresh(); authStatus = 401
    client(hostId = "auth/fail").getSecret("v") match {
      case Left(_: ConjurAuthenticationError) =>
      case other => fail(s"expected ConjurAuthenticationError, got $other")
    }
  }

  test("an empty access token is rejected") {
    fresh(); tokenBody = () => "  "
    client(hostId = "auth/empty").getSecret("v") match {
      case Left(e: ConjurAuthenticationError) => assert(e.message.contains("empty"))
      case other => fail(s"expected empty-token error, got $other")
    }
  }

  test("403 maps to authorization error, 404 to not-found, blank secret rejected") {
    fresh(); secretStatus = 403
    assert(client(hostId = "h403").getSecret("v").left.get.isInstanceOf[ConjurAuthorizationError])

    fresh(); secretStatus = 404
    client(hostId = "h404").getSecret("missing/var") match {
      case Left(e: ConjurSecretNotFoundError) => assert(e.message.contains("missing/var"))
      case other => fail(s"expected not-found, got $other")
    }

    fresh(); secretValue = "   "
    client(hostId = "hblank").getSecret("blank/var") match {
      case Left(e: ConjurEmptySecretError) => assert(e.message.contains("blank/var"))
      case other => fail(s"expected empty-secret error, got $other")
    }
  }

  test("503 responses retry and then succeed") {
    fresh(); secretFailuresRemaining = 2
    assert(client(hostId = "retry/ok").getSecret("v") == Right("s3cret-value"))
    assert(secretCalls.get() == 3)
  }

  test("exhausted retries surface the last status") {
    fresh(); secretFailuresRemaining = 10
    client(hostId = "retry/gone", maxAttempts = 2).getSecret("v") match {
      case Left(e: ConjurHttpError) => assert(e.statusCode == 503)
      case other => fail(s"expected http error, got $other")
    }
    assert(secretCalls.get() == 2, "exactly maxAttempts requests")
  }

  test("client errors (400) are not retried") {
    fresh(); secretStatus = 400
    client(hostId = "h400").getSecret("v") match {
      case Left(e: ConjurHttpError) => assert(e.statusCode == 400)
      case other => fail(s"expected http error, got $other")
    }
    assert(secretCalls.get() == 1)
  }

  test("a server-side-expired cached token triggers exactly one re-authentication") {
    fresh()
    var counter = 0
    tokenBody = () => { counter += 1; s"tok-$counter" }

    acceptOnlyToken = Some("tok-1")
    assert(client(hostId = "exp/host").getSecret("v") == Right("s3cret-value"))

    // Simulate server-side expiry: only a fresh token is now accepted.
    acceptOnlyToken = Some("tok-2")
    assert(client(hostId = "exp/host").getSecret("v") == Right("s3cret-value"))
    assert(authCalls.get() == 2, "one initial auth + one re-auth")
  }

  test("DNS failures are terminal, not retried") {
    ConjurClient.clearTokenCache()
    val bad = new ConjurClient(ConjurConfig(
      "https://nonexistent-host-zz.invalid", "a", "h", "k",
      connectTimeoutMs = 2000, readTimeoutMs = 2000, initialBackoffMs = 1L))
    bad.getSecret("v") match {
      case Left(_: ConjurNetworkError) =>
      case other => fail(s"expected network error, got $other")
    }
  }

  test("non-http(s) urls are rejected outright") {
    intercept[IllegalArgumentException] {
      new ConjurClient(ConjurConfig("ftp://vault.example.com", "a", "h", "k"))
    }
  }

  test("credential material never reaches toString") {
    val config = ConjurConfig("https://conjur.example.com", "myorg", "h", "api-key-123")
    assert(!config.toString.contains("api-key-123"))
    assert(config.toString.contains("myorg"))

    val credentials = UsernamePassword("svc_ingest", "hunter2")
    assert(!credentials.toString.contains("hunter2"))
    assert(credentials.toString.contains("svc_ingest"))
  }
}
