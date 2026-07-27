package com.hcsc.generic.ingest.jdbc.auth

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicInteger

/** Conjur hardening: plain-http rejection (explicit opt-in only) and
  * per-host-identity secret caching. */
class ConjurHardeningTest extends AnyFunSuite with BeforeAndAfterAll {

  private var server: HttpServer = _
  private var port: Int = 0
  private val secretCalls = new AtomicInteger(0)

  override def beforeAll(): Unit = {
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0)
    server.createContext("/", new HttpHandler {
      override def handle(exchange: HttpExchange): Unit = {
        val path = exchange.getRequestURI.getRawPath
        val body =
          if (path.startsWith("/authn/")) "stub-token"
          else { secretCalls.incrementAndGet(); "resolved-secret" }
        val bytes = body.getBytes(StandardCharsets.UTF_8)
        exchange.sendResponseHeaders(200, bytes.length)
        val out = exchange.getResponseBody
        try out.write(bytes) finally out.close()
        exchange.close()
      }
    })
    server.start()
    port = server.getAddress.getPort
  }

  override def afterAll(): Unit = if (server != null) server.stop(0)

  private def ref(hostId: String, extra: String = "allow_insecure_http = true") =
    ConfigFactory.parseString(
      s"""provider = "conjur"
         |url = "http://localhost:$port"
         |account = "myorg"
         |host_id = "$hostId"
         |variable = "shared/db/password"
         |api_key = "dev-key"
         |$extra""".stripMargin)

  test("plain http is rejected without the explicit opt-in") {
    val ex = intercept[IllegalArgumentException] {
      SecretProviders.resolveRef(ref("data/ingestion", extra = ""))
    }
    assert(ex.getMessage.contains("JDBC_003"))
    assert(ex.getMessage.contains("allow_insecure_http"))
    assert(ex.getMessage.contains("cleartext"))
  }

  test("allow_insecure_http = true permits http for isolated dev setups") {
    ConjurSecretProvider.clearCache()
    assert(SecretProviders.resolveRef(ref("data/ingestion")) == "resolved-secret")
  }

  test("the secret cache is scoped per host identity, not shared across identities") {
    ConjurSecretProvider.clearCache()
    secretCalls.set(0)

    // Identity A resolves and caches the variable.
    SecretProviders.resolveRef(ref("identity/a"))
    assert(secretCalls.get() == 1)

    // Identity B reading the SAME variable must pass its own authorization —
    // a shared cache entry would hand it A's value without any Conjur check.
    SecretProviders.resolveRef(ref("identity/b"))
    assert(secretCalls.get() == 2, "different host identity requires its own fetch")

    // Identity A again: served from its own cache entry.
    SecretProviders.resolveRef(ref("identity/a"))
    assert(secretCalls.get() == 2)
  }
}
