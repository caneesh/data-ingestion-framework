package com.hcsc.generic.ingest.jdbc.auth

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicInteger

/** ConjurSecretProvider through the SecretProviders registry: reference
  * parsing (JDBC_003), end-to-end resolution, caching and failure codes
  * (JDBC_002). */
class ConjurSecretProviderTest extends AnyFunSuite with BeforeAndAfterAll {

  private var server: HttpServer = _
  private var port: Int = 0
  private val secretCalls = new AtomicInteger(0)
  @volatile private var secretStatus = 200

  override def beforeAll(): Unit = {
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0)
    server.createContext("/", new HttpHandler {
      override def handle(exchange: HttpExchange): Unit = {
        val path = exchange.getRequestURI.getRawPath
        val body =
          if (path.startsWith("/authn/")) "stub-token"
          else { secretCalls.incrementAndGet(); "resolved-secret" }
        val status = if (path.startsWith("/secrets/")) secretStatus else 200
        val bytes = (if (status == 200) body else "error").getBytes(StandardCharsets.UTF_8)
        exchange.sendResponseHeaders(status, bytes.length)
        val out = exchange.getResponseBody
        try out.write(bytes) finally out.close()
        exchange.close()
      }
    })
    server.start()
    port = server.getAddress.getPort
  }

  override def afterAll(): Unit = if (server != null) server.stop(0)

  private def ref(extra: String = "", apiKey: String = """api_key = { provider = "sysprop", key = "conjur.test.apikey" }""") =
    ConfigFactory.parseString(
      s"""provider = "conjur"
         |url = "http://localhost:$port"
         |account = "myorg"
         |host_id = "data/ingestion"
         |variable = "app/db/password"
         |$apiKey
         |$extra""".stripMargin)

  test("conjur is registered and listed") {
    assert(SecretProviders.availableNames.contains("conjur"))
  }

  test("resolves a variable end-to-end with the api key from a nested reference") {
    ConjurSecretProvider.clearCache()
    System.setProperty("conjur.test.apikey", "host-api-key")
    secretStatus = 200
    assert(SecretProviders.resolveRef(ref()) == "resolved-secret")
  }

  test("resolved secrets are cached for the TTL") {
    ConjurSecretProvider.clearCache()
    System.setProperty("conjur.test.apikey", "host-api-key")
    secretStatus = 200
    secretCalls.set(0)
    SecretProviders.resolveRef(ref())
    SecretProviders.resolveRef(ref())
    assert(secretCalls.get() == 1, "second resolve served from cache")

    ConjurSecretProvider.clearCache()
    SecretProviders.resolveRef(ref("""cache_ttl_ms = 0"""))
    SecretProviders.resolveRef(ref("""cache_ttl_ms = 0"""))
    assert(secretCalls.get() == 3, "cache disabled with ttl 0")
  }

  test("missing required fields fail with JDBC_003 naming the field") {
    Seq("url", "account", "host_id", "variable").foreach { field =>
      val incomplete = ref().withoutPath(field)
      val ex = intercept[IllegalArgumentException] { SecretProviders.resolveRef(incomplete) }
      assert(ex.getMessage.contains("JDBC_003") && ex.getMessage.contains(field), ex.getMessage)
    }
    val noKey = ref(apiKey = "")
    val ex = intercept[IllegalArgumentException] { SecretProviders.resolveRef(noKey) }
    assert(ex.getMessage.contains("api_key"))
  }

  test("non-http(s) urls are rejected with JDBC_003") {
    val bad = ref().withValue("url",
      com.typesafe.config.ConfigValueFactory.fromAnyRef("ftp://vault"))
    val ex = intercept[IllegalArgumentException] { SecretProviders.resolveRef(bad) }
    assert(ex.getMessage.contains("JDBC_003"))
  }

  test("retrieval failures surface as JDBC_002 with the variable name") {
    ConjurSecretProvider.clearCache()
    System.setProperty("conjur.test.apikey", "host-api-key")
    secretStatus = 404
    val ex = intercept[IllegalArgumentException] { SecretProviders.resolveRef(ref()) }
    assert(ex.getMessage.contains("JDBC_002"))
    assert(ex.getMessage.contains("app/db/password"))
    secretStatus = 200
  }

  test("an inline api_key string is accepted (dev use)") {
    ConjurSecretProvider.clearCache()
    secretStatus = 200
    assert(SecretProviders.resolveRef(ref(apiKey = """api_key = "inline-dev-key"""")) == "resolved-secret")
  }
}
