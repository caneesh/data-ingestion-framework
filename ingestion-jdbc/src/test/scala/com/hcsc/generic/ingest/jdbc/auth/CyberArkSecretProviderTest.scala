package com.hcsc.generic.ingest.jdbc.auth

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentHashMap

/** Exercises the CyberArk CCP provider against a stub AIM web service. */
class CyberArkSecretProviderTest extends AnyFunSuite with BeforeAndAfterAll {

  private var server: HttpServer = _
  private var baseUrl: String = _
  /** Requests served, keyed by the Object query parameter. */
  private val hitsByObject = new ConcurrentHashMap[String, Integer]()

  override def beforeAll(): Unit = {
    super.beforeAll()
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext("/AIMWebService/api/Accounts", new HttpHandler {
      override def handle(exchange: HttpExchange): Unit = {
        val query = Option(exchange.getRequestURI.getRawQuery).getOrElse("")
        val params = query.split("&").filter(_.contains("=")).map { p =>
          val Array(k, v) = p.split("=", 2)
          k -> java.net.URLDecoder.decode(v, "UTF-8")
        }.toMap
        val objectName = params.getOrElse("Object", "")
        hitsByObject.merge(objectName, 1, (a, b) => a + b)

        val (status, body) =
          if (!params.contains("AppID"))
            (400, """{"ErrorCode":"AIMWS030E","ErrorMsg":"Invalid query format"}""")
          else if (objectName == "missing-object")
            (404, """{"ErrorCode":"APPAP004E","ErrorMsg":"Safe or Object not found"}""")
          else
            (200,
              """{"Content":"s3cr3t-pwd","UserName":"svc_ingest","Address":"db.example.com",
                |"Safe":"AAM_DB_Safe","PolicyID":"AzureSQL"}""".stripMargin)

        val bytes = body.getBytes(StandardCharsets.UTF_8)
        exchange.getResponseHeaders.set("Content-Type", "application/json")
        exchange.sendResponseHeaders(status, bytes.length)
        exchange.getResponseBody.write(bytes)
        exchange.close()
      }
    })
    server.start()
    baseUrl = s"http://127.0.0.1:${server.getAddress.getPort}"
  }

  override def afterAll(): Unit = {
    if (server != null) server.stop(0)
    CyberArkSecretProvider.clearCache()
    super.afterAll()
  }

  private def reference(objectName: String, extra: String = ""): String =
    s"""
       |password = {
       |  provider = "cyberark"
       |  url = "$baseUrl"
       |  app_id = "APP_Test"
       |  safe = "AAM_DB_Safe"
       |  object = "$objectName"
       |  $extra
       |}
    """.stripMargin

  private def resolve(hocon: String): Option[String] =
    SecretProviders.resolveAt(ConfigFactory.parseString(hocon), "password")

  test("user and password resolve from one cached CCP response") {
    CyberArkSecretProvider.clearCache()
    val password = resolve(reference("db-cred"))
    val user = resolve(reference("db-cred", """attribute = "UserName""""))

    assert(password.contains("s3cr3t-pwd"))
    assert(user.contains("svc_ingest"))
    assert(hitsByObject.get("db-cred") == 1, "second attribute must come from the cache")
  }

  test("cache_ttl_ms = 0 disables caching") {
    CyberArkSecretProvider.clearCache()
    resolve(reference("no-cache", "cache_ttl_ms = 0"))
    resolve(reference("no-cache", "cache_ttl_ms = 0"))
    assert(hitsByObject.get("no-cache") == 2)
  }

  test("CCP error responses map to JDBC_002 with the CyberArk error code") {
    val ex = intercept[IllegalArgumentException] { resolve(reference("missing-object")) }
    assert(ex.getMessage.contains("JDBC_002"))
    assert(ex.getMessage.contains("404"))
    assert(ex.getMessage.contains("APPAP004E"))
  }

  test("unknown attribute fails with the available attribute names, not a secret dump") {
    CyberArkSecretProvider.clearCache()
    val ex = intercept[IllegalArgumentException] {
      resolve(reference("db-cred", """attribute = "NoSuchField""""))
    }
    assert(ex.getMessage.contains("JDBC_002"))
    assert(ex.getMessage.contains("UserName"))
    assert(!ex.getMessage.contains("s3cr3t-pwd"), "secret values must never appear in errors")
  }

  test("missing required reference fields fail with JDBC_003") {
    val ex = intercept[IllegalArgumentException] {
      resolve(s"""password = {{ provider = "cyberark", url = "$baseUrl", safe = "s", object = "o" }}"""
        .replace("{{", "{").replace("}}", "}"))
    }
    assert(ex.getMessage.contains("JDBC_003"))
    assert(ex.getMessage.contains("app_id"))
  }

  test("unreachable CCP fails with JDBC_002, not a raw connection exception") {
    val ex = intercept[IllegalArgumentException] {
      resolve(
        """password = { provider = "cyberark", url = "https://127.0.0.1:1",
          |  app_id = "a", safe = "s", object = "o", connect_timeout_ms = 200 }""".stripMargin)
    }
    assert(ex.getMessage.contains("JDBC_002"))
  }

  test("extra query parameters are forwarded to CCP") {
    CyberArkSecretProvider.clearCache()
    val resolved = resolve(reference("with-params",
      """params { Query = "Address=db.example.com" }"""))
    assert(resolved.contains("s3cr3t-pwd"))
  }
}
