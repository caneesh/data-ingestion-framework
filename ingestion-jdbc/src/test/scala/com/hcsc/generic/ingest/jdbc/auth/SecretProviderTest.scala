package com.hcsc.generic.ingest.jdbc.auth

import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.Files

class SecretProviderTest extends AnyFunSuite {

  private def resolve(hocon: String): Option[String] =
    SecretProviders.resolveAt(ConfigFactory.parseString(hocon), "password")

  test("absent path resolves to None") {
    assert(resolve("""other = "x"""").isEmpty)
  }

  test("bare string is treated as inline (backward compatible)") {
    assert(resolve("""password = "plain"""").contains("plain"))
  }

  test("inline provider resolves the value field") {
    assert(resolve("""password = { provider = "inline", value = "v" }""").contains("v"))
  }

  test("sysprop provider resolves and fails clearly when missing") {
    System.setProperty("secret.test.key", "from-sysprop")
    assert(resolve("""password = { provider = "sysprop", key = "secret.test.key" }""")
      .contains("from-sysprop"))

    val ex = intercept[IllegalArgumentException] {
      resolve("""password = { provider = "sysprop", key = "secret.test.absent" }""")
    }
    assert(ex.getMessage.contains("JDBC_002"))
  }

  test("file provider reads and trims the secret file") {
    val f = Files.createTempFile("secret", ".txt")
    Files.write(f, "  file-secret\n".getBytes("UTF-8"))
    assert(resolve(s"""password = {{ provider = "file", path = "$f" }}""".replace("{{", "{").replace("}}", "}"))
      .contains("file-secret"))

    val ex = intercept[IllegalArgumentException] {
      resolve("""password = { provider = "file", path = "/nonexistent/secret" }""")
    }
    assert(ex.getMessage.contains("JDBC_002"))
  }

  test("env provider fails clearly for unset variables") {
    val ex = intercept[IllegalArgumentException] {
      resolve("""password = { provider = "env", key = "DEFINITELY_NOT_SET_12345" }""")
    }
    assert(ex.getMessage.contains("JDBC_002"))
  }

  test("unknown provider is rejected with the available list") {
    val ex = intercept[IllegalArgumentException] {
      resolve("""password = { provider = "vault", key = "k" }""")
    }
    assert(ex.getMessage.contains("Unknown secret provider"))
    assert(ex.getMessage.contains("env"))
  }
}
