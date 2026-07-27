package com.hcsc.generic.ingest.jdbc.auth

import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite

/** Blank-secret guard in the registry: store-backed providers must never
  * hand back an empty credential; only an explicit inline value may be
  * blank (e.g. a local database with no password). */
class SecretResolveGuardTest extends AnyFunSuite {

  test("a store-backed secret resolving to whitespace is rejected (JDBC_002)") {
    System.setProperty("guard.blank.secret", "   ")
    val ex = intercept[IllegalArgumentException] {
      SecretProviders.resolveRef(ConfigFactory.parseString(
        """provider = "sysprop", key = "guard.blank.secret""""))
    }
    assert(ex.getMessage.contains("JDBC_002"))
    assert(ex.getMessage.contains("empty"))
    assert(ex.getMessage.contains("sysprop"))
  }

  test("an explicit inline blank stays allowed (local dev databases)") {
    assert(SecretProviders.resolveRef(ConfigFactory.parseString(
      """provider = "inline", value = """"")) == "")
  }

  test("non-blank store-backed secrets pass through unchanged") {
    System.setProperty("guard.real.secret", "s3cret")
    assert(SecretProviders.resolveRef(ConfigFactory.parseString(
      """provider = "sysprop", key = "guard.real.secret"""")) == "s3cret")
  }
}
