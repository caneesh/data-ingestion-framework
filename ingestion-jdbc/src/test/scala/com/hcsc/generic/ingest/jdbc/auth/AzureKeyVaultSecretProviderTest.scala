package com.hcsc.generic.ingest.jdbc.auth

import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite

/**
  * Offline-safe tests for the Azure Key Vault provider. These exercise ONLY the
  * eager config-validation layer and the registry/cache plumbing. No test here
  * builds a DefaultAzureCredential or contacts a vault — validation is designed
  * to run and fail before any Azure SDK class is referenced, so CI never hangs
  * on network or credential acquisition.
  */
class AzureKeyVaultSecretProviderTest extends AnyFunSuite {

  private def resolve(hocon: String): Option[String] =
    SecretProviders.resolveAt(ConfigFactory.parseString(hocon), "password")

  private def interceptMessage(hocon: String): String =
    intercept[IllegalArgumentException](resolve(hocon)).getMessage

  test("provider is registered under azure_keyvault") {
    assert(SecretProviders.availableNames.contains("azure_keyvault"))
  }

  test("unknown-provider listing now includes azure_keyvault") {
    val ex = intercept[IllegalArgumentException] {
      resolve("""password = { provider = "not-a-real-provider" }""")
    }
    assert(ex.getMessage.contains("Unknown secret provider"))
    assert(ex.getMessage.contains("azure_keyvault"))
  }

  test("missing vault_url fails with JDBC_003") {
    val msg = interceptMessage(
      """password = { provider = "azure_keyvault", secret_name = "sqlserver-pwd" }""")
    assert(msg.contains("JDBC_003"))
    assert(msg.contains("vault_url"))
  }

  test("http:// vault_url is rejected with JDBC_003") {
    val msg = interceptMessage(
      """password = { provider = "azure_keyvault",
        |  vault_url = "http://myvault.vault.azure.net", secret_name = "sqlserver-pwd" }""".stripMargin)
    assert(msg.contains("JDBC_003"))
    assert(msg.contains("https"))
  }

  test("non-Key-Vault host is rejected with JDBC_003") {
    val msg = interceptMessage(
      """password = { provider = "azure_keyvault",
        |  vault_url = "https://example.com", secret_name = "sqlserver-pwd" }""".stripMargin)
    assert(msg.contains("JDBC_003"))
  }

  test("missing secret_name fails with JDBC_003") {
    val msg = interceptMessage(
      """password = { provider = "azure_keyvault",
        |  vault_url = "https://myvault.vault.azure.net" }""".stripMargin)
    assert(msg.contains("JDBC_003"))
    assert(msg.contains("secret_name"))
  }

  test("secret_name with illegal characters is rejected with JDBC_003") {
    val msg = interceptMessage(
      """password = { provider = "azure_keyvault",
        |  vault_url = "https://myvault.vault.azure.net", secret_name = "bad_name!" }""".stripMargin)
    assert(msg.contains("JDBC_003"))
    assert(msg.contains("secret_name"))
  }

  test("empty secret_name is rejected with JDBC_003") {
    val msg = interceptMessage(
      """password = { provider = "azure_keyvault",
        |  vault_url = "https://myvault.vault.azure.net", secret_name = "" }""".stripMargin)
    assert(msg.contains("JDBC_003"))
    assert(msg.contains("secret_name"))
  }

  test("clearCache is idempotent and safe to call") {
    AzureKeyVaultSecretProvider.clearCache()
    AzureKeyVaultSecretProvider.clearCache()
    assert(SecretProviders.availableNames.contains("azure_keyvault"))
  }

  // ---------------------------------------------------------------------------
  // TTL cache. No live vault is reachable from CI, so a real fetch always
  // fails with a JDBC_002 (missing SDK on a lean classpath, or a network/
  // credential failure when the optional SDK is present) — which is exactly
  // what makes TTL observable: a seeded entry served from cache returns the
  // value, while an expired (or bypassed) entry attempts a re-fetch and hits
  // the JDBC_002 instead of serving the stale value.
  // ---------------------------------------------------------------------------

  private val vaultUrl = "https://myvault.vault.azure.net"

  private def cachedReference(extra: String = ""): String =
    s"""password = {
       |  provider = "azure_keyvault"
       |  vault_url = "$vaultUrl"
       |  secret_name = "rotating-pwd"
       |  $extra
       |}""".stripMargin

  test("a fresh cache entry is served without touching the vault") {
    AzureKeyVaultSecretProvider.clearCache()
    AzureKeyVaultSecretProvider.seedCache(vaultUrl, "rotating-pwd", None, "cached-v1", System.currentTimeMillis())
    assert(resolve(cachedReference()).contains("cached-v1"))
  }

  test("an entry older than cache_ttl_ms is refetched (rotated secrets get picked up)") {
    AzureKeyVaultSecretProvider.clearCache()
    val expiredAt = System.currentTimeMillis() - 10000
    AzureKeyVaultSecretProvider.seedCache(vaultUrl, "rotating-pwd", None, "stale-v0", expiredAt)

    val msg = interceptMessage(cachedReference("cache_ttl_ms = 5000"))
    assert(msg.contains("JDBC_002"), "expired entry must trigger a re-fetch, not serve the stale value")
    assert(!msg.contains("stale-v0"), "cached secret values must never leak into error messages")
  }

  test("cache_ttl_ms = 0 disables caching entirely") {
    AzureKeyVaultSecretProvider.clearCache()
    AzureKeyVaultSecretProvider.seedCache(vaultUrl, "rotating-pwd", None, "cached-v1", System.currentTimeMillis())

    val msg = interceptMessage(cachedReference("cache_ttl_ms = 0"))
    assert(msg.contains("JDBC_002"), "ttl 0 must bypass even a fresh cache entry")
  }

  test("cache entries are isolated by secret version") {
    AzureKeyVaultSecretProvider.clearCache()
    AzureKeyVaultSecretProvider.seedCache(vaultUrl, "rotating-pwd", Some("v7"), "versioned", System.currentTimeMillis())

    // Same name, no version: different cache key -> attempted fetch -> missing SDK
    val msg = interceptMessage(cachedReference())
    assert(msg.contains("JDBC_002"))

    // The versioned entry itself is served from cache
    assert(resolve(cachedReference("""secret_version = "v7"""")).contains("versioned"))
  }
}
