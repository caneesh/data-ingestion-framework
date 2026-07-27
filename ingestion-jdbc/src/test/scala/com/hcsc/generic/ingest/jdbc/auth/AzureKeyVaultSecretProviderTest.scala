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
}
