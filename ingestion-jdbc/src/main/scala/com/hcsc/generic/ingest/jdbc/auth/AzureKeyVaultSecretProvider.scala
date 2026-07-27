package com.hcsc.generic.ingest.jdbc.auth

import com.hcsc.generic.ingest.config.ConfigUtils
import com.typesafe.config.Config
import org.apache.log4j.Logger

/**
  * Azure Key Vault secret provider. Retrieves a secret value from an Azure Key
  * Vault instance. Reference form in HOCON:
  *
  *   password = {
  *     provider    = "azure_keyvault"
  *     vault_url   = "https://myvault.vault.azure.net"
  *     secret_name = "sqlserver-pwd"
  *     secret_version = "..."   # optional; omit for the current version
  *   }
  *
  * Authentication uses `DefaultAzureCredentialBuilder`, which walks a chain of
  * credential sources without any secret material in this framework's config:
  * managed identity (AKS/VM/App Service), environment service principal
  * (AZURE_CLIENT_ID / AZURE_CLIENT_SECRET / AZURE_TENANT_ID), and finally the
  * Azure CLI login for local development. Deployments choose the mechanism by
  * how the runtime environment is provisioned.
  *
  * Optional dependency: `azure-security-keyvault-secrets` and `azure-identity`
  * are declared `<optional>true</optional>` in ingestion-jdbc's pom, so they are
  * compile-time available but NOT forced onto consumers. A deployment that uses
  * this provider must bundle both artifacts (e.g. via the assembly / spark
  * `--packages`). When the SDK is absent at runtime, retrieval fails fast with a
  * JDBC_002 error explaining what to bundle rather than a raw
  * NoClassDefFoundError.
  *
  * Retrieved secrets are memoized per JVM keyed by
  * (vault_url, secret_name, secret_version) so repeated resolutions within a run
  * (e.g. driver plus multiple executor tasks in the same JVM) do not repeatedly
  * hit the vault. `clearCache()` resets the cache for tests.
  */
object AzureKeyVaultSecretProvider extends SecretProvider {
  private val logger = Logger.getLogger(getClass.getName)

  val name = "azure_keyvault"

  /** Key Vault secret names allow only alphanumerics and dashes. */
  private val SecretNamePattern = "^[0-9a-zA-Z-]+$".r

  /**
    * Recognised Key Vault DNS suffixes across Azure clouds. `vault_url` must be
    * https and its host must end with one of these so we fail fast on typos and
    * on URLs that are clearly not Key Vault endpoints.
    */
  private val VaultDomainSuffixes = Seq(
    ".vault.azure.net",           // Azure public cloud
    ".vault.azure.cn",            // Azure China
    ".vault.usgovcloudapi.net",   // Azure US Government
    ".vault.microsoftazure.de"    // Azure Germany (legacy)
  )

  private final case class CacheKey(vaultUrl: String, secretName: String, secretVersion: Option[String])
  private val cache = new java.util.concurrent.ConcurrentHashMap[CacheKey, String]()

  /** Test hook: drop all memoized secrets. */
  private[auth] def clearCache(): Unit = cache.clear()

  override def resolve(conf: Config): String = {
    val spec = validate(conf)
    val key = CacheKey(spec.vaultUrl, spec.secretName, spec.secretVersion)

    // computeIfAbsent so concurrent executor threads in one JVM share a single
    // fetch. null return from the mapping function is avoided by throwing.
    Option(cache.get(key)).getOrElse {
      val value = fetch(spec)
      cache.put(key, value)
      value
    }
  }

  private final case class SecretSpec(vaultUrl: String, secretName: String, secretVersion: Option[String])

  /**
    * Eager, SDK-free validation. All checks here run before any Azure class is
    * touched, so callers (and tests) can rely on JDBC_003 for malformed
    * references without needing the SDK on the classpath or a live vault.
    */
  private def validate(conf: Config): SecretSpec = {
    def required(key: String): String =
      ConfigUtils.optString(conf, key).map(_.trim).filter(_.nonEmpty).getOrElse(
        throw new IllegalArgumentException(s"JDBC_003 azure_keyvault secret reference requires '$key'"))

    val vaultUrl = required("vault_url").stripSuffix("/")
    if (!vaultUrl.startsWith("https://"))
      throw new IllegalArgumentException(
        s"JDBC_003 azure_keyvault vault_url '$vaultUrl' must be an https URL")

    val host =
      try new java.net.URI(vaultUrl).getHost
      catch {
        case _: Exception =>
          throw new IllegalArgumentException(
            s"JDBC_003 azure_keyvault vault_url '$vaultUrl' is not a valid URL")
      }
    if (host == null || !VaultDomainSuffixes.exists(host.toLowerCase.endsWith))
      throw new IllegalArgumentException(
        s"JDBC_003 azure_keyvault vault_url '$vaultUrl' host must be an Azure Key Vault domain " +
          s"(one of: ${VaultDomainSuffixes.mkString(", ")})")

    val secretName = required("secret_name")
    if (SecretNamePattern.findFirstIn(secretName).isEmpty)
      throw new IllegalArgumentException(
        s"JDBC_003 azure_keyvault secret_name '$secretName' must match [0-9a-zA-Z-]+")

    val secretVersion = ConfigUtils.optString(conf, "secret_version").map(_.trim).filter(_.nonEmpty)

    SecretSpec(vaultUrl, secretName, secretVersion)
  }

  /**
    * Perform the actual vault retrieval via the Azure SDK. Isolated in its own
    * method so the SDK classes are only linked when a secret is genuinely
    * fetched. NoClassDefFoundError (optional dependency missing) and all
    * runtime failures are mapped to JDBC_002 with sanitized messages — secret
    * material and credentials never appear in the message or logs.
    */
  private def fetch(spec: SecretSpec): String = {
    logger.info(
      s"[AzureKeyVault] Retrieving secret '${spec.secretName}'" +
        spec.secretVersion.map(v => s"@$v").getOrElse("") + s" from ${spec.vaultUrl}")
    try {
      import com.azure.identity.DefaultAzureCredentialBuilder
      import com.azure.security.keyvault.secrets.SecretClientBuilder

      val client = new SecretClientBuilder()
        .vaultUrl(spec.vaultUrl)
        .credential(new DefaultAzureCredentialBuilder().build())
        .buildClient()

      val secret = spec.secretVersion match {
        case Some(version) => client.getSecret(spec.secretName, version)
        case None          => client.getSecret(spec.secretName)
      }

      val value = secret.getValue
      if (value == null)
        throw new IllegalArgumentException(
          s"JDBC_002 azure_keyvault secret '${spec.secretName}' in ${spec.vaultUrl} has no value")
      value
    } catch {
      case e: IllegalArgumentException => throw e
      case e: NoClassDefFoundError =>
        throw new IllegalArgumentException(
          "JDBC_002 azure_keyvault provider requires the Azure SDK, but it is not on the classpath; " +
            "bundle azure-security-keyvault-secrets and azure-identity with the deployment. " +
            s"(missing class: ${e.getMessage})")
      case e: Throwable =>
        // Never surface the raw exception message verbatim to guarantee no
        // token/credential material leaks; report class + secret identity only.
        throw new IllegalArgumentException(
          s"JDBC_002 azure_keyvault failed to retrieve secret '${spec.secretName}' from " +
            s"${spec.vaultUrl}: ${sanitize(e)}", e)
    }
  }

  /** Sanitized failure summary: exception type plus a trimmed message with no
    * newlines, so multi-line SDK dumps that might echo request/response bodies
    * cannot flood or leak into the error. */
  private def sanitize(e: Throwable): String = {
    val msg = Option(e.getMessage).map(_.replaceAll("\\s+", " ").trim).getOrElse("")
    val short = if (msg.length <= 200) msg else msg.take(200) + "..."
    if (short.isEmpty) e.getClass.getSimpleName else s"${e.getClass.getSimpleName}: $short"
  }
}
