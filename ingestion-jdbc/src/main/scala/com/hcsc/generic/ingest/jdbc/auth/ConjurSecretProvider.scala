package com.hcsc.generic.ingest.jdbc.auth

import com.hcsc.generic.ingest.config.ConfigUtils
import com.typesafe.config.{Config, ConfigValueType}
import org.apache.log4j.Logger

/**
  * CyberArk Conjur secret provider. Reference form:
  *
  *   password = {
  *     provider = "conjur"
  *     url      = "https://conjur.example.com"
  *     account  = "myorg"
  *     host_id  = "data/ingestion/spark"          # 'host/' prefix optional
  *     api_key  = { provider = "env", key = "CONJUR_API_KEY" }
  *     variable = "applications/ingestion/database/password"
  *   }
  *
  * `api_key` is itself a secret reference resolved through the registry
  * (env/file/sysprop/...); a bare string is accepted for dev use with the
  * standard inline warning. Plain-http urls are rejected unless
  * `allow_insecure_http = true` (isolated dev only). Optional fields:
  * `connect_timeout_ms` (5000),
  * `read_timeout_ms` (10000), `max_attempts` (3), `cache_ttl_ms` (300000;
  * 0 disables the resolved-secret cache — the short-lived auth token is
  * cached separately inside ConjurClient).
  */
object ConjurSecretProvider extends SecretProvider {
  private val logger = Logger.getLogger(getClass.getName)

  val name = "conjur"

  private final case class CachedSecret(fetchedAt: Long, value: String)
  private val cache = new java.util.concurrent.ConcurrentHashMap[String, CachedSecret]()

  private[auth] def clearCache(): Unit = {
    cache.clear()
    ConjurClient.clearTokenCache()
  }

  override def resolve(conf: Config): String = {
    def required(key: String): String =
      ConfigUtils.optString(conf, key).map(_.trim).filter(_.nonEmpty).getOrElse(
        throw new IllegalArgumentException(s"JDBC_003 conjur secret reference requires '$key'"))

    // Authentication POSTs the host API key in the request body: plain http
    // ships it in cleartext, hence the shared insecure-http gate.
    val baseUrl = VaultHttp.requireSecureBase(conf, required("url"),
      provider = "conjur", transmitted = "the host API key and secrets")

    val account = required("account")
    val hostId = required("host_id")
    val variable = required("variable")
    val ttlMs = ConfigUtils.optLong(conf, "cache_ttl_ms").getOrElse(300000L)

    def fetchValue(): String = {
      val client = new ConjurClient(ConjurConfig(
        baseUrl = baseUrl,
        account = account,
        hostId = hostId,
        apiKey = resolveApiKey(conf),
        connectTimeoutMs = ConfigUtils.optInt(conf, "connect_timeout_ms").getOrElse(5000),
        readTimeoutMs = ConfigUtils.optInt(conf, "read_timeout_ms").getOrElse(10000),
        maxAttempts = ConfigUtils.optInt(conf, "max_attempts").getOrElse(3)))

      logger.info(s"[Conjur] Fetching variable '$variable' (account=$account, $baseUrl)")
      client.getSecret(variable) match {
        case Right(value) => value
        case Left(error) =>
          throw new IllegalArgumentException(
            s"JDBC_002 Conjur variable '$variable' could not be retrieved: ${error.message}",
            error.cause.orNull)
      }
    }

    // Atomic TTL cache: compute holds the per-key lock so concurrent
    // resolvers share one Conjur round-trip instead of racing check-then-put.
    // The HOST IDENTITY is part of the key: two identities reading the same
    // variable in one JVM must each pass their own authorization — a shared
    // entry would hand identity B a value it may not be permitted to read.
    val cacheKey = s"$baseUrl|$account|$hostId|$variable"
    if (ttlMs <= 0) fetchValue()
    else cache.compute(cacheKey, (_, existing) =>
      if (existing != null && System.currentTimeMillis() - existing.fetchedAt < ttlMs) existing
      else CachedSecret(System.currentTimeMillis(), fetchValue())
    ).value
  }

  /** The host API key is itself a secret: object form resolves through the
    * registry; a bare string works for dev/test with the inline warning. */
  private def resolveApiKey(conf: Config): String = {
    if (!conf.hasPath("api_key"))
      throw new IllegalArgumentException("JDBC_003 conjur secret reference requires 'api_key'")
    conf.getValue("api_key").valueType() match {
      case ConfigValueType.OBJECT => SecretProviders.resolveRef(conf.getConfig("api_key"))
      case ConfigValueType.STRING =>
        logger.warn("[Conjur] api_key is inline plaintext; prefer a provider reference " +
          "(env/file/sysprop) so the host credential stays out of config files")
        conf.getString("api_key")
      case other =>
        throw new IllegalArgumentException(
          s"JDBC_003 conjur api_key must be a string or secret reference object, found $other")
    }
  }

}
