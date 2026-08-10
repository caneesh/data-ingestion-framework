package com.hcsc.generic.ingest.jdbc.auth

import com.hcsc.generic.ingest.config.ConfigUtils
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.Logger

import java.net.{HttpURLConnection, URL, URLEncoder}
import scala.collection.JavaConverters._

/**
  * CyberArk CCP secret provider. Exposes any response attribute (`Content`,
  * the default, or `UserName`), so one vault object can serve both
  * credentials. Configuration reference: docs/architecture/CONFIGURATION_MODEL.md.
  *
  * Plain-http urls are rejected unless `allow_insecure_http = true`: CCP
  * responses carry the retrieved credential and must not transit
  * unencrypted. TLS verification is never disabled here; client-certificate
  * auth comes from the standard JVM TLS configuration.
  *
  * Responses are cached per request URL, so resolving user and password from
  * the same object costs one CCP call per run.
  */
object CyberArkSecretProvider extends SecretProvider {
  private val logger = Logger.getLogger(getClass.getName)

  val name = "cyberark"

  private final case class CachedResponse(fetchedAt: Long, fields: Map[String, String])
  private val cache = new java.util.concurrent.ConcurrentHashMap[String, CachedResponse]()

  private[auth] def clearCache(): Unit = cache.clear()

  override def resolve(conf: Config): String = {
    val requestUrl = buildRequestUrl(conf)
    val attribute = ConfigUtils.optString(conf, "attribute").getOrElse("Content")
    val ttlMs = ConfigUtils.optLong(conf, "cache_ttl_ms").getOrElse(300000L)
    val connectMs = ConfigUtils.optInt(conf, "connect_timeout_ms").getOrElse(5000)
    val readMs = ConfigUtils.optInt(conf, "read_timeout_ms").getOrElse(10000)

    // Atomic TTL cache: compute holds the per-key lock so concurrent
    // resolvers share one CCP round-trip instead of racing check-then-put.
    val fields =
      if (ttlMs <= 0) fetch(requestUrl, connectMs, readMs)
      else cache.compute(requestUrl, (_, existing) =>
        if (existing != null && System.currentTimeMillis() - existing.fetchedAt < ttlMs) existing
        else CachedResponse(System.currentTimeMillis(), fetch(requestUrl, connectMs, readMs))
      ).fields

    fields.getOrElse(attribute,
      throw new IllegalArgumentException(
        s"JDBC_002 CyberArk response has no attribute '$attribute'; " +
          s"available attributes: ${fields.keys.toSeq.sorted.mkString(", ")}"))
  }

  private def buildRequestUrl(conf: Config): String = {
    def required(key: String): String = ConfigUtils.optString(conf, key).getOrElse(
      throw new IllegalArgumentException(s"JDBC_003 cyberark secret reference requires '$key'"))

    val base = VaultHttp.requireSecureBase(conf, required("url"),
      provider = "cyberark", transmitted = "retrieved credentials")
    if (base.startsWith("http://"))
      logger.warn("[CyberArk] CCP url uses plain http (allow_insecure_http=true); " +
        "credentials transit unencrypted — never use this outside isolated development")

    val params =
      Seq("AppID" -> required("app_id"), "Safe" -> required("safe"), "Object" -> required("object")) ++
        ConfigUtils.optString(conf, "folder").map("Folder" -> _).toSeq ++
        ConfigUtils.optConfig(conf, "params").map(ConfigUtils.flatStringMap).getOrElse(Map.empty)

    val query = params.map { case (k, v) =>
      s"${URLEncoder.encode(k, "UTF-8")}=${URLEncoder.encode(v, "UTF-8")}"
    }.mkString("&")

    s"$base/AIMWebService/api/Accounts?$query"
  }

  /** GET the account from CCP; the JSON response fields become the attribute
    * map. Secret values are never logged. */
  private def fetch(requestUrl: String, connectMs: Int, readMs: Int): Map[String, String] = {
    logger.info(s"[CyberArk] Fetching credential ${sanitized(requestUrl)}")
    val connection = new URL(requestUrl).openConnection().asInstanceOf[HttpURLConnection]
    connection.setRequestMethod("GET")
    connection.setRequestProperty("Accept", "application/json")
    connection.setConnectTimeout(connectMs)
    connection.setReadTimeout(readMs)
    try {
      val status = connection.getResponseCode
      val body = VaultHttp.readAll(
        if (status >= 200 && status < 300) connection.getInputStream else connection.getErrorStream)
      if (status < 200 || status >= 300)
        throw new IllegalArgumentException(
          s"JDBC_002 CyberArk CCP returned HTTP $status for ${sanitized(requestUrl)}: ${errorSummary(body)}")
      parseJsonObject(body)
    } catch {
      case e: IllegalArgumentException => throw e
      case e: Exception =>
        throw new IllegalArgumentException(
          s"JDBC_002 CyberArk CCP request to ${sanitized(requestUrl)} failed: ${e.getMessage}", e)
    } finally connection.disconnect()
  }

  /** JSON is valid HOCON, so the response parses without a JSON dependency.
    * Only top-level scalar fields are kept (the CCP response is flat). */
  private def parseJsonObject(body: String): Map[String, String] =
    ConfigFactory.parseString(body).root().unwrapped().asScala.collect {
      case (key, value: Any) if !value.isInstanceOf[java.util.Map[_, _]] &&
        !value.isInstanceOf[java.util.List[_]] => key -> String.valueOf(value)
    }.toMap

  /** CCP error bodies carry ErrorCode/ErrorMsg; surface those, never a raw
    * dump that could echo credential material. */
  private def errorSummary(body: String): String =
    try {
      val fields = parseJsonObject(body)
      Seq(fields.get("ErrorCode"), fields.get("ErrorMsg")).flatten match {
        case Nil     => "no error detail in response"
        case details => details.mkString(" ")
      }
    } catch { case _: Exception => "unparseable error response" }

  /** The request URL identifies the object, not the secret, but keep logs
    * tidy by trimming the query when it is long. */
  private def sanitized(url: String): String =
    if (url.length <= 200) url else url.take(200) + "..."
}
