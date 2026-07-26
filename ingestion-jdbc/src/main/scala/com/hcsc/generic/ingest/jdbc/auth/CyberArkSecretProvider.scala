package com.hcsc.generic.ingest.jdbc.auth

import com.hcsc.generic.ingest.config.ConfigUtils
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.Logger

import java.net.{HttpURLConnection, URL, URLEncoder}
import java.nio.charset.StandardCharsets
import scala.collection.JavaConverters._

/**
  * CyberArk Central Credential Provider (CCP) secret provider. Retrieves an
  * account from the AIM web service and exposes any response attribute —
  * `Content` (the password, default) or `UserName` — so ONE vault object
  * serves both credentials:
  *
  *   auth {
  *     user = {
  *       provider = "cyberark"
  *       url      = "https://ccp.example.com"
  *       app_id   = "APP_DataIngestion"
  *       safe     = "AAM_DB_Safe"
  *       object   = "sqlserver-svc-account"
  *       attribute = "UserName"
  *     }
  *     password = {
  *       provider = "cyberark"
  *       url      = "https://ccp.example.com"
  *       app_id   = "APP_DataIngestion"
  *       safe     = "AAM_DB_Safe"
  *       object   = "sqlserver-svc-account"
  *       # attribute = "Content" is the default
  *     }
  *   }
  *
  * Optional fields: `folder` (CCP Folder), `params { ... }` (extra query
  * parameters such as Query/QueryFormat), `connect_timeout_ms` (default
  * 5000), `read_timeout_ms` (default 10000), `cache_ttl_ms` (default 300000;
  * 0 disables caching).
  *
  * Responses are cached per request URL for the TTL, so resolving user and
  * password from the same object costs a single CCP call per run.
  *
  * Client-certificate authentication to CCP uses the standard JVM TLS
  * configuration (`-Djavax.net.ssl.keyStore=...` via
  * `spark.driver.extraJavaOptions`); the JDK HTTP stack picks it up
  * automatically. TLS verification is never disabled by this provider.
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

    val fields = cached(requestUrl, ttlMs).getOrElse {
      val fetched = fetch(requestUrl, connectMs, readMs)
      if (ttlMs > 0) cache.put(requestUrl, CachedResponse(System.currentTimeMillis(), fetched))
      fetched
    }

    fields.getOrElse(attribute,
      throw new IllegalArgumentException(
        s"JDBC_002 CyberArk response has no attribute '$attribute'; " +
          s"available attributes: ${fields.keys.toSeq.sorted.mkString(", ")}"))
  }

  private def cached(requestUrl: String, ttlMs: Long): Option[Map[String, String]] =
    Option(cache.get(requestUrl))
      .filter(c => ttlMs > 0 && System.currentTimeMillis() - c.fetchedAt < ttlMs)
      .map(_.fields)

  private def buildRequestUrl(conf: Config): String = {
    def required(key: String): String = ConfigUtils.optString(conf, key).getOrElse(
      throw new IllegalArgumentException(s"JDBC_003 cyberark secret reference requires '$key'"))

    val base = required("url").stripSuffix("/")
    if (base.startsWith("http://"))
      logger.warn("[CyberArk] CCP url uses plain http; credentials will transit unencrypted — use https in production")
    else if (!base.startsWith("https://"))
      throw new IllegalArgumentException(s"JDBC_003 cyberark url '$base' must be an http(s) URL")

    val params =
      Seq("AppID" -> required("app_id"), "Safe" -> required("safe"), "Object" -> required("object")) ++
        ConfigUtils.optString(conf, "folder").map("Folder" -> _).toSeq ++
        ConfigUtils.optConfig(conf, "params").map(p => ConfigUtils.stringMap(p.atKey("p"), "p")).getOrElse(Map.empty)

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
      val body = readAll(
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

  private def readAll(stream: java.io.InputStream): String = {
    if (stream == null) return ""
    try {
      val out = new java.io.ByteArrayOutputStream()
      val buffer = new Array[Byte](8192)
      var read = stream.read(buffer)
      while (read != -1) {
        out.write(buffer, 0, read)
        read = stream.read(buffer)
      }
      new String(out.toByteArray, StandardCharsets.UTF_8)
    } finally stream.close()
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
