package com.hcsc.generic.ingest.jdbc.auth

import org.apache.log4j.Logger

import java.net.{HttpURLConnection, URL, URLEncoder}
import java.nio.charset.StandardCharsets
import scala.util.control.NonFatal

/**
  * CyberArk Conjur client (authn + secrets retrieval). Deliberately built on
  * `HttpURLConnection` — like [[CyberArkSecretProvider]] — so the framework
  * takes no HTTP-client dependency and cannot drift against Spark's
  * transitive versions.
  *
  * Design points (from the security review of the original draft):
  *   - retry is a loop, transient-only (429/5xx, timeouts, resets); DNS, TLS
  *     and protocol errors fail immediately
  *   - `Retry-After` is honored when present; otherwise exponential backoff
  *     with jitter, capped
  *   - interruption during backoff restores the interrupt flag and returns
  *     an error instead of escaping the `Either` contract
  *   - access tokens are cached per (url, account, host) for a short TTL
  *     (Conjur tokens live ~8 minutes); a 401 on a secret fetch triggers
  *     exactly one fresh re-authentication
  *   - secrets resolving to blank are rejected
  *   - no credential material ever reaches `toString`, logs or error messages
  */
final case class ConjurConfig(
  baseUrl: String,
  account: String,
  hostId: String,
  apiKey: String,
  connectTimeoutMs: Int = 5000,
  readTimeoutMs: Int = 10000,
  maxAttempts: Int = 3,
  initialBackoffMs: Long = 1000L,
  maxBackoffMs: Long = 10000L,
  tokenTtlMs: Long = 300000L
) {
  override def toString: String =
    s"ConjurConfig(baseUrl=$baseUrl, account=$account, hostId=$hostId, apiKey=********)"
}

sealed trait ConjurError {
  def message: String
  def cause: Option[Throwable]
}
final case class ConjurAuthenticationError(message: String, cause: Option[Throwable] = None) extends ConjurError
final case class ConjurAuthorizationError(message: String, cause: Option[Throwable] = None) extends ConjurError
final case class ConjurSecretNotFoundError(message: String, cause: Option[Throwable] = None) extends ConjurError
final case class ConjurEmptySecretError(message: String, cause: Option[Throwable] = None) extends ConjurError
final case class ConjurHttpError(statusCode: Int, message: String, cause: Option[Throwable] = None) extends ConjurError
final case class ConjurNetworkError(message: String, cause: Option[Throwable] = None) extends ConjurError

final case class UsernamePassword(username: String, password: String) {
  override def toString: String = s"UsernamePassword(username=$username, password=********)"
}

object ConjurClient {
  private final case class CachedToken(token: String, fetchedAt: Long)
  private val tokenCache =
    new java.util.concurrent.ConcurrentHashMap[String, CachedToken]()

  private[auth] def clearTokenCache(): Unit = tokenCache.clear()

  private val RetryableStatuses = Set(429, 500, 502, 503, 504)
}

final class ConjurClient(config: ConjurConfig) {
  import ConjurClient._

  private val logger = Logger.getLogger(getClass.getName)

  private val normalizedBaseUrl: String = {
    val base = config.baseUrl.trim.stripSuffix("/")
    if (base.startsWith("http://"))
      logger.warn("[Conjur] url uses plain http; credentials will transit unencrypted — use https in production")
    else if (!base.startsWith("https://"))
      throw new IllegalArgumentException(s"Conjur url '$base' must be an http(s) URL")
    base
  }

  private val tokenCacheKey = s"$normalizedBaseUrl|${config.account}|${config.hostId}"

  // ---------------------------------------------------------------------------
  // Public API
  // ---------------------------------------------------------------------------

  /** Authenticates the Conjur host and returns a Base64-encoded short-lived
    * access token (cached for `tokenTtlMs`). */
  def authenticate(): Either[ConjurError, String] =
    cachedToken().map(Right(_): Either[ConjurError, String]).getOrElse {
      val url = s"$normalizedBaseUrl/authn/${encodePathSegment(config.account)}/" +
        s"${encodePathSegment(hostLogin)}/authenticate"

      // Asking Conjur for the token already Base64 encoded avoids
      // transforming the raw JSON token manually.
      executeWithRetry("authentication") { () =>
        http("POST", url,
          Map("Accept-Encoding" -> "base64", "Content-Type" -> "text/plain"),
          Some(config.apiKey))
      } {
        case (status, body) if status >= 200 && status < 300 =>
          val token = body.trim
          if (token.isEmpty)
            Left(ConjurAuthenticationError("Conjur authentication returned an empty access token."))
          else {
            tokenCache.put(tokenCacheKey, CachedToken(token, System.currentTimeMillis()))
            Right(token)
          }
        case (401, _) =>
          Left(ConjurAuthenticationError(
            "Conjur authentication failed. Check the host ID and API key."))
        case (status, body) =>
          Left(ConjurHttpError(status, safeErrorMessage("Conjur authentication failed", body)))
      }
    }

  /**
    * Retrieves the latest value of one Conjur variable (id relative to the
    * account, e.g. `applications/my-app/database/password`). A 401 with a
    * cached token triggers exactly one fresh re-authentication.
    */
  def getSecret(variableId: String): Either[ConjurError, String] = {
    val hadCachedToken = cachedToken().isDefined
    authenticate().flatMap { token =>
      getSecretWithToken(variableId, token) match {
        case Left(_: ConjurAuthenticationError) if hadCachedToken =>
          logger.info(s"[Conjur] cached token rejected for variable '$variableId'; re-authenticating once")
          invalidateToken()
          authenticate().flatMap(fresh => getSecretWithToken(variableId, fresh))
        case other => other
      }
    }
  }

  /** Retrieves a username and password from two Conjur variables with a
    * single authentication. */
  def getUsernamePassword(
    usernameVariableId: String,
    passwordVariableId: String
  ): Either[ConjurError, UsernamePassword] =
    for {
      username <- getSecret(usernameVariableId)
      password <- getSecret(passwordVariableId)
    } yield UsernamePassword(username, password)

  private[auth] def invalidateToken(): Unit = tokenCache.remove(tokenCacheKey)

  // ---------------------------------------------------------------------------
  // Secret fetch
  // ---------------------------------------------------------------------------

  private def getSecretWithToken(variableId: String, token: String): Either[ConjurError, String] = {
    val url = s"$normalizedBaseUrl/secrets/${encodePathSegment(config.account)}/variable/" +
      encodePathSegment(variableId)

    executeWithRetry(s"secret fetch '$variableId'") { () =>
      http("GET", url, Map("Authorization" -> s"""Token token="$token""""), None)
    } {
      case (status, body) if status >= 200 && status < 300 =>
        if (body.trim.isEmpty)
          Left(ConjurEmptySecretError(
            s"Conjur variable '$variableId' resolved to an empty value."))
        else Right(body)
      case (401, _) =>
        Left(ConjurAuthenticationError(
          "The Conjur access token is missing, invalid, or expired."))
      case (403, _) =>
        Left(ConjurAuthorizationError(
          s"The Conjur host is not authorized to read variable '$variableId'."))
      case (404, _) =>
        Left(ConjurSecretNotFoundError(
          s"Conjur variable '$variableId' was not found or has no secret value."))
      case (status, body) =>
        Left(ConjurHttpError(status,
          safeErrorMessage(s"Failed to retrieve Conjur variable '$variableId'", body)))
    }
  }

  // ---------------------------------------------------------------------------
  // HTTP + retry
  // ---------------------------------------------------------------------------

  private case class HttpResult(status: Int, body: String, retryAfterMs: Option[Long])

  /** One attempt; the connection is fully consumed and released before the
    * retry decision is made. */
  private def http(
    method: String,
    url: String,
    headers: Map[String, String],
    body: Option[String]
  ): HttpResult = {
    val connection = new URL(url).openConnection().asInstanceOf[HttpURLConnection]
    try {
      connection.setRequestMethod(method)
      connection.setConnectTimeout(config.connectTimeoutMs)
      connection.setReadTimeout(config.readTimeoutMs)
      headers.foreach { case (k, v) => connection.setRequestProperty(k, v) }
      body.foreach { content =>
        connection.setDoOutput(true)
        val bytes = content.getBytes(StandardCharsets.UTF_8)
        connection.setFixedLengthStreamingMode(bytes.length)
        val out = connection.getOutputStream
        try out.write(bytes) finally out.close()
      }
      val status = connection.getResponseCode
      val text = VaultHttp.readAll(
        if (status >= 200 && status < 300) connection.getInputStream
        else connection.getErrorStream)
      val retryAfterMs = Option(connection.getHeaderField("Retry-After"))
        .flatMap(v => scala.util.Try(v.trim.toLong * 1000L).toOption)
        .filter(_ >= 0)
      HttpResult(status, text, retryAfterMs)
    } finally connection.disconnect()
  }

  /**
    * Loop-based retry (a recursive call inside try/catch cannot be a tail
    * call). Retries only transient failures; every other outcome is passed
    * to the handler / reported immediately.
    */
  private def executeWithRetry[A](description: String)(
    run: () => HttpResult
  )(
    handler: (Int, String) => Either[ConjurError, A]
  ): Either[ConjurError, A] = {
    var attempt = 1
    var result: Option[Either[ConjurError, A]] = None

    while (result.isEmpty) {
      var retryDelayMs: Option[Long] = None
      try {
        val response = run()
        if (RetryableStatuses.contains(response.status) && attempt < config.maxAttempts)
          retryDelayMs = Some(response.retryAfterMs.getOrElse(backoffMs(attempt))
            .min(config.maxBackoffMs))
        else
          result = Some(handler(response.status, response.body))
      } catch {
        case NonFatal(e) if transient(e) && attempt < config.maxAttempts =>
          retryDelayMs = Some(backoffMs(attempt))
        case NonFatal(e) =>
          result = Some(Left(ConjurNetworkError(
            s"Unable to communicate with Conjur ($description): " +
              s"${e.getClass.getSimpleName}: ${e.getMessage}", Some(e))))
      }

      retryDelayMs.foreach { delayMs =>
        logger.warn(s"[Conjur] $description transient failure " +
          s"(attempt $attempt/${config.maxAttempts}); retrying in ${delayMs}ms")
        if (!sleep(delayMs))
          result = Some(Left(ConjurNetworkError(
            s"Interrupted while waiting to retry $description.")))
        attempt += 1
      }
    }
    result.get
  }

  /** DNS, TLS and protocol failures are not transient; timeouts, refused
    * connections and resets are (mirrors the transient-only retry policy of
    * SqlFailureClassifier on the JDBC side). */
  private def transient(e: Throwable): Boolean = e match {
    case _: javax.net.ssl.SSLException => false
    case _: java.net.UnknownHostException => false
    case _: java.net.MalformedURLException => false
    case _: java.net.SocketTimeoutException => true
    case _: java.net.ConnectException => true
    case _: java.net.SocketException => true
    case _: java.io.EOFException => true
    case _ => false
  }

  /** Exponential backoff with jitter, capped. */
  private def backoffMs(attempt: Int): Long = {
    val exponential = config.initialBackoffMs * (1L << math.min(attempt - 1, 20))
    val capped = math.min(exponential, config.maxBackoffMs)
    val jitter = (capped * 0.2 * scala.util.Random.nextDouble()).toLong
    math.min(capped + jitter, config.maxBackoffMs)
  }

  /** Returns false when interrupted (interrupt flag restored). */
  private def sleep(delayMs: Long): Boolean =
    try { Thread.sleep(delayMs); true }
    catch {
      case _: InterruptedException =>
        Thread.currentThread().interrupt()
        false
    }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private def cachedToken(): Option[String] =
    Option(tokenCache.get(tokenCacheKey))
      .filter(c => config.tokenTtlMs > 0 &&
        System.currentTimeMillis() - c.fetchedAt < config.tokenTtlMs)
      .map(_.token)

  private def hostLogin: String = {
    val trimmed = config.hostId.trim
    if (trimmed.startsWith("host/")) trimmed else s"host/$trimmed"
  }

  /** Encodes one path segment; '/' inside ids becomes %2F as Conjur requires. */
  private def encodePathSegment(value: String): String =
    URLEncoder.encode(value, StandardCharsets.UTF_8.name()).replace("+", "%20")

  /** Error bodies are truncated — authentication systems sometimes echo
    * sensitive request details. */
  private def safeErrorMessage(prefix: String, responseBody: String): String = {
    val sanitized = Option(responseBody).map(_.trim).filter(_.nonEmpty)
      .map(_.take(300)).getOrElse("No response body")
    s"$prefix: $sanitized"
  }
}
