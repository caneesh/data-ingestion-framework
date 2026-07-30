package com.hcsc.generic.ingest.jdbc.auth

import com.hcsc.generic.ingest.config.ConfigUtils
import com.typesafe.config.Config

import java.nio.charset.StandardCharsets

/** Shared HTTP plumbing for the vault-backed secret providers, so the
  * byte-draining loop and the insecure-http gate exist exactly once. */
private[auth] object VaultHttp {

  /** Fully drains a response stream as UTF-8; null streams read as empty. */
  def readAll(stream: java.io.InputStream): String = {
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

  /**
    * Validates a vault base URL: http(s) scheme required, and plain http is
    * rejected unless `allow_insecure_http = true` (isolated dev only).
    * `transmitted` names what the connection would leak in cleartext, so
    * each provider's historical error message is preserved verbatim.
    * Returns the trimmed base URL without a trailing slash.
    */
  def requireSecureBase(conf: Config, url: String, provider: String, transmitted: String): String = {
    val base = url.trim.stripSuffix("/")
    if (!base.startsWith("https://") && !base.startsWith("http://"))
      throw new IllegalArgumentException(s"JDBC_003 $provider url '$base' must be an http(s) URL")
    if (base.startsWith("http://") &&
        !ConfigUtils.optBoolean(conf, "allow_insecure_http").getOrElse(false))
      throw new IllegalArgumentException(
        s"JDBC_003 $provider url '$base' uses plain http, which transmits $transmitted " +
          "in cleartext; use https, or set allow_insecure_http = true only for isolated " +
          "non-production environments")
    base
  }
}
