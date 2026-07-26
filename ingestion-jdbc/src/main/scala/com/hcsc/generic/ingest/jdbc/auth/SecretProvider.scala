package com.hcsc.generic.ingest.jdbc.auth

import com.hcsc.generic.ingest.config.ConfigUtils
import com.typesafe.config.Config

/**
  * Secret resolution abstraction so credentials never need to live in feed
  * configuration files. Reference form in HOCON:
  *
  *   password { provider = "env",     key = "SQLSERVER_PWD" }
  *   password { provider = "file",    path = "/etc/secrets/sqlserver" }
  *   password { provider = "sysprop", key = "sqlserver.pwd" }
  *   password { provider = "inline",  value = "..." }   # dev/test only
  *
  * A bare string value is treated as inline for backward compatibility (with
  * a warning). Extend by registering a new provider (e.g. Vault, Azure Key
  * Vault) — no framework changes needed.
  */
trait SecretProvider {
  def name: String
  def resolve(conf: Config): String
}

object InlineSecretProvider extends SecretProvider {
  val name = "inline"
  def resolve(conf: Config): String = conf.getString("value")
}

object EnvSecretProvider extends SecretProvider {
  val name = "env"
  def resolve(conf: Config): String = {
    val key = conf.getString("key")
    sys.env.getOrElse(key,
      throw new IllegalArgumentException(s"JDBC_002 Environment variable '$key' is not set"))
  }
}

object SysPropSecretProvider extends SecretProvider {
  val name = "sysprop"
  def resolve(conf: Config): String = {
    val key = conf.getString("key")
    Option(System.getProperty(key)).getOrElse(
      throw new IllegalArgumentException(s"JDBC_002 System property '$key' is not set"))
  }
}

object FileSecretProvider extends SecretProvider {
  val name = "file"
  def resolve(conf: Config): String = {
    val path = java.nio.file.Paths.get(conf.getString("path"))
    if (!java.nio.file.Files.isReadable(path))
      throw new IllegalArgumentException(s"JDBC_002 Secret file '$path' is not readable")
    new String(java.nio.file.Files.readAllBytes(path), "UTF-8").trim
  }
}

object SecretProviders {
  private val providers = new java.util.concurrent.ConcurrentHashMap[String, SecretProvider]()

  def register(p: SecretProvider): Unit = providers.put(p.name.toLowerCase, p)

  Seq(InlineSecretProvider, EnvSecretProvider, SysPropSecretProvider, FileSecretProvider,
    CyberArkSecretProvider).foreach(register)

  /** Resolves an object-form secret reference ({ provider = ..., key/path/value }). */
  def resolveRef(ref: Config): String = {
    val providerName = ConfigUtils.optString(ref, "provider").getOrElse("inline")
    val provider = Option(providers.get(providerName.toLowerCase)).getOrElse(
      throw new IllegalArgumentException(
        s"JDBC_002 Unknown secret provider '$providerName'. Available: ${availableNames.mkString(", ")}"))
    provider.resolve(ref)
  }

  /** Resolves a secret reference at `path` in conf: object form with a
    * `provider` field, or a bare string treated as inline. `warnInline`
    * controls the plaintext warning — user ids are provider-resolvable but
    * not secret, so their bare form should not warn. */
  def resolveAt(conf: Config, path: String, warnInline: Boolean = true): Option[String] = {
    if (!conf.hasPath(path)) return None
    conf.getValue(path).valueType() match {
      case com.typesafe.config.ConfigValueType.STRING =>
        if (warnInline)
          org.apache.log4j.Logger.getLogger(getClass.getName).warn(
            s"[SecretProviders] '$path' is an inline plaintext secret; prefer a provider reference " +
              "(env/file/sysprop/cyberark) so credentials stay out of config files")
        Some(conf.getString(path))
      case com.typesafe.config.ConfigValueType.OBJECT =>
        Some(resolveRef(conf.getConfig(path)))
      case other =>
        throw new IllegalArgumentException(s"JDBC_003 Secret reference at '$path' must be a string or object, found $other")
    }
  }

  def availableNames: Seq[String] = {
    import scala.collection.JavaConverters._
    providers.keySet.asScala.toSeq.sorted
  }
}
