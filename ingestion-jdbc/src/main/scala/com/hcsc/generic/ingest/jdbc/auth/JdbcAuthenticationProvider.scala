package com.hcsc.generic.ingest.jdbc.auth

import com.hcsc.generic.ingest.config.ConfigUtils
import com.typesafe.config.Config

/** Credentials and driver properties produced by an authentication mode. */
final case class ResolvedAuth(
  user: Option[String],
  password: Option[String],
  properties: Map[String, String]
)

/**
  * Pluggable authentication for JDBC sources. Each mode is one provider,
  * registered by canonical name plus accepted aliases; new modes register
  * without touching existing code. Azure/Entra modes map onto Microsoft
  * JDBC driver `authentication` properties — the driver performs the token
  * flows, so no Azure SDK is required, and executor connections
  * re-authenticate through the same driver properties.
  *
  * Validation rules enforced here:
  *   - required fields fail fast (JDBC_003)
  *   - incompatible fields are rejected (e.g. a password on MANAGED_IDENTITY)
  *   - secrets resolved through providers must be non-blank (JDBC_002)
  */
trait JdbcAuthenticationProvider {
  def authenticationType: String
  def aliases: Seq[String] = Seq.empty
  def requiresSqlServerDialect: Boolean = true
  def resolve(auth: Option[Config], source: Config): ResolvedAuth

  protected def fail(message: String): Nothing =
    throw new IllegalArgumentException(s"JDBC_003 $message")

  /** Secret-backed value that must resolve to something non-blank. */
  protected def requiredSecret(conf: Config, path: String, what: String): String = {
    val value = SecretProviders.resolveAt(conf, path).getOrElse(fail(s"$what is required"))
    if (value.trim.isEmpty)
      throw new IllegalArgumentException(s"JDBC_002 $what resolved to an empty value")
    value
  }

  protected def rejectFields(auth: Option[Config], fields: Seq[String]): Unit =
    auth.foreach { a =>
      val present = fields.filter(a.hasPath)
      if (present.nonEmpty)
        fail(s"auth.type $authenticationType does not accept: ${present.mkString(", ")}")
    }
}

object SqlPasswordAuth extends JdbcAuthenticationProvider {
  val authenticationType = "SQL_PASSWORD"
  override val requiresSqlServerDialect = false
  def resolve(auth: Option[Config], source: Config): ResolvedAuth = {
    // The user id resolves through the same provider chain as the password
    // (CyberArk serves both from one vault object); a bare string stays a
    // plain value and does not warn.
    val user = auth.flatMap(a => SecretProviders.resolveAt(a, "user", warnInline = false))
      .orElse(ConfigUtils.optString(source, "user"))
    val password = auth.flatMap(a => SecretProviders.resolveAt(a, "password"))
      .orElse(SecretProviders.resolveAt(source, "password"))
    ResolvedAuth(user, password, Map.empty)
  }
}

object ManagedIdentityAuth extends JdbcAuthenticationProvider {
  val authenticationType = "MANAGED_IDENTITY"
  override val aliases = Seq("AZURE_MANAGED_IDENTITY")
  def resolve(auth: Option[Config], source: Config): ResolvedAuth = {
    rejectFields(auth, Seq("user", "password", "client_secret", "token"))
    val props = Map("authentication" -> "ActiveDirectoryMSI") ++
      auth.flatMap(a => ConfigUtils.optString(a, "client_id")).map("msiClientId" -> _)
    ResolvedAuth(None, None, props)
  }
}

object EntraServicePrincipalAuth extends JdbcAuthenticationProvider {
  val authenticationType = "ENTRA_SERVICE_PRINCIPAL"
  override val aliases = Seq("AZURE_SERVICE_PRINCIPAL")
  def resolve(auth: Option[Config], source: Config): ResolvedAuth = {
    val a = auth.getOrElse(fail(s"auth block required for $authenticationType"))
    rejectFields(auth, Seq("user", "password", "token"))
    val clientId = ConfigUtils.optString(a, "client_id")
      .orElse(SecretProviders.resolveAt(a, "client_id_secret"))
      .getOrElse(fail("auth.client_id (or client_id_secret) is required for ENTRA_SERVICE_PRINCIPAL"))
    val clientSecret = requiredSecret(a, "client_secret", "auth.client_secret")
    ResolvedAuth(Some(clientId), Some(clientSecret),
      Map("authentication" -> "ActiveDirectoryServicePrincipal"))
  }
}

object EntraPasswordAuth extends JdbcAuthenticationProvider {
  val authenticationType = "ENTRA_PASSWORD"
  override val aliases = Seq("ENTRA_ID_PASSWORD")
  def resolve(auth: Option[Config], source: Config): ResolvedAuth = {
    val a = auth.getOrElse(fail(s"auth block required for $authenticationType"))
    rejectFields(auth, Seq("client_secret", "token", "client_id"))
    val user = ConfigUtils.optString(a, "user").getOrElse(fail("auth.user is required for ENTRA_PASSWORD"))
    val password = requiredSecret(a, "password", "auth.password")
    ResolvedAuth(Some(user), Some(password), Map("authentication" -> "ActiveDirectoryPassword"))
  }
}

/** DefaultAzureCredential chain inside the Microsoft driver: environment,
  * managed identity, IntelliJ/az-cli — for environments where the concrete
  * mechanism varies per host. */
object EntraDefaultAuth extends JdbcAuthenticationProvider {
  val authenticationType = "ENTRA_DEFAULT"
  override val aliases = Seq("AZURE_DEFAULT_CREDENTIAL")
  def resolve(auth: Option[Config], source: Config): ResolvedAuth = {
    rejectFields(auth, Seq("user", "password", "client_secret", "token"))
    ResolvedAuth(None, None, Map("authentication" -> "ActiveDirectoryDefault"))
  }
}

/** Kerberos/Windows integrated auth — requires OS-level configuration on
  * every Spark host (driver AND executors); documented in DEPLOYMENT.md. */
object EntraIntegratedAuth extends JdbcAuthenticationProvider {
  val authenticationType = "ENTRA_INTEGRATED"
  def resolve(auth: Option[Config], source: Config): ResolvedAuth = {
    rejectFields(auth, Seq("user", "password", "client_secret", "token", "client_id"))
    ResolvedAuth(None, None, Map("authentication" -> "ActiveDirectoryIntegrated"))
  }
}

/** Pre-fetched token. Lifecycle warning: a token generated on the driver can
  * expire during long jobs — prefer MANAGED_IDENTITY for long extractions. */
object AccessTokenAuth extends JdbcAuthenticationProvider {
  val authenticationType = "ACCESS_TOKEN"
  def resolve(auth: Option[Config], source: Config): ResolvedAuth = {
    val a = auth.getOrElse(fail(s"auth block required for $authenticationType"))
    rejectFields(auth, Seq("user", "password", "client_secret", "client_id"))
    val token = requiredSecret(a, "token", "auth.token")
    ResolvedAuth(None, None, Map("accessToken" -> token))
  }
}

object JdbcAuthenticationProviders {
  private val providers = new java.util.concurrent.ConcurrentHashMap[String, JdbcAuthenticationProvider]()

  def register(p: JdbcAuthenticationProvider): Unit = {
    providers.put(p.authenticationType.toUpperCase, p)
    p.aliases.foreach(a => providers.put(a.toUpperCase, p))
  }

  Seq(SqlPasswordAuth, ManagedIdentityAuth, EntraServicePrincipalAuth, EntraPasswordAuth,
    EntraDefaultAuth, EntraIntegratedAuth, AccessTokenAuth).foreach(register)

  def resolve(name: String): JdbcAuthenticationProvider =
    Option(providers.get(name.toUpperCase)).getOrElse(
      throw new IllegalArgumentException(
        s"JDBC_003 auth.type '$name' must be one of ${availableNames.mkString(", ")}"))

  def availableNames: Seq[String] = {
    import scala.collection.JavaConverters._
    providers.values().asScala.map(_.authenticationType).toSeq.distinct.sorted
  }
}
