package com.hcsc.generic.ingest.confgen.summary

import com.typesafe.config.{Config, ConfigObject, ConfigValue, ConfigValueFactory, ConfigValueType}

import scala.collection.JavaConverters._

/**
  * Exportable configuration summary with every secret masked. Masking is
  * structural (walks the config tree), not textual, so a secret can never
  * leak through quoting or line wrapping:
  *
  *   - any scalar under a sensitive key (password, secret, token, jaas) is
  *     replaced with ********
  *   - inside secret-reference objects only `value` (the inline secret) is
  *     masked; provider/key/vault metadata stays readable for review
  */
object ConfigSummary {

  private val SensitiveKeys = Seq("password", "secret", "token", "jaas", "credential")

  def masked(config: Config): Config = mask(config.root(), sensitive = false) match {
    case obj: ConfigObject => obj.toConfig
    case _ => config
  }

  private def sensitiveKey(key: String): Boolean = {
    val lower = key.toLowerCase
    SensitiveKeys.exists(lower.contains)
  }

  private def mask(value: ConfigValue, sensitive: Boolean): ConfigValue = value match {
    case obj: ConfigObject =>
      val isSecretRef = obj.containsKey("provider")
      obj.keySet().asScala.foldLeft(obj: ConfigObject) { (acc, key) =>
        val childSensitive =
          if (isSecretRef) key == "value" // mask only the inline secret payload
          else sensitive || sensitiveKey(key)
        acc.withValue(key, mask(obj.get(key), childSensitive))
      }
    case list if list.valueType() == ConfigValueType.LIST =>
      if (sensitive) ConfigValueFactory.fromAnyRef("********") else list
    case scalar =>
      if (sensitive) ConfigValueFactory.fromAnyRef("********") else scalar
  }
}
