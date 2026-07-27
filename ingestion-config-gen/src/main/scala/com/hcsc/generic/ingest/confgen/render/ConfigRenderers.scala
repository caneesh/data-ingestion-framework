package com.hcsc.generic.ingest.confgen.render

import com.typesafe.config.{Config, ConfigList, ConfigObject, ConfigRenderOptions, ConfigValue, ConfigValueType}

import scala.collection.JavaConverters._

/**
  * Output renderers. HOCON and JSON delegate to Typesafe Config (guaranteed
  * to re-parse with the framework's own parser); YAML is emitted by a small
  * dependency-free printer for teams that keep configuration in YAML tooling
  * — the framework itself consumes the HOCON/JSON forms.
  */
object ConfigRenderers {

  val formats: Seq[String] = Seq("hocon", "json", "yaml")

  def extensionFor(format: String): String = format.toLowerCase match {
    case "hocon" => "conf"
    case "json" => "json"
    case "yaml" => "yaml"
    case other => throw new IllegalArgumentException(
      s"Unknown output format '$other'; expected one of ${formats.mkString(", ")}")
  }

  def render(config: Config, format: String): String = format.toLowerCase match {
    case "hocon" =>
      config.root().render(ConfigRenderOptions.defaults()
        .setOriginComments(false).setComments(false).setJson(false).setFormatted(true))
    case "json" =>
      config.root().render(ConfigRenderOptions.concise().setFormatted(true))
    case "yaml" =>
      yaml(config.root(), 0)
    case other =>
      throw new IllegalArgumentException(
        s"Unknown output format '$other'; expected one of ${formats.mkString(", ")}")
  }

  // ---------------------------------------------------------------------------
  // Minimal YAML emitter
  // ---------------------------------------------------------------------------

  private def yaml(value: ConfigValue, indent: Int): String = value match {
    case obj: ConfigObject =>
      obj.entrySet().asScala.toSeq.sortBy(_.getKey).map { entry =>
        val key = yamlKey(entry.getKey)
        entry.getValue match {
          case o: ConfigObject if o.isEmpty => s"${pad(indent)}$key: {}"
          case o: ConfigObject => s"${pad(indent)}$key:\n${yaml(o, indent + 1)}"
          case l: ConfigList if l.isEmpty => s"${pad(indent)}$key: []"
          case l: ConfigList => s"${pad(indent)}$key:\n${yaml(l, indent + 1)}"
          case scalar => s"${pad(indent)}$key: ${yamlScalar(scalar)}"
        }
      }.mkString("\n")
    case list: ConfigList =>
      list.asScala.map {
        case o: ConfigObject =>
          val body = yaml(o, indent + 1)
          s"${pad(indent)}-\n$body"
        case scalar => s"${pad(indent)}- ${yamlScalar(scalar)}"
      }.mkString("\n")
    case scalar => s"${pad(indent)}${yamlScalar(scalar)}"
  }

  private def yamlScalar(value: ConfigValue): String = value.valueType() match {
    case ConfigValueType.NUMBER | ConfigValueType.BOOLEAN => value.unwrapped().toString
    case ConfigValueType.NULL => "null"
    case _ =>
      // Strings are always single-quoted: immune to YAML's implicit typing
      // (ON/OFF, dates, octals) and safe for SQL fragments.
      "'" + value.unwrapped().toString.replace("'", "''") + "'"
  }

  private def yamlKey(key: String): String =
    if (key.matches("[a-zA-Z0-9_-]+")) key else "'" + key.replace("'", "''") + "'"

  private def pad(indent: Int): String = "  " * indent
}
