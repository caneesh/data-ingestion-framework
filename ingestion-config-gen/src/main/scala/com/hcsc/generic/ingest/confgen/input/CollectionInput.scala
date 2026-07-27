package com.hcsc.generic.ingest.confgen.input

import com.hcsc.generic.ingest.confgen.model.AnswerValue
import com.typesafe.config.{Config, ConfigFactory}

import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
  * Parses "large collection" answers (column lists, primary keys, filter
  * definitions, ...). Accepted forms:
  *
  *   - inline CSV:        `a, b, c`
  *   - inline JSON array: `["a","b"]` or `[{"name":"a","type":"string"}]`
  *   - file reference:    `@/path/to/list.txt|.csv|.json`
  *
  * Text/CSV files contribute one item per line (lines may themselves be
  * comma-separated); JSON files must contain a single array. File references
  * are validated for existence, parsed eagerly and previewed so input
  * problems surface while the user is still in the wizard.
  */
object CollectionInput {

  case class Parsed(value: AnswerValue, preview: Seq[String], sourceDescription: String)

  private val PreviewLimit = 5

  def parse(raw: String): Either[String, Parsed] = {
    val trimmed = raw.trim
    if (trimmed.isEmpty) Right(Parsed(AnswerValue.Items(Seq.empty), Seq.empty, "empty"))
    else if (trimmed.startsWith("@")) fromFile(trimmed.drop(1).trim)
    else if (trimmed.startsWith("[")) fromJson(trimmed, "inline JSON")
    else fromCsv(trimmed, "inline CSV")
  }

  private def fromFile(path: String): Either[String, Parsed] = {
    val p = Paths.get(path)
    if (!Files.exists(p)) Left(s"File not found: $path")
    else if (!Files.isReadable(p)) Left(s"File is not readable: $path")
    else {
      val content = new String(Files.readAllBytes(p), java.nio.charset.StandardCharsets.UTF_8)
      val lower = path.toLowerCase
      if (lower.endsWith(".json") || content.trim.startsWith("[")) fromJson(content, s"file $path")
      else {
        val items = content.split("\r?\n").toSeq
          .map(_.trim).filter(_.nonEmpty).filterNot(_.startsWith("#"))
          .flatMap(_.split(",").map(_.trim).filter(_.nonEmpty))
        if (items.isEmpty) Left(s"File $path contains no items")
        else Right(Parsed(AnswerValue.Items(items), preview(items), s"file $path (${items.size} items)"))
      }
    }
  }

  private def fromCsv(text: String, description: String): Either[String, Parsed] = {
    val items = text.split(",").map(_.trim).filter(_.nonEmpty).toSeq
    if (items.isEmpty) Left("no items found in CSV input")
    else Right(Parsed(AnswerValue.Items(items), preview(items), s"$description (${items.size} items)"))
  }

  /** JSON arrays are parsed with the same HOCON parser the framework uses,
    * so anything accepted here is guaranteed to round-trip into the feed. */
  private def fromJson(text: String, description: String): Either[String, Parsed] = {
    Try(ConfigFactory.parseString(s"items = ${text.trim}").getList("items")) match {
      case Failure(e) =>
        Left(s"Invalid JSON array in $description: ${e.getMessage}")
      case Success(list) =>
        val values = list.asScala.toSeq
        val objects = values.forall(_.valueType() == com.typesafe.config.ConfigValueType.OBJECT)
        if (values.isEmpty) Left(s"JSON array in $description is empty")
        else if (objects) {
          val configs: Seq[Config] = values.map(v =>
            v.asInstanceOf[com.typesafe.config.ConfigObject].toConfig)
          val names = configs.map(c =>
            if (c.hasPath("name")) c.getString("name") else c.root().render(com.typesafe.config.ConfigRenderOptions.concise()))
          Right(Parsed(AnswerValue.Blocks(configs), preview(names), s"$description (${configs.size} objects)"))
        } else {
          val items = values.map(_.unwrapped().toString)
          Right(Parsed(AnswerValue.Items(items), preview(items), s"$description (${items.size} items)"))
        }
    }
  }

  private def preview(items: Seq[String]): Seq[String] =
    if (items.size <= PreviewLimit) items
    else items.take(PreviewLimit) :+ s"... and ${items.size - PreviewLimit} more"
}
