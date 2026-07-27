package com.hcsc.generic.ingest.confgen.draft

import com.hcsc.generic.ingest.confgen.model.{Answers, AnswerValue}
import com.typesafe.config.{ConfigFactory, ConfigRenderOptions, ConfigValueFactory}

import java.nio.file.{Files, Paths, StandardCopyOption}
import scala.collection.JavaConverters._

/**
  * Draft persistence: a JSON file capturing the source type and every answer
  * in order, so a partially completed wizard session can resume exactly where
  * it stopped. JSON is valid HOCON, so drafts are read back with the same
  * parser used everywhere else in the framework.
  */
object Drafts {

  case class Draft(sourceType: String, answers: Answers)

  def save(path: String, sourceType: String, answers: Answers): Unit = {
    val entries: java.util.List[Object] = answers.ordered.map { case (id, value) =>
      val entry = new java.util.LinkedHashMap[String, Object]()
      entry.put("id", id)
      value match {
        case AnswerValue.Scalar(v) => entry.put("scalar", v)
        case AnswerValue.Items(v) => entry.put("items", v.asJava)
        case AnswerValue.Blocks(v) =>
          entry.put("blocks", v.map(_.root().unwrapped()).asJava)
      }
      entry: Object
    }.asJava

    val root = ConfigFactory.empty()
      .withValue("source_type", ConfigValueFactory.fromAnyRef(sourceType))
      .withValue("answers", ConfigValueFactory.fromIterable(entries))
    val rendered = root.root().render(ConfigRenderOptions.concise().setFormatted(true))

    // Write via a temp file + atomic move so an interrupt never truncates a draft.
    val target = Paths.get(path)
    if (target.getParent != null) Files.createDirectories(target.getParent)
    val tmp = Files.createTempFile(
      Option(target.getParent).getOrElse(Paths.get(".")), ".draft", ".tmp")
    Files.write(tmp, rendered.getBytes(java.nio.charset.StandardCharsets.UTF_8))
    Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING)
  }

  def load(path: String): Draft = {
    val p = Paths.get(path)
    require(Files.exists(p), s"Draft file not found: $path")
    val conf = ConfigFactory.parseString(
      new String(Files.readAllBytes(p), java.nio.charset.StandardCharsets.UTF_8))
    val answers = new Answers
    conf.getConfigList("answers").asScala.foreach { entry =>
      val id = entry.getString("id")
      if (entry.hasPath("scalar"))
        answers.put(id, AnswerValue.Scalar(entry.getString("scalar")))
      else if (entry.hasPath("items"))
        answers.put(id, AnswerValue.Items(entry.getStringList("items").asScala.toSeq))
      else if (entry.hasPath("blocks"))
        answers.put(id, AnswerValue.Blocks(
          entry.getConfigList("blocks").asScala.toSeq))
      else throw new IllegalArgumentException(s"Draft entry '$id' has no value")
    }
    Draft(conf.getString("source_type"), answers)
  }
}
