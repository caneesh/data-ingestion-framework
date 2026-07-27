package com.hcsc.generic.ingest.confgen.model

import com.typesafe.config.Config
import scala.collection.mutable

/** How a question is asked and how its raw text answer is typed when the
  * configuration is assembled. */
sealed trait QuestionKind
object QuestionKind {
  case object Text extends QuestionKind
  case object BoolKind extends QuestionKind
  case object IntKind extends QuestionKind
  /** Rendered as an unquoted string in the config (e.g. overlap = "300"). */
  case object DecimalText extends QuestionKind
  case class Choice(options: Seq[String]) extends QuestionKind
  /** Large collection: inline CSV, inline JSON array, or @file (text/CSV/JSON). */
  case object ListKind extends QuestionKind
  /** Collection of structured objects (JSON array of objects, inline or @file);
    * plain CSV input is accepted and wrapped via `wrapPlain`. */
  case class BlockList(wrapPlain: String => Config) extends QuestionKind
}

/**
  * One questionnaire entry. `id` is the HOCON path of the value relative to
  * the feed root (e.g. `source.incremental.watermark_type`); ids starting
  * with `_` are wizard metadata and never emitted into the configuration.
  */
case class Question(
  id: String,
  group: String,
  prompt: String,
  help: String = "",
  kind: QuestionKind = QuestionKind.Text,
  default: Option[String] = None,
  /** Dynamic default computed from earlier answers; wins over `default`. */
  defaultFrom: Answers => Option[String] = _ => None,
  required: Boolean = true,
  secret: Boolean = false,
  appliesWhen: Answers => Boolean = _ => true,
  validate: String => Either[String, String] = v => Right(v)
)

/** Question groups, in presentation order (mirrors the design spec). */
object QuestionGroups {
  val General = "General"
  val Source = "Source"
  val Authentication = "Authentication"
  val Extraction = "Extraction"
  val Watermark = "Watermark"
  val Retry = "Retry"
  val Audit = "Audit"
  val Performance = "Performance"
  val Destination = "Destination"
  val ordered = Seq(General, Source, Authentication, Extraction, Watermark,
    Retry, Audit, Performance, Destination)
}

sealed trait AnswerValue
object AnswerValue {
  case class Scalar(value: String) extends AnswerValue
  case class Items(values: Seq[String]) extends AnswerValue
  /** Structured objects (e.g. schema contract columns, filter definitions). */
  case class Blocks(values: Seq[Config]) extends AnswerValue
}

/** Ordered answer store; the insertion order drives draft round-trips and
  * the generated configuration layout. */
class Answers {
  private val values = mutable.LinkedHashMap[String, AnswerValue]()

  def put(id: String, value: AnswerValue): Unit = values.put(id, value)
  def get(id: String): Option[AnswerValue] = values.get(id)
  def contains(id: String): Boolean = values.contains(id)
  def remove(id: String): Unit = values.remove(id)
  def ordered: Seq[(String, AnswerValue)] = values.toSeq

  def scalar(id: String): Option[String] = values.get(id).collect {
    case AnswerValue.Scalar(v) => v
  }

  def items(id: String): Seq[String] = values.get(id) match {
    case Some(AnswerValue.Items(v)) => v
    case Some(AnswerValue.Scalar(v)) if v.nonEmpty => Seq(v)
    case _ => Seq.empty
  }

  /** Case-insensitive scalar comparison, the workhorse of `appliesWhen`. */
  def is(id: String, expected: String): Boolean =
    scalar(id).exists(_.equalsIgnoreCase(expected))

  def isTrue(id: String): Boolean =
    scalar(id).exists(v => v.equalsIgnoreCase("true") || v.equalsIgnoreCase("y") || v.equalsIgnoreCase("yes"))
}

/** Shared per-answer validators (validation happens while prompting). */
object Validators {
  val identifier: String => Either[String, String] = v =>
    if (v.matches("[a-zA-Z_][a-zA-Z0-9_]*")) Right(v)
    else Left(s"'$v' is not a valid identifier (letters, digits, underscore; must not start with a digit)")

  val jdbcUrl: String => Either[String, String] = v =>
    if (v.startsWith("jdbc:")) Right(v) else Left(s"JDBC url must start with 'jdbc:', got '$v'")

  val positiveInt: String => Either[String, String] = v =>
    try { if (v.trim.toInt > 0) Right(v.trim) else Left(s"'$v' must be a positive integer") }
    catch { case _: NumberFormatException => Left(s"'$v' is not an integer") }

  val nonNegativeNumber: String => Either[String, String] = v =>
    try { if (BigDecimal(v.trim) >= 0) Right(v.trim) else Left(s"'$v' must be >= 0") }
    catch { case _: NumberFormatException => Left(s"'$v' is not a number") }

  val integer: String => Either[String, String] = v =>
    try { Right(v.trim.toLong.toString) }
    catch { case _: NumberFormatException => Left(s"'$v' is not an integer") }

  val nonEmpty: String => Either[String, String] = v =>
    if (v.trim.nonEmpty) Right(v.trim) else Left("value must not be empty")

  def oneOf(options: Seq[String]): String => Either[String, String] = v =>
    options.find(_.equalsIgnoreCase(v.trim)) match {
      case Some(canonical) => Right(canonical)
      case None => Left(s"'$v' is not one of: ${options.mkString(", ")}")
    }
}
