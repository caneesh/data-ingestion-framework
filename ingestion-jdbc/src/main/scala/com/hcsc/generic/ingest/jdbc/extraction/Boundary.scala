package com.hcsc.generic.ingest.jdbc.extraction

/**
  * Domain model for extraction windows. Pure data plus pure arithmetic —
  * nothing in this file touches Spark, JDBC or configuration.
  */

/** Supported boundary column types. STRING is ordering-only (composite
  * tie-break components); it supports no overlap arithmetic. */
object BoundaryType {
  val Timestamp = "TIMESTAMP"
  val Numeric = "NUMERIC"
  val StringType = "STRING"
  val all: Seq[String] = Seq(Timestamp, Numeric, StringType)
}

/** One boundary column: canonical name plus its BoundaryType. */
final case class BoundaryColumn(name: String, columnType: String)

/** A typed position on the boundary axis; composite boundaries carry one
  * value per column, ordered lexicographically. Serialized with '|'. */
final case class BoundaryValue(values: Seq[String]) {
  def serialized: String = values.mkString("|")
}

object BoundaryValue {
  def deserialize(s: String): BoundaryValue = BoundaryValue(s.split("\\|", -1).toSeq)
}

/** The extraction window. HalfOpen is strictly-greater-than `lower` and, when
  * `upper` is present, at-most `upper` (the value captured on the driver
  * before extraction, so every partition sees one reproducible window). The
  * lower value arrives with overlap ALREADY applied — rendering never does
  * arithmetic. */
sealed trait ExtractionBoundary
object ExtractionBoundary {
  case object Unbounded extends ExtractionBoundary
  final case class HalfOpen(lower: BoundaryValue, upper: Option[BoundaryValue]) extends ExtractionBoundary
}

/** Lexicographic comparison of boundary values under their column types. */
object BoundaryOrdering {

  def compare(columns: Seq[BoundaryColumn], a: BoundaryValue, b: BoundaryValue): Int = {
    require(a.values.size == columns.size && b.values.size == columns.size,
      s"EXT_002 boundary value arity ${a.values.size}/${b.values.size} does not match ${columns.size} column(s)")
    columns.indices.foreach { i =>
      val c = comparePart(columns(i).columnType, a.values(i), b.values(i))
      if (c != 0) return c
    }
    0
  }

  private def comparePart(columnType: String, a: String, b: String): Int = columnType match {
    case BoundaryType.Numeric    => parseNumeric(a).compare(parseNumeric(b))
    case BoundaryType.StringType => a.compareTo(b)
    case _                       => parseTimestamp(a).compareTo(parseTimestamp(b))
  }

  private[extraction] def parseNumeric(value: String): BigDecimal =
    try BigDecimal(value.trim)
    catch { case _: NumberFormatException =>
      throw new IllegalArgumentException(s"EXT_002 boundary value '$value' is not numeric") }

  private[extraction] def parseTimestamp(value: String): java.sql.Timestamp =
    try java.sql.Timestamp.valueOf(value.trim)
    catch { case _: IllegalArgumentException =>
      throw new IllegalArgumentException(
        s"EXT_002 boundary value '$value' is not a timestamp (expected yyyy-MM-dd HH:mm:ss[.fff])") }
}

/** Overlap arithmetic: widens the LOWER edge of a window by re-reading a
  * margin behind the last checkpoint. Applied to the first (driving) column
  * only, matching lexicographic window semantics. */
object OverlapPolicy {

  def applyTo(value: BoundaryValue, columns: Seq[BoundaryColumn], overlap: Option[BigDecimal]): BoundaryValue =
    overlap match {
      case None => value
      case Some(amount) =>
        require(columns.nonEmpty && value.values.nonEmpty, "EXT_002 overlap requires at least one boundary column")
        val widened = columns.head.columnType match {
          case BoundaryType.Numeric =>
            (BoundaryOrdering.parseNumeric(value.values.head) - amount).toString
          case BoundaryType.StringType =>
            throw new IllegalArgumentException(
              "EXT_002 overlap is not supported for STRING boundary columns (no arithmetic ordering)")
          case _ =>
            val ts = BoundaryOrdering.parseTimestamp(value.values.head)
            new java.sql.Timestamp(ts.getTime - (amount * 1000).toLong).toString
        }
        BoundaryValue(widened +: value.values.tail)
    }
}
