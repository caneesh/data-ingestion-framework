package com.hcsc.generic.ingest.jdbc.extraction

/**
  * Domain model for extraction windows. Pure data plus pure arithmetic —
  * nothing in this file touches Spark, JDBC or configuration.
  */

/** Supported boundary column types. STRING is ordering-only (composite
  * tie-break components); it supports no overlap arithmetic. CDC_OFFSET and
  * SNAPSHOT_ID are numerically ordered positions (log offsets, LSN/SCN as
  * numbers, snapshot sequence numbers): CDC offsets allow numeric overlap
  * (deliberate offset re-reads), snapshot ids do not — a snapshot id is an
  * identity, not a measure. */
object BoundaryType {
  val Timestamp = "TIMESTAMP"
  val Numeric = "NUMERIC"
  val StringType = "STRING"
  val CdcOffset = "CDC_OFFSET"
  val SnapshotId = "SNAPSHOT_ID"
  val all: Seq[String] = Seq(Timestamp, Numeric, StringType, CdcOffset, SnapshotId)

  /** Types whose values compare and render as numbers. */
  private[extraction] val numericFamily: Set[String] = Set(Numeric, CdcOffset, SnapshotId)
}

/** One boundary column: canonical name plus its BoundaryType. */
final case class BoundaryColumn(name: String, columnType: String)

/** A typed position on the boundary axis; composite boundaries carry one
  * value per column, ordered lexicographically. Serialized with '|', with
  * backslash-escaping of '|' and '\' inside values — STRING tie-break
  * values come from live source rows, and an unescaped pipe would poison
  * the stored checkpoint permanently (arity mismatch on every later run).
  * Values without those characters serialize exactly as before, so existing
  * stored checkpoints parse unchanged. */
final case class BoundaryValue(values: Seq[String]) {
  def serialized: String =
    values.map(_.replace("\\", "\\\\").replace("|", "\\|")).mkString("|")
}

object BoundaryValue {
  def deserialize(s: String): BoundaryValue = {
    val parts = scala.collection.mutable.ArrayBuffer.empty[String]
    val current = new StringBuilder
    var i = 0
    while (i < s.length) {
      val c = s.charAt(i)
      if (c == '\\' && i + 1 < s.length) { current.append(s.charAt(i + 1)); i += 2 }
      else if (c == '|') { parts += current.toString; current.clear(); i += 1 }
      else { current.append(c); i += 1 }
    }
    parts += current.toString
    BoundaryValue(parts.toList)
  }
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

  private def comparePart(columnType: String, a: String, b: String): Int =
    if (BoundaryType.numericFamily.contains(columnType)) parseNumeric(a).compare(parseNumeric(b))
    else if (columnType == BoundaryType.StringType) a.compareTo(b)
    else parseTimestamp(a).compareTo(parseTimestamp(b))

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
          case BoundaryType.Numeric | BoundaryType.CdcOffset =>
            (BoundaryOrdering.parseNumeric(value.values.head) - amount).toString
          case BoundaryType.StringType =>
            throw new IllegalArgumentException(
              "EXT_002 overlap is not supported for STRING boundary columns (no arithmetic ordering)")
          case BoundaryType.SnapshotId =>
            throw new IllegalArgumentException(
              "EXT_002 overlap is not supported for SNAPSHOT_ID boundaries (a snapshot id is an identity, not a measure)")
          case _ =>
            val ts = BoundaryOrdering.parseTimestamp(value.values.head)
            new java.sql.Timestamp(ts.getTime - (amount * 1000).toLong).toString
        }
        BoundaryValue(widened +: value.values.tail)
    }
}
