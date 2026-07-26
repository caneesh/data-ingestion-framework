package com.hcsc.generic.ingest.jdbc.watermark

import com.hcsc.generic.ingest.jdbc.{WatermarkConfig, WatermarkType}
import com.hcsc.generic.ingest.jdbc.dialect.JdbcDialect
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

/** A watermark value; composite watermarks carry one value per column. */
final case class WatermarkValue(values: Seq[String]) {
  def serialized: String = values.mkString("|")
}

object WatermarkValue {
  def deserialize(s: String): WatermarkValue = WatermarkValue(s.split("\\|", -1).toSeq)
}

/**
  * Watermark predicate construction and next-value computation.
  *
  * - TIMESTAMP:  col > 'value - overlap_seconds'
  * - NUMERIC:    col > (value - overlap_amount)
  * - COMPOSITE:  (c1 > v1) OR (c1 = v1 AND c2 > v2) ... with the overlap
  *               applied to the first column only
  *
  * Values are validated on parse (JDBC_004) so a corrupt stored watermark
  * can never inject SQL or silently select the wrong rows.
  */
object Watermarks {

  /** Bounded extraction window: strictly greater than `lower` AND at most
    * `upper` (the upper watermark captured on the driver before extraction),
    * so every Spark partition observes the same reproducible window even
    * while the source keeps changing. */
  def boundedPredicate(
    cfg: WatermarkConfig,
    dialect: JdbcDialect,
    lower: WatermarkValue,
    upper: WatermarkValue
  ): String =
    // Overlap widens only the LOWER side of the window; the captured upper
    // is exact so the committed boundary equals what was extracted.
    s"(${predicate(cfg, dialect, lower)}) AND NOT (${predicate(cfg, dialect, upper, useOverlap = false)})"

  def predicate(
    cfg: WatermarkConfig,
    dialect: JdbcDialect,
    latest: WatermarkValue,
    useOverlap: Boolean = true
  ): String = {
    require(latest.values.size == cfg.columns.size,
      s"JDBC_004 Watermark value has ${latest.values.size} part(s), expected ${cfg.columns.size}")

    val overlapped = latest.values.zipWithIndex.map { case (v, i) =>
      if (i == 0 && useOverlap) applyOverlap(cfg.columnTypes.head, v, cfg.overlap) else v
    }

    if (cfg.columns.size == 1) {
      s"${dialect.quoteIdentifier(cfg.columns.head)} > ${literal(cfg.columnTypes.head, overlapped.head)}"
    } else {
      // Lexicographic strictly-greater over (c1..cn)
      val clauses = cfg.columns.indices.map { i =>
        val equalPrefix = (0 until i).map { j =>
          s"${dialect.quoteIdentifier(cfg.columns(j))} = ${literal(cfg.columnTypes(j), overlapped(j))}"
        }
        val greater =
          s"${dialect.quoteIdentifier(cfg.columns(i))} > ${literal(cfg.columnTypes(i), overlapped(i))}"
        (equalPrefix :+ greater).mkString("(", " AND ", ")")
      }
      clauses.mkString("(", " OR ", ")")
    }
  }

  /** Highest watermark present in the extracted data, or None when empty.
    * Composite watermarks use lexicographic ordering over the columns. */
  def computeNext(df: DataFrame, cfg: WatermarkConfig): Option[WatermarkValue] = {
    val actualCols = cfg.columns.map { c =>
      df.columns.find(_.equalsIgnoreCase(c)).getOrElse(
        throw new IllegalArgumentException(
          s"JDBC_004 Watermark column '$c' not present in extracted data (columns: ${df.columns.mkString(",")})"))
    }
    val top = df
      .select(actualCols.map(col): _*)
      .na.drop("any")
      .orderBy(actualCols.map(c => col(c).desc): _*)
      .limit(1)
      .collect()

    top.headOption.map { row =>
      WatermarkValue(actualCols.indices.map(i => stringifyValue(row.get(i))))
    }
  }

  /** Canonical string forms so values round-trip through the typed parsers
    * (rowversion binaries become hex, temporals use their SQL forms). */
  private def stringifyValue(value: Any): String = value match {
    case bytes: Array[Byte] => bytes.map("%02X".format(_)).mkString
    case odt: java.time.OffsetDateTime => formatOffset(odt)
    case other => String.valueOf(other)
  }

  /** Advance-only merge: never move a watermark backwards (e.g. an overlap
    * window re-read returning older max than the stored value). */
  def max(cfg: WatermarkConfig, a: WatermarkValue, b: WatermarkValue): WatermarkValue =
    if (compare(cfg, a, b) >= 0) a else b

  def compare(cfg: WatermarkConfig, a: WatermarkValue, b: WatermarkValue): Int = {
    cfg.columnTypes.indices.foreach { i =>
      val c = compareTyped(cfg.columnTypes(i), a.values(i), b.values(i))
      if (c != 0) return c
    }
    0
  }

  /** Overlap semantics per type: TIMESTAMP/DATETIMEOFFSET seconds, DATE
    * days, NUMERIC/ROWVERSION subtraction. */
  private def applyOverlap(columnType: String, value: String, overlap: Option[BigDecimal]): String =
    overlap match {
      case None => value
      case Some(amount) => columnType match {
        case WatermarkType.Numeric =>
          (parseNumeric(value) - amount).toString
        case WatermarkType.Date =>
          parseDate(value).minusDays(amount.toLong).toString
        case WatermarkType.DatetimeOffset =>
          formatOffset(parseOffset(value).minusSeconds(amount.toLong))
        case WatermarkType.RowVersion =>
          val reduced = parseRowVersion(value) - BigInt(amount.toBigInt.bigInteger)
          "%016X".format(if (reduced < 0) BigInt(0) else reduced)
        case _ =>
          val ts = parseTimestamp(value)
          new java.sql.Timestamp(ts.getTime - (amount * 1000).toLong).toString
      }
    }

  /** SQL literal with parse validation — the value round-trips through a
    * typed representation, so stored garbage cannot become SQL. */
  private[watermark] def literal(columnType: String, value: String): String = columnType match {
    case WatermarkType.Numeric        => parseNumeric(value).toString
    case WatermarkType.Date           => s"'${parseDate(value).toString}'"
    case WatermarkType.DatetimeOffset => s"'${formatOffset(parseOffset(value))}'"
    case WatermarkType.RowVersion     => "0x%016X".format(parseRowVersion(value))
    case _                            => s"'${parseTimestamp(value).toString}'"
  }

  private[watermark] def compareTyped(columnType: String, a: String, b: String): Int = columnType match {
    case WatermarkType.Numeric        => parseNumeric(a).compare(parseNumeric(b))
    case WatermarkType.Date           => parseDate(a).compareTo(parseDate(b))
    case WatermarkType.DatetimeOffset => parseOffset(a).compareTo(parseOffset(b))
    case WatermarkType.RowVersion     => parseRowVersion(a).compare(parseRowVersion(b))
    case _                            => parseTimestamp(a).compareTo(parseTimestamp(b))
  }

  private def parseNumeric(value: String): BigDecimal =
    try BigDecimal(value.trim)
    catch { case _: NumberFormatException =>
      throw new IllegalArgumentException(s"JDBC_004 Watermark value '$value' is not numeric") }

  private def parseTimestamp(value: String): java.sql.Timestamp =
    try java.sql.Timestamp.valueOf(value.trim)
    catch { case _: IllegalArgumentException =>
      throw new IllegalArgumentException(
        s"JDBC_004 Watermark value '$value' is not a timestamp (expected yyyy-MM-dd HH:mm:ss[.fff])") }

  private def parseDate(value: String): java.time.LocalDate =
    try java.time.LocalDate.parse(value.trim)
    catch { case _: java.time.format.DateTimeParseException =>
      throw new IllegalArgumentException(s"JDBC_004 Watermark value '$value' is not a date (expected yyyy-MM-dd)") }

  private val OffsetFormat = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS XXX")
  private val OffsetParse = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.SSS] XXX")

  private def parseOffset(value: String): java.time.OffsetDateTime = {
    val v = value.trim
    try java.time.OffsetDateTime.parse(v)
    catch { case _: java.time.format.DateTimeParseException =>
      try java.time.OffsetDateTime.parse(v, OffsetParse)
      catch { case _: java.time.format.DateTimeParseException =>
        throw new IllegalArgumentException(
          s"JDBC_004 Watermark value '$value' is not a datetimeoffset " +
            "(expected ISO-8601 or 'yyyy-MM-dd HH:mm:ss[.SSS] +HH:MM')") }
    }
  }

  private def formatOffset(value: java.time.OffsetDateTime): String = value.format(OffsetFormat)

  /** SQL Server rowversion: 8-byte binary, stored as hex (0x prefix optional),
    * compared as an unsigned big integer. */
  private def parseRowVersion(value: String): BigInt = {
    val hex = value.trim.stripPrefix("0x").stripPrefix("0X")
    if (!hex.matches("[0-9A-Fa-f]{1,16}"))
      throw new IllegalArgumentException(s"JDBC_004 Watermark value '$value' is not a rowversion hex string")
    BigInt(hex, 16)
  }
}
