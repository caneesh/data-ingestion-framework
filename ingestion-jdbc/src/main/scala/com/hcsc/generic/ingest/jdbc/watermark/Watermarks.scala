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
      WatermarkValue(actualCols.indices.map(i => String.valueOf(row.get(i))))
    }
  }

  /** Advance-only merge: never move a watermark backwards (e.g. an overlap
    * window re-read returning older max than the stored value). */
  def max(cfg: WatermarkConfig, a: WatermarkValue, b: WatermarkValue): WatermarkValue =
    if (compare(cfg, a, b) >= 0) a else b

  def compare(cfg: WatermarkConfig, a: WatermarkValue, b: WatermarkValue): Int = {
    cfg.columnTypes.indices.foreach { i =>
      val c = cfg.columnTypes(i) match {
        case WatermarkType.Numeric =>
          parseNumeric(a.values(i)).compare(parseNumeric(b.values(i)))
        case _ =>
          parseTimestamp(a.values(i)).compareTo(parseTimestamp(b.values(i)))
      }
      if (c != 0) return c
    }
    0
  }

  private def applyOverlap(columnType: String, value: String, overlap: Option[BigDecimal]): String =
    overlap match {
      case None => value
      case Some(amount) => columnType match {
        case WatermarkType.Numeric =>
          (parseNumeric(value) - amount).toString
        case _ =>
          val ts = parseTimestamp(value)
          new java.sql.Timestamp(ts.getTime - (amount * 1000).toLong).toString
      }
    }

  /** SQL literal with parse validation — the value round-trips through a
    * typed representation, so stored garbage cannot become SQL. */
  private[watermark] def literal(columnType: String, value: String): String = columnType match {
    case WatermarkType.Numeric => parseNumeric(value).toString
    case _                     => s"'${parseTimestamp(value).toString}'"
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
}
