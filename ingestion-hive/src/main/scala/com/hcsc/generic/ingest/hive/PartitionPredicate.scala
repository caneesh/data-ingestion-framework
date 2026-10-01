package com.hcsc.generic.ingest.hive

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.parser.{CatalystSqlParser, ParserInterface}
import org.apache.spark.sql.functions.{col, expr, lit}

import java.time.LocalDate
import java.time.format.{DateTimeFormatter, DateTimeParseException}

/**
  * Composite partition-tuple predicate over `columns` in comparison order:
  * `tuple > lower` (exclusive) and/or `tuple <= upper` (inclusive),
  * lexicographic, AND-ed with an optional `where` over partition columns.
  *
  * Rendered three ways from one definition so the catalog enumeration and
  * the data read can never disagree: SQL text, a Spark `Column`, and bound
  * catalyst `Expression`s for `SessionCatalog.listPartitionsByFilter` (the
  * Hive shim needs attributes resolved against the partition schema).
  */
final case class PartitionPredicate(
  columns: Seq[String],
  lower: Option[Seq[String]],
  upper: Option[Seq[String]],
  where: Option[String]
) {
  require(lower.forall(_.size == columns.size) && upper.forall(_.size == columns.size),
    "bound arity must equal the number of watermark columns")

  def sql: String = {
    val parts = Seq(
      lower.map(b => PartitionPredicate.greaterThanSql(columns, b)),
      upper.map(b => s"NOT (${PartitionPredicate.greaterThanSql(columns, b)})"),
      where.map(w => s"($w)")).flatten
    if (parts.isEmpty) "true" else parts.map(p => s"($p)").mkString(" AND ")
  }

  def column: Column = {
    val parts = Seq(
      lower.map(b => PartitionPredicate.greaterThanColumn(columns, b)),
      upper.map(b => !PartitionPredicate.greaterThanColumn(columns, b)),
      where.map(w => expr(w))).flatten
    parts.reduceOption(_ && _).getOrElse(lit(true))
  }

  /** Catalyst expression with every attribute bound to the table's partition
    * schema. */
  def expression(parser: ParserInterface, partitionAttrs: Seq[AttributeReference]): Expression =
    PartitionPredicate.bind(parser.parseExpression(sql), partitionAttrs)
}

object PartitionPredicate {

  /** Single-quoted SQL literal; the bound values are trusted but rendered
    * safely anyway. */
  def quote(v: String): String = "'" + v.replace("'", "''") + "'"

  /** `(c1 > v1) OR (c1 = v1 AND c2 > v2) OR (c1 = v1 AND c2 = v2 AND c3 > v3)` */
  def greaterThanSql(columns: Seq[String], bound: Seq[String]): String =
    columns.indices.map { i =>
      val eqs = (0 until i).map(j => s"${columns(j)} = ${quote(bound(j))}")
      val gt = s"${columns(i)} > ${quote(bound(i))}"
      "(" + (eqs :+ gt).mkString(" AND ") + ")"
    }.mkString(" OR ")

  def greaterThanColumn(columns: Seq[String], bound: Seq[String]): Column =
    columns.indices.map { i =>
      val eqs = (0 until i).map(j => col(columns(j)) === lit(bound(j)))
      val gt = col(columns(i)) > lit(bound(i))
      (eqs :+ gt).reduce(_ && _)
    }.reduce(_ || _)

  /** Lexicographic comparison of two tuples; shorter-is-smaller on a prefix tie. */
  def compare(a: Seq[String], b: Seq[String]): Int = {
    val it = a.iterator.zip(b.iterator)
    while (it.hasNext) {
      val (x, y) = it.next()
      val c = x.compareTo(y)
      if (c != 0) return c
    }
    a.size.compareTo(b.size)
  }

  val tupleOrdering: Ordering[Seq[String]] = Ordering.fromLessThan((a, b) => compare(a, b) < 0)

  /** Names a `where` text references, for the partition-columns-only rule. */
  def attributeNames(where: String, parser: ParserInterface = CatalystSqlParser): Seq[String] =
    parser.parseExpression(where).collect { case u: UnresolvedAttribute => u.name }.distinct

  def bind(e: Expression, attrs: Seq[AttributeReference]): Expression = e.transformUp {
    case u: UnresolvedAttribute =>
      attrs.find(_.name.equalsIgnoreCase(u.name)).getOrElse(
        throw new IllegalArgumentException(
          s"HIVE_004 '${u.name}' is not a partition column of this table " +
            s"(partition columns: ${attrs.map(_.name).mkString(", ")})"))
  }

  /** Rewinds the FIRST component by `days`; the rest become "" so every
    * partition on or after that date qualifies under `tuple > lower`. */
  def rewindDays(bound: Seq[String], days: Int, format: Option[String]): Seq[String] = {
    val fmt = format.map(DateTimeFormatter.ofPattern).getOrElse(DateTimeFormatter.ISO_LOCAL_DATE)
    val d = LocalDate.parse(bound.head, fmt)
    fmt.format(d.minusDays(days.toLong)) +: bound.tail.map(_ => "")
  }

  /**
    * HIVE_006 guard over the SELECTED values of each watermark column. With
    * `formats`, every value must parse with its pattern; without, every
    * value of a column must have the same length — the cheap proxy for
    * zero-padding, which lexicographic ordering depends on.
    */
  def formatGuard(columns: Seq[String], valuesPerColumn: Seq[Iterable[String]], formats: Seq[String]): Option[String] =
    columns.indices.iterator.map { i =>
      val values = valuesPerColumn(i)
      if (formats.nonEmpty) {
        val f = DateTimeFormatter.ofPattern(formats(i))
        values.find(v => try { f.parse(v); false } catch { case _: DateTimeParseException => true })
          .map(bad => s"HIVE_006 partition value '$bad' of column '${columns(i)}' does not match " +
            s"watermark_formats pattern '${formats(i)}'")
      } else {
        val lengths = values.map(_.length).toSet
        if (lengths.size > 1)
          Some(s"HIVE_006 partition values of column '${columns(i)}' are not uniformly zero-padded " +
            s"(lengths ${lengths.toSeq.sorted.mkString(",")}); lexicographic ordering would select the " +
            "wrong partitions — fix the source values or declare watermark_formats")
        else None
      }
    }.collectFirst { case Some(message) => message }
}
