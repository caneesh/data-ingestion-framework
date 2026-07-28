package com.hcsc.generic.ingest.jdbc.extraction.render

import com.hcsc.generic.ingest.jdbc.dialect.JdbcDialect
import com.hcsc.generic.ingest.jdbc.extraction._
import com.hcsc.generic.ingest.jdbc.extraction.ExtractionPartition.{PredicateSlice, RangeSlice}

/** A plan rendered to what the Spark JDBC reader needs. */
final case class RenderedPlan(dbtable: String, partitionPredicates: Seq[String], fetchSize: Int)

/**
  * The only place boundary semantics become SQL text. Identifiers are
  * dialect-quoted and boundary literals round-trip through the dialect's
  * typed rendering, so a corrupt stored checkpoint can never inject SQL.
  * Pure: plan in, strings out.
  */
final class SqlPlanRenderer(dialect: JdbcDialect) {

  def render(plan: ExtractionPlan): RenderedPlan = {
    val table = plan.relation match { case Relation.Table(name) => name }
    val predicates = plan.staticFilters.map(f => s"($f)") ++ boundaryPredicate(plan).toSeq

    val dbtable =
      if (plan.projection.isEmpty && predicates.isEmpty) table
      else {
        val projection =
          if (plan.projection.isEmpty) "*"
          else plan.projection.map(dialect.quoteQualified).mkString(", ")
        val whereClause =
          if (predicates.isEmpty) "" else predicates.mkString(" WHERE ", " AND ", "")
        s"(SELECT $projection FROM $table$whereClause) src"
      }

    RenderedPlan(dbtable, plan.partitions.map(partitionPredicate), plan.fetchSize)
  }

  /** HalfOpen(lower, upper) -> "strictly greater than lower AND NOT strictly
    * greater than upper" — the lexicographic bounded window. */
  private def boundaryPredicate(plan: ExtractionPlan): Option[String] = plan.boundary match {
    case ExtractionBoundary.Unbounded => None
    case ExtractionBoundary.HalfOpen(lower, upper) =>
      val greaterThanLower = strictlyGreater(plan.boundaryColumns, lower)
      Some(upper match {
        case None    => s"($greaterThanLower)"
        case Some(u) => s"(($greaterThanLower) AND NOT (${strictlyGreater(plan.boundaryColumns, u)}))"
      })
  }

  private def strictlyGreater(columns: Seq[BoundaryColumn], value: BoundaryValue): String = {
    require(columns.nonEmpty, "EXT_002 bounded plan without boundary columns")
    require(value.values.size == columns.size,
      s"EXT_002 boundary value has ${value.values.size} part(s), expected ${columns.size}")
    if (columns.size == 1) s"${quoted(columns.head)} > ${literal(columns.head, value.values.head)}"
    else {
      val clauses = columns.indices.map { i =>
        val equalPrefix = (0 until i).map(j => s"${quoted(columns(j))} = ${literal(columns(j), value.values(j))}")
        val greater = s"${quoted(columns(i))} > ${literal(columns(i), value.values(i))}"
        (equalPrefix :+ greater).mkString("(", " AND ", ")")
      }
      clauses.mkString("(", " OR ", ")")
    }
  }

  private def partitionPredicate(partition: ExtractionPartition): String = partition match {
    case PredicateSlice(predicate, _) => predicate
    case RangeSlice(column, lo, hi, upperInclusive, includeNulls, _) =>
      val q = dialect.quoteIdentifier(column)
      val upperOp = if (upperInclusive) "<=" else "<"
      val range = s"$q >= $lo AND $q $upperOp $hi"
      if (includeNulls) s"($range) OR $q IS NULL" else range
  }

  private def quoted(column: BoundaryColumn): String = dialect.quoteQualified(column.name)

  private def literal(column: BoundaryColumn, value: String): String =
    dialect.renderLiteral(literalType(column.columnType), value)

  /** BoundaryType -> the dialect's literal-type vocabulary. */
  private def literalType(boundaryType: String): String = boundaryType match {
    case BoundaryType.Numeric    => "NUMBER"
    case BoundaryType.StringType => "STRING"
    case _                       => "TIMESTAMP"
  }
}
