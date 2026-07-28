package com.hcsc.generic.ingest.jdbc.extraction.strategies

import com.hcsc.generic.ingest.jdbc.extraction._

/** Timestamp + Primary Key: lexicographic composite boundary whose leading
  * TIMESTAMP is tie-broken by one or more key columns, so equal-timestamp
  * rows at the window edge are neither lost nor re-read forever. */
final class TimestampKeyStrategy(probe: SourceBoundaryProbe, splitter: PartitionSplitter)
  extends ExtractionStrategy {

  private val windows = new BoundedWindowPlanning(probe, splitter)

  val kind: String = StrategyKinds.TimestampKey

  def validate(context: ExtractionContext): Seq[String] = {
    val spec = context.spec
    windows.validateBoundary(context, kind) ++ {
      if (spec.boundaryColumns.size < 2)
        Seq(s"$kind requires a TIMESTAMP column plus at least one key column")
      else {
        val head = spec.boundaryColumns.head
        val headError =
          if (head.columnType != BoundaryType.Timestamp)
            Seq(s"$kind requires the first boundary column to be TIMESTAMP, found ${head.columnType}")
          else Seq.empty
        val tieError = spec.boundaryColumns.tail
          .filter(c => c.columnType != BoundaryType.Numeric && c.columnType != BoundaryType.StringType)
          .map(c => s"$kind key column '${c.name}' must be NUMERIC or STRING, found ${c.columnType}")
        headError ++ tieError
      }
    }
  }

  def plan(context: ExtractionContext): ExtractionPlan = windows.plan(context, kind)
}
