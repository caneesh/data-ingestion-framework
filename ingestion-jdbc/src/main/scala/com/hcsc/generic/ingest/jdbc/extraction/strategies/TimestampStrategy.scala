package com.hcsc.generic.ingest.jdbc.extraction.strategies

import com.hcsc.generic.ingest.jdbc.extraction._

/** Timestamp Incremental: one TIMESTAMP boundary column, overlap-widened
  * lower edge, driver-captured upper edge. */
final class TimestampStrategy(probe: SourceBoundaryProbe, splitter: PartitionSplitter)
  extends ExtractionStrategy {

  private val windows = new BoundedWindowPlanning(probe, splitter)

  val kind: String = StrategyKinds.Timestamp

  def validate(context: ExtractionContext): Seq[String] = {
    val spec = context.spec
    windows.validateBoundary(context, kind) ++ {
      if (spec.boundaryColumns.size > 1)
        Seq(s"$kind uses exactly one boundary column; declare composites with ${StrategyKinds.TimestampKey}")
      else if (spec.boundaryColumns.headOption.exists(_.columnType != BoundaryType.Timestamp))
        Seq(s"$kind requires a TIMESTAMP boundary column, found ${spec.boundaryColumns.head.columnType}")
      else Seq.empty
    }
  }

  def plan(context: ExtractionContext): ExtractionPlan = windows.plan(context, kind)
}
