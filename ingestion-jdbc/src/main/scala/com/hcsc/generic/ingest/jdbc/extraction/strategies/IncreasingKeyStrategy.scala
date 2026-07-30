package com.hcsc.generic.ingest.jdbc.extraction.strategies

import com.hcsc.generic.ingest.jdbc.extraction._

/** Increasing Key: one monotonically increasing NUMERIC column (surrogate
  * key, sequence, rowversion-as-number). Overlap is permitted but unusual —
  * a true monotone key needs none. */
final class IncreasingKeyStrategy(probe: SourceBoundaryProbe, splitter: PartitionSplitter)
  extends ExtractionStrategy {

  private val windows = new BoundedWindowPlanning(probe, splitter)

  val kind: String = StrategyKinds.IncreasingKey

  def validate(context: ExtractionContext): Seq[String] = {
    val spec = context.spec
    windows.validateBoundary(context, kind) ++ {
      if (spec.boundaryColumns.size > 1)
        Seq(s"$kind uses exactly one boundary column")
      else if (spec.boundaryColumns.headOption.exists(_.columnType != BoundaryType.Numeric))
        Seq(s"$kind requires a NUMERIC boundary column, found ${spec.boundaryColumns.head.columnType}")
      else Seq.empty
    }
  }

  def plan(context: ExtractionContext): ExtractionPlan = windows.plan(context, kind)
}
