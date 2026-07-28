package com.hcsc.generic.ingest.jdbc.extraction

import java.sql.Timestamp

/**
  * The one seam callers use: resolve strategy -> load checkpoint -> assemble
  * context -> validate -> plan. Sequencing only; contains no strategy logic.
  */
final class ExtractionPlanner(registry: ExtractionStrategyRegistry, checkpoints: CheckpointStore) {

  def plan(entity: String, runId: String, spec: ExtractionSpec, capturedAt: Timestamp): ExtractionPlan = {
    val strategy = registry.resolve(spec.strategyKind)
    val context = ExtractionContext(entity, runId, spec, checkpoints.latest(entity), capturedAt)
    val errors = strategy.validate(context)
    if (errors.nonEmpty)
      throw new IllegalArgumentException(
        s"EXT_001 extraction configuration for entity '$entity' is invalid:\n" +
          errors.map(e => s"  - $e").mkString("\n"))
    strategy.plan(context)
  }
}
