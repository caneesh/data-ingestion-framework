package com.hcsc.generic.ingest.jdbc.extraction

import java.sql.Timestamp

/**
  * The one seam callers use: resolve strategy -> load checkpoint -> assemble
  * context -> validate -> plan. Sequencing only; contains no strategy logic.
  */
final class ExtractionPlanner(registry: ExtractionStrategyRegistry, checkpoints: CheckpointStore) {

  def plan(entity: String, runId: String, spec: ExtractionSpec, capturedAt: Timestamp): ExtractionPlan = {
    val strategy = registry.resolve(spec.strategyKind)
    val previous = checkpoints.latest(entity)

    // A stored checkpoint that no longer matches the spec means the feed was
    // reconfigured (strategy or boundary shape changed). Planning from it
    // would apply a stale position to different semantics — fail loudly
    // before any extraction instead of mid-run or, worse, silently.
    previous.foreach { cp =>
      if (!cp.strategyKind.equalsIgnoreCase(spec.strategyKind))
        throw new IllegalArgumentException(
          s"EXT_006 stored checkpoint for '$entity' was written by strategy '${cp.strategyKind}' " +
            s"but the feed now configures '${spec.strategyKind}'; clear or migrate the checkpoint deliberately")
      if (spec.boundaryColumns.nonEmpty && cp.value.values.size != spec.boundaryColumns.size)
        throw new IllegalArgumentException(
          s"EXT_006 stored checkpoint for '$entity' has ${cp.value.values.size} value part(s) " +
            s"but the feed now declares ${spec.boundaryColumns.size} boundary column(s); " +
            "clear or migrate the checkpoint deliberately")
    }

    val context = ExtractionContext(entity, runId, spec, previous, capturedAt)
    val errors = strategy.validate(context)
    if (errors.nonEmpty)
      throw new IllegalArgumentException(
        s"EXT_001 extraction configuration for entity '$entity' is invalid:\n" +
          errors.map(e => s"  - $e").mkString("\n"))
    strategy.plan(context)
  }
}
