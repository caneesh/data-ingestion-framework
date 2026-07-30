package com.hcsc.generic.ingest.curated

import com.hcsc.generic.ingest.runtime.RunContext
import com.hcsc.generic.ingest.schema.{CuratedContractValidator, SchemaContract, SchemaContractViolationException}
import com.hcsc.generic.ingest.transform.CuratedTransform
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

/**
  * How curated output materializes from accepted RAW data. Every strategy:
  *   - receives audit-columned input and gates it through the HDR_018
  *     contract validation on the FINAL frame it is about to write (after
  *     key filtering, deduplication and target alignment — so cast-induced
  *     corruption cannot slip past a pre-transform check)
  *   - replacing strategies (TYPE1_MERGE, SNAPSHOT_REPLACE) publish through
  *     the transactional staging swap; APPEND appends directly by design
  *     (event-history semantics — replay idempotency is owned by the RAW
  *     layer's guards upstream)
  *   - reports CuratedMetrics for audit and reconciliation
  */
trait CuratedStrategy {
  def kind: String
  def process(input: DataFrame, target: CuratedTarget, ctx: RunContext): CuratedMetrics
}

/** Shared steps composed into strategies: audit columns, the HDR_018 gate,
  * timing. `validateFinal` must be called on the frame actually being
  * published; any violation blocks the write. */
private[curated] final class CuratedPreparation(
  transform: CuratedTransform,
  contract: Option[SchemaContract],
  logger: Logger
) {
  def ensureAudit(input: DataFrame): DataFrame = transform.ensureAudit(input)

  def validateFinal(toPublish: DataFrame, target: CuratedTarget): Unit =
    contract.foreach { c =>
      val violations = CuratedContractValidator.validate(
        toPublish, c, target.businessKeys, target.orderBy, logger)
      if (violations.nonEmpty) throw new SchemaContractViolationException(violations)
    }

  def timed[A](body: => A): (A, Long) = {
    val start = System.nanoTime()
    val result = body
    (result, (System.nanoTime() - start) / 1000000L)
  }
}
