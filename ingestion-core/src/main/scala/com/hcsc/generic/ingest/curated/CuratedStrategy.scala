package com.hcsc.generic.ingest.curated

import com.hcsc.generic.ingest.runtime.RunContext
import com.hcsc.generic.ingest.schema.{CuratedContractValidator, SchemaContract, SchemaContractViolationException}
import com.hcsc.generic.ingest.transform.CuratedTransform
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

/**
  * How curated output materializes from accepted RAW data. Every strategy:
  *   - receives audit-columned, contract-validated input (the shared
  *     preparation step, composed in — not inherited)
  *   - publishes through the transactional staging swap (never a direct
  *     destructive write against the target)
  *   - reports CuratedMetrics for audit and reconciliation
  *
  * Strategies are constructed fully wired by CuratedStrategies.forKind with
  * the SparkSession injected once; process() is then a pure orchestration
  * over the injected collaborators.
  */
trait CuratedStrategy {
  def kind: String
  def process(input: DataFrame, target: CuratedTarget, ctx: RunContext): CuratedMetrics
}

/** Shared preparation: audit columns, the HDR_018 contract gate, timing.
  * Validation runs against the fully prepared frame — the same guarantee
  * the existing pipeline gate gives — and any violation blocks the publish. */
private[curated] final class CuratedPreparation(
  transform: CuratedTransform,
  contract: Option[SchemaContract],
  logger: Logger
) {
  def prepare(input: DataFrame, target: CuratedTarget): DataFrame = {
    val prepared = transform.ensureAudit(input)
    contract.foreach { c =>
      val violations = CuratedContractValidator.validate(
        prepared, c, target.businessKeys, target.orderBy, logger)
      if (violations.nonEmpty) throw new SchemaContractViolationException(violations)
    }
    prepared
  }

  def timed[A](body: => A): (A, Long) = {
    val start = System.nanoTime()
    val result = body
    (result, (System.nanoTime() - start) / 1000000L)
  }
}
