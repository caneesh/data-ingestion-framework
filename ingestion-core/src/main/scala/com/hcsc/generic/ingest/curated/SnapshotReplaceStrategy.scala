package com.hcsc.generic.ingest.curated

import com.hcsc.generic.ingest.publish.{PublishRequest, PublishService}
import com.hcsc.generic.ingest.runtime.RunContext
import com.hcsc.generic.ingest.schema.SchemaContract
import com.hcsc.generic.ingest.transform.CuratedTransform
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

/**
  * Snapshot replace: the incoming (deduplicated) data becomes the entire
  * curated table via the transactional staging swap. When business keys are
  * configured, insert/update/delete counts are derived from key overlap with
  * the previous image (deletes = keys that vanished); without keys the
  * replacement is counted as inserts only.
  */
final class SnapshotReplaceStrategy(
  spark: SparkSession,
  preparation: CuratedPreparation,
  publisher: PublishService,
  keys: BusinessKeyResolver,
  duplicates: DuplicateResolver,
  transform: CuratedTransform,
  contract: Option[SchemaContract],
  logger: Logger
) extends CuratedStrategy {

  val kind: String = CuratedStrategies.SnapshotReplace

  def process(input: DataFrame, target: CuratedTarget, ctx: RunContext): CuratedMetrics = {
    val (metrics, elapsed) = preparation.timed {
      val prepared = preparation.prepare(input, target)
      val inputRows = prepared.count()

      val (deduplicated, duplicateRows, resolvedKeys) =
        if (target.businessKeys.isEmpty) (prepared, 0L, Seq.empty[String])
        else {
          val resolved = keys.resolve(prepared, target.businessKeys)
          val valid = keys.filterInvalidKeys(prepared, resolved)
          val validRows = valid.count()
          val outcome = duplicates.resolve(valid, resolved, target.orderBy, validRows)
          (outcome.deduplicated, (inputRows - validRows) + outcome.duplicateRows, resolved)
        }

      val (inserts, updates, deletes) =
        if (resolvedKeys.isEmpty || !spark.catalog.tableExists(target.fullTable)) {
          (-1L, -1L, -1L) // not measurable without keys or a previous image
        } else {
          val previousKeys = spark.table(target.fullTable).select(resolvedKeys.map(col): _*).distinct()
          val incomingKeys = deduplicated.select(resolvedKeys.map(col): _*).distinct().persist()
          try {
            val incoming = incomingKeys.count()
            val matched = incomingKeys.join(previousKeys, resolvedKeys, "inner").count()
            val previous = previousKeys.count()
            (incoming - matched, matched, previous - matched)
          } finally incomingKeys.unpersist(false)
        }

      val toPublish =
        if (spark.catalog.tableExists(target.fullTable))
          transform.align(deduplicated, spark.table(target.fullTable).schema, contract)
        else deduplicated
      val published = publisher.publish(toPublish, request(target), ctx)

      CuratedMetrics(inputRows, duplicateRows, published.publishedCount,
        insertRows = if (inserts >= 0) inserts else published.publishedCount,
        updateRows = math.max(updates, 0L), deleteRows = math.max(deletes, 0L), durationMillis = 0L)
    }
    logger.info(s"[Curated] $kind ${target.fullTable}: $metrics")
    metrics.copy(durationMillis = elapsed)
  }

  private def request(target: CuratedTarget) = PublishRequest(
    database = target.database, table = target.table, format = target.format,
    path = target.path, allowEmpty = target.allowEmpty, validationQuery = target.validationQuery)
}
