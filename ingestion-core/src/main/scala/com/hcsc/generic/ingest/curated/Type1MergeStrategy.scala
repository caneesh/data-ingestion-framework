package com.hcsc.generic.ingest.curated

import com.hcsc.generic.ingest.publish.{PublishRequest, PublishService}
import com.hcsc.generic.ingest.runtime.RunContext
import com.hcsc.generic.ingest.schema.SchemaContract
import com.hcsc.generic.ingest.transform.CuratedTransform
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{broadcast, col}

/**
  * Type 1 merge (upsert, attribute-overwrite): the latest record per
  * business key replaces the target's record; unmatched target rows are
  * kept; history is not preserved (that is Type 2's job, not yet
  * implemented). The rewritten table publishes through the transactional
  * staging swap, so a failed merge leaves the target untouched.
  */
final class Type1MergeStrategy(
  spark: SparkSession,
  preparation: CuratedPreparation,
  publisher: PublishService,
  keys: BusinessKeyResolver,
  duplicates: DuplicateResolver,
  transform: CuratedTransform,
  contract: Option[SchemaContract],
  logger: Logger
) extends CuratedStrategy {

  val kind: String = CuratedStrategies.Type1Merge

  def process(input: DataFrame, target: CuratedTarget, ctx: RunContext): CuratedMetrics = {
    require(target.businessKeys.nonEmpty, "CUR_001 TYPE1_MERGE requires business keys")

    val (metrics, elapsed) = preparation.timed {
      val prepared = preparation.prepare(input, target)
      val resolvedKeys = keys.resolve(prepared, target.businessKeys)
      val validKeyed = keys.filterInvalidKeys(prepared, resolvedKeys)
      val inputRows = validKeyed.count()
      val dedup = duplicates.resolve(validKeyed, resolvedKeys, target.orderBy, inputRows)

      if (!spark.catalog.tableExists(target.fullTable)) {
        // First run: everything inserts (dry-run counts through publish's gate)
        val published = publisher.publish(dedup.deduplicated, request(target), ctx)
        CuratedMetrics(inputRows, dedup.duplicateRows, published.publishedCount,
          insertRows = published.publishedCount, updateRows = 0L, deleteRows = 0L, durationMillis = 0L)
      } else {
        val existing = spark.table(target.fullTable)
        val aligned = transform.align(dedup.deduplicated, existing.schema, contract).persist()
        val incomingKeys = aligned.select(resolvedKeys.map(col): _*).distinct().persist()
        try {
          val targetKeys = existing.select(resolvedKeys.map(col): _*).distinct()
          val totalIncoming = incomingKeys.count()
          val updates = incomingKeys.join(targetKeys, resolvedKeys, "inner").count()

          val unchanged = existing.join(broadcast(incomingKeys), resolvedKeys, "left_anti")
          val merged = unchanged.unionByName(aligned)
          val published = publisher.publish(merged, request(target), ctx)

          CuratedMetrics(inputRows, dedup.duplicateRows, published.publishedCount,
            insertRows = totalIncoming - updates, updateRows = updates, deleteRows = 0L, durationMillis = 0L)
        } finally {
          incomingKeys.unpersist(false)
          aligned.unpersist(false)
        }
      }
    }
    logger.info(s"[Curated] $kind ${target.fullTable}: $metrics")
    metrics.copy(durationMillis = elapsed)
  }

  private def request(target: CuratedTarget) = PublishRequest(
    database = target.database, table = target.table, format = target.format,
    path = target.path, allowEmpty = target.allowEmpty, validationQuery = target.validationQuery)
}
