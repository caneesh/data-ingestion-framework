package com.hcsc.generic.ingest.curated

import com.hcsc.generic.ingest.transform.CuratedTransform
import org.apache.spark.sql.DataFrame

/** The deduplicated frame plus how many rows deduplication removed. */
final case class DeduplicationOutcome(deduplicated: DataFrame, duplicateRows: Long)

/**
  * One record per business key: latest-wins by the configured ordering
  * (descending, nulls last — the existing merge semantics), arbitrary-wins
  * when no ordering is configured. Wraps the existing CuratedTransform
  * deduplication so curated has exactly one dedup implementation.
  */
final class DuplicateResolver(transform: CuratedTransform) {

  /** `countedInput` is the caller's already-computed input count so the
    * duplicate count costs one action here, not two. */
  def resolve(df: DataFrame, keys: Seq[String], orderBy: Seq[String], countedInput: Long): DeduplicationOutcome = {
    if (keys.isEmpty) return DeduplicationOutcome(df, 0L)
    val deduplicated = transform.deduplicate(df, keys, orderBy)
    val remaining = deduplicated.count()
    DeduplicationOutcome(deduplicated, countedInput - remaining)
  }
}
