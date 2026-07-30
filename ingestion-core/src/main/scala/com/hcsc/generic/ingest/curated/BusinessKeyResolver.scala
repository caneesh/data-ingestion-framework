package com.hcsc.generic.ingest.curated

import com.hcsc.generic.ingest.transform.CuratedTransform
import org.apache.spark.sql.DataFrame

/**
  * Resolves configured business keys against the actual frame: keys must
  * exist (case-insensitive, resolved to actual column names) and rows with
  * null/blank keys are excluded before matching — a null key can neither
  * match nor be deduplicated meaningfully.
  */
final class BusinessKeyResolver(transform: CuratedTransform) {

  /** Actual column names for the configured keys; CUR_001 when absent. */
  def resolve(df: DataFrame, configuredKeys: Seq[String]): Seq[String] =
    configuredKeys.map { key =>
      df.columns.find(_.equalsIgnoreCase(key)).getOrElse(
        throw new IllegalArgumentException(
          s"CUR_001 business key '$key' not present in curated input (columns: ${df.columns.mkString(", ")})"))
    }

  /** Drops rows whose key is null or blank (blank-as-null matches the
    * existing merge semantics). */
  def filterInvalidKeys(df: DataFrame, resolvedKeys: Seq[String]): DataFrame =
    transform.filterNullKeys(df, resolvedKeys, drop = true, blanks = true)
}
