package com.hcsc.generic.ingest.curated

import com.hcsc.generic.ingest.publish.PublishService
import com.hcsc.generic.ingest.schema.SchemaContract
import com.hcsc.generic.ingest.transform.CuratedTransform
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

/**
  * Composition root for curated strategies: the SparkSession and optional
  * schema contract are injected once, and every strategy comes back fully
  * wired (transform, publisher, key/duplicate resolvers). No mutable
  * registry — a fresh, independent instance per call.
  *
  * Type 2 (SCD2 effective dating) and CDC delete application are designed
  * but not yet implemented; the factory says so instead of pretending.
  */
object CuratedStrategies {
  val Append = "APPEND"
  val Type1Merge = "TYPE1_MERGE"
  val SnapshotReplace = "SNAPSHOT_REPLACE"

  val all: Seq[String] = Seq(Append, Type1Merge, SnapshotReplace)

  def forKind(spark: SparkSession, kind: String, contract: Option[SchemaContract] = None): CuratedStrategy = {
    val logger = Logger.getLogger(getClass.getName)
    val transform = new CuratedTransform(spark)
    val preparation = new CuratedPreparation(transform, contract, logger)
    val publisher = new PublishService(spark, logger)
    val keys = new BusinessKeyResolver(transform)
    val duplicates = new DuplicateResolver(transform)

    kind.toUpperCase match {
      case Append =>
        new AppendStrategy(spark, preparation, logger)
      case "MERGE" | Type1Merge =>
        new Type1MergeStrategy(spark, preparation, publisher, keys, duplicates, transform, contract, logger)
      case SnapshotReplace =>
        new SnapshotReplaceStrategy(spark, preparation, publisher, keys, duplicates, transform, contract, logger)
      case "TYPE2_MERGE" =>
        throw new IllegalArgumentException(
          "CUR_003 TYPE2_MERGE (SCD2) is designed but not yet implemented in this framework version")
      case other =>
        throw new IllegalArgumentException(
          s"CUR_003 Unknown curated strategy '$other'. Available: ${all.mkString(", ")}")
    }
  }
}
