package com.hcsc.generic.ingest.schema

import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.hcsc.generic.ingest.schema.ColumnMapping.quotedCol

/**
  * Second validation gate, run immediately before CURATED publication.
  * Successful RAW mapping does not guarantee correct CURATED output — the
  * curated transforms (casts, derives, key normalization, dedup) can drop
  * or corrupt columns. Any HDR_018 violation blocks the publish.
  */
object CuratedContractValidator {

  def validate(
    df: DataFrame,
    contract: SchemaContract,
    mergeKeys: Seq[String],
    dedupOrderColumns: Seq[String],
    logger: Logger
  ): Seq[SchemaViolation] = {
    val violations = scala.collection.mutable.ArrayBuffer.empty[SchemaViolation]
    val columnsLower = df.columns.map(_.toLowerCase)

    // Required curated business columns must exist
    contract.requiredColumns
      .filter(c => c.category == "business")
      .filterNot(c => columnsLower.contains(c.name.toLowerCase))
      .foreach { c =>
        violations += SchemaViolation(ViolationKind.CuratedContract,
          s"Required column '${c.name}' missing before CURATED publish")
      }

    // No duplicate/ambiguous output names
    df.columns.groupBy(_.toLowerCase).filter(_._2.length > 1).foreach { case (name, dupes) =>
      violations += SchemaViolation(ViolationKind.CuratedContract,
        s"Duplicate curated output column '$name' (${dupes.mkString(", ")})")
    }

    // Merge keys and dedup order columns must exist when configured
    mergeKeys.filterNot(k => columnsLower.contains(k.toLowerCase)).foreach { k =>
      violations += SchemaViolation(ViolationKind.CuratedContract,
        s"Configured merge key '$k' missing before CURATED publish")
    }
    dedupOrderColumns.filterNot(k => columnsLower.contains(k.toLowerCase)).foreach { k =>
      violations += SchemaViolation(ViolationKind.CuratedContract,
        s"Configured dedup order column '$k' missing before CURATED publish")
    }

    // Required columns entirely null (single pass), when content validation
    // has the all-null check enabled
    if (contract.contentValidation.failOnAllNullRequired && violations.isEmpty) {
      val checked = contract.requiredColumns
        .flatMap(c => df.columns.find(_.equalsIgnoreCase(c.name)).map(actual => c.name -> actual))
      if (checked.nonEmpty) {
        val aggs = count(lit(1)).alias("__total") +:
          checked.map { case (name, actual) =>
            sum(when(quotedCol(actual).isNotNull, 1).otherwise(0)).alias(name)
          }
        val row = df.agg(aggs.head, aggs.tail: _*).first()
        val total = row.getLong(0)
        if (total > 0) {
          checked.zipWithIndex.foreach { case ((name, _), idx) =>
            val nonNull = Option(row.get(idx + 1)).map(_.toString.toLong).getOrElse(0L)
            if (nonNull == 0)
              violations += SchemaViolation(ViolationKind.CuratedContract,
                s"Required column '$name' is entirely null before CURATED publish")
          }
        }
      }
    }

    if (violations.nonEmpty)
      violations.foreach(v => logger.error(s"[CuratedContractValidator] $v"))
    violations.toList
  }
}
