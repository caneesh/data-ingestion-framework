package com.hcsc.generic.ingest.schema

import org.apache.log4j.Logger
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

/**
  * Column-level content validation to detect swapped or incorrectly mapped
  * columns (e.g. after positional fallback). Rules per contract column:
  * regex, min_length, max_length, allowed_values, nonblank. Runs on a sample
  * (content_validation.mode = SAMPLE, sample_rows) or the full DataFrame,
  * in a single Spark action. Columns whose failure percentage exceeds
  * maximum_failure_percentage produce HDR_007 violations.
  */
object ContentValidator {

  def validate(df: DataFrame, contract: SchemaContract, logger: Logger): Seq[SchemaViolation] = {
    val cfg = contract.contentValidation
    if (!cfg.enabled) return Seq.empty

    val ruledColumns = contract.columns
      .filter(_.validation.isDefined)
      .flatMap(c => df.columns.find(_.equalsIgnoreCase(c.name)).map(actual => (c, actual)))

    // Required columns present in the data, checked for being entirely null
    val allNullChecked =
      if (!cfg.failOnAllNullRequired) Seq.empty
      else contract.requiredColumns
        .flatMap(c => df.columns.find(_.equalsIgnoreCase(c.name)).map(actual => (c, actual)))

    if (ruledColumns.isEmpty && allNullChecked.isEmpty) return Seq.empty

    val scope = if (cfg.sampleMode) df.limit(cfg.sampleRows) else df

    // Single Spark action: rule failures, per-column null counts, and
    // non-null counts for the all-null check
    val aggregations =
      count(lit(1)).alias("__total") +:
        (ruledColumns.map { case (contractCol, actual) =>
          sum(when(failsRules(col(actual), contractCol.validation.get), 1).otherwise(0))
            .alias(s"fail_${contractCol.name}")
        } ++
          ruledColumns.map { case (contractCol, actual) =>
            sum(when(col(actual).isNull, 1).otherwise(0)).alias(s"null_${contractCol.name}")
          } ++
          allNullChecked.map { case (contractCol, actual) =>
            sum(when(col(actual).isNotNull, 1).otherwise(0)).alias(s"nonnull_${contractCol.name}")
          })

    val row = scope.agg(aggregations.head, aggregations.tail: _*).first()
    val total = row.getLong(0)
    if (total == 0) return Seq.empty

    def longAt(idx: Int): Long = Option(row.get(idx)).map(_.toString.toLong).getOrElse(0L)

    val ruleViolations = ruledColumns.zipWithIndex.flatMap { case ((contractCol, _), i) =>
      val failures = longAt(1 + i)
      val pct = failures * 100.0 / total
      if (pct > cfg.maxFailurePercentage) {
        logger.error(f"[ContentValidator] Column '${contractCol.name}' failed content validation: " +
          f"$failures of $total sampled rows ($pct%.2f%% > ${cfg.maxFailurePercentage}%%)")
        Some(SchemaViolation(
          ViolationKind.ContentValidation,
          f"Column '${contractCol.name}' failed content validation for $failures of $total rows ($pct%.2f%%)"
        ))
      } else None
    }

    val nullPctViolations = ruledColumns.zipWithIndex.flatMap { case ((contractCol, _), i) =>
      contractCol.validation.get.maxNullPercentage.flatMap { maxNull =>
        val nulls = longAt(1 + ruledColumns.size + i)
        val pct = nulls * 100.0 / total
        if (pct > maxNull)
          Some(SchemaViolation(
            ViolationKind.ContentValidation,
            f"Column '${contractCol.name}' is $pct%.2f%% null, exceeding max_null_percentage=$maxNull%%"
          ))
        else None
      }
    }

    val allNullViolations = allNullChecked.zipWithIndex.flatMap { case ((contractCol, _), i) =>
      val nonNull = longAt(1 + 2 * ruledColumns.size + i)
      if (nonNull == 0)
        Some(SchemaViolation(
          ViolationKind.ContentValidation,
          s"Required column '${contractCol.name}' is entirely null in the validated rows"
        ))
      else None
    }

    ruleViolations ++ nullPctViolations ++ allNullViolations
  }

  /** True when the value breaks any configured rule. Null values fail every
    * configured rule: an unparseable/absent value cannot be validated and
    * treating it as passing would defeat swapped-column detection. */
  private def failsRules(value: Column, rules: ColumnValidation): Column = {
    val str = trim(value.cast("string"))

    val checks = scala.collection.mutable.ArrayBuffer.empty[Column]
    rules.regex.foreach(r => checks += !str.rlike(r))
    rules.minLength.foreach(min => checks += length(str) < min)
    rules.maxLength.foreach(max => checks += length(str) > max)
    if (rules.allowedValues.nonEmpty) checks += !str.isin(rules.allowedValues: _*)
    if (rules.nonblank) checks += (str === "")
    if (rules.numericParse) checks += str.cast("double").isNull
    if (rules.dateFormats.nonEmpty)
      checks += coalesce(rules.dateFormats.map(f => to_date(str, f)): _*).isNull
    if (rules.timestampFormats.nonEmpty)
      checks += coalesce(rules.timestampFormats.map(f => to_timestamp(str, f)): _*).isNull

    if (checks.isEmpty) return lit(false) // only max_null_percentage configured

    val anyRuleBroken = checks.reduce(_ || _)
    when(value.isNull, lit(true)).otherwise(coalesce(anyRuleBroken, lit(true)))
  }
}
