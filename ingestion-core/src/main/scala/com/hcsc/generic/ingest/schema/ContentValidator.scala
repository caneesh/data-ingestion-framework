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

    if (ruledColumns.isEmpty) return Seq.empty

    val scope = if (cfg.sampleMode) df.limit(cfg.sampleRows) else df

    val aggregations =
      count(lit(1)).alias("__total") +:
        ruledColumns.map { case (contractCol, actual) =>
          sum(when(failsRules(col(actual), contractCol.validation.get), 1).otherwise(0))
            .alias(contractCol.name)
        }

    val row = scope.agg(aggregations.head, aggregations.tail: _*).first()
    val total = row.getLong(0)
    if (total == 0) return Seq.empty

    ruledColumns.zipWithIndex.flatMap { case ((contractCol, _), idx) =>
      val failures = Option(row.get(idx + 1)).map(_.toString.toLong).getOrElse(0L)
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

    val anyRuleBroken = checks.reduce(_ || _)
    when(value.isNull, lit(true)).otherwise(coalesce(anyRuleBroken, lit(true)))
  }
}
