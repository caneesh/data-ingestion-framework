package com.hcsc.generic.ingest.schema

import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataType

sealed trait ViolationKind {
  def label: String
  def code: String
  def policyOf(p: SchemaPolicies): PolicyAction
}

object ViolationKind {
  case object MissingColumn extends ViolationKind {
    val label = "missing_column"; val code = "HDR_001"
    def policyOf(p: SchemaPolicies): PolicyAction = p.onMissingColumn
  }
  case object DuplicateHeader extends ViolationKind {
    val label = "duplicate_header"; val code = "HDR_002"
    def policyOf(p: SchemaPolicies): PolicyAction = p.onDuplicateHeader
  }
  case object AmbiguousMapping extends ViolationKind {
    val label = "ambiguous_mapping"; val code = "HDR_003"
    def policyOf(p: SchemaPolicies): PolicyAction = PolicyAction.Fail
  }
  case object ExtraColumn extends ViolationKind {
    val label = "extra_column"; val code = "HDR_004"
    def policyOf(p: SchemaPolicies): PolicyAction = p.onExtraColumn
  }
  case object CountMismatch extends ViolationKind {
    val label = "column_count_mismatch"; val code = "HDR_005"
    def policyOf(p: SchemaPolicies): PolicyAction = PolicyAction.Fail
  }
  case object InvalidPositional extends ViolationKind {
    val label = "invalid_positional_mapping"; val code = "HDR_006"
    def policyOf(p: SchemaPolicies): PolicyAction = PolicyAction.Fail
  }
  case object ContentValidation extends ViolationKind {
    val label = "content_validation_failure"; val code = "HDR_007"
    def policyOf(p: SchemaPolicies): PolicyAction = PolicyAction.Fail
  }
  case object TypeChange extends ViolationKind {
    val label = "type_change"; val code = "type_change"
    def policyOf(p: SchemaPolicies): PolicyAction = p.onTypeChange
  }
  case object OrderChange extends ViolationKind {
    val label = "order_change"; val code = "order_change"
    def policyOf(p: SchemaPolicies): PolicyAction = p.onOrderChange
  }
  case object NullabilityViolation extends ViolationKind {
    val label = "nullability_violation"; val code = "nullability_violation"
    def policyOf(p: SchemaPolicies): PolicyAction = p.onNullabilityViolation
  }
  case object VersionMismatch extends ViolationKind {
    val label = "version_mismatch"; val code = "version_mismatch"
    def policyOf(p: SchemaPolicies): PolicyAction = p.onVersionMismatch
  }
}

final case class SchemaViolation(kind: ViolationKind, message: String) {
  override def toString: String = s"[${kind.code}] $message"
}

final class SchemaContractViolationException(
  val violations: Seq[SchemaViolation],
  val resolution: Option[HeaderResolution] = None
) extends RuntimeException(
    s"Schema contract violated:\n${violations.map(v => s"  - $v").mkString("\n")}"
  )

/** Result of resolving raw source headers against a contract. */
final case class HeaderResolution(
  renames: Seq[(String, String)],
  violations: Seq[SchemaViolation],
  actualHeaders: Seq[String] = Seq.empty,
  normalizedHeaders: Seq[String] = Seq.empty,
  canonicalToActual: Map[String, String] = Map.empty,
  missingRequired: Seq[String] = Seq.empty,
  missingOptional: Seq[ColumnContract] = Seq.empty,
  positionalFallbackUsed: Boolean = false
) {
  def valid: Boolean = violations.isEmpty
}

object SchemaValidator {

  /**
    * Validates raw source headers against the contract. Detects duplicate
    * headers, renamed columns (matched via aliases per strategy), missing
    * required/optional columns, extra columns and column-order changes.
    * Returns the renames needed to map actual headers to canonical names,
    * plus all violations found. Missing optional columns are NOT violations
    * (unless header_validation.on_missing_optional = FAIL); the caller adds
    * their configured defaults.
    */
  def validateHeaders(headers: Seq[String], contract: SchemaContract, logger: Logger): HeaderResolution = {
    val violations = scala.collection.mutable.ArrayBuffer.empty[SchemaViolation]

    val resolved: Seq[(String, Option[String])] = headers.map(h => h -> contract.resolve(h))

    // Duplicate headers: two source headers resolving to the same contract
    // column, or identical after normalization.
    resolved
      .groupBy { case (h, target) => target.getOrElse(SchemaContract.normalize(h)) }
      .filter(_._2.size > 1)
      .foreach { case (name, dupes) =>
        violations += SchemaViolation(
          ViolationKind.DuplicateHeader,
          s"Headers ${dupes.map(_._1).mkString(", ")} all map to '$name'"
        )
      }

    // Renamed columns (matched through an alias, not the canonical name).
    val renames = resolved.collect {
      case (h, Some(target)) if !h.equalsIgnoreCase(target) =>
        logger.info(s"[SchemaValidator] Header '$h' recognized as renamed column '$target'")
        h -> target
    }

    val matched = resolved.collect { case (h, Some(target)) => target }

    // Missing columns: required ones are violations; optional ones get their
    // configured defaults downstream.
    val missingRequired = contract.requiredColumns.map(_.name)
      .filterNot(n => matched.exists(_.equalsIgnoreCase(n)))
    missingRequired.foreach { n =>
      violations += SchemaViolation(ViolationKind.MissingColumn, s"Required column '$n' not found in source headers")
    }

    val missingOptional = contract.optionalColumns
      .filterNot(c => matched.exists(_.equalsIgnoreCase(c.name)))
    if (contract.policies.failOnMissingOptional) {
      missingOptional.foreach { c =>
        violations += SchemaViolation(ViolationKind.MissingColumn, s"Optional column '${c.name}' not found and on_missing_optional=FAIL")
      }
    }

    // Unexpected columns.
    resolved.collect { case (h, None) => h }.foreach { h =>
      violations += SchemaViolation(ViolationKind.ExtraColumn, s"Unexpected source column '$h' not declared in schema contract")
    }

    // Column-order changes against declared positions (compared over the
    // sequence of matched contract columns).
    val matchedInOrder = resolved.flatMap(_._2)
    matchedInOrder.zipWithIndex.foreach { case (name, actualIdx) =>
      contract.column(name).flatMap(_.position).foreach { expected =>
        if (expected != actualIdx)
          violations += SchemaViolation(
            ViolationKind.OrderChange,
            s"Column '$name' found at position $actualIdx, contract declares position $expected"
          )
      }
    }

    HeaderResolution(
      renames = renames,
      violations = violations.toList,
      actualHeaders = headers,
      normalizedHeaders = headers.map(SchemaContract.normalize),
      canonicalToActual = resolved.collect { case (h, Some(t)) => t -> h }.toMap,
      missingRequired = missingRequired,
      missingOptional = missingOptional
    )
  }

  /**
    * Validates a DataFrame against the contract: data-type changes and
    * runtime nullability violations. Type validation compares Spark types,
    * so it is most meaningful for typed sources (JDBC, Parquet); CSV columns
    * arrive as strings and should be declared as such in the contract.
    */
  def validateData(df: DataFrame, contract: SchemaContract): Seq[SchemaViolation] = {
    val violations = scala.collection.mutable.ArrayBuffer.empty[SchemaViolation]

    val fieldsByName = df.schema.fields.map(f => f.name.toLowerCase -> f).toMap

    contract.columns.foreach { c =>
      fieldsByName.get(c.name.toLowerCase).foreach { field =>
        val expected =
          try DataType.fromDDL(c.dataType)
          catch {
            case e: Exception =>
              throw new IllegalArgumentException(
                s"Schema contract column '${c.name}' has invalid type '${c.dataType}'", e
              )
          }
        if (field.dataType != expected)
          violations += SchemaViolation(
            ViolationKind.TypeChange,
            s"Column '${c.name}' has type ${field.dataType.simpleString}, contract declares ${expected.simpleString}"
          )
      }
    }

    val nonNullable = contract.columns
      .filter(!_.nullable)
      .flatMap(c => fieldsByName.get(c.name.toLowerCase).map(f => c.name -> f.name))

    if (nonNullable.nonEmpty) {
      val counts = df.select(
        nonNullable.map { case (contractName, actual) =>
          sum(when(col(actual).isNull, 1).otherwise(0)).alias(contractName)
        }: _*
      ).first()

      nonNullable.map(_._1).zipWithIndex.foreach { case (name, idx) =>
        val nullCount = Option(counts.get(idx)).map(_.toString.toLong).getOrElse(0L)
        if (nullCount > 0)
          violations += SchemaViolation(
            ViolationKind.NullabilityViolation,
            s"Column '$name' is declared non-nullable but contains $nullCount null value(s)"
          )
      }
    }

    violations.toList
  }

  def versionMismatch(storedVersion: Option[String], contract: SchemaContract): Seq[SchemaViolation] =
    storedVersion match {
      case Some(stored) if stored != contract.version =>
        Seq(SchemaViolation(
          ViolationKind.VersionMismatch,
          s"Schema version mismatch: table was last written with version '$stored', " +
            s"contract declares '${contract.version}' (compatibility=${contract.compatibility})"
        ))
      case _ => Seq.empty
    }

  /**
    * Applies the feed's policies to the violations found: WARN violations are
    * logged, IGNORE violations dropped, and if any FAIL violation is present
    * a SchemaContractViolationException carrying all of them is thrown.
    */
  def enforce(
    violations: Seq[SchemaViolation],
    policies: SchemaPolicies,
    logger: Logger,
    resolution: Option[HeaderResolution] = None
  ): Unit = {
    val actionable = violations.map(v => v -> v.kind.policyOf(policies))

    actionable.collect { case (v, PolicyAction.Warn) => v }
      .foreach(v => logger.warn(s"[SchemaValidator] $v"))

    val failures = actionable.collect { case (v, PolicyAction.Fail) => v }
    if (failures.nonEmpty) {
      failures.foreach(v => logger.error(s"[SchemaValidator] $v"))
      throw new SchemaContractViolationException(failures, resolution)
    }
  }
}
