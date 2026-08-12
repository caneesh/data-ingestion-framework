package com.hcsc.generic.ingest.schema

import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataType
import com.hcsc.generic.ingest.schema.ColumnMapping.quotedCol

sealed trait ViolationKind {
  def label: String
  def code: String
  def policyOf(p: SchemaPolicies): PolicyAction
}

/** Stable, documented error-code catalog (HDR_001..HDR_018). */
object ViolationKind {
  case object MissingColumn extends ViolationKind {
    val label = "missing_required_header"; val code = "HDR_001"
    def policyOf(p: SchemaPolicies): PolicyAction = p.onMissingColumn
  }
  case object DuplicatePhysicalHeader extends ViolationKind {
    val label = "duplicate_physical_header"; val code = "HDR_002"
    def policyOf(p: SchemaPolicies): PolicyAction = p.onDuplicateHeader
  }
  case object DuplicateHeader extends ViolationKind {
    val label = "duplicate_normalized_header"; val code = "HDR_003"
    def policyOf(p: SchemaPolicies): PolicyAction = p.onDuplicateHeader
  }
  case object AmbiguousMapping extends ViolationKind {
    val label = "ambiguous_alias_mapping"; val code = "HDR_004"
    def policyOf(p: SchemaPolicies): PolicyAction = PolicyAction.Fail
  }
  case object MultipleSourcesOneCanonical extends ViolationKind {
    val label = "multiple_source_columns_one_canonical"; val code = "HDR_005"
    def policyOf(p: SchemaPolicies): PolicyAction = PolicyAction.Fail
  }
  case object ExtraColumn extends ViolationKind {
    val label = "unexpected_header"; val code = "HDR_006"
    def policyOf(p: SchemaPolicies): PolicyAction = p.onExtraColumn
  }
  case object CountMismatch extends ViolationKind {
    val label = "column_count_mismatch"; val code = "HDR_007"
    def policyOf(p: SchemaPolicies): PolicyAction = PolicyAction.Fail
  }
  case object InvalidPositional extends ViolationKind {
    val label = "invalid_positional_mapping"; val code = "HDR_008"
    def policyOf(p: SchemaPolicies): PolicyAction = PolicyAction.Fail
  }
  case object DelimiterMismatch extends ViolationKind {
    val label = "suspected_delimiter_mismatch"; val code = "HDR_009"
    def policyOf(p: SchemaPolicies): PolicyAction = PolicyAction.Fail
  }
  case object BlankHeader extends ViolationKind {
    val label = "blank_header"; val code = "HDR_010"
    def policyOf(p: SchemaPolicies): PolicyAction = PolicyAction.Fail
  }
  case object MalformedQuote extends ViolationKind {
    val label = "malformed_quoted_header"; val code = "HDR_011"
    def policyOf(p: SchemaPolicies): PolicyAction = PolicyAction.Fail
  }
  case object UnsupportedEncoding extends ViolationKind {
    val label = "unsupported_encoding"; val code = "HDR_012"
    def policyOf(p: SchemaPolicies): PolicyAction = PolicyAction.Fail
  }
  case object HeaderOnlyFile extends ViolationKind {
    val label = "header_only_file"; val code = "HDR_013"
    def policyOf(p: SchemaPolicies): PolicyAction = PolicyAction.Fail
  }
  case object VersionMismatch extends ViolationKind {
    val label = "schema_version_mismatch"; val code = "HDR_014"
    def policyOf(p: SchemaPolicies): PolicyAction = p.onVersionMismatch
  }
  case object ContentValidation extends ViolationKind {
    val label = "content_validation_failure"; val code = "HDR_015"
    def policyOf(p: SchemaPolicies): PolicyAction = PolicyAction.Fail
  }
  case object RepeatedHeader extends ViolationKind {
    val label = "repeated_header_row"; val code = "HDR_016"
    def policyOf(p: SchemaPolicies): PolicyAction = PolicyAction.Fail
  }
  case object ContractCollision extends ViolationKind {
    val label = "contract_configuration_collision"; val code = "HDR_017"
    def policyOf(p: SchemaPolicies): PolicyAction = PolicyAction.Fail
  }
  case object CuratedContract extends ViolationKind {
    val label = "required_output_column_missing_before_publish"; val code = "HDR_018"
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
    val resolved = headers.map(h => h -> contract.resolve(h))
    val matched = resolved.collect { case (h, Some(target)) => target }

    val duplicateViolations = detectDuplicateHeaders(headers)
    val conflictViolations = detectConflictingMappings(resolved)
    val renames = extractRenames(resolved, logger)
    val (missingRequired, missingRequiredViolations) = detectMissingRequired(matched, contract)
    val (missingOptional, missingOptionalViolations) = detectMissingOptional(matched, contract)
    val extraViolations = detectExtraColumns(resolved)
    val orderViolations = detectOrderChanges(resolved, contract)

    val violations = duplicateViolations ++ conflictViolations ++
      missingRequiredViolations ++ missingOptionalViolations ++
      extraViolations ++ orderViolations

    HeaderResolution(
      renames = renames,
      violations = violations,
      actualHeaders = headers,
      normalizedHeaders = headers.map(SchemaContract.normalize),
      canonicalToActual = resolved.collect { case (h, Some(t)) => t -> h }.toMap,
      missingRequired = missingRequired,
      missingOptional = missingOptional
    )
  }

  private def detectDuplicateHeaders(headers: Seq[String]): Seq[SchemaViolation] =
    headers.groupBy(SchemaContract.normalize).filter(_._2.size > 1).map { case (norm, dupes) =>
      SchemaViolation(ViolationKind.DuplicateHeader, s"Headers ${dupes.mkString(", ")} all normalize to '$norm'")
    }.toSeq

  private def detectConflictingMappings(resolved: Seq[(String, Option[String])]): Seq[SchemaViolation] =
    resolved
      .collect { case (h, Some(target)) => (h, target) }
      .groupBy(_._2)
      .filter { case (_, hs) => hs.map(h => SchemaContract.normalize(h._1)).distinct.size > 1 }
      .map { case (canonical, dupes) =>
        SchemaViolation(ViolationKind.MultipleSourcesOneCanonical,
          s"Source columns ${dupes.map(_._1).mkString(", ")} all map to canonical '$canonical' " +
            "(multiple_source_mapping_policy=FAIL)")
      }.toSeq

  private def extractRenames(resolved: Seq[(String, Option[String])], logger: Logger): Seq[(String, String)] =
    resolved.collect {
      case (h, Some(target)) if h != target =>
        if (!h.equalsIgnoreCase(target))
          logger.info(s"[SchemaValidator] Header '$h' recognized as renamed column '$target'")
        h -> target
    }

  private def detectMissingRequired(matched: Seq[String], contract: SchemaContract): (Seq[String], Seq[SchemaViolation]) = {
    val missing = contract.requiredColumns.map(_.name)
      .filterNot(n => matched.exists(_.equalsIgnoreCase(n)))
    val violations = missing.map(n =>
      SchemaViolation(ViolationKind.MissingColumn, s"Required column '$n' not found in source headers"))
    (missing, violations)
  }

  private def detectMissingOptional(matched: Seq[String], contract: SchemaContract): (Seq[ColumnContract], Seq[SchemaViolation]) = {
    val missing = contract.optionalColumns
      .filterNot(c => matched.exists(_.equalsIgnoreCase(c.name)))
    val violations = if (contract.policies.failOnMissingOptional)
      missing.map(c => SchemaViolation(ViolationKind.MissingColumn,
        s"Optional column '${c.name}' not found and on_missing_optional=FAIL"))
    else Seq.empty
    (missing, violations)
  }

  private def detectExtraColumns(resolved: Seq[(String, Option[String])]): Seq[SchemaViolation] =
    resolved.collect { case (h, None) =>
      SchemaViolation(ViolationKind.ExtraColumn, s"Unexpected source column '$h' not declared in schema contract")
    }

  private def detectOrderChanges(resolved: Seq[(String, Option[String])], contract: SchemaContract): Seq[SchemaViolation] =
    resolved.flatMap(_._2).zipWithIndex.flatMap { case (name, actualIdx) =>
      contract.column(name).flatMap(_.position).flatMap { expected =>
        if (expected != actualIdx)
          Some(SchemaViolation(ViolationKind.OrderChange,
            s"Column '$name' found at position $actualIdx, contract declares position $expected"))
        else None
      }
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
          sum(when(quotedCol(actual).isNull, 1).otherwise(0)).alias(contractName)
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
    versionMismatch(storedVersion, None, contract)

  /**
    * Version drift plus real BACKWARD-compatibility enforcement: with the
    * previous contract's required-column snapshot available, a new contract
    * that REMOVES a previously required column or CHANGES its type breaks
    * consumers of already-written data and is reported as a violation
    * (policy on_version_mismatch decides warn/fail). compatibility=NONE
    * skips the structural check.
    */
  def versionMismatch(
    storedVersion: Option[String],
    storedRequired: Option[Map[String, String]],
    contract: SchemaContract
  ): Seq[SchemaViolation] = {
    val versionDrift = storedVersion match {
      case Some(stored) if stored != contract.version =>
        Seq(SchemaViolation(
          ViolationKind.VersionMismatch,
          s"Schema version mismatch: table was last written with version '$stored', " +
            s"contract declares '${contract.version}' (compatibility=${contract.compatibility})"
        ))
      case _ => Seq.empty
    }

    val structural =
      if (contract.compatibility.equalsIgnoreCase("NONE")) Seq.empty
      else storedRequired.toSeq.flatMap { previous =>
        val current = contract.requiredColumns.map(c => c.name.toLowerCase -> c.dataType).toMap
        previous.toSeq.flatMap { case (name, prevType) =>
          current.get(name.toLowerCase) match {
            case None => Some(SchemaViolation(ViolationKind.VersionMismatch,
              s"BACKWARD compatibility broken: previously required column '$name' was removed " +
                "from the contract; existing raw data still carries it"))
            case Some(newType) if !newType.equalsIgnoreCase(prevType) =>
              Some(SchemaViolation(ViolationKind.VersionMismatch,
                s"BACKWARD compatibility broken: required column '$name' changed type " +
                  s"'$prevType' -> '$newType'"))
            case _ => None
          }
        }
      }

    versionDrift ++ structural
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
