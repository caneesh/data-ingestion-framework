package com.hcsc.generic.ingest.schema

import com.hcsc.generic.ingest.config.ConfigUtils
import com.typesafe.config.Config
import scala.collection.JavaConverters._

sealed trait PolicyAction
object PolicyAction {
  case object Fail extends PolicyAction
  case object Warn extends PolicyAction
  case object Ignore extends PolicyAction

  def parse(value: String): PolicyAction = value.toUpperCase match {
    case "FAIL"   => Fail
    case "WARN"   => Warn
    case "IGNORE" => Ignore
    case other =>
      throw new IllegalArgumentException(
        s"Unknown schema policy action '$other'; expected FAIL, WARN or IGNORE"
      )
  }
}

/** Header matching strategy. Positional mapping is never the default and,
  * under the fallback strategy, only runs after name and alias matching
  * fail — and only when the vendor guarantees stable column order. */
sealed trait HeaderStrategy
object HeaderStrategy {
  case object StrictName extends HeaderStrategy
  case object NameWithAliases extends HeaderStrategy
  case object NameAliasPositionFallback extends HeaderStrategy

  def parse(value: String): HeaderStrategy = value.toUpperCase match {
    case "STRICT_NAME"                  => StrictName
    case "NAME_WITH_ALIASES"            => NameWithAliases
    case "NAME_ALIAS_POSITION_FALLBACK" => NameAliasPositionFallback
    case other =>
      throw new IllegalArgumentException(
        s"Unknown header_validation.strategy '$other'; expected STRICT_NAME, " +
          "NAME_WITH_ALIASES or NAME_ALIAS_POSITION_FALLBACK"
      )
  }
}

/** Optional per-column content validation rules (detect swapped columns). */
final case class ColumnValidation(
  regex: Option[String],
  minLength: Option[Int],
  maxLength: Option[Int],
  allowedValues: Seq[String],
  nonblank: Boolean,
  numericParse: Boolean = false,
  dateFormats: Seq[String] = Seq.empty,
  timestampFormats: Seq[String] = Seq.empty,
  maxNullPercentage: Option[Double] = None
) {
  def isDefined: Boolean =
    regex.isDefined || minLength.isDefined || maxLength.isDefined ||
      allowedValues.nonEmpty || nonblank || numericParse ||
      dateFormats.nonEmpty || timestampFormats.nonEmpty || maxNullPercentage.isDefined
}

final case class ColumnContract(
  name: String,
  dataType: String,
  nullable: Boolean,
  aliases: Seq[String],
  position: Option[Int],
  required: Boolean = true,
  default: Option[String] = None,
  category: String = "business",
  validation: Option[ColumnValidation] = None
)

final case class PositionalFallback(
  enabled: Boolean,
  requireExactColumnCount: Boolean,
  requireContentValidation: Boolean
)

final case class ContentValidationConfig(
  enabled: Boolean,
  sampleMode: Boolean,
  sampleRows: Int,
  maxFailurePercentage: Double,
  failOnAllNullRequired: Boolean = false
)

object BatchPolicy {
  val FileAtomic = "FILE_ATOMIC"
  val BatchAtomic = "BATCH_ATOMIC"
}

final case class SchemaPolicies(
  onMissingColumn: PolicyAction,
  onExtraColumn: PolicyAction,
  onTypeChange: PolicyAction,
  onOrderChange: PolicyAction,
  onDuplicateHeader: PolicyAction,
  onNullabilityViolation: PolicyAction,
  onVersionMismatch: PolicyAction,
  failOnMissingOptional: Boolean = false
)

final case class SchemaContract(
  version: String,
  compatibility: String,
  columns: Seq[ColumnContract],
  policies: SchemaPolicies,
  strategy: HeaderStrategy = HeaderStrategy.NameWithAliases,
  positionalFallback: PositionalFallback = PositionalFallback(enabled = false, requireExactColumnCount = true, requireContentValidation = false),
  contentValidation: ContentValidationConfig = ContentValidationConfig(enabled = false, sampleMode = true, sampleRows = 1000, maxFailurePercentage = 0.0),
  quarantineOnFailure: Boolean = false,
  batchPolicy: String = BatchPolicy.FileAtomic,
  repeatedHeaderPolicy: Option[String] = None,
  headerOnlyPolicy: String = "WARN_AND_SKIP"
) {
  def columnNames: Seq[String] = columns.map(_.name)

  def requiredColumns: Seq[ColumnContract] = columns.filter(_.required)
  def optionalColumns: Seq[ColumnContract] = columns.filterNot(_.required)

  /** Normalized canonical name -> canonical name (STRICT_NAME matching). */
  lazy val nameLookup: Map[String, String] =
    columns.map(c => SchemaContract.normalize(c.name) -> c.name).toMap

  /** Normalized alias or canonical name -> canonical name. */
  lazy val aliasLookup: Map[String, String] =
    columns.flatMap(c => (c.name +: c.aliases).map(a => SchemaContract.normalize(a) -> c.name)).toMap

  /** Resolves a source header per the configured strategy: canonical names
    * always match; aliases only under alias-aware strategies. */
  def resolve(header: String): Option[String] = strategy match {
    case HeaderStrategy.StrictName => nameLookup.get(SchemaContract.normalize(header))
    case _                         => aliasLookup.get(SchemaContract.normalize(header))
  }

  def column(name: String): Option[ColumnContract] = columns.find(_.name.equalsIgnoreCase(name))

  /** Columns ordered by their declared position; only valid when every column declares one. */
  def positionalOrder: Seq[ColumnContract] = {
    require(
      columns.forall(_.position.isDefined),
      "HDR_008 Positional mapping requires every schema column to declare a position"
    )
    columns.sortBy(_.position.get)
  }
}

object SchemaContract {

  private[schema] def normalize(s: String): String = HeaderNormalizer.normalize(s)

  /** Parses the optional `schema` block (plus optional sibling
    * `header_validation` / `content_validation` blocks) from a feed or
    * source config. */
  def parse(conf: Config): Option[SchemaContract] = {
    if (!conf.hasPath("schema")) return None
    val s = conf.getConfig("schema")

    def block(name: String): Option[Config] =
      ConfigUtils.optConfig(s, name).orElse(ConfigUtils.optConfig(conf, name))

    val headerValidation = block("header_validation")
    val contentValidationConf = block("content_validation")

    val columns = parseColumns(s)
    validateColumns(columns)

    val policies = parsePolicies(s, headerValidation)
    val strategy = parseStrategy(s, headerValidation)
    val fallback = parsePositionalFallback(headerValidation, strategy)
    val contentValidation = parseContentValidation(contentValidationConf)
    val (batchPolicy, repeatedHeaderPolicy, headerOnlyPolicy, quarantineOnFailure) =
      parseHeaderValidationOptions(headerValidation)

    Some(SchemaContract(
      version = s.getString("version"),
      compatibility = ConfigUtils.optString(s, "compatibility").getOrElse("BACKWARD").toUpperCase,
      columns = columns,
      policies = policies,
      strategy = strategy,
      positionalFallback = fallback,
      contentValidation = contentValidation,
      quarantineOnFailure = quarantineOnFailure,
      batchPolicy = batchPolicy,
      repeatedHeaderPolicy = repeatedHeaderPolicy,
      headerOnlyPolicy = headerOnlyPolicy
    ))
  }

  private def parseColumns(s: Config): Seq[ColumnContract] =
    s.getConfigList("columns").asScala.map(parseColumn).toSeq

  private def parseColumn(c: Config): ColumnContract = {
    val v: Config = ConfigUtils.optConfig(c, "validation").getOrElse(c)
    val validation = ColumnValidation(
      regex = ConfigUtils.optString(v, "regex"),
      minLength = ConfigUtils.optInt(v, "min_length"),
      maxLength = ConfigUtils.optInt(v, "max_length"),
      allowedValues = ConfigUtils.stringList(v, "allowed_values"),
      nonblank = ConfigUtils.optBoolean(v, "nonblank").getOrElse(false),
      numericParse = ConfigUtils.optBoolean(v, "numeric_parse").getOrElse(false),
      dateFormats = ConfigUtils.stringList(v, "accepted_date_formats"),
      timestampFormats = ConfigUtils.stringList(v, "accepted_timestamp_formats"),
      maxNullPercentage = ConfigUtils.optString(v, "max_null_percentage").map(_.toDouble)
    )
    val default =
      if (c.hasPathOrNull("default") && !c.getIsNull("default")) Some(c.getString("default"))
      else None
    ColumnContract(
      name = c.getString("name"),
      dataType = ConfigUtils.optString(c, "type")
        .orElse(ConfigUtils.optString(c, "data_type")).getOrElse("string"),
      nullable = ConfigUtils.optBoolean(c, "nullable").getOrElse(true),
      aliases = parseAliases(c),
      position = ConfigUtils.optInt(c, "position"),
      required = ConfigUtils.optBoolean(c, "required").getOrElse(true),
      default = default,
      category = ConfigUtils.optString(c, "category").getOrElse("business").toLowerCase,
      validation = if (validation.isDefined) Some(validation) else None
    )
  }

  private def validateColumns(columns: Seq[ColumnContract]): Unit = {
    require(columns.nonEmpty, "schema.columns must not be empty")

    val duplicateNames = columns.groupBy(c => normalize(c.name)).filter(_._2.size > 1).keys.toSeq
    require(duplicateNames.isEmpty,
      s"HDR_017 schema.columns contains duplicate canonical names after normalization: ${duplicateNames.mkString(",")}")

    val aliasOwners = columns.flatMap(c => (c.name +: c.aliases).map(a => normalize(a) -> c.name))
    val conflictingAliases = aliasOwners.groupBy(_._1)
      .filter(_._2.map(_._2).distinct.size > 1)
      .map { case (alias, owners) => s"'$alias' -> ${owners.map(_._2).distinct.mkString("/")}" }
    require(conflictingAliases.isEmpty,
      s"HDR_017 schema.columns aliases/names collide across columns after normalization: ${conflictingAliases.mkString("; ")}")

    val declaredPositions = columns.flatMap(_.position)
    val duplicatePositions = declaredPositions.groupBy(identity).filter(_._2.size > 1).keys.toSeq
    require(duplicatePositions.isEmpty,
      s"HDR_017 schema.columns contains duplicate positions: ${duplicatePositions.mkString(",")}")
    require(declaredPositions.forall(_ >= 0),
      s"HDR_017 schema.columns contains negative positions: ${declaredPositions.filter(_ < 0).mkString(",")}")

    columns.foreach { c =>
      try org.apache.spark.sql.types.DataType.fromDDL(c.dataType)
      catch {
        case e: Exception =>
          throw new IllegalArgumentException(
            s"HDR_017 schema.columns['${c.name}'] has invalid data type '${c.dataType}'", e)
      }
    }

    val validCategories = Set("business", "optional", "audit", "generated", "deprecated")
    columns.foreach { c =>
      require(validCategories.contains(c.category),
        s"HDR_017 schema.columns['${c.name}'] has unknown category '${c.category}'; " +
          s"expected one of ${validCategories.mkString(", ")}")
    }
  }

  private def parsePolicies(s: Config, headerValidation: Option[Config]): SchemaPolicies = {
    def policy(path: String, default: PolicyAction): PolicyAction =
      ConfigUtils.optString(s, path).map(PolicyAction.parse).getOrElse(default)

    def hvPolicy(hvKey: String, fallback: PolicyAction): PolicyAction =
      headerValidation.flatMap(h => ConfigUtils.optString(h, hvKey))
        .map(PolicyAction.parse).getOrElse(fallback)

    SchemaPolicies(
      onMissingColumn = hvPolicy("on_missing_required", policy("on_missing_column", PolicyAction.Fail)),
      onExtraColumn = hvPolicy("on_extra_columns", policy("on_extra_column", PolicyAction.Warn)),
      onTypeChange = policy("on_type_change", PolicyAction.Fail),
      onOrderChange = policy("on_order_change", PolicyAction.Warn),
      onDuplicateHeader = hvPolicy("on_duplicate_columns", policy("on_duplicate_header", PolicyAction.Fail)),
      onNullabilityViolation = policy("on_nullability_violation", PolicyAction.Fail),
      onVersionMismatch = policy("on_version_mismatch", PolicyAction.Warn),
      failOnMissingOptional = headerValidation
        .flatMap(h => ConfigUtils.optString(h, "on_missing_optional"))
        .exists(_.equalsIgnoreCase("FAIL"))
    )
  }

  private def parseStrategy(s: Config, headerValidation: Option[Config]): HeaderStrategy =
    headerValidation
      .flatMap(h => ConfigUtils.optString(h, "strategy"))
      .orElse(ConfigUtils.optString(s, "strategy"))
      .map(HeaderStrategy.parse)
      .getOrElse(HeaderStrategy.NameWithAliases)

  private def parsePositionalFallback(headerValidation: Option[Config], strategy: HeaderStrategy): PositionalFallback =
    headerValidation.flatMap(h => ConfigUtils.optConfig(h, "positional_fallback")) match {
      case Some(f) => PositionalFallback(
        enabled = ConfigUtils.optBoolean(f, "enabled").getOrElse(false),
        requireExactColumnCount = ConfigUtils.optBoolean(f, "require_exact_column_count").getOrElse(true),
        requireContentValidation = ConfigUtils.optBoolean(f, "require_content_validation").getOrElse(false)
      )
      case None => PositionalFallback(
        enabled = strategy == HeaderStrategy.NameAliasPositionFallback,
        requireExactColumnCount = true,
        requireContentValidation = false
      )
    }

  private def parseContentValidation(conf: Option[Config]): ContentValidationConfig =
    conf match {
      case Some(cv) => ContentValidationConfig(
        enabled = ConfigUtils.optBoolean(cv, "enabled").getOrElse(true),
        sampleMode = ConfigUtils.optString(cv, "mode").forall(_.equalsIgnoreCase("SAMPLE")),
        sampleRows = ConfigUtils.optInt(cv, "sample_rows").getOrElse(1000),
        maxFailurePercentage = ConfigUtils.optString(cv, "maximum_failure_percentage").map(_.toDouble).getOrElse(0.0),
        failOnAllNullRequired = ConfigUtils.optBoolean(cv, "fail_on_all_null_required_column").getOrElse(false)
      )
      case None => ContentValidationConfig(enabled = false, sampleMode = true, sampleRows = 1000, maxFailurePercentage = 0.0)
    }

  private def parseHeaderValidationOptions(headerValidation: Option[Config]): (String, Option[String], String, Boolean) = {
    val quarantineOnFailure = headerValidation
      .flatMap(h => ConfigUtils.optBoolean(h, "quarantine_on_failure"))
      .getOrElse(false)

    val batchPolicy = headerValidation
      .flatMap(h => ConfigUtils.optString(h, "batch_policy")).map(_.toUpperCase)
      .getOrElse(BatchPolicy.FileAtomic)
    require(Set(BatchPolicy.FileAtomic, BatchPolicy.BatchAtomic).contains(batchPolicy),
      s"HDR_017 header_validation.batch_policy '$batchPolicy' must be FILE_ATOMIC or BATCH_ATOMIC")

    val repeatedHeaderPolicy = headerValidation
      .flatMap(h => ConfigUtils.optString(h, "repeated_header_policy")).map(_.toUpperCase)
    repeatedHeaderPolicy.foreach(p => require(
      Set("FAIL", "REJECT_ROW", "DROP_WITH_WARNING").contains(p),
      s"HDR_017 header_validation.repeated_header_policy '$p' must be FAIL, REJECT_ROW or DROP_WITH_WARNING"))

    val headerOnlyPolicy = headerValidation
      .flatMap(h => ConfigUtils.optString(h, "header_only_policy")).map(_.toUpperCase)
      .getOrElse("WARN_AND_SKIP")
    require(Set("FAIL", "WARN_AND_SKIP").contains(headerOnlyPolicy),
      s"HDR_017 header_validation.header_only_policy '$headerOnlyPolicy' must be FAIL or WARN_AND_SKIP")

    (batchPolicy, repeatedHeaderPolicy, headerOnlyPolicy, quarantineOnFailure)
  }

  /** Aliases may be plain strings or objects with governance metadata:
    *   { value = "plan_hios_id", effective_from = "2026-07-01",
    *     valid_until = "2027-01-31", approval_reference = "CHG123456" }
    * Expired or not-yet-effective aliases are excluded with a warning. */
  // Alias effective windows are evaluated against a single date captured at
  // class-load time, so every parse within one JVM run sees the same alias
  // set (a run straddling midnight cannot flip an alias mid-run).
  private lazy val aliasEvaluationDate: java.time.LocalDate = java.time.LocalDate.now()

  private def parseAliases(c: Config): Seq[String] = {
    if (!c.hasPath("aliases")) return Seq.empty
    import com.typesafe.config.ConfigValueType
    val today = aliasEvaluationDate
    c.getList("aliases").asScala.flatMap { v =>
      v.valueType() match {
        case ConfigValueType.STRING => Some(v.unwrapped().toString)
        case ConfigValueType.OBJECT =>
          val a = v.asInstanceOf[com.typesafe.config.ConfigObject].toConfig
          val value = a.getString("value")
          val effectiveOk = ConfigUtils.optString(a, "effective_from")
            .forall(d => !today.isBefore(java.time.LocalDate.parse(d)))
          val notExpired = ConfigUtils.optString(a, "valid_until")
            .forall(d => !today.isAfter(java.time.LocalDate.parse(d)))
          if (effectiveOk && notExpired) Some(value)
          else {
            org.apache.log4j.Logger.getLogger(getClass.getName)
              .warn(s"[SchemaContract] Alias '$value' is outside its effective window and was excluded")
            None
          }
        case other =>
          throw new IllegalArgumentException(s"HDR_017 alias entries must be strings or objects, found $other")
      }
    }.toList
  }
}
