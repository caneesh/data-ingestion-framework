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
  nonblank: Boolean
) {
  def isDefined: Boolean =
    regex.isDefined || minLength.isDefined || maxLength.isDefined ||
      allowedValues.nonEmpty || nonblank
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
  maxFailurePercentage: Double
)

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
  quarantineOnFailure: Boolean = false
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
      "HDR_006 Positional mapping requires every schema column to declare a position"
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

    // header_validation / content_validation may live inside the schema block
    // or as siblings of it (source-level layout).
    def block(name: String): Option[Config] =
      ConfigUtils.optConfig(s, name).orElse(ConfigUtils.optConfig(conf, name))

    val headerValidation = block("header_validation")
    val contentValidationConf = block("content_validation")

    val columns = s.getConfigList("columns").asScala.map { c =>
      val validation = ColumnValidation(
        regex = ConfigUtils.optString(c, "regex"),
        minLength = ConfigUtils.optInt(c, "min_length"),
        maxLength = ConfigUtils.optInt(c, "max_length"),
        allowedValues = ConfigUtils.stringList(c, "allowed_values"),
        nonblank = ConfigUtils.optBoolean(c, "nonblank").getOrElse(false)
      )
      // `default = null` in HOCON is an explicit null default; hasPath is
      // false for null values so hasPathOrNull must be used.
      val default =
        if (c.hasPathOrNull("default") && !c.getIsNull("default")) Some(c.getString("default"))
        else None
      ColumnContract(
        name = c.getString("name"),
        dataType = ConfigUtils.optString(c, "type").getOrElse("string"),
        nullable = ConfigUtils.optBoolean(c, "nullable").getOrElse(true),
        aliases = ConfigUtils.stringList(c, "aliases"),
        position = ConfigUtils.optInt(c, "position"),
        required = ConfigUtils.optBoolean(c, "required").getOrElse(true),
        default = default,
        category = ConfigUtils.optString(c, "category").getOrElse("business"),
        validation = if (validation.isDefined) Some(validation) else None
      )
    }.toSeq

    require(columns.nonEmpty, "schema.columns must not be empty")

    val duplicateNames = columns.groupBy(c => normalize(c.name)).filter(_._2.size > 1).keys.toSeq
    require(
      duplicateNames.isEmpty,
      s"schema.columns contains duplicate column names: ${duplicateNames.mkString(",")}"
    )

    val aliasOwners = columns.flatMap(c => (c.name +: c.aliases).map(a => normalize(a) -> c.name))
    val conflictingAliases = aliasOwners.groupBy(_._1)
      .filter(_._2.map(_._2).distinct.size > 1)
      .map { case (alias, owners) => s"'$alias' -> ${owners.map(_._2).distinct.mkString("/")}" }
    require(
      conflictingAliases.isEmpty,
      s"HDR_003 schema.columns aliases map to more than one column: ${conflictingAliases.mkString("; ")}"
    )

    val declaredPositions = columns.flatMap(_.position)
    val duplicatePositions = declaredPositions.groupBy(identity).filter(_._2.size > 1).keys.toSeq
    require(
      duplicatePositions.isEmpty,
      s"HDR_006 schema.columns contains duplicate positions: ${duplicatePositions.mkString(",")}"
    )

    def policy(path: String, default: PolicyAction): PolicyAction = {
      val fromSchema = ConfigUtils.optString(s, path)
      fromSchema.map(PolicyAction.parse).getOrElse(default)
    }

    // header_validation block keys override the schema-level on_* keys.
    def hvPolicy(hvKey: String, fallback: PolicyAction): PolicyAction =
      headerValidation.flatMap(h => ConfigUtils.optString(h, hvKey))
        .map(PolicyAction.parse).getOrElse(fallback)

    val policies = SchemaPolicies(
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

    val strategy = headerValidation
      .flatMap(h => ConfigUtils.optString(h, "strategy"))
      .orElse(ConfigUtils.optString(s, "strategy"))
      .map(HeaderStrategy.parse)
      .getOrElse(HeaderStrategy.NameWithAliases)

    val fallback = headerValidation.flatMap(h => ConfigUtils.optConfig(h, "positional_fallback")) match {
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

    val contentValidation = contentValidationConf match {
      case Some(cv) => ContentValidationConfig(
        enabled = ConfigUtils.optBoolean(cv, "enabled").getOrElse(true),
        sampleMode = ConfigUtils.optString(cv, "mode").forall(_.equalsIgnoreCase("SAMPLE")),
        sampleRows = ConfigUtils.optInt(cv, "sample_rows").getOrElse(1000),
        maxFailurePercentage = ConfigUtils.optString(cv, "maximum_failure_percentage").map(_.toDouble).getOrElse(0.0)
      )
      case None => ContentValidationConfig(enabled = false, sampleMode = true, sampleRows = 1000, maxFailurePercentage = 0.0)
    }

    val quarantineOnFailure = headerValidation
      .flatMap(h => ConfigUtils.optBoolean(h, "quarantine_on_failure"))
      .getOrElse(false)

    Some(SchemaContract(
      version = s.getString("version"),
      compatibility = ConfigUtils.optString(s, "compatibility").getOrElse("BACKWARD").toUpperCase,
      columns = columns,
      policies = policies,
      strategy = strategy,
      positionalFallback = fallback,
      contentValidation = contentValidation,
      quarantineOnFailure = quarantineOnFailure
    ))
  }
}
