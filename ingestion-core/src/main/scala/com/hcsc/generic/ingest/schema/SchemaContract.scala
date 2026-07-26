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

final case class ColumnContract(
  name: String,
  dataType: String,
  nullable: Boolean,
  aliases: Seq[String],
  position: Option[Int]
)

final case class SchemaPolicies(
  onMissingColumn: PolicyAction,
  onExtraColumn: PolicyAction,
  onTypeChange: PolicyAction,
  onOrderChange: PolicyAction,
  onDuplicateHeader: PolicyAction,
  onNullabilityViolation: PolicyAction,
  onVersionMismatch: PolicyAction
)

final case class SchemaContract(
  version: String,
  compatibility: String,
  columns: Seq[ColumnContract],
  policies: SchemaPolicies
) {
  def columnNames: Seq[String] = columns.map(_.name)

  /** Normalized alias or canonical name -> canonical contract name. */
  lazy val aliasLookup: Map[String, String] =
    columns.flatMap(c => (c.name +: c.aliases).map(a => SchemaContract.normalize(a) -> c.name)).toMap

  def resolve(header: String): Option[String] = aliasLookup.get(SchemaContract.normalize(header))

  def column(name: String): Option[ColumnContract] = columns.find(_.name.equalsIgnoreCase(name))

  /** Columns ordered by their declared position; only valid when every column declares one. */
  def positionalOrder: Seq[ColumnContract] = {
    require(
      columns.forall(_.position.isDefined),
      "Positional mapping requires every schema column to declare a position"
    )
    columns.sortBy(_.position.get)
  }
}

object SchemaContract {

  private[schema] def normalize(s: String): String = s.trim.toLowerCase

  /** Parses the optional `schema` block from a feed (or source) config. */
  def parse(conf: Config): Option[SchemaContract] = {
    if (!conf.hasPath("schema")) return None
    val s = conf.getConfig("schema")

    val columns = s.getConfigList("columns").asScala.map { c =>
      ColumnContract(
        name = c.getString("name"),
        dataType = ConfigUtils.optString(c, "type").getOrElse("string"),
        nullable = ConfigUtils.optBoolean(c, "nullable").getOrElse(true),
        aliases = ConfigUtils.stringList(c, "aliases"),
        position = ConfigUtils.optInt(c, "position")
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
      s"schema.columns aliases map to more than one column: ${conflictingAliases.mkString("; ")}"
    )

    val declaredPositions = columns.flatMap(_.position)
    val duplicatePositions = declaredPositions.groupBy(identity).filter(_._2.size > 1).keys.toSeq
    require(
      duplicatePositions.isEmpty,
      s"schema.columns contains duplicate positions: ${duplicatePositions.mkString(",")}"
    )

    def policy(path: String, default: PolicyAction): PolicyAction =
      ConfigUtils.optString(s, path).map(PolicyAction.parse).getOrElse(default)

    Some(SchemaContract(
      version = s.getString("version"),
      compatibility = ConfigUtils.optString(s, "compatibility").getOrElse("BACKWARD").toUpperCase,
      columns = columns,
      policies = SchemaPolicies(
        onMissingColumn = policy("on_missing_column", PolicyAction.Fail),
        onExtraColumn = policy("on_extra_column", PolicyAction.Warn),
        onTypeChange = policy("on_type_change", PolicyAction.Fail),
        onOrderChange = policy("on_order_change", PolicyAction.Warn),
        onDuplicateHeader = policy("on_duplicate_header", PolicyAction.Fail),
        onNullabilityViolation = policy("on_nullability_violation", PolicyAction.Fail),
        onVersionMismatch = policy("on_version_mismatch", PolicyAction.Warn)
      )
    ))
  }
}
