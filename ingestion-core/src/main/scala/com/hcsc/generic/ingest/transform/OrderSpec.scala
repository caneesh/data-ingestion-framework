package com.hcsc.generic.ingest.transform

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

/**
  * One ordering term of a dedup / tie-breaker specification. The extended
  * string form is `"column [asc|desc] [nulls_first|nulls_last]"`; a bare
  * column name keeps the framework's historical default (descending, nulls
  * last — "highest value wins, absent values lose"), so every existing
  * configuration parses unchanged.
  */
final case class OrderSpec(
  column: String,
  ascending: Boolean,
  nullsFirst: Boolean,
  /** Logical COMPARISON type ("as bigint", "as timestamp", ...): the value
    * is cast for ordering only — physical storage is untouched. Lets an
    * all-string layout still order numerically/chronologically. */
  compareAs: Option[String] = None
) {
  def toColumn(actualColumn: String): Column =
    applyDirection(compareAs.fold(col(actualColumn))(t => col(actualColumn).cast(t)))

  /** Direction + null ordering applied to an arbitrary comparison
    * expression (used when the caller supplies the logical-type cast). */
  def applyDirection(base: Column): Column = (ascending, nullsFirst) match {
    case (true, true)   => base.asc_nulls_first
    case (true, false)  => base.asc_nulls_last
    case (false, true)  => base.desc_nulls_first
    case (false, false) => base.desc_nulls_last
  }
}

object OrderSpec {
  private val Known = Set("asc", "desc", "nulls_first", "nulls_last")

  def parse(spec: String): OrderSpec = {
    val tokens = spec.trim.split("\\s+").toSeq
    require(tokens.nonEmpty && tokens.head.nonEmpty, "HDR_020 empty ordering specification")
    val column = tokens.head
    // Optional trailing "as <type>" declares the logical comparison type;
    // the type token is validated as parseable Spark DDL at parse time so
    // a typo fails configuration, not ordering.
    val (modifierTokens, compareAs) = tokens.tail.map(_.toLowerCase).toList match {
      case mods if mods.contains("as") =>
        val idx = mods.indexOf("as")
        require(idx == mods.length - 2,
          s"HDR_020 ordering '$spec': 'as' must be followed by exactly one type token " +
            "(column [asc|desc] [nulls_first|nulls_last] [as <type>])")
        val t = mods.last
        try org.apache.spark.sql.types.DataType.fromDDL(t)
        catch { case e: Exception => throw new IllegalArgumentException(
          s"HDR_020 ordering '$spec': comparison type '$t' is not a valid Spark type", e) }
        (mods.dropRight(2), Some(t))
      case mods => (mods, None)
    }
    val unknown = modifierTokens.filterNot(Known.contains)
    require(unknown.isEmpty,
      s"HDR_020 unrecognized ordering token(s) [${unknown.mkString(", ")}] in '$spec'; " +
        "expected: column [asc|desc] [nulls_first|nulls_last] [as <type>]")
    require(!(modifierTokens.contains("asc") && modifierTokens.contains("desc")),
      s"HDR_020 ordering '$spec' declares both asc and desc")
    require(!(modifierTokens.contains("nulls_first") && modifierTokens.contains("nulls_last")),
      s"HDR_020 ordering '$spec' declares both nulls_first and nulls_last")
    OrderSpec(column,
      ascending = modifierTokens.contains("asc"),
      nullsFirst = modifierTokens.contains("nulls_first"),
      compareAs = compareAs)
  }
}
