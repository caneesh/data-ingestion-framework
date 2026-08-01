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
final case class OrderSpec(column: String, ascending: Boolean, nullsFirst: Boolean) {
  def toColumn(actualColumn: String): Column = (ascending, nullsFirst) match {
    case (true, true)   => col(actualColumn).asc_nulls_first
    case (true, false)  => col(actualColumn).asc_nulls_last
    case (false, true)  => col(actualColumn).desc_nulls_first
    case (false, false) => col(actualColumn).desc_nulls_last
  }
}

object OrderSpec {
  private val Known = Set("asc", "desc", "nulls_first", "nulls_last")

  def parse(spec: String): OrderSpec = {
    val tokens = spec.trim.split("\\s+").toSeq
    require(tokens.nonEmpty && tokens.head.nonEmpty, "HDR_020 empty ordering specification")
    val column = tokens.head
    val modifiers = tokens.tail.map(_.toLowerCase)
    val unknown = modifiers.filterNot(Known.contains)
    require(unknown.isEmpty,
      s"HDR_020 unrecognized ordering token(s) [${unknown.mkString(", ")}] in '$spec'; " +
        "expected: column [asc|desc] [nulls_first|nulls_last]")
    require(!(modifiers.contains("asc") && modifiers.contains("desc")),
      s"HDR_020 ordering '$spec' declares both asc and desc")
    require(!(modifiers.contains("nulls_first") && modifiers.contains("nulls_last")),
      s"HDR_020 ordering '$spec' declares both nulls_first and nulls_last")
    OrderSpec(column,
      ascending = modifiers.contains("asc"),
      nullsFirst = modifiers.contains("nulls_first"))
  }
}
