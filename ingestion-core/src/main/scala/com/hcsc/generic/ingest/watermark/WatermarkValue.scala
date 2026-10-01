package com.hcsc.generic.ingest.watermark

/** A watermark value; composite watermarks carry one value per column.
  *
  * Serialization escapes the '|' delimiter (and '\'): STRING tie-break
  * values come from live source rows, and an unescaped pipe would poison
  * the stored watermark permanently (arity mismatch JDBC_004 on every
  * later run). Legacy stored values without escapes deserialize
  * identically. Same scheme as extraction.Boundary.BoundaryValue. */
final case class WatermarkValue(values: Seq[String]) {
  def serialized: String =
    values.map(v => v.replace("\\", "\\\\").replace("|", "\\|")).mkString("|")
}

object WatermarkValue {
  def deserialize(s: String): WatermarkValue = {
    val parts = scala.collection.mutable.ArrayBuffer.empty[String]
    val current = new StringBuilder
    var i = 0
    while (i < s.length) {
      s.charAt(i) match {
        case '\\' if i + 1 < s.length =>
          current.append(s.charAt(i + 1)); i += 2
        case '|' =>
          parts += current.toString; current.clear(); i += 1
        case c =>
          current.append(c); i += 1
      }
    }
    parts += current.toString
    WatermarkValue(parts.toSeq)
  }
}
