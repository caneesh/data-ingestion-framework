package com.hcsc.generic.ingest.jdbc.extraction

/**
  * Immutable strategy lookup, built once at wiring time — no mutable global
  * registry, no reflection, no classpath scanning. Extending the framework
  * is `ExtractionStrategyRegistry(builtIn :+ myStrategy)`.
  */
final class ExtractionStrategyRegistry private (strategies: Map[String, ExtractionStrategy]) {

  def resolve(kind: String): ExtractionStrategy =
    strategies.getOrElse(kind.toUpperCase,
      throw new IllegalArgumentException(
        s"EXT_003 Unknown extraction strategy '$kind'. Available: ${availableKinds.mkString(", ")}"))

  def availableKinds: Seq[String] = strategies.keys.toSeq.sorted
}

object ExtractionStrategyRegistry {
  def apply(strategies: Seq[ExtractionStrategy]): ExtractionStrategyRegistry = {
    val byKind = strategies.map(s => s.kind.toUpperCase -> s).toMap
    require(byKind.size == strategies.size,
      s"EXT_003 duplicate strategy kinds in ${strategies.map(_.kind).mkString(", ")}")
    new ExtractionStrategyRegistry(byKind)
  }
}
