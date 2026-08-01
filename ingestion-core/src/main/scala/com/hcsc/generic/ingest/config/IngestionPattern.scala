package com.hcsc.generic.ingest.config

import com.typesafe.config.Config

/**
  * Typed ingestion-pattern model over the existing configuration surface.
  *
  * The pattern is DERIVED from the keys feeds already use (source.mode,
  * incremental.watermark_type / overlap / upper_bound, curated.merge) so
  * existing feeds parse unchanged; an explicit `ingestion.pattern` may be
  * declared and is validated against the derived shape. Two pattern names
  * are intent overrides rather than shapes — BACKFILL and RAW_REPLAY — and
  * default the watermark-commit policy to false (a backfill or replay must
  * never burn the live incremental window unless the operator explicitly
  * says so with `ingestion.watermark_commit = true`).
  *
  * CHANGE_TRACKING and CDC_BATCH are declared capability names: the
  * extraction is not implemented, so declaring them fails fast (CFG_010)
  * instead of silently misbehaving.
  */
object IngestionPattern {

  val FullSnapshot          = "FULL_SNAPSHOT"
  val FullThenIncremental   = "FULL_THEN_INCREMENTAL"
  val TimestampIncremental  = "TIMESTAMP_INCREMENTAL"
  val TimestampOverlap      = "TIMESTAMP_OVERLAP"
  val CompositeWatermark    = "COMPOSITE_WATERMARK"
  val RowversionIncremental = "ROWVERSION_INCREMENTAL"
  val ChangeTracking        = "CHANGE_TRACKING"
  val CdcBatch              = "CDC_BATCH"
  val Backfill              = "BACKFILL"
  val RawReplay             = "RAW_REPLAY"

  val all: Seq[String] = Seq(
    FullSnapshot, FullThenIncremental, TimestampIncremental, TimestampOverlap,
    CompositeWatermark, RowversionIncremental, ChangeTracking, CdcBatch,
    Backfill, RawReplay)

  /** Declared-but-unimplemented extraction capabilities. */
  val unsupported: Set[String] = Set(ChangeTracking, CdcBatch)

  /** Intent overrides: legal over any derived incremental/full shape. */
  private val intentOverrides: Set[String] = Set(Backfill, RawReplay)

  /** The decomposed model the spec asks for: every axis separately. */
  final case class IngestionSpec(
    pattern: String,
    extractionMode: String,     // FILE | KAFKA | FULL_TABLE | SELECT_QUERY | CUSTOM_SQL | INCREMENTAL
    watermarkStrategy: String,  // NONE | TIMESTAMP | NUMERIC | DATE | DATETIMEOFFSET | ROWVERSION | STRING | COMPOSITE
    upperBoundStrategy: String, // NONE | MAX_VALUE | SOURCE_CLOCK
    rawPolicy: String,          // APPEND (append-only raw; delivery mode refines it)
    curatedStrategy: String,    // DISABLED | KEYED_MERGE | REPLACE
    deleteStrategy: String,     // IGNORE | SOFT
    watermarkCommitAllowed: Boolean
  )

  /** Derives the typed spec; configuration problems come back as CFG_0xx
    * strings alongside a best-effort spec (callers that only validate can
    * ignore the spec; callers that need it get the derived shape). */
  def derive(feed: Config): (IngestionSpec, Seq[String]) = {
    val errors = Seq.newBuilder[String]

    val sourceType = ConfigUtils.optString(feed, "source.type").map(_.toLowerCase).getOrElse("file")
    val jdbcMode = ConfigUtils.optString(feed, "source.mode").map(_.trim.toUpperCase)
    val incremental = ConfigUtils.optConfig(feed, "source")
      .flatMap(s => ConfigUtils.optConfig(s, "incremental"))
    val watermarkType = incremental
      .flatMap(i => ConfigUtils.optString(i, "watermark_type")).map(_.trim.toUpperCase)
    val hasOverlap = incremental.exists(_.hasPath("overlap"))
    val upperBound = incremental
      .flatMap(i => ConfigUtils.optString(i, "upper_bound")).map(_.trim.toUpperCase)
      .getOrElse(if (incremental.isDefined) "MAX_VALUE" else "NONE")

    val extractionMode = sourceType match {
      case "jdbc"  => jdbcMode.getOrElse("FULL_TABLE")
      case "kafka" => "KAFKA"
      case _       => "FILE"
    }

    val derivedPattern =
      if (sourceType != "jdbc") FullSnapshot
      else if (jdbcMode.contains("INCREMENTAL")) watermarkType match {
        case Some("COMPOSITE")  => CompositeWatermark
        case Some("ROWVERSION") => RowversionIncremental
        case _ if hasOverlap    => TimestampOverlap
        case _                  => TimestampIncremental
      }
      else if (incremental.isDefined) FullThenIncremental
      else FullSnapshot

    val explicit = ConfigUtils.optString(feed, "ingestion.pattern").map(_.trim.toUpperCase)
    explicit.foreach { p =>
      if (!all.contains(p))
        errors += s"CFG_011 ingestion.pattern '$p' is unknown; expected one of ${all.mkString(", ")}"
      else if (unsupported.contains(p))
        errors += s"CFG_010 ingestion.pattern $p is a declared capability that is NOT implemented " +
          "yet (no Change Tracking / CDC extraction exists); use an implemented pattern or remove " +
          "the declaration"
      else if (!intentOverrides.contains(p) && p != derivedPattern)
        errors += s"CFG_011 ingestion.pattern $p contradicts the configured source: the " +
          s"configuration derives $derivedPattern (source.mode=${jdbcMode.getOrElse("absent")}, " +
          s"watermark_type=${watermarkType.getOrElse("absent")}, overlap=${hasOverlap})"
    }

    val pattern = explicit.filter(p => all.contains(p) && !unsupported.contains(p))
      .getOrElse(derivedPattern)

    // Watermark-commit policy: intent overrides default to NO commit; an
    // explicit ingestion.watermark_commit wins in both directions.
    val explicitCommit = ConfigUtils.optConfig(feed, "ingestion")
      .flatMap(i => ConfigUtils.optBoolean(i, "watermark_commit"))
    val commitAllowed = explicitCommit.getOrElse(!intentOverrides.contains(pattern))
    if (intentOverrides.contains(pattern) && !commitAllowed &&
        ConfigUtils.optString(feed, "watermark.advance_after").isDefined)
      errors += s"CFG_012 ingestion.pattern $pattern does not commit the watermark, but " +
        "watermark.advance_after is configured; remove one, or set " +
        "ingestion.watermark_commit = true to make the backfill/replay commit deliberately"

    val mergeKeys = ConfigUtils.optConfig(feed, "curated")
      .flatMap(c => ConfigUtils.optConfig(c, "merge"))
      .map(m => ConfigUtils.stringList(m, "keys")).getOrElse(Seq.empty)
    val curatedStrategy =
      if (!feed.hasPath("curated") ||
          ConfigUtils.optConfig(feed, "curated").exists(c =>
            ConfigUtils.optBoolean(c, "enabled").contains(false))) "DISABLED"
      else if (mergeKeys.nonEmpty) "KEYED_MERGE"
      else "REPLACE"

    val deleteStrategy = ConfigUtils.optString(feed, "curated.merge.deletes.mode")
      .map(_.trim.toUpperCase).getOrElse("IGNORE")

    (IngestionSpec(
      pattern = pattern,
      extractionMode = extractionMode,
      watermarkStrategy = watermarkType.getOrElse("NONE"),
      upperBoundStrategy = upperBound,
      rawPolicy = "APPEND",
      curatedStrategy = curatedStrategy,
      deleteStrategy = deleteStrategy,
      watermarkCommitAllowed = commitAllowed
    ), errors.result())
  }

  /** The single question the pipeline's watermark gate asks. */
  def commitAllowed(feed: Config): Boolean = derive(feed)._1.watermarkCommitAllowed
}
