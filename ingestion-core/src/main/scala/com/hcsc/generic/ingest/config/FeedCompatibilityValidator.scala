package com.hcsc.generic.ingest.config

import com.typesafe.config.Config

/**
  * Static cross-section validation of one feed's configuration: catches
  * combinations that are individually valid but jointly wrong, at startup
  * (or in the config generator's dry run) instead of mid-pipeline. Rules
  * are string-based deliberately, so core does not depend on the strategy
  * modules; each rule cites the sections it joins.
  */
object FeedCompatibilityValidator {

  private val IncrementalExtraction = Set("TIMESTAMP", "TIMESTAMP_KEY", "INCREASING_KEY")

  def validate(feed: Config): Seq[String] = {
    val errors = Seq.newBuilder[String]
    def opt(path: String): Option[String] = ConfigUtils.optString(feed, path).map(_.trim.toUpperCase)

    val sourceType = ConfigUtils.optString(feed, "source.type").map(_.toLowerCase)
    val extractionStrategy = opt("source.extraction.strategy")
    val rawStrategy = opt("raw.strategy")
    val curatedStrategy = opt("curated.strategy")
    val mergeKeys = ConfigUtils.optConfig(feed, "curated")
      .map(c => ConfigUtils.optConfig(c, "merge").map(m => ConfigUtils.stringList(m, "keys")).getOrElse(Seq.empty))
      .getOrElse(Seq.empty)

    // Extraction strategies plan JDBC reads; they are meaningless elsewhere.
    if (extractionStrategy.isDefined && !sourceType.contains("jdbc"))
      errors += "CFG_001 source.extraction applies to jdbc sources only " +
        s"(source.type is '${sourceType.getOrElse("absent")}')"

    // Incremental extraction needs a boundary; a snapshot must not carry one.
    extractionStrategy.foreach { s =>
      val hasBoundary = feed.hasPath("source.extraction.boundary")
      if (IncrementalExtraction.contains(s) && !hasBoundary)
        errors += s"CFG_002 extraction strategy $s requires source.extraction.boundary"
      if (s == "FULL_SNAPSHOT" && feed.hasPath("source.extraction.boundary.overlap"))
        errors += "CFG_003 FULL_SNAPSHOT has no incremental window; boundary.overlap is contradictory"
    }

    // CDC events can only become current state through a keyed merge.
    if (rawStrategy.contains("CDC_EVENTS") &&
        (curatedStrategy.contains("APPEND") || (curatedStrategy.isEmpty && mergeKeys.isEmpty)))
      errors += "CFG_004 raw.strategy CDC_EVENTS requires a keyed curated merge " +
        "(curated.strategy TYPE1_MERGE with merge.keys); APPEND would never apply updates or deletes"

    // A keyed merge without keys fails at runtime; catch it at startup.
    if (curatedStrategy.contains("TYPE1_MERGE") && mergeKeys.isEmpty)
      errors += "CFG_005 curated.strategy TYPE1_MERGE requires curated.merge.keys"

    // Contract-derived reject rules need a contract to derive from.
    val rejectUsesContract = ConfigUtils.optConfig(feed, "rejects")
      .flatMap(r => ConfigUtils.optBoolean(r, "use_contract_nullability")).getOrElse(false)
    if (rejectUsesContract && !feed.hasPath("schema"))
      errors += "CFG_006 rejects.use_contract_nullability requires a schema contract block"

    // Failing reconciliation needs the audit tables it reads/writes.
    val auditEnabled = ConfigUtils.optConfig(feed, "audit")
      .map(a => ConfigUtils.optBoolean(a, "enabled").getOrElse(true))
    val reconciliationFails = ConfigUtils.optString(feed, "audit.reconciliation.on_mismatch")
      .exists(_.equalsIgnoreCase("FAIL"))
    if (reconciliationFails && auditEnabled.contains(false))
      errors += "CFG_007 audit.reconciliation.on_mismatch=FAIL requires audit.enabled=true"

    // File feeds use folder/watermark-free intake; JDBC incremental blocks
    // configured on them will be silently ignored — reject instead.
    if (sourceType.contains("file") && feed.hasPath("source.incremental"))
      errors += "CFG_008 source.incremental (JDBC watermarks) has no effect on file sources"

    errors.result()
  }
}
