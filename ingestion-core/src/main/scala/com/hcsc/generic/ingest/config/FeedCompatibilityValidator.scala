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

    val curatedPresent = feed.hasPath("curated")
    // Explicit APPEND is keyless by design (event-history curated tables);
    // every other curated shape derives state and needs keys.
    val keylessDerivingCurated =
      curatedPresent && !curatedStrategy.contains("APPEND") && mergeKeys.isEmpty

    // CDC events can only become current state through a keyed merge. A feed
    // with NO curated block (raw-only CDC archive) is legitimate, as is an
    // explicit APPEND event-history curated layer.
    if (rawStrategy.contains("CDC_EVENTS") && keylessDerivingCurated)
      errors += "CFG_004 raw.strategy CDC_EVENTS with a state-deriving curated layer requires " +
        "merge.keys (curated.strategy TYPE1_MERGE); use curated.strategy APPEND for event history"

    // A keyed merge without keys fails at runtime; catch it at startup.
    // MERGE is the accepted alias for TYPE1_MERGE.
    if (curatedStrategy.exists(s => s == "TYPE1_MERGE" || s == "MERGE") && mergeKeys.isEmpty)
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

    // An incremental JDBC feed produces window deltas — via the legacy
    // source.mode OR an incremental extraction strategy. A state-deriving
    // curated layer without merge keys publishes FULL overwrites of exactly
    // those deltas: every run would replace the curated table with only the
    // latest window, silently discarding earlier state. (Static limits: the
    // actual overwrite is also selected by the CLI --mode at runtime, which
    // configuration validation cannot see — this rule closes the config-side
    // half of that hazard.)
    val jdbcIncremental = sourceType.contains("jdbc") &&
      (opt("source.mode").contains("INCREMENTAL") ||
        extractionStrategy.exists(IncrementalExtraction.contains))
    if (jdbcIncremental && keylessDerivingCurated)
      errors += "CFG_009 an incremental jdbc source feeding a state-deriving curated layer requires " +
        "curated.merge.keys; a keyless overwrite would keep only the latest extraction window " +
        "(use curated.strategy APPEND for delta-history tables)"

    // Typed ingestion-pattern model (CFG_010 unsupported capability,
    // CFG_011 pattern/config contradiction, CFG_012 backfill commit clash).
    errors ++= IngestionPattern.derive(feed)._2

    // Raw delivery mode: deduplicated appends need a SOURCE record-version
    // identity, and run_id can never be one (it identifies the pipeline
    // execution, not the record).
    val deliveryMode = opt("raw.delivery_mode")
    val rawIdempotencyKey = ConfigUtils.optConfig(feed, "raw")
      .map(r => ConfigUtils.stringList(r, "idempotency_key")).getOrElse(Seq.empty)
    deliveryMode.foreach { m =>
      if (!Seq("AT_LEAST_ONCE_APPEND", "DEDUPLICATED_APPEND").contains(m))
        errors += s"CFG_013 raw.delivery_mode '$m' must be AT_LEAST_ONCE_APPEND or DEDUPLICATED_APPEND"
      else if (m == "DEDUPLICATED_APPEND" && rawIdempotencyKey.isEmpty)
        errors += "CFG_013 raw.delivery_mode DEDUPLICATED_APPEND requires raw.idempotency_key " +
          "(source PK + version column[s] identifying one source record version)"
    }
    if (rawIdempotencyKey.exists(_.equalsIgnoreCase("run_id")))
      errors += "CFG_013 raw.idempotency_key must identify a SOURCE record version; run_id " +
        "identifies the pipeline execution and would defeat cross-run deduplication"

    // Absence-based deletion is only sound over a COMPLETE extract: any
    // source-side filtering would tombstone every excluded row. Unsupported
    // delete capabilities fail here too (runtime parse also guards).
    val deleteMode = opt("curated.merge.deletes.mode")
    if (deleteMode.contains("FULL_SNAPSHOT_ABSENCE")) {
      val filtered = feed.hasPath("source.where") || feed.hasPath("source.filters") ||
        feed.hasPath("source.sql") || feed.hasPath("source.query")
      if (filtered)
        errors += "CFG_014 deletes.mode FULL_SNAPSHOT_ABSENCE with source-side filtering " +
          "(where/filters/sql): absence over a PARTIAL extract tombstones every excluded row; " +
          "remove the filters or use SOFT/IGNORE"
      val confirmed = ConfigUtils.optConfig(feed, "curated")
        .flatMap(c => ConfigUtils.optConfig(c, "merge"))
        .flatMap(m => ConfigUtils.optConfig(m, "deletes"))
        .flatMap(d => ConfigUtils.optBoolean(d, "confirm_complete_extract")).getOrElse(false)
      if (!confirmed)
        errors += "CFG_014 deletes.mode FULL_SNAPSHOT_ABSENCE requires " +
          "deletes.confirm_complete_extract = true"
    }
    if (deleteMode.exists(m => Seq("CHANGE_TRACKING", "CDC", "RECONCILE",
        "PERIODIC_KEY_RECONCILIATION").contains(m)))
      errors += s"CFG_014 deletes.mode ${deleteMode.get} is a declared capability that is not " +
        "implemented; use IGNORE, SOFT or FULL_SNAPSHOT_ABSENCE"

    errors.result()
  }
}
