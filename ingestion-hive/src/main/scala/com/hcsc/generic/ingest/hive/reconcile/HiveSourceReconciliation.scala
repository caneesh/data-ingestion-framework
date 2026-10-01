package com.hcsc.generic.ingest.hive.reconcile

import com.hcsc.generic.ingest.config.ConfigUtils
import com.hcsc.generic.ingest.hive.HiveSource
import com.hcsc.generic.ingest.reconcile.{ReconcileCheck, SourceReconciler}
import com.hcsc.generic.ingest.schema.SchemaContract
import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Independent SOURCE-vs-CURATED reconciliation for a hive source — the
  * same two tiers as the JDBC service, with both sides as Spark tables.
  *
  * Every other check in this framework is a WITHIN-RUN identity: it proves
  * no rows vanished between two stages of one execution. None of them ever
  * asks whether curated, cumulatively, still agrees with the source — so a
  * batch lost outside the pipeline (a partition that never landed, a run
  * that never happened, a manual edit) leaves the ledger perfectly clean and
  * the consumer as the detector.
  *
  * ==Why the source side is the whole table, not the watermark window==
  *
  * Comparing "as of the committed watermark" reports drift forever: a row
  * delivered in an older partition carries that partition in curated, and
  * when a later partition re-delivers it the source row now falls OUTSIDE a
  * `<= watermark` filter while the curated copy still falls inside. KEY
  * EXISTENCE is partition-independent: a row re-delivered after the
  * watermark still exists on both sides. The source side is therefore
  * `HiveSource.readAll` — every partition under the feed's `where` (the
  * same scoping the extraction uses, so deliberately excluded partitions
  * are not reported as missing) and never the watermark.
  *
  * ==Tiers==
  *
  *  - Tier 1 `source_curated_cardinality` — `COUNT(*)` both sides. Cheap,
  *    catches gross loss. Curated may legitimately EXCEED source when source
  *    deletes are retained, and legitimately be SHORTER on a keyed feed
  *    (in-batch dedup, quarantined keys), so the assertion is strict only
  *    for keyless feeds; for keyed feeds it is the recorded trend.
  *  - Tier 2 `source_keys_present_in_curated` — business keys in source with
  *    no curated match. Non-zero is real data loss; this is the alarm.
  *  - Tier 2 `curated_keys_absent_from_source` — recorded INFORMATIONAL
  *    (always passing): under `deletes.mode = IGNORE` these are rows deleted
  *    upstream and deliberately retained, under SOFT they are retained
  *    tombstones. The NUMBER is the finding.
  */
final class HiveSourceReconciliation(
  spark: SparkSession,
  feedConf: Config,
  logger: Logger
) extends SourceReconciler {

  private val reconcileConf = ConfigUtils.optConfig(feedConf, "reconcile")

  def configured: Boolean =
    reconcileConf.exists(c => ConfigUtils.optBoolean(c, "enabled").getOrElse(true))

  /** REPORT (default) records findings without failing; FAIL turns a Tier 2
    * loss into a run failure. REPORT is the default deliberately: unlike an
    * in-run check there is no batch to stop and nothing to roll back, and a
    * nightly comparison that goes red gets silenced rather than read. */
  def onMismatch: String =
    reconcileConf.flatMap(c => ConfigUtils.optString(c, "on_mismatch"))
      .getOrElse("REPORT").toUpperCase

  def run(): Seq[ReconcileCheck] = {
    require(Seq("REPORT", "FAIL").contains(onMismatch),
      s"CFG_022 reconcile.on_mismatch '$onMismatch' must be REPORT or FAIL")

    val baseSourceConf = feedConf.getConfig("source")
    val contract = SchemaContract.parse(feedConf).orElse(SchemaContract.parse(baseSourceConf))
    // The pipeline attaches the feed-level contract to the source config so
    // the connector resolves aliases; do the same so the source frame carries
    // canonical names and its keys line up with curated's.
    val sourceConf =
      if (feedConf.hasPath("schema")) baseSourceConf.withValue("schema", feedConf.getValue("schema"))
      else baseSourceConf

    val curatedConf = ConfigUtils.optConfig(feedConf, "curated").getOrElse(
      throw new IllegalArgumentException(
        "CFG_022 --stage reconcile compares against the CURATED table; this feed has no " +
          "curated block to compare with"))
    val curatedTable =
      s"${ConfigUtils.sqlIdentifier(curatedConf, "database")}.${ConfigUtils.sqlIdentifier(curatedConf, "table")}"
    require(spark.catalog.tableExists(curatedTable),
      s"CFG_022 curated table $curatedTable does not exist; nothing to reconcile against")

    val curated = spark.table(curatedTable)
    val source = HiveSource.readAll(spark, sourceConf)
    val checks = scala.collection.mutable.ArrayBuffer.empty[ReconcileCheck]

    // ---- Tier 1: cardinality -------------------------------------------------
    // STRICT ONLY FOR KEYLESS FEEDS. With business keys, curated is
    // legitimately SHORTER than the source's raw row count (in-batch
    // duplicates collapse to one row per key, null-key rows are quarantined)
    // and legitimately LONGER (retained deletes). For keyed feeds Tier 2 is
    // the alarm and this becomes the recorded trend.
    val keys = mergeKeys(curatedConf, contract)
    val sourceCount = source.count()
    val curatedCount = curated.count()
    logger.info(s"[Reconcile] cardinality source=$sourceCount curated=$curatedCount")
    if (keys.isEmpty)
      checks += ReconcileCheck("source_curated_cardinality", s">=$sourceCount", curatedCount.toString,
        curatedCount >= sourceCount)
    else
      checks += ReconcileCheck("source_curated_cardinality",
        s"source=$sourceCount", s"curated=$curatedCount", passed = true)

    // ---- Tier 2: key sets ----------------------------------------------------
    if (keys.isEmpty)
      logger.warn("[Reconcile] no business keys (curated.merge.keys or a contract business_key); " +
        "Tier 2 key comparison skipped — cardinality alone cannot identify WHICH rows differ")
    else
      checks ++= compareKeys(source, curated, curatedTable, keys)

    checks.toSeq
  }

  /** Curated business keys: explicit config wins, else the contract's
    * declared business_key columns — the same resolution the merge uses. */
  private def mergeKeys(curatedConf: Config, contract: Option[SchemaContract]): Seq[String] = {
    val configured = ConfigUtils.optConfig(curatedConf, "merge")
      .filter(_.hasPath("keys")).map(m => ConfigUtils.stringList(m, "keys"))
    configured.getOrElse(contract.map(_.businessKeyColumns).getOrElse(Seq.empty))
  }

  private def compareKeys(
    source: DataFrame,
    curated: DataFrame,
    curatedTable: String,
    keys: Seq[String]
  ): Seq[ReconcileCheck] = {
    import org.apache.spark.sql.functions.{col, trim => sqlTrim}

    // NULL and BLANK keys are excluded on BOTH sides. The curated merge
    // treats blank business keys as null (treat_blank_as_null defaults true)
    // and quarantines them (CUR_001), so a null- or blank-keyed source row
    // can never have a curated row and must not read as data loss; a feed
    // running null_handling ALLOW retains keyless passthrough rows, which
    // must not pollute the absent-from-source count either.
    def keyedOnly(df: DataFrame): DataFrame =
      keys.foldLeft(df)((acc, k) =>
        acc.filter(col(k).isNotNull && sqlTrim(col(k).cast("string")) =!= ""))

    // Key names are matched case-insensitively to the canonical names, as
    // everywhere else in the framework.
    def keyColumns(df: DataFrame, side: String): DataFrame = df.select(keys.map { k =>
      val actual = df.columns.find(_.equalsIgnoreCase(k)).getOrElse(
        throw new IllegalStateException(s"CFG_022 business key '$k' is not a column of $side"))
      col(actual).as(k)
    }: _*)

    val sourceKeys = keyedOnly(keyColumns(source, "the hive source (after contract resolution)"))
      .distinct().persist()
    val curatedKeys = keyedOnly(keyColumns(curated, curatedTable)).distinct().persist()

    try {
      val missing = sourceKeys.join(curatedKeys, keys, "left_anti").count()
      val extra = curatedKeys.join(sourceKeys, keys, "left_anti").count()

      if (missing > 0)
        logger.error(s"[Reconcile] DATA LOSS: $missing source key(s) have no row in " +
          s"$curatedTable. These were never ingested, or were removed outside the pipeline.")
      if (extra > 0)
        logger.warn(s"[Reconcile] $extra curated key(s) no longer exist at source. Whether that " +
          "is expected depends on this feed's delete policy: under deletes.mode = IGNORE they " +
          "are upstream deletions deliberately retained — and a NON-ZERO count is the evidence " +
          "that source deletes DO occur, which a feed assuming otherwise should revisit.")

      Seq(
        ReconcileCheck("source_keys_present_in_curated", "0", missing.toString, missing == 0),
        // Informational by construction — see the class comment.
        ReconcileCheck("curated_keys_absent_from_source", extra.toString, extra.toString, true))
    } finally {
      try sourceKeys.unpersist(false) catch { case _: Exception => () }
      try curatedKeys.unpersist(false) catch { case _: Exception => () }
    }
  }
}
