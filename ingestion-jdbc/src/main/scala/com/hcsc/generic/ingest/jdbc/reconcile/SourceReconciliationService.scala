package com.hcsc.generic.ingest.jdbc.reconcile

import com.hcsc.generic.ingest.config.ConfigUtils
import com.hcsc.generic.ingest.jdbc.JdbcSourceConfig
import com.hcsc.generic.ingest.jdbc.read.{DriverQueries, QueryBuilder}
import com.hcsc.generic.ingest.schema.SchemaContract
import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

/** One comparison, in the shape the reconciliation ledger already stores. */
final case class ReconcileCheck(name: String, expected: String, actual: String, passed: Boolean)

/**
  * Independent SOURCE-vs-CURATED reconciliation.
  *
  * Every other check in this framework is a WITHIN-RUN identity: it proves
  * no rows vanished between two stages of one execution. None of them ever
  * asks whether curated, cumulatively, still agrees with the source — so a
  * batch lost outside the pipeline (a dropped partition, a run that never
  * happened, a manual edit) leaves the ledger perfectly clean and the
  * consumer as the detector.
  *
  * ==Why this does not filter by timestamp==
  *
  * The obvious design compares both sides "as of the committed watermark".
  * It is wrong, and it reports drift forever. A row last modified BEFORE the
  * watermark was ingested and carries that old timestamp in curated; when
  * the source edits it again AFTER the watermark, the source row now falls
  * OUTSIDE a `<= watermark` filter while the curated copy still falls
  * inside. The counts then differ by exactly the number of rows edited since
  * the last run — normal lag, reported as loss, until everyone ignores the
  * alert.
  *
  * KEY EXISTENCE is timestamp-independent: a row edited after the watermark
  * still exists on both sides. That is what makes this comparison
  * trustworthy, and why Tier 2 is the check that earns its keep.
  *
  * ==Tiers==
  *
  *  - Tier 1 `source_curated_cardinality` — `COUNT(*)` both sides. Cheap,
  *    catches gross loss. Curated may legitimately EXCEED source when source
  *    deletes are not applied, so the assertion is only that curated is not
  *    SHORT.
  *  - Tier 2 `source_keys_present_in_curated` — business keys in source with
  *    no curated match. Non-zero is real data loss; this is the alarm.
  *  - Tier 2 `curated_keys_absent_from_source` — recorded INFORMATIONAL
  *    (always passing) because whether it is expected depends entirely on
  *    the delete policy: under `deletes.mode = IGNORE` these are rows
  *    deleted upstream and deliberately retained, and under SOFT they are
  *    retained tombstones. The NUMBER is the finding — for a feed that
  *    declares no source deletes are expected, it is the measurement that
  *    tests that assumption.
  */
final class SourceReconciliationService(
  spark: SparkSession,
  feedConf: Config,
  logger: Logger
) {

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

    val sourceConf = feedConf.getConfig("source")
    val cfg = JdbcSourceConfig.parse(sourceConf)
    val contract = SchemaContract.parse(feedConf).orElse(SchemaContract.parse(sourceConf))

    val curatedConf = ConfigUtils.optConfig(feedConf, "curated").getOrElse(
      throw new IllegalArgumentException(
        "CFG_022 --stage reconcile compares against the CURATED table; this feed has no " +
          "curated block to compare with"))
    val curatedTable =
      s"${ConfigUtils.sqlIdentifier(curatedConf, "database")}.${ConfigUtils.sqlIdentifier(curatedConf, "table")}"
    require(spark.catalog.tableExists(curatedTable),
      s"CFG_022 curated table $curatedTable does not exist; nothing to reconcile against")

    val curated = spark.table(curatedTable)
    val checks = scala.collection.mutable.ArrayBuffer.empty[ReconcileCheck]

    // ---- Tier 1: cardinality -------------------------------------------------
    // STRICT ONLY FOR KEYLESS FEEDS. With business keys, curated is
    // legitimately SHORTER than the source's raw row count: in-batch
    // duplicates collapse to one row per key, and null-key / rule-rejected
    // rows are quarantined by design. Asserting curated >= source there
    // manufactures a false alarm out of normal operation (the e2e seed data
    // trips it on its FIRST scenario). For keyed feeds Tier 2 is the alarm
    // and this becomes the recorded trend; for keyless append-only feeds a
    // short curated really does mean loss, so the assertion stays.
    val keys = mergeKeys(curatedConf, contract)
    val sourceCount = countSource(cfg)
    val curatedCount = curated.count()
    sourceCount.foreach { sc =>
      logger.info(s"[Reconcile] cardinality source=$sc curated=$curatedCount")
      if (keys.isEmpty)
        checks += ReconcileCheck("source_curated_cardinality", s">=$sc", curatedCount.toString,
          curatedCount >= sc)
      else
        checks += ReconcileCheck("source_curated_cardinality",
          s"source=$sc", s"curated=$curatedCount", passed = true)
    }

    // ---- Tier 2: key sets ----------------------------------------------------
    if (keys.isEmpty)
      logger.warn("[Reconcile] no business keys (curated.merge.keys or a contract business_key); " +
        "Tier 2 key comparison skipped — cardinality alone cannot identify WHICH rows differ")
    else
      checks ++= compareKeys(cfg, contract, curated, curatedTable, keys)

    checks.toSeq
  }

  /** Tier 1 source side. Scoped by the SAME non-watermark filters the
    * extraction uses — comparing against an unfiltered source would report
    * every deliberately excluded row as missing. */
  private def countSource(cfg: JdbcSourceConfig): Option[Long] =
    QueryBuilder.validatedBase(cfg, Seq.empty).flatMap { base =>
      val sql = s"SELECT COUNT(1) FROM $base${QueryBuilder.filterWhereClause(cfg)}"
      logger.info(s"[Reconcile] source cardinality query issued")
      DriverQueries.firstRow(cfg, sql, logger).flatMap(_.headOption.flatten).map(_.toLong)
    }

  /** Curated business keys: explicit config wins, else the contract's
    * declared business_key columns — the same resolution the merge uses. */
  private def mergeKeys(curatedConf: Config, contract: Option[SchemaContract]): Seq[String] = {
    val configured = ConfigUtils.optConfig(curatedConf, "merge")
      .filter(_.hasPath("keys")).map(m => ConfigUtils.stringList(m, "keys"))
    configured.getOrElse(contract.map(_.businessKeyColumns).getOrElse(Seq.empty))
  }

  /**
    * The curated key name is CANONICAL; the source column may be named
    * differently and the contract already records that as an alias
    * (`{ name = "file_name", aliases = ["FileName"] }`). Resolving through
    * the contract means a correctly-onboarded feed needs no extra
    * configuration to reconcile.
    */
  private def sourceColumnFor(canonical: String, contract: Option[SchemaContract]): String =
    contract.flatMap(_.column(canonical)).flatMap(_.aliases.headOption).getOrElse(canonical)

  private def compareKeys(
    cfg: JdbcSourceConfig,
    contract: Option[SchemaContract],
    curated: DataFrame,
    curatedTable: String,
    keys: Seq[String]
  ): Seq[ReconcileCheck] = {
    val base = QueryBuilder.validatedBase(cfg, Seq.empty).getOrElse(
      return Seq.empty)

    // Only the key columns leave the source: on a 364-column table this is
    // the difference between a cheap comparison and a second full extract.
    val projection = keys.map { k =>
      val src = sourceColumnFor(k, contract)
      s"${cfg.dialect.quoteIdentifier(src)} AS ${cfg.dialect.quoteIdentifier(k)}"
    }.mkString(", ")
    // NULL keys are excluded in the source SQL (saves transfer); BLANK
    // keys are excluded Spark-side after load — see keyedOnly below. Both
    // are quarantined by the curated merge (CUR_001) and accounted for in
    // ingest_rejects, never here.
    val notNull = keys.map { k =>
      val q = cfg.dialect.quoteIdentifier(sourceColumnFor(k, contract))
      s"$q IS NOT NULL"
    }.mkString(" AND ")
    val baseWhere = QueryBuilder.filterWhereClause(cfg)
    val keyWhere =
      if (baseWhere.isEmpty) s" WHERE $notNull" else s"$baseWhere AND ($notNull)"
    val sql = s"(SELECT $projection FROM $base$keyWhere) src_keys"

    var reader = spark.read.format("jdbc")
      .option("url", cfg.url).option("driver", cfg.driver)
      .option("dbtable", sql).option("fetchsize", cfg.fetchSize.toString)
    cfg.user.foreach(u => reader = reader.option("user", u))
    cfg.password.foreach(p => reader = reader.option("password", p))
    cfg.connectionProperties.foreach { case (k, v) => reader = reader.option(k, v) }

    // BLANK keys are excluded Spark-side (portable across dialects; SQL
    // above already excluded NULLs to save transfer). The curated merge
    // treats blank business keys as null (treat_blank_as_null defaults
    // true) and quarantines them, so a blank-keyed source row can no more
    // have a curated row than a null-keyed one — without this filter it
    // reads as data loss on every run. The same filter applies to the
    // CURATED side: a feed running null_handling ALLOW retains keyless
    // passthrough rows, which must not pollute the absent-from-source count.
    import org.apache.spark.sql.functions.{col, trim => sqlTrim}
    def keyedOnly(df: DataFrame): DataFrame =
      keys.foldLeft(df)((acc, k) =>
        acc.filter(col(k).isNotNull && sqlTrim(col(k).cast("string")) =!= ""))

    val sourceKeys = keyedOnly(reader.load()).distinct().persist()
    // Curated keys are matched case-insensitively to the canonical names, as
    // everywhere else in the framework.
    val curatedKeys = keyedOnly(curated.select(keys.map { k =>
      val actual = curated.columns.find(_.equalsIgnoreCase(k)).getOrElse(
        throw new IllegalStateException(
          s"CFG_022 business key '$k' is not a column of $curatedTable"))
      org.apache.spark.sql.functions.col(actual).as(k)
    }: _*)).distinct().persist()

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
