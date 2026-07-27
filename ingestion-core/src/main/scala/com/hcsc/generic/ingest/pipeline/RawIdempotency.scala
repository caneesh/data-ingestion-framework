package com.hcsc.generic.ingest.pipeline

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

/**
  * Content-scoped RAW idempotency for managed file feeds.
  *
  * The per-run guard (skip the append when RAW already holds this run_id)
  * only protects resume with the SAME run id. When a run crashes after the
  * RAW append but before file completion, the staged file stays in
  * inprogress and the next execution — a NEW run id — re-reads it. file_id
  * (SHA-256 of the staged path, stable across restarts) identifies those
  * rows regardless of which run wrote them.
  *
  * `--force-reprocess` bypasses this (approved deliberate re-loads).
  */
object RawIdempotency {

  /** file_ids present in both `incoming` and the RAW table. */
  def alreadyLoadedFileIds(spark: SparkSession, rawTable: String, incoming: DataFrame): Seq[String] = {
    if (!incoming.columns.contains("file_id")) return Seq.empty
    if (!spark.catalog.tableExists(rawTable)) return Seq.empty
    if (!spark.table(rawTable).columns.contains("file_id")) return Seq.empty
    spark.table(rawTable)
      .select("file_id")
      .join(incoming.select("file_id").distinct(), Seq("file_id"))
      .distinct()
      .collect()
      .map(_.getString(0))
      .toSeq
  }

  /** Drops rows whose file was already committed to RAW by a prior run. */
  def excludeLoadedFiles(
    spark: SparkSession,
    rawTable: String,
    incoming: DataFrame,
    logger: Logger
  ): DataFrame = {
    val loaded = alreadyLoadedFileIds(spark, rawTable, incoming)
    if (loaded.isEmpty) incoming
    else {
      logger.warn(s"[RawIdempotency] ${loaded.size} staged file(s) already have rows in " +
        s"$rawTable from a prior run (crash-recovery replay); excluding them from this " +
        "run. Use --resume --run-id <original> to resume the original run instead, or " +
        "--force-reprocess for a deliberate re-load.")
      incoming.filter(!col("file_id").isin(loaded: _*))
    }
  }
}
