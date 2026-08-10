package com.hcsc.generic.ingest.retention

import com.hcsc.generic.ingest.config.ConfigUtils
import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, row_number}

import java.time.LocalDate

/**
  * Config-driven retention/purge for the append-only stores (spec §17):
  *
  * retention {
  *   raw = "400d"               # drop ingest_dt partitions older than this
  *   rejects = "90d"            # reject_ts cutoff for the reject table
  *   audit = "730d"             # event_ts cutoff for the audit tables
  *   watermarks_keep_last = 50  # versions retained per entity
  * }
  *
  * Semantics:
  * - RAW: partition drops only (ALTER TABLE DROP PARTITION on ingest_dt) —
  *   cheap and metadata-only. An unpartitioned raw table is skipped with a
  *   warning; row-level raw purging requires an ACID table format.
  * - Reject/audit tables: rewritten through a staging table keeping only
  *   rows newer than the cutoff (Hive ORC has no row deletes).
  * - Watermark history: keep-last-N versions per entity, same rewrite.
  * - dry-run: reports what WOULD be purged, touches nothing.
  *
  * Invoked via `--stage retention` (IngestMain.runRetention), which holds
  * the entity lock for the duration — a staged rewrite racing a live run's
  * watermark/audit appends would silently discard them — and records the
  * outcome as a run-audit row AFTER purging (so the record of the purge
  * itself survives it). This class performs only the purges.
  */
final class RetentionService(spark: SparkSession, feedConf: Config, logger: Logger) {

  private val retention = ConfigUtils.optConfig(feedConf, "retention")

  def configured: Boolean = retention.isDefined

  def run(dryRun: Boolean): Seq[(String, String, Long)] = {
    val conf = retention.getOrElse(throw new IllegalArgumentException(
      "PIPE_004 --stage retention requires a retention { } block " +
        "(raw / rejects / audit periods and/or watermarks_keep_last)"))
    val results = scala.collection.mutable.ArrayBuffer.empty[(String, String, Long)]

    ConfigUtils.optString(conf, "raw").map(parseDays).foreach { days =>
      results ++= purgeRawPartitions(cutoffDate(days), dryRun)
    }
    ConfigUtils.optString(conf, "rejects").map(parseDays).foreach { days =>
      rejectTable.foreach { t =>
        results += purgeByTimestamp(t, "reject_ts", days, dryRun)
      }
    }
    ConfigUtils.optString(conf, "audit").map(parseDays).foreach { days =>
      auditTables.foreach { t =>
        results += purgeByTimestamp(t, "event_ts", days, dryRun)
      }
    }
    ConfigUtils.optInt(conf, "watermarks_keep_last").foreach { keep =>
      watermarkTable.foreach { t =>
        results += trimWatermarkHistory(t, keep, dryRun)
      }
    }

    results.foreach { case (table, action, count) =>
      logger.info(s"[Retention]${if (dryRun) " DRY-RUN:" else ""} $table $action count=$count")
    }
    results.toSeq
  }

  // ---------------------------------------------------------------------------

  private def parseDays(spec: String): Int = {
    val m = "^(\\d+)d$".r.findFirstMatchIn(spec.trim).getOrElse(
      throw new IllegalArgumentException(
        s"PIPE_004 retention period '$spec' must be '<days>d' (e.g. 400d)"))
    m.group(1).toInt
  }

  /** Cutoffs are computed in the SPARK SESSION time zone — the zone the
    * pipeline writes its timestamps and ingest_dt values in (the app pins it
    * to UTC) — never the JVM default, which on a non-UTC edge node would
    * shift every cutoff by the offset. */
  private def sessionZone: java.time.ZoneId =
    java.time.ZoneId.of(
      spark.conf.get("spark.sql.session.timeZone", java.time.ZoneId.systemDefault().getId))

  private[retention] def cutoffDate(days: Int): LocalDate =
    LocalDate.now(sessionZone).minusDays(days.toLong)

  private def rawTable: Option[String] =
    ConfigUtils.optConfig(feedConf, "raw").map { r =>
      s"${ConfigUtils.sqlIdentifier(r, "database")}.${ConfigUtils.sqlIdentifier(r, "table")}"
    }

  private def rejectTable: Option[String] =
    ConfigUtils.optConfig(feedConf, "rejects").filter(_.hasPath("database")).map { r =>
      val db = ConfigUtils.sqlIdentifier(r, "database")
      val t = ConfigUtils.optString(r, "table").getOrElse("ingest_rejects")
      ConfigUtils.requireSqlIdentifier(t, "rejects.table")
      s"$db.$t"
    }

  private def auditTables: Seq[String] =
    ConfigUtils.optConfig(feedConf, "audit").filter(_.hasPath("database")).toSeq.flatMap { a =>
      val db = ConfigUtils.sqlIdentifier(a, "database")
      Seq("ingest_run_audit", "ingest_file_audit", "ingest_reconciliation", "ingest_header_audit")
        .map(t => s"$db.$t")
    }

  private def watermarkTable: Option[String] =
    ConfigUtils.optConfig(feedConf, "source")
      .flatMap(s => ConfigUtils.optConfig(s, "incremental"))
      .flatMap(i => ConfigUtils.optConfig(i, "watermark_store"))
      .filter(ws => ConfigUtils.optString(ws, "type").forall(_.equalsIgnoreCase("hive")))
      .map { ws =>
        val db = ConfigUtils.optString(ws, "database").getOrElse("ingest_audit")
        val t = ConfigUtils.optString(ws, "table").getOrElse("ingest_watermarks")
        ConfigUtils.requireSqlIdentifier(db, "watermark_store.database")
        ConfigUtils.requireSqlIdentifier(t, "watermark_store.table")
        s"$db.$t"
      }

  /** RAW purge: metadata-only partition drops on ingest_dt. */
  private def purgeRawPartitions(cutoff: LocalDate, dryRun: Boolean): Seq[(String, String, Long)] =
    rawTable.toSeq.flatMap { table =>
      if (!spark.catalog.tableExists(table)) Seq((table, "skipped (absent)", 0L))
      else {
        val partitions =
          try spark.sql(s"SHOW PARTITIONS $table").collect().map(_.getString(0)).toSeq
          catch {
            case _: Exception =>
              logger.warn(s"[Retention] $table is not partitioned; RAW retention needs " +
                "ingest_dt partitioning (row-level raw purges require an ACID table format)")
              return Seq((table, "skipped (unpartitioned)", 0L))
          }
        // Multi-key layouts ("ingest_dt=.../region=...") would make every
        // ingest_dt value unparseable and turn retention into a silent
        // no-op run after run — skip LOUDLY instead.
        if (partitions.exists(_.contains("/"))) {
          logger.warn(s"[Retention] $table uses multi-key partitioning; RAW retention " +
            "supports single-key ingest_dt partitions only — nothing dropped")
          return Seq((table, "skipped (multi-key partitioning unsupported)", 0L))
        }
        // A single-key partition under ANY OTHER NAME is the quiet failure:
        // SHOW PARTITIONS succeeds, there is no '/' to trip the multi-key
        // guard, and every partition then fails the name match — leaving
        // retention to report "0 dropped" run after run, indistinguishable
        // from a table that simply has nothing expired yet. Skip loudly, like
        // the two cases above.
        val partitionKeys = partitions.flatMap(_.split("=", 2).headOption).distinct
        if (partitionKeys.nonEmpty && !partitionKeys.exists(_.equalsIgnoreCase("ingest_dt"))) {
          logger.warn(s"[Retention] $table is partitioned by " +
            s"${partitionKeys.mkString(", ")}, not ingest_dt; RAW retention drops partitions " +
            "by that exact name — nothing dropped. Rename the partition column to ingest_dt " +
            "(raw.partitioning.keys and the table DDL) to enable RAW retention.")
          return Seq((table, s"skipped (partitioned by ${partitionKeys.mkString(",")}, not ingest_dt)", 0L))
        }
        val expired = partitions.flatMap { p =>
          // "ingest_dt=2026-01-01"
          p.split("=", 2) match {
            case Array(k, v) if k.equalsIgnoreCase("ingest_dt") =>
              val parsed = scala.util.Try(LocalDate.parse(v)).toOption
              if (parsed.isEmpty)
                logger.warn(s"[Retention] $table partition '$p' has an unparseable ingest_dt; retained")
              parsed.filter(_.isBefore(cutoff)).map(_ => (k, v))
            case _ => None
          }
        }
        if (!dryRun) expired.foreach { case (k, v) =>
          spark.sql(s"ALTER TABLE $table DROP IF EXISTS PARTITION ($k='${v.replace("'", "''")}')")
        }
        Seq((table, s"partitions older than $cutoff dropped", expired.size.toLong))
      }
    }

  /** Append-only table purge: staged rewrite keeping rows >= cutoff. */
  private def purgeByTimestamp(table: String, tsColumn: String, days: Int, dryRun: Boolean): (String, String, Long) = {
    if (!spark.catalog.tableExists(table)) return (table, "skipped (absent)", 0L)
    val cutoffTs = java.sql.Timestamp.from(cutoffDate(days).atStartOfDay(sessionZone).toInstant)
    val cutoffLit = org.apache.spark.sql.functions.lit(cutoffTs)
    val expired = spark.table(table)
      .filter(col(tsColumn).isNotNull && col(tsColumn) < cutoffLit).count()
    if (expired == 0L) return (table, s"rows older than $days d purged", 0L)
    // NULL-timestamped rows (legacy) are retained, never silently purged.
    if (!dryRun) rewriteKeeping(table,
      spark.table(table).filter(col(tsColumn).isNull || col(tsColumn) >= cutoffLit))
    (table, s"rows older than $days d purged", expired)
  }

  /** Watermark history trim: newest N versions per entity survive. */
  private def trimWatermarkHistory(table: String, keepLast: Int, dryRun: Boolean): (String, String, Long) = {
    if (!spark.catalog.tableExists(table)) return (table, "skipped (absent)", 0L)
    require(keepLast >= 1, "PIPE_004 retention.watermarks_keep_last must be >= 1")
    val w = org.apache.spark.sql.expressions.Window
      .partitionBy(col("entity")).orderBy(col("watermark_version").desc, col("updated_ts").desc)
    val ranked = spark.table(table).withColumn("_rank", row_number().over(w))
    val expired = ranked.filter(col("_rank") > keepLast).count()
    if (expired == 0L) return (table, s"versions beyond last $keepLast trimmed", 0L)
    if (!dryRun) rewriteKeeping(table, ranked.filter(col("_rank") <= keepLast).drop("_rank"))
    (table, s"versions beyond last $keepLast trimmed", expired)
  }

  /** Hive ORC has no row deletes: materialize survivors into a staging
    * table, INSERT OVERWRITE the target from it, drop staging — the same
    * isolation pattern the curated publish uses (never read-and-overwrite
    * one table in a single statement). */
  private def rewriteKeeping(table: String, survivors: org.apache.spark.sql.DataFrame): Unit = {
    val staging = s"${table}_retention_stg"
    survivors.write.mode(SaveMode.Overwrite).format("orc").saveAsTable(staging)
    try spark.sql(s"INSERT OVERWRITE TABLE $table SELECT * FROM $staging")
    finally spark.sql(s"DROP TABLE IF EXISTS $staging")
  }
}
