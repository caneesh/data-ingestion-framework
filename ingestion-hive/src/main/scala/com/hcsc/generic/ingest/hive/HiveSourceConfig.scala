package com.hcsc.generic.ingest.hive

import com.hcsc.generic.ingest.config.ConfigUtils
import com.hcsc.generic.ingest.watermark.WatermarkValue
import com.typesafe.config.Config

import java.time.format.DateTimeFormatter

/** Partition lookback: re-read partitions newer than (watermark − N).
  * Exactly one of the two is set (CFG_027). */
final case class Lookback(days: Option[Int], partitions: Option[Int])

final case class HiveWatermarkConfig(
  /** Partition columns, in comparison order. Explicit — no contract fallback. */
  columns: Seq[String],
  initialValue: WatermarkValue,
  /** One DateTimeFormatter pattern per column, or empty for the equal-length guard. */
  formats: Seq[String],
  lookback: Option[Lookback],
  storeType: String,
  storeDatabase: Option[String],
  storeTable: String
)

final case class HiveSourceConfig(
  database: String,
  table: String,
  where: Option[String],
  /** FAIL | IGNORE — pins spark.sql.files.ignoreMissingFiles for the read. */
  missingFiles: String,
  logPartitionCounts: Boolean,
  watermark: Option[HiveWatermarkConfig],
  entity: Option[String],
  runId: Option[String],
  runMode: Option[String],
  auditDatabase: Option[String],
  auditRunTable: Option[String]
) {
  def fullTable: String = s"$database.$table"
}

/**
  * `source { type = "hive" }` configuration. Shape errors carry CFG_023
  * (the rule the static validator shares); lookback shape errors CFG_027;
  * an initial_value whose arity disagrees with watermark_columns HIVE_005.
  * The pipeline-injected keys (entity, run_id, run_mode, audit_database,
  * audit_run_table) are all optional here — their absence selects validate
  * mode in HiveSource.read.
  */
object HiveSourceConfig {

  private def fail(message: String): Nothing = throw new IllegalArgumentException(message)

  private def nonEmpty(c: Config, path: String): Option[String] =
    ConfigUtils.optString(c, path).map(_.trim).filter(_.nonEmpty)

  def parse(conf: Config): HiveSourceConfig = {
    val database = nonEmpty(conf, "database").getOrElse(
      fail("CFG_023 source.database is required for source.type = hive"))
    ConfigUtils.requireSqlIdentifier(database, "source.database")
    val table = nonEmpty(conf, "table").getOrElse(
      fail("CFG_023 source.table is required for source.type = hive"))
    ConfigUtils.requireSqlIdentifier(table, "source.table")

    val missingFiles = nonEmpty(conf, "missing_files").map(_.toUpperCase).getOrElse("FAIL")
    if (!Seq("FAIL", "IGNORE").contains(missingFiles))
      fail(s"CFG_023 source.missing_files '$missingFiles' must be FAIL or IGNORE")

    HiveSourceConfig(
      database = database,
      table = table,
      where = nonEmpty(conf, "where"),
      missingFiles = missingFiles,
      logPartitionCounts = ConfigUtils.optBoolean(conf, "log_partition_counts").getOrElse(false),
      watermark = ConfigUtils.optConfig(conf, "incremental").map(parseWatermark),
      entity = nonEmpty(conf, "entity"),
      runId = nonEmpty(conf, "run_id"),
      runMode = nonEmpty(conf, "run_mode").map(_.toUpperCase),
      auditDatabase = nonEmpty(conf, "audit_database"),
      auditRunTable = nonEmpty(conf, "audit_run_table")
    )
  }

  private def parseWatermark(inc: Config): HiveWatermarkConfig = {
    val columns = ConfigUtils.stringList(inc, "watermark_columns").map(_.trim).filter(_.nonEmpty)
    if (columns.isEmpty)
      fail("CFG_023 source.incremental.watermark_columns must name at least one partition column " +
        "explicitly; a hive source has no contract fallback for them")
    columns.foreach(c => ConfigUtils.requireSqlIdentifier(c, "source.incremental.watermark_columns"))
    if (columns.map(_.toLowerCase).distinct.size != columns.size)
      fail(s"CFG_023 source.incremental.watermark_columns repeats a column: ${columns.mkString(", ")}")

    val initialText = nonEmpty(inc, "initial_value").getOrElse(
      fail("CFG_023 source.incremental.initial_value is required (one component per watermark " +
        "column, separated by '|')"))
    val initial = WatermarkValue.deserialize(initialText)
    if (initial.values.size != columns.size)
      fail(s"HIVE_005 initial_value '$initialText' has ${initial.values.size} component(s) but " +
        s"watermark_columns has ${columns.size}; separate components with '|'")

    val formats = ConfigUtils.stringList(inc, "watermark_formats").map(_.trim)
    if (formats.nonEmpty && formats.size != columns.size)
      fail(s"CFG_023 source.incremental.watermark_formats must have one pattern per watermark " +
        s"column (${columns.size}), found ${formats.size}")
    formats.foreach { f =>
      try DateTimeFormatter.ofPattern(f)
      catch { case e: IllegalArgumentException =>
        fail(s"CFG_023 source.incremental.watermark_formats pattern '$f' is invalid: ${e.getMessage}") }
    }

    val lookback = ConfigUtils.optConfig(inc, "lookback").map { lb =>
      val days = ConfigUtils.optInt(lb, "days")
      val partitions = ConfigUtils.optInt(lb, "partitions")
      (days, partitions) match {
        case (Some(_), Some(_)) =>
          fail("CFG_027 source.incremental.lookback must name exactly one of days | partitions, not both")
        case (None, None) =>
          fail("CFG_027 source.incremental.lookback must name one of days | partitions")
        case (Some(d), None) if d <= 0 =>
          fail(s"CFG_027 source.incremental.lookback.days must be a positive integer, found $d")
        case (None, Some(p)) if p <= 0 =>
          fail(s"CFG_027 source.incremental.lookback.partitions must be a positive integer, found $p")
        case _ => Lookback(days, partitions)
      }
    }

    val store = ConfigUtils.optConfig(inc, "watermark_store")
    val storeType = store.flatMap(nonEmpty(_, "type")).map(_.toLowerCase).getOrElse("hive")
    val storeDatabase = store.flatMap(nonEmpty(_, "database"))
    val storeTable = store.flatMap(nonEmpty(_, "table")).getOrElse("ingest_watermarks")
    storeType match {
      case "memory" => ()
      case "hive" =>
        val db = storeDatabase.getOrElse(
          fail("CFG_023 source.incremental.watermark_store.database is required for the hive store"))
        ConfigUtils.requireSqlIdentifier(db, "watermark_store.database")
        ConfigUtils.requireSqlIdentifier(storeTable, "watermark_store.table")
      case other =>
        fail(s"CFG_023 Unknown watermark_store.type '$other'; expected hive or memory")
    }

    HiveWatermarkConfig(columns, initial, formats, lookback, storeType, storeDatabase, storeTable)
  }
}
