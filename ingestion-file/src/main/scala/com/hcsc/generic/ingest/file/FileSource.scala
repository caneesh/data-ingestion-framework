package com.hcsc.generic.ingest.file

import com.hcsc.generic.ingest.config.ConfigUtils
import com.hcsc.generic.ingest.schema.{SchemaContract, SchemaValidator}
import com.hcsc.generic.ingest.source.{Source, SourceRegistry}
import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object FileSource extends Source {
  private val logger = Logger.getLogger(getClass.getName)

  override def sourceType: String = "file"

  override def read(spark: SparkSession, sourceConf: Config): DataFrame = {
    val format = ConfigUtils.optString(sourceConf, "file_type").getOrElse("csv")
    val header = ConfigUtils.optBoolean(sourceConf, "header").getOrElse(false)
    val delimiter = ConfigUtils.optString(sourceConf, "delimiter").getOrElse(",")
    val multiline = ConfigUtils.optBoolean(sourceConf, "multiline").getOrElse(false)

    var reader = spark.read
      .format(format)
      .option("header", header.toString)
      .option("delimiter", delimiter)
      .option("multiLine", multiline.toString)
      .option("mode", "PERMISSIVE")

    ConfigUtils.optString(sourceConf, "quote").foreach(v => reader = reader.option("quote", v))
    ConfigUtils.optString(sourceConf, "escape").foreach(v => reader = reader.option("escape", v))

    // Managed feeds pass explicit staged file paths; legacy feeds use a glob.
    val explicitPaths = ConfigUtils.stringList(sourceConf, "paths")
    val loaded0 =
      if (explicitPaths.nonEmpty) reader.load(explicitPaths: _*)
      else reader.load(sourceConf.getString("path"))
    val loaded = loaded0.withColumn("source_file", input_file_name())

    val mapped = SchemaContract.parse(sourceConf) match {
      case Some(contract) =>
        applyContract(loaded, contract, sourceConf, header)
      case None =>
        val renamed = applyHeaderAliases(loaded, sourceConf)
        applyConfiguredColumns(renamed, sourceConf, header)
    }

    val withoutTrailer = removeTrailer(mapped, sourceConf)
    skipFirstRows(withoutTrailer, sourceConf)
  }

  /**
    * Contract-driven column resolution. With a header, columns are matched by
    * name or declared alias and every drift (missing, extra, renamed,
    * reordered, duplicated columns) is validated against the feed's policies.
    * Positional mapping is never applied silently: it requires either
    * header=false or an explicit force_columns_by_position=true, always
    * validates the column count against the contract, and with a header the
    * headers are still validated first so drift is surfaced before recovery.
    */
  private def applyContract(df: DataFrame, contract: SchemaContract, source: Config, header: Boolean): DataFrame = {
    val businessColumns = df.columns.filterNot(_.equalsIgnoreCase("source_file"))
    val forceByPosition = ConfigUtils.optBoolean(source, "force_columns_by_position").getOrElse(false)

    if (header) {
      val resolution = SchemaValidator.validateHeaders(businessColumns, contract, logger)
      SchemaValidator.enforce(resolution.violations, contract.policies, logger)

      if (forceByPosition) mapByPosition(df, businessColumns, contract)
      else
        resolution.renames.foldLeft(df) {
          case (acc, (from, to)) => acc.withColumnRenamed(from, to)
        }
    } else {
      mapByPosition(df, businessColumns, contract)
    }
  }

  private def mapByPosition(df: DataFrame, businessColumns: Seq[String], contract: SchemaContract): DataFrame = {
    val ordered = contract.positionalOrder
    require(
      businessColumns.length == ordered.length,
      s"Positional mapping requires exactly ${ordered.length} source columns, found ${businessColumns.length}. " +
        s"Actual=${businessColumns.mkString(",")} Contract=${ordered.map(_.name).mkString(",")}"
    )
    logger.warn(
      s"[FileSource] Applying positional column mapping from schema contract v${contract.version}: " +
        ordered.map(_.name).mkString(",")
    )
    businessColumns.zip(ordered.map(_.name)).foldLeft(df) {
      case (acc, (actual, target)) =>
        if (actual == target) acc else acc.withColumnRenamed(actual, target)
    }
  }

  private def applyHeaderAliases(df: DataFrame, source: Config): DataFrame = {
    val aliases = ConfigUtils.stringMap(source, "header_aliases")
    aliases.foldLeft(df) {
      case (acc, (configuredSource, target)) =>
        actualColumn(acc, configuredSource) match {
          case Some(actual) if !actual.equalsIgnoreCase(target) =>
            acc.withColumnRenamed(actual, target)
          case _ => acc
        }
    }
  }

  private def applyConfiguredColumns(df: DataFrame, source: Config, header: Boolean): DataFrame = {
    val configured = ConfigUtils.stringList(source, "columns")
    if (configured.isEmpty) return normalizeHeaders(df)

    val forceByPosition =
      ConfigUtils.optBoolean(source, "force_columns_by_position").getOrElse(!header)

    val businessColumns = df.columns.filterNot(_.equalsIgnoreCase("source_file"))

    if (forceByPosition) {
      require(
        businessColumns.length == configured.length,
        s"Source column count ${businessColumns.length} does not match configured count ${configured.length}. " +
          s"Actual=${businessColumns.mkString(",")} Configured=${configured.mkString(",")}"
      )

      businessColumns.zip(configured).foldLeft(df) {
        case (acc, (actual, target)) =>
          if (actual == target) acc else acc.withColumnRenamed(actual, target)
      }
    } else {
      val normalized = normalizeHeaders(df)
      val missing = configured.filterNot(c => normalized.columns.exists(_.equalsIgnoreCase(c)))
      require(
        missing.isEmpty,
        s"Required source columns not found after header aliases: ${missing.mkString(",")}. " +
          s"Actual=${normalized.columns.mkString(",")}"
      )
      normalized
    }
  }

  private def normalizeHeaders(df: DataFrame): DataFrame = {
    df.columns.foldLeft(df) { (acc, current) =>
      val normalized =
        if (current.equalsIgnoreCase("source_file")) "source_file"
        else current.trim.toLowerCase
          .replaceAll("[^a-z0-9]+", "_")
          .replaceAll("^_+|_+$", "")

      if (normalized.isEmpty) {
        throw new IllegalArgumentException(
          s"Column '$current' normalizes to an empty name; rename it via header_aliases"
        )
      } else if (current == normalized) acc
      else if (acc.columns.exists(c => c != current && c.equalsIgnoreCase(normalized))) {
        throw new IllegalArgumentException(
          s"Column '$current' normalizes to '$normalized' which collides with another column; " +
            "resolve via header_aliases"
        )
      } else acc.withColumnRenamed(current, normalized)
    }
  }

  private def removeTrailer(df: DataFrame, source: Config): DataFrame = {
    if (!ConfigUtils.optBoolean(source, "drop_trailer").getOrElse(false)) return df

    val dataCols = df.columns.filterNot(_.equalsIgnoreCase("source_file"))
    val defaultCol = dataCols.headOption.getOrElse(
      throw new IllegalArgumentException("Cannot remove trailer because no source columns are available")
    )

    if (source.hasPath("trailer.marker")) {
      val marker = source.getString("trailer.marker")
      val configuredCol = ConfigUtils.optString(source, "trailer.column").getOrElse(defaultCol)
      val colName = actualColumn(df, configuredCol).getOrElse(
        throw new IllegalArgumentException(s"Trailer column '$configuredCol' was not found")
      )
      df.filter(!lower(trim(col(colName))).startsWith(marker.trim.toLowerCase))
    } else if (ConfigUtils.optBoolean(source, "trailer.by_last_row").getOrElse(false)) {
      dropRowsPerFile(df, 1, descending = true)
    } else df
  }

  private def skipFirstRows(df: DataFrame, source: Config): DataFrame = {
    val n = ConfigUtils.optInt(source, "skip_first_n").getOrElse(0)
    if (n <= 0) df else dropRowsPerFile(df, n, descending = false)
  }

  // LIMITATION: monotonically_increasing_id() only preserves physical line
  // order when each source file is read as a single input split. Files larger
  // than one HDFS block are split across partitions and row order within a
  // source_file window is not guaranteed — trailer/skip-row removal may then
  // drop the wrong rows.
  private def dropRowsPerFile(df: DataFrame, n: Int, descending: Boolean): DataFrame = {
    logger.warn(
      "[FileSource] skip_first_n/trailer removal relies on file read order; " +
        "only reliable for files that fit in a single input split (one HDFS block)"
    )
    val ordering = if (descending) monotonically_increasing_id().desc else monotonically_increasing_id()
    val w = Window.partitionBy(col("source_file")).orderBy(ordering)
    df.withColumn("_row_to_skip", row_number().over(w))
      .filter(col("_row_to_skip") > lit(n))
      .drop("_row_to_skip")
  }

  private def actualColumn(df: DataFrame, requested: String): Option[String] =
    df.columns.find(_.trim.equalsIgnoreCase(requested.trim))

  def register(): Unit = SourceRegistry.register(this)
}
