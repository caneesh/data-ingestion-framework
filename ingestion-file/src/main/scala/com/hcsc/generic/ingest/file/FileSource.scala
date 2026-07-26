package com.hcsc.generic.ingest.file

import com.hcsc.generic.ingest.config.ConfigUtils
import com.hcsc.generic.ingest.source.{Source, SourceRegistry}
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object FileSource extends Source {

  override def sourceType: String = "file"

  override def read(spark: SparkSession, sourceConf: Config): DataFrame = {
    val path = sourceConf.getString("path")
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

    val loaded = reader.load(path).withColumn("source_file", input_file_name())
    val renamed = applyHeaderAliases(loaded, sourceConf)
    val mapped = applyConfiguredColumns(renamed, sourceConf, header)
    val withoutTrailer = removeTrailer(mapped, sourceConf)
    skipFirstRows(withoutTrailer, sourceConf)
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

      if (current == normalized || normalized.isEmpty) acc
      else if (acc.columns.exists(c => c != current && c.equalsIgnoreCase(normalized))) acc
      else acc.withColumnRenamed(current, normalized)
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

  private def dropRowsPerFile(df: DataFrame, n: Int, descending: Boolean): DataFrame = {
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
