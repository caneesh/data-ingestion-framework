package com.hcsc.generic.ingest.file

import com.hcsc.generic.ingest.config.ConfigUtils
import com.hcsc.generic.ingest.schema.{HeaderStrategy, SchemaContract, SchemaValidator, SchemaViolation, ViolationKind}
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
        val cleaned = handleRepeatedHeaders(loaded, contract, header)
        applyContract(cleaned, contract, sourceConf, header)
      case None =>
        if (sourceConf.hasPath("header_aliases") || sourceConf.hasPath("columns"))
          logger.warn(
            "[FileSource] DEPRECATED: source.columns / source.header_aliases are legacy options; " +
              "migrate this feed to a schema contract (schema { columns = [...] }) for validated header handling")
        val renamed = applyHeaderAliases(loaded, sourceConf)
        applyConfiguredColumns(renamed, sourceConf, header)
    }

    val withoutTrailer = removeTrailer(mapped, sourceConf)
    skipFirstRows(withoutTrailer, sourceConf)
  }

  /**
    * Contract-driven column resolution.
    *
    * With a header, columns are matched per the configured strategy
    * (STRICT_NAME: canonical names only; NAME_WITH_ALIASES: names + approved
    * aliases; NAME_ALIAS_POSITION_FALLBACK: names + aliases, then a guarded
    * positional fallback). Every drift (missing, extra, renamed, reordered,
    * duplicated columns) is validated against the feed's policies.
    *
    * Positional mapping is never applied silently: it runs only for
    * header=false feeds, explicit force_columns_by_position=true, or as the
    * audited fallback after name/alias matching fails — and always validates
    * the column count against the contract. Position mapping is only safe
    * when the vendor guarantees stable column order.
    *
    * Missing optional columns are added with their configured default cast
    * to the contract type. Required columns are never auto-added.
    */
  private def applyContract(df: DataFrame, contract: SchemaContract, source: Config, header: Boolean): DataFrame = {
    val businessColumns = df.columns.filterNot(_.equalsIgnoreCase("source_file"))
    val forceByPosition = ConfigUtils.optBoolean(source, "force_columns_by_position").getOrElse(false)

    if (!header) return addOptionalDefaults(mapByPosition(df, businessColumns, contract), contract)

    val resolution = SchemaValidator.validateHeaders(businessColumns, contract, logger)
    logger.info(
      s"[FileSource] Header validation: actual=[${resolution.actualHeaders.mkString(",")}] " +
        s"normalized=[${resolution.normalizedHeaders.mkString(",")}] " +
        s"mapped=${resolution.canonicalToActual.map { case (c, a) => s"$a->$c" }.mkString(",")} " +
        s"missingRequired=[${resolution.missingRequired.mkString(",")}]"
    )

    if (forceByPosition) {
      SchemaValidator.enforce(resolution.violations, contract.policies, logger, Some(resolution))
      return addOptionalDefaults(mapByPosition(df, businessColumns, contract), contract)
    }

    val fallbackEligible =
      contract.strategy == HeaderStrategy.NameAliasPositionFallback &&
        contract.positionalFallback.enabled &&
        resolution.missingRequired.nonEmpty

    if (fallbackEligible) {
      if (contract.positionalFallback.requireExactColumnCount &&
          businessColumns.length != contract.columns.size) {
        val violations = resolution.violations :+ SchemaViolation(
          ViolationKind.CountMismatch,
          s"Positional fallback requires exactly ${contract.columns.size} source columns, " +
            s"found ${businessColumns.length}"
        )
        SchemaValidator.enforce(violations, contract.policies, logger, Some(resolution))
      }
      if (contract.positionalFallback.requireContentValidation)
        require(
          contract.contentValidation.enabled,
          "HDR_008 positional_fallback.require_content_validation=true but content_validation is not enabled"
        )
      logger.warn(
        s"[FileSource] Name/alias matching failed for required columns " +
          s"[${resolution.missingRequired.mkString(",")}]; engaging POSITIONAL FALLBACK " +
          s"(strategy=NAME_ALIAS_POSITION_FALLBACK, contract v${contract.version})"
      )
      addOptionalDefaults(mapByPosition(df, businessColumns, contract), contract)
    } else {
      SchemaValidator.enforce(resolution.violations, contract.policies, logger, Some(resolution))
      val renamed = resolution.renames.foldLeft(df) {
        case (acc, (from, to)) => acc.withColumnRenamed(from, to)
      }
      addOptionalDefaults(renamed, contract)
    }
  }

  /**
    * Detects data rows that repeat the header record (vendors concatenating
    * files). A repeated header row has every business column value equal to
    * that column's original header text.
    * Policies: FAIL (HDR_016) | REJECT_ROW / DROP_WITH_WARNING (both drop
    * the rows with a warning; REJECT_ROW rows are excluded before the reject
    * stage since they are structural artifacts, not business records).
    */
  private def handleRepeatedHeaders(df: DataFrame, contract: SchemaContract, header: Boolean): DataFrame = {
    val policy = contract.repeatedHeaderPolicy.getOrElse(return df)
    if (!header) return df

    val businessColumns = df.columns.filterNot(_.equalsIgnoreCase("source_file"))
    if (businessColumns.isEmpty) return df
    val isHeaderRow = businessColumns
      .map(c => trim(col(c)) === lit(c.trim))
      .reduce(_ && _)

    policy match {
      case "FAIL" =>
        // Deliberate tradeoff: FAIL requires an eager probe (one extra Spark
        // action per read, early-terminated by limit(1)) so a concatenated
        // file is rejected before anything downstream runs. The drop
        // policies below stay fully lazy.
        if (df.filter(isHeaderRow).limit(1).count() > 0)
          throw new com.hcsc.generic.ingest.schema.SchemaContractViolationException(Seq(
            SchemaViolation(ViolationKind.RepeatedHeader,
              "Repeated header row(s) detected in data (repeated_header_policy=FAIL)")))
        df
      case _ => // REJECT_ROW | DROP_WITH_WARNING
        logger.warn(s"[FileSource] Dropping repeated header rows per repeated_header_policy=$policy")
        df.filter(!isHeaderRow)
    }
  }

  /** Missing optional columns receive their configured default (or null)
    * cast to the contract type. Required columns are never auto-added. */
  private def addOptionalDefaults(df: DataFrame, contract: SchemaContract): DataFrame =
    contract.optionalColumns
      .filterNot(c => df.columns.exists(_.equalsIgnoreCase(c.name)))
      .foldLeft(df) { (acc, c) =>
        logger.info(s"[FileSource] Adding missing optional column '${c.name}' with default=${c.default.getOrElse("null")}")
        acc.withColumn(c.name, lit(c.default.orNull).cast(c.dataType))
      }

  private def mapByPosition(df: DataFrame, businessColumns: Seq[String], contract: SchemaContract): DataFrame = {
    val ordered = contract.positionalOrder
    require(
      ordered.flatMap(_.position).forall(p => p >= 0 && p < ordered.length),
      s"HDR_008 Contract positions must be contiguous and in range 0..${ordered.length - 1}: " +
        ordered.map(c => s"${c.name}=${c.position.get}").mkString(",")
    )
    require(
      businessColumns.length == ordered.length,
      s"HDR_007 Positional mapping requires exactly ${ordered.length} source columns, found ${businessColumns.length}. " +
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
