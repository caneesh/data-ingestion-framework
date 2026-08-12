package com.hcsc.generic.ingest.transform

import com.hcsc.generic.ingest.config.ConfigUtils
import com.hcsc.generic.ingest.schema.{ColumnMapping, SchemaContract}
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import scala.collection.JavaConverters._
import com.hcsc.generic.ingest.schema.ColumnMapping.quotedCol

final class CuratedTransform(spark: SparkSession) {
  private val logger = org.apache.log4j.Logger.getLogger(getClass.getName)

  def castConfigured(df: DataFrame, conf: Config): DataFrame = {
    // Single projection instead of a withColumn chain: at wide-table column
    // counts the chained form re-analyzes a growing plan per column
    // (quadratic driver time). Each cast reads only its own column, so
    // collapsing is semantics-preserving.
    val casts = ConfigUtils.stringMap(conf, "column_types").toSeq.flatMap { case (name, dataType) =>
      ColumnMapping.findColumn(df, name).map(actual => actual -> quotedCol(actual).cast(dataType))
    }
    if (casts.isEmpty) df else df.withColumns(casts.toMap)
  }

  /** Contract-declared per-column transforms (mapping-spec attribute):
    * TRIM | UPPER | LOWER, or a Spark SQL expression containing {col}.
    * Applied at the curated stage — RAW stays untransformed.
    *
    * All transforms are applied in ONE projection and observe the
    * PRE-TRANSFORM input values: a transform expression referencing another
    * column sees that column's original value, never another transform's
    * output. (Also the performance fix for 350-column contracts — a
    * withColumn chain re-analyzes a growing plan per column.) */
  def applyContractTransforms(df: DataFrame, contract: Option[com.hcsc.generic.ingest.schema.SchemaContract]): DataFrame =
    contract.fold(df) { c =>
      val transforms = c.columns.filter(_.transform.isDefined).flatMap { cc =>
        df.columns.find(_.equalsIgnoreCase(cc.name)).map { actual =>
          val quoted = s"`${actual.replace("`", "``")}`"
          val exprStr = cc.transform.get.trim match {
            case t if t.equalsIgnoreCase("TRIM")  => s"trim($quoted)"
            case t if t.equalsIgnoreCase("UPPER") => s"upper($quoted)"
            case t if t.equalsIgnoreCase("LOWER") => s"lower($quoted)"
            case t if t.contains("{col}")         => t.replace("{col}", quoted)
            case other => throw new IllegalArgumentException(
              s"HDR_017 column '${cc.name}' transform '$other' must be TRIM, UPPER, LOWER " +
                "or a Spark SQL expression containing {col}")
          }
          actual -> expr(exprStr)
        }
      }
      if (transforms.isEmpty) df else df.withColumns(transforms.toMap)
    }

  def applyTransforms(df: DataFrame, conf: Config): DataFrame = {
    if (!conf.hasPath("transform.derive")) return df
    conf.getConfigList("transform.derive").asScala.foldLeft(df) { (acc, item) =>
      acc.withColumn(item.getString("name"), expr(item.getString("expr")))
    }
  }

  def ensureAudit(df: DataFrame): DataFrame = {
    val withCreated =
      if (ColumnMapping.hasColumn(df, "create_timestamp")) df
      else df.withColumn("create_timestamp", current_timestamp())

    val withModified =
      if (ColumnMapping.hasColumn(withCreated, "last_modified_ts")) withCreated
      else withCreated.withColumn("last_modified_ts", current_timestamp())

    if (ColumnMapping.hasColumn(withModified, "last_modified_op")) withModified
    else withModified.withColumn("last_modified_op", lit("I"))
  }

  def normalizeKeys(df: DataFrame, conf: Config): DataFrame = {
    ConfigUtils.stringMap(conf, "merge.normalize").foldLeft(df) {
      case (acc, (name, sql)) =>
        ColumnMapping.findColumn(acc, name) match {
          case Some(actual) => acc.withColumn(actual, expr(sql))
          case None => acc
        }
    }
  }

  def filterNullKeys(df: DataFrame, keys: Seq[String], drop: Boolean, blanks: Boolean): DataFrame =
    splitNullKeys(df, keys, drop, blanks)._1

  /**
    * Splits rows into (valid, dropped) on null/blank business keys so the
    * dropped side can be counted and quarantined instead of vanishing.
    * With drop=false or no keys the dropped side is empty.
    */
  def splitNullKeys(df: DataFrame, keys: Seq[String], drop: Boolean, blanks: Boolean): (DataFrame, DataFrame) = {
    if (!drop || keys.isEmpty) return (df, df.filter(lit(false)))
    val invalid = keys.map { key =>
      val c = quotedCol(key)
      if (blanks) c.isNull || trim(c.cast("string")) === "" else c.isNull
    }.reduce(_ || _)
    (df.filter(!invalid), df.filter(invalid))
  }

  /**
    * Deterministic latest-record selection. A configured ordering column
    * missing from the DataFrame is a hard error (HDR_019) — silently
    * narrowing the ordering would pick nondeterministic winners. An empty
    * ordering falls back to dropDuplicates with a loud warning: the survivor
    * is arbitrary and feeds should configure dedup.order_by or
    * merge.freshness.column.
    */
  def deduplicate(df: DataFrame, keys: Seq[String], orderBy: Seq[String]): DataFrame =
    deduplicate(df, keys, orderBy, Map.empty)

  /** overrides: lowercase configured column name -> logical comparison
    * expression (pre-direction) — lets the freshness column order by its
    * declared comparison type while physical storage stays untouched. */
  def deduplicate(
    df: DataFrame,
    keys: Seq[String],
    orderBy: Seq[String],
    overrides: Map[String, org.apache.spark.sql.Column]
  ): DataFrame = {
    if (keys.isEmpty) return df
    if (orderBy.isEmpty) {
      logger.warn("[CuratedTransform] Deduplicating with NO ordering columns; " +
        "the surviving row per key is nondeterministic. Configure curated.dedup.order_by " +
        "or curated.merge.freshness.column for deterministic results.")
      return df.dropDuplicates(keys)
    }
    // Entries parse as "column [asc|desc] [nulls_first|nulls_last]"; a bare
    // name keeps the historical desc_nulls_last, so existing configurations
    // order identically.
    val specs = orderBy.map(OrderSpec.parse)
    val missing = specs.map(_.column).filterNot(o => df.columns.exists(_.equalsIgnoreCase(o)))
    if (missing.nonEmpty)
      throw new IllegalStateException(
        s"HDR_019 Dedup ordering column(s) ${missing.mkString(", ")} not present in the data; " +
          "refusing nondeterministic deduplication"
      )
    val existingOrder = specs.flatMap(s =>
      df.columns.find(_.equalsIgnoreCase(s.column)).map { actual =>
        overrides.get(s.column.toLowerCase)
          .map(s.applyDirection).getOrElse(s.toColumn(actual))
      })
    // Rows tied on every order_by column previously produced an arbitrary
    // winner (partition-layout dependent). row_idx — when present from RAW
    // metadata — is a defined final tie-break, so one computed frame always
    // yields one winner. Honest scope: row_idx itself is not stable across
    // re-reads (see RawMetadata), so determinism holds within a run / over
    // a persisted RAW slice, not across independent source re-extractions.
    val tieBreak = df.columns.find(_.equalsIgnoreCase("row_idx"))
      .map(c => quotedCol(c).desc_nulls_last).toSeq
    val w = Window.partitionBy(keys.map(col): _*)
      .orderBy(existingOrder ++ tieBreak: _*)
    df.withColumn("_rn", row_number().over(w))
      .filter(col("_rn") === 1)
      .drop("_rn")
  }

  /**
    * Counts values a cast would silently turn into NULL (Spark's cast never
    * fails): per column, rows where the source is non-null but the casted
    * value is null. One aggregate pass over all candidate columns.
    */
  def detectCastLoss(df: DataFrame, casts: Seq[(String, org.apache.spark.sql.types.DataType)]): Seq[(String, Long)] = {
    if (casts.isEmpty) return Seq.empty
    val aggs = casts.map { case (c, t) =>
      sum(when(quotedCol(c).isNotNull && quotedCol(c).cast(t).isNull, 1).otherwise(0)).alias(c)
    }
    val row = df.agg(aggs.head, aggs.tail: _*).first()
    casts.indices.flatMap { i =>
      Option(row.get(i)).map(_.toString.toLong).filter(_ > 0).map(n => (casts(i)._1, n))
    }
  }

  def align(df: DataFrame, schema: StructType): DataFrame =
    align(df, schema, None)

  /**
    * Aligns df to the target schema. Missing target columns are handled by
    * classification, never blindly null-filled when a contract is present:
    *   - contract-required business columns: refuse to auto-create as null
    *   - contract-optional columns: configured default (or null)
    *   - non-contract columns (technical/audit/generated/deprecated
    *     target-only columns): null-filled as before
    */
  def align(df: DataFrame, schema: StructType, contract: Option[SchemaContract]): DataFrame = {
    val columnLookup = ColumnMapping.buildLookup(df.columns)
    val completed = schema.fields.foldLeft(df) { (acc, field) =>
      columnLookup.get(field.name.toLowerCase) match {
        case Some(_) => acc
        case None =>
          contract.flatMap(_.column(field.name)) match {
            case Some(cc) if cc.required && cc.category.equalsIgnoreCase("business") =>
              throw new IllegalStateException(
                s"HDR_001 Required column '${field.name}' is missing from the incoming data; " +
                  "refusing to auto-create it as null during schema alignment"
              )
            case Some(cc) =>
              acc.withColumn(field.name, lit(cc.default.orNull).cast(field.dataType))
            case None =>
              acc.withColumn(field.name, lit(null).cast(field.dataType))
          }
      }
    }

    completed.select(schema.fields.map { field =>
      val actual = ColumnMapping.findColumn(completed, field.name).get
      quotedCol(actual).cast(field.dataType).as(field.name)
    }: _*)
  }
}
