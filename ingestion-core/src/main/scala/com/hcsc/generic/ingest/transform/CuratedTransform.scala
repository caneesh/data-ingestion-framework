package com.hcsc.generic.ingest.transform

import com.hcsc.generic.ingest.config.ConfigUtils
import com.hcsc.generic.ingest.schema.SchemaContract
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import scala.collection.JavaConverters._

final class CuratedTransform(spark: SparkSession) {

  def castConfigured(df: DataFrame, conf: Config): DataFrame = {
    ConfigUtils.stringMap(conf, "column_types").foldLeft(df) {
      case (acc, (name, dataType)) if acc.columns.exists(_.equalsIgnoreCase(name)) =>
        val actual = acc.columns.find(_.equalsIgnoreCase(name)).get
        acc.withColumn(actual, col(actual).cast(dataType))
      case (acc, _) => acc
    }
  }

  def applyTransforms(df: DataFrame, conf: Config): DataFrame = {
    if (!conf.hasPath("transform.derive")) return df
    conf.getConfigList("transform.derive").asScala.foldLeft(df) { (acc, item) =>
      acc.withColumn(item.getString("name"), expr(item.getString("expr")))
    }
  }

  def ensureAudit(df: DataFrame): DataFrame = {
    val withCreated =
      if (df.columns.exists(_.equalsIgnoreCase("create_timestamp"))) df
      else df.withColumn("create_timestamp", current_timestamp())

    val withModified =
      if (withCreated.columns.exists(_.equalsIgnoreCase("last_modified_ts"))) withCreated
      else withCreated.withColumn("last_modified_ts", current_timestamp())

    if (withModified.columns.exists(_.equalsIgnoreCase("last_modified_op"))) withModified
    else withModified.withColumn("last_modified_op", lit("I"))
  }

  def normalizeKeys(df: DataFrame, conf: Config): DataFrame = {
    ConfigUtils.stringMap(conf, "merge.normalize").foldLeft(df) {
      case (acc, (name, sql)) if acc.columns.exists(_.equalsIgnoreCase(name)) =>
        val actual = acc.columns.find(_.equalsIgnoreCase(name)).get
        acc.withColumn(actual, expr(sql))
      case (acc, _) => acc
    }
  }

  def filterNullKeys(df: DataFrame, keys: Seq[String], drop: Boolean, blanks: Boolean): DataFrame = {
    if (!drop || keys.isEmpty) return df
    val invalid = keys.map { key =>
      val c = col(key)
      if (blanks) c.isNull || trim(c.cast("string")) === "" else c.isNull
    }.reduce(_ || _)
    df.filter(!invalid)
  }

  def deduplicate(df: DataFrame, keys: Seq[String], orderBy: Seq[String]): DataFrame = {
    if (keys.isEmpty) return df
    val existingOrder = orderBy.flatMap(o => df.columns.find(_.equalsIgnoreCase(o)))
    if (existingOrder.isEmpty) df.dropDuplicates(keys)
    else {
      // Rows tied on every order_by column previously produced an arbitrary
      // winner (partition-layout dependent). row_idx — when present from RAW
      // metadata — is a defined final tie-break, so one computed frame always
      // yields one winner. Honest scope: row_idx itself is not stable across
      // re-reads (see RawMetadata), so determinism holds within a run / over
      // a persisted RAW slice, not across independent source re-extractions.
      val tieBreak = df.columns.find(_.equalsIgnoreCase("row_idx"))
        .map(c => col(c).desc_nulls_last).toSeq
      val w = Window.partitionBy(keys.map(col): _*)
        .orderBy(existingOrder.map(c => col(c).desc_nulls_last) ++ tieBreak: _*)
      df.withColumn("_rn", row_number().over(w))
        .filter(col("_rn") === 1)
        .drop("_rn")
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
    val lowerToActual = df.columns.map(c => c.toLowerCase -> c).toMap
    val completed = schema.fields.foldLeft(df) { (acc, field) =>
      lowerToActual.get(field.name.toLowerCase) match {
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
      val actual = completed.columns.find(_.equalsIgnoreCase(field.name)).get
      col(actual).cast(field.dataType).as(field.name)
    }: _*)
  }
}
