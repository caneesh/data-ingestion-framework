package com.hcsc.generic.ingest.stage

import com.hcsc.generic.ingest.config.ConfigUtils
import com.hcsc.generic.ingest.transform.CuratedTransform
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{broadcast, col}

final class CuratedService(spark: SparkSession, conf: Config) {
  private val transform = new CuratedTransform(spark)

  val enabled: Boolean =
    ConfigUtils.optBoolean(conf, "enabled").getOrElse(true)

  def process(rawDf: DataFrame, runMode: String): Unit = {
    if (!enabled) return

    val database = conf.getString("database")
    val table = conf.getString("table")
    val fullTable = s"$database.$table"
    val path = conf.getString("path")
    val format = ConfigUtils.optString(conf, "format").getOrElse("orc")

    val prepared0 = transform.castConfigured(rawDf, conf)
    val prepared1 = transform.applyTransforms(prepared0, conf)
    val prepared2 = transform.ensureAudit(prepared1)
    val prepared = transform.normalizeKeys(prepared2, conf)

    if (runMode.equalsIgnoreCase("FULL") || !spark.catalog.tableExists(fullTable)) {
      publishFull(prepared, fullTable, path, format)
    } else {
      publishIncremental(prepared, fullTable)
    }
  }

  private def publishFull(df: DataFrame, fullTable: String, path: String, format: String): Unit = {
    val publishDf =
      if (spark.catalog.tableExists(fullTable)) transform.align(df, spark.table(fullTable).schema)
      else df

    publishDf.write
      .format(format)
      .mode(SaveMode.Overwrite)
      .option("path", path)
      .saveAsTable(fullTable)
  }

  private def publishIncremental(incoming: DataFrame, fullTable: String): Unit = {
    val target = spark.table(fullTable)
    val merge = conf.getConfig("merge")
    val keys = ConfigUtils.stringList(merge, "keys")

    require(keys.nonEmpty, "INCR mode requires curated.merge.keys")

    val missingKeys = keys.filterNot { key =>
      incoming.columns.exists(_.equalsIgnoreCase(key)) &&
        target.columns.exists(_.equalsIgnoreCase(key))
    }
    require(missingKeys.isEmpty, s"Merge keys missing from incoming or target: ${missingKeys.mkString(",")}")

    val dropNull = ConfigUtils.optBoolean(merge, "null_handling.drop_null_keys").getOrElse(true)
    val blanksAsNull = ConfigUtils.optBoolean(merge, "null_handling.treat_blank_as_null").getOrElse(true)
    val orderBy = ConfigUtils.stringList(conf, "dedup.order_by")

    val cleaned = transform.filterNullKeys(incoming, keys, dropNull, blanksAsNull)
    val deduped = transform.deduplicate(cleaned, keys, orderBy)
    val alignedIncoming = transform.align(deduped, target.schema)

    val incomingKeys = alignedIncoming.select(keys.map(col): _*).distinct()
    val unchanged = target.join(broadcast(incomingKeys), keys, "left_anti")
    val merged = unchanged.unionByName(alignedIncoming)

    val stagingTable = s"${conf.getString("database")}.__stg_${conf.getString("table")}_${System.currentTimeMillis()}"
    merged.write.mode(SaveMode.Overwrite).saveAsTable(stagingTable)
    try {
      spark.sql(s"INSERT OVERWRITE TABLE $fullTable SELECT * FROM $stagingTable")
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $stagingTable")
    }
  }
}
